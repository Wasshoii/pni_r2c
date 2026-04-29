#pragma once

#include <pni/PnI-Config.hpp>

#include <pni/io/IO.hpp>
#include <pni/io/v1/V1.hpp>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <span>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace openpni
{
namespace distributed
{
namespace coreio
{
    enum class IOBackend
    {
        V1,
        Latest,
    };

    struct IOBackendRuntimeConfig
    {
        IOBackend rawdataReader = IOBackend::V1;
        IOBackend rawdataWriter = IOBackend::V1;
        IOBackend singlesWriter = IOBackend::V1;
        IOBackend listmodeWriter = IOBackend::V1;
    };

    inline IOBackend ParseIOBackend(std::string_view value, IOBackend fallback = IOBackend::V1)
    {
        if (value == "latest" || value == "LATEST" || value == "Latest")
        {
            return IOBackend::Latest;
        }
        if (value == "v1" || value == "V1")
        {
            return IOBackend::V1;
        }
        return fallback;
    }

    inline IOBackendRuntimeConfig LoadIOBackendRuntimeConfigFromEnv()
    {
        IOBackendRuntimeConfig cfg;

        if (const char *global = std::getenv("PNI_R2C_IO_BACKEND"); global && global[0] != '\0')
        {
            const auto backend = ParseIOBackend(global, IOBackend::V1);
            cfg.rawdataReader = backend;
            cfg.rawdataWriter = backend;
            cfg.singlesWriter = backend;
            cfg.listmodeWriter = backend;
        }

        if (const char *v = std::getenv("PNI_R2C_IO_BACKEND_RAWDATA_READER"); v && v[0] != '\0')
        {
            cfg.rawdataReader = ParseIOBackend(v, cfg.rawdataReader);
        }
        if (const char *v = std::getenv("PNI_R2C_IO_BACKEND_RAWDATA_WRITER"); v && v[0] != '\0')
        {
            cfg.rawdataWriter = ParseIOBackend(v, cfg.rawdataWriter);
        }
        if (const char *v = std::getenv("PNI_R2C_IO_BACKEND_SINGLES_WRITER"); v && v[0] != '\0')
        {
            cfg.singlesWriter = ParseIOBackend(v, cfg.singlesWriter);
        }
        if (const char *v = std::getenv("PNI_R2C_IO_BACKEND_LISTMODE_WRITER"); v && v[0] != '\0')
        {
            cfg.listmodeWriter = ParseIOBackend(v, cfg.listmodeWriter);
        }

        return cfg;
    }

    class IOBackendContext
    {
    public:
        static const IOBackendRuntimeConfig &Get()
        {
            return Mutable();
        }

        static void Set(const IOBackendRuntimeConfig &cfg)
        {
            Mutable() = cfg;
        }

    private:
        static IOBackendRuntimeConfig &Mutable()
        {
            static IOBackendRuntimeConfig cfg = LoadIOBackendRuntimeConfigFromEnv();
            return cfg;
        }
    };

    struct IOCommonOptions
    {
        uint64_t reservedBytes = 20ull * 1024ull * 1024ull * 1024ull;
        bool createPathIfNotExist = true;
        bool enableOverrideExistingFile = true;
        unsigned ioQueueSize = 2;
    };

    struct RawDataWriterOptions
    {
        IOBackend backend = IOBackend::V1;
        IOCommonOptions io;
        uint16_t channelNum = 0;
        std::vector<std::string> channelTypeNames;
    };

    struct RawDataFileInfo
    {
        uint16_t channelNum = 0;
        uint32_t segmentNum = 0;
        std::vector<std::string> channelTypeNames;
    };

    class RawDataSegmentHandle
    {
    public:
        RawDataSegmentHandle() = default;

        explicit RawDataSegmentHandle(::openpni::io::rawdata::RawDataSegment &&segment)
            : latestSegment_(std::move(segment))
        {
        }

        RawDataSegmentHandle(::openpni::io::v1::rawdata::RawdataSegment &&segment,
                             ::openpni::io::v1::rawdata::RawdataHeader header,
                             ::openpni::io::v1::rawdata::SegmentHeader segHeader)
            : v1Segment_(std::move(segment))
            , v1Header_(header)
            , v1SegHeader_(segHeader)
        {
        }

        ::openpni::RawDataView View() const
        {
            if (latestSegment_)
            {
                return latestSegment_->HostView();
            }

            if (v1Segment_)
            {
                auto view = v1Segment_->view(v1Header_, v1SegHeader_);
                view.clock_ms = v1SegHeader_.clock;
                view.duration_ms = v1SegHeader_.duration;
                return view;
            }

            return {};
        }

        bool Valid() const
        {
            return static_cast<bool>(latestSegment_) || static_cast<bool>(v1Segment_);
        }

    private:
        std::optional<::openpni::io::rawdata::RawDataSegment> latestSegment_;
        std::optional<::openpni::io::v1::rawdata::RawdataSegment> v1Segment_;
        ::openpni::io::v1::rawdata::RawdataHeader v1Header_{};
        ::openpni::io::v1::rawdata::SegmentHeader v1SegHeader_{};
    };

    class RawDataFileReader
    {
    public:
        explicit RawDataFileReader(IOBackend backend = IOBackend::V1)
            : backend_(backend)
        {
            if (backend_ == IOBackend::Latest)
            {
                latestReader_ = std::make_unique<::openpni::io::RawFileInput>();
            }
            else
            {
                v1Reader_ = std::make_unique<::openpni::io::v1::RawFileInput>();
            }
        }

        void Open(const std::string &path)
        {
            info_ = {};
            if (backend_ == IOBackend::Latest)
            {
                latestReader_->Open(path);
                const auto &header = latestReader_->Header();
                info_.channelNum = header.ChannelNum();
                info_.segmentNum = latestReader_->SegmentNum();
                info_.channelTypeNames.reserve(info_.channelNum);
                for (uint16_t i = 0; i < info_.channelNum; ++i)
                {
                    info_.channelTypeNames.push_back(header.TypeNameOfChannel(i));
                }
                return;
            }

            v1Reader_->open(path);
            v1Header_ = v1Reader_->header();
            info_.channelNum = v1Header_.channelNum;
            info_.segmentNum = v1Header_.segmentNum;
            info_.channelTypeNames.reserve(info_.channelNum);
            for (uint16_t i = 0; i < info_.channelNum; ++i)
            {
                info_.channelTypeNames.push_back(v1Reader_->typeNameOfChannel(i));
            }
        }

        const RawDataFileInfo &Info() const
        {
            return info_;
        }

        uint32_t SegmentNum() const
        {
            return info_.segmentNum;
        }

        RawDataSegmentHandle ReadSegment(uint32_t segmentIndex, uint32_t prefetchIndex = uint32_t(-1))
        {
            if (backend_ == IOBackend::Latest)
            {
                return RawDataSegmentHandle(latestReader_->ReadSegment(segmentIndex, prefetchIndex));
            }

            auto segment = v1Reader_->readSegment(segmentIndex, prefetchIndex);
            const auto segHeader = v1Reader_->segmentHeader(segmentIndex);
            return RawDataSegmentHandle(std::move(segment), v1Header_, segHeader);
        }

    private:
        IOBackend backend_;
        RawDataFileInfo info_;

        std::unique_ptr<::openpni::io::RawFileInput> latestReader_;
        std::unique_ptr<::openpni::io::v1::RawFileInput> v1Reader_;
        ::openpni::io::v1::rawdata::RawdataHeader v1Header_{};
    };

    class RawDataFileWriter
    {
    public:
        explicit RawDataFileWriter(RawDataWriterOptions options)
            : options_(std::move(options))
        {
            if (options_.backend == IOBackend::Latest)
            {
                ::openpni::io::IOOptions ioOptions;
                ioOptions.SetReservedBytes(options_.io.reservedBytes);
                ioOptions.SetCreatePathIfNotExist(options_.io.createPathIfNotExist);
                ioOptions.SetEnableOverrideExistingFile(options_.io.enableOverrideExistingFile);
                ioOptions.SetIOQueueSize(options_.io.ioQueueSize);

                ::openpni::io::rawdata::RawDataFileHeader header;
                header.SetChannelNum(options_.channelNum);
                for (size_t i = 0; i < options_.channelTypeNames.size() && i < options_.channelNum; ++i)
                {
                    header.SetNameOfChannel(static_cast<uint16_t>(i), options_.channelTypeNames[i]);
                }

                latestWriter_ = std::make_unique<::openpni::io::RawFileOutput>(std::move(header), std::move(ioOptions));
            }
            else
            {
                v1Writer_ = std::make_unique<::openpni::io::v1::RawFileOutput>();
                v1Writer_->setReservedBytes(options_.io.reservedBytes);
                v1Writer_->setChannelNum(options_.channelNum);
                for (size_t i = 0; i < options_.channelTypeNames.size() && i < options_.channelNum; ++i)
                {
                    v1Writer_->setTypeNameOfChannel(static_cast<uint16_t>(i), options_.channelTypeNames[i]);
                }
            }
        }

        void Open(const std::string &path)
        {
            if (latestWriter_)
            {
                latestWriter_->Open(path);
                return;
            }

            if (v1Writer_)
            {
                v1Writer_->open(path);
                return;
            }

            throw std::runtime_error("RawDataFileWriter is not initialized");
        }

        bool AppendSegment(const ::openpni::RawDataView &view)
        {
            if (latestWriter_)
            {
                latestWriter_->AppendSegment(view);
                return (latestWriter_->GetStatus() & ::openpni::io::IOStatus_DiskSpaceNotEnough) == 0;
            }

            if (v1Writer_)
            {
                return v1Writer_->appendSegment(view);
            }

            return false;
        }

    private:
        RawDataWriterOptions options_;
        std::unique_ptr<::openpni::io::RawFileOutput> latestWriter_;
        std::unique_ptr<::openpni::io::v1::RawFileOutput> v1Writer_;
    };

    struct SingleWriterOptions
    {
        IOBackend backend = IOBackend::V1;
        IOCommonOptions io;
    };

    class SinglesFileWriter
    {
    public:
        explicit SinglesFileWriter(SingleWriterOptions options = {})
            : options_(std::move(options))
        {
            output_ = std::make_unique<::openpni::io::v1::single::SingleFileOutput>();
        }

        void Open(const std::string &path, uint32_t totalCrystals)
        {
            output_->setBytes4CrystalIndex(::openpni::io::v1::single::CrystalIndexType::UINT32);
            output_->setBytes4TimeValue(::openpni::io::v1::single::TimeValueType::UINT64);
            output_->setBytes4Energy(::openpni::io::v1::single::EnergyType::FLT32);
            output_->setTotalCrystalNum(totalCrystals);
            output_->setReservedBytes(options_.io.reservedBytes);
            output_->open(path);
        }

        bool AppendSegment(std::span<const ::openpni::v1::basic::GlobalSingle_t> singles,
                           uint64_t clockMs,
                           uint32_t durationMs)
        {
            if (singles.empty())
            {
                return true;
            }
            return output_->appendSegment(singles.data(), singles.size(), clockMs, durationMs);
        }

        ::openpni::io::v1::single::SingleFileOutput *RawHandle()
        {
            return output_.get();
        }

    private:
        SingleWriterOptions options_;
        std::unique_ptr<::openpni::io::v1::single::SingleFileOutput> output_;
    };

    struct ListmodeWriterOptions
    {
        IOBackend backend = IOBackend::V1;
        IOCommonOptions io;
        uint32_t totalCrystals = 0;
    };

    class ListmodeFileWriter
    {
    public:
        explicit ListmodeFileWriter(ListmodeWriterOptions options)
            : options_(std::move(options))
        {
            if (options_.backend == IOBackend::Latest)
            {
                ::openpni::io::IOOptions ioOptions;
                ioOptions.SetReservedBytes(options_.io.reservedBytes);
                ioOptions.SetCreatePathIfNotExist(options_.io.createPathIfNotExist);
                ioOptions.SetEnableOverrideExistingFile(options_.io.enableOverrideExistingFile);
                ioOptions.SetIOQueueSize(options_.io.ioQueueSize);

                using Fields = ::openpni::io::listmode::SupportedFields;
                auto fieldsInUse = static_cast<Fields>(
                    Fields::global_crystal_index1 |
                    Fields::global_crystal_index2 |
                    Fields::time_of_flight);

                ::openpni::io::listmode::ListmodeFileHeader header;
                header.SetFieldsInUse(fieldsInUse);
                header.SetBitsForStorage(Fields::global_crystal_index1, 32);
                header.SetBitsForStorage(Fields::global_crystal_index2, 32);
                header.SetBitsForStorage(Fields::time_of_flight, 16);
                header.SetFileTypeName(::openpni::io::listmode::fields::file_type_coin_listmode);

                latestWriter_ = std::make_unique<::openpni::io::ListmodeFileOutput>(std::move(header), std::move(ioOptions));
            }
            else
            {
                v1Writer_ = std::make_unique<::openpni::io::v1::listmode::ListmodeFileOutput>();
                v1Writer_->setBytes4CrystalIndex(::openpni::io::v1::single::CrystalIndexType::UINT32);
                v1Writer_->setBytes4TimeValue1_2(::openpni::io::v1::listmode::TimeValue1_2Type::INT16);
                v1Writer_->setTotalCrystalNum(options_.totalCrystals);
                v1Writer_->setReservedBytes(options_.io.reservedBytes);
            }
        }

        void Open(const std::string &path)
        {
            if (latestWriter_)
            {
                latestWriter_->Open(path);
                return;
            }

            if (v1Writer_)
            {
                v1Writer_->open(path);
                return;
            }

            throw std::runtime_error("ListmodeFileWriter is not initialized");
        }

        bool AppendSegment(std::span<const ::openpni::v1::basic::Listmode_t> listmodes,
                           uint64_t clockMs,
                           uint32_t durationMs)
        {
            if (listmodes.empty())
            {
                return true;
            }

            if (v1Writer_)
            {
                return v1Writer_->appendSegment(listmodes.data(), listmodes.size(), clockMs, durationMs);
            }

            if (latestWriter_)
            {
                std::vector<uint32_t> g1(listmodes.size());
                std::vector<uint32_t> g2(listmodes.size());
                std::vector<uint16_t> tof(listmodes.size());
                for (size_t i = 0; i < listmodes.size(); ++i)
                {
                    g1[i] = listmodes[i].globalCrystalIndex1;
                    g2[i] = listmodes[i].globalCrystalIndex2;
                    tof[i] = static_cast<uint16_t>(listmodes[i].time1_2pico);
                }

                ::openpni::io::listmode::ListmodeFileSegment segment;
                ::openpni::io::listmode::ListmodeFileSegment::ListmodeAnyData data;
                data.count = listmodes.size();
                data.global_crystal_index1 = g1.data();
                data.global_crystal_index2 = g2.data();
                data.time_of_flight = tof.data();
                segment.SetAnyData(data);
                segment.SetClockMs(clockMs);
                segment.SetDurationMs(durationMs);
                latestWriter_->AppendSegment(std::move(segment));
                return (latestWriter_->GetStatus() & ::openpni::io::IOStatus_DiskSpaceNotEnough) == 0;
            }

            return false;
        }

    private:
        ListmodeWriterOptions options_;
        std::unique_ptr<::openpni::io::ListmodeFileOutput> latestWriter_;
        std::unique_ptr<::openpni::io::v1::listmode::ListmodeFileOutput> v1Writer_;
    };

} // namespace coreio
} // namespace distributed
} // namespace openpni
