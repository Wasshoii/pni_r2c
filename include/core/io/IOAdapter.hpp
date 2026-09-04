#pragma once

#include <pni/PnI-Config.hpp>

#include <pni/io/IO.hpp>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace openpni
{
namespace distributed
{
namespace coreio
{
    struct IOCommonOptions
    {
        uint64_t reservedBytes = 20ull * 1024ull * 1024ull * 1024ull;
        bool createPathIfNotExist = true;
        bool enableOverrideExistingFile = true;
        unsigned ioQueueSize = 2;

        // 文件写盘/分卷策略：maxFileSizeBytes == 0 表示不分卷（单文件，与历史行为一致）；
        // > 0 时由 RollingFileWriter 负责在累计写入量超过该阈值时滚动到下一个文件。
        uint64_t maxFileSizeBytes = 0;
    };

    struct RawDataWriterOptions
    {
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

        ::openpni::RawDataView View() const
        {
            if (latestSegment_)
            {
                return latestSegment_->HostView();
            }

            return {};
        }

        bool Valid() const
        {
            return static_cast<bool>(latestSegment_);
        }

    private:
        std::optional<::openpni::io::rawdata::RawDataSegment> latestSegment_;
    };

    class RawDataFileReader
    {
    public:
        RawDataFileReader()
            : latestReader_(std::make_unique<::openpni::io::RawFileInput>())
        {
        }

        void Open(const std::string &path)
        {
            info_ = {};
            latestReader_->Open(path);
            const auto &header = latestReader_->Header();
            info_.channelNum = header.ChannelNum();
            info_.segmentNum = latestReader_->SegmentNum();
            info_.channelTypeNames.reserve(info_.channelNum);
            for (uint16_t i = 0; i < info_.channelNum; ++i)
            {
                info_.channelTypeNames.push_back(header.TypeNameOfChannel(i));
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
            return RawDataSegmentHandle(latestReader_->ReadSegment(segmentIndex, prefetchIndex));
        }

    private:
        RawDataFileInfo info_;
        std::unique_ptr<::openpni::io::RawFileInput> latestReader_;
    };

    class RawDataFileWriter
    {
    public:
        explicit RawDataFileWriter(RawDataWriterOptions options)
            : options_(std::move(options))
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

        void Open(const std::string &path)
        {
            if (!latestWriter_)
            {
                throw std::runtime_error("RawDataFileWriter is not initialized");
            }

            latestWriter_->Open(path);
        }

        bool AppendSegment(const ::openpni::RawDataView &view)
        {
            if (!latestWriter_)
            {
                return false;
            }

            latestWriter_->AppendSegment(view);
            return (latestWriter_->GetStatus() & ::openpni::io::IOStatus_DiskSpaceNotEnough) == 0;
        }

    private:
        RawDataWriterOptions options_;
        std::unique_ptr<::openpni::io::RawFileOutput> latestWriter_;
    };

    struct SingleWriterOptions
    {
        IOCommonOptions io;
    };

    class SinglesFileWriter
    {
    public:
        explicit SinglesFileWriter(SingleWriterOptions options = {})
            : options_(std::move(options))
        {
            ::openpni::io::IOOptions ioOptions;
            ioOptions.SetReservedBytes(options_.io.reservedBytes);
            ioOptions.SetCreatePathIfNotExist(options_.io.createPathIfNotExist);
            ioOptions.SetEnableOverrideExistingFile(options_.io.enableOverrideExistingFile);
            ioOptions.SetIOQueueSize(options_.io.ioQueueSize);

            using Fields = ::openpni::io::listmode::SupportedFields;
            auto fieldsInUse = static_cast<Fields>(
                Fields::local_crystal_index1 |
                Fields::channel_index1 |
                Fields::energy1 |
                Fields::absolute_timestamp1_100fs);

            ::openpni::io::listmode::ListmodeFileHeader header;
            header.SetFieldsInUse(fieldsInUse);
            header.SetBitsForStorage(Fields::local_crystal_index1, 16);
            header.SetBitsForStorage(Fields::channel_index1, 16);
            header.SetBitsForStorage(Fields::energy1, 32);
            header.SetBitsForStorage(Fields::absolute_timestamp1_100fs, 64);
            header.SetFileTypeName(::openpni::io::listmode::fields::file_type_single_listmode);

            output_ = std::make_unique<::openpni::io::ListmodeFileOutput>(std::move(header), std::move(ioOptions));
        }

        void Open(const std::string &path, uint32_t totalCrystals)
        {
            (void)totalCrystals;
            output_->Open(path);
        }

        bool AppendSegment(std::span<const ::openpni::Single> singles,
                           uint64_t clockMs,
                           uint32_t durationMs)
        {
            if (singles.empty())
            {
                return true;
            }

            ::openpni::io::listmode::ListmodeFileSegment segment;
            segment.SetSingles(singles);
            segment.SetClockMs(clockMs);
            segment.SetDurationMs(durationMs);
            output_->AppendSegment(std::move(segment));
            return (output_->GetStatus() & ::openpni::io::IOStatus_DiskSpaceNotEnough) == 0;
        }

        void FlushToDisk()
        {
            if (output_)
            {
                output_->FlushToDisk();
            }
        }

        ::openpni::io::IOStatus GetStatus() const
        {
            return output_ ? output_->GetStatus() : ::openpni::io::IOStatus_Success;
        }

        ::openpni::io::ListmodeFileOutput *RawHandle()
        {
            return output_.get();
        }

    private:
        SingleWriterOptions options_;
        std::unique_ptr<::openpni::io::ListmodeFileOutput> output_;
    };

    struct ListmodeWriterOptions
    {
        IOCommonOptions io;
        uint32_t totalCrystals = 0;
    };

    class ListmodeFileWriter
    {
    public:
        explicit ListmodeFileWriter(ListmodeWriterOptions options)
            : options_(std::move(options))
        {
            ::openpni::io::IOOptions ioOptions;
            ioOptions.SetReservedBytes(options_.io.reservedBytes);
            ioOptions.SetCreatePathIfNotExist(options_.io.createPathIfNotExist);
            ioOptions.SetEnableOverrideExistingFile(options_.io.enableOverrideExistingFile);
            ioOptions.SetIOQueueSize(options_.io.ioQueueSize);

            using Fields = ::openpni::io::listmode::SupportedFields;
            auto fieldsInUse = static_cast<Fields>(
                Fields::local_crystal_index1 |
                Fields::local_crystal_index2 |
                Fields::channel_index1 |
                Fields::channel_index2 |
                Fields::time_of_flight_100fs);

            ::openpni::io::listmode::ListmodeFileHeader header;
            header.SetFieldsInUse(fieldsInUse);
            header.SetBitsForStorage(Fields::local_crystal_index1, 16);
            header.SetBitsForStorage(Fields::local_crystal_index2, 16);
            header.SetBitsForStorage(Fields::channel_index1, 16);
            header.SetBitsForStorage(Fields::channel_index2, 16);
            header.SetBitsForStorage(Fields::time_of_flight_100fs, 16);
            header.SetFileTypeName(::openpni::io::listmode::fields::file_type_coin_listmode);

            latestWriter_ = std::make_unique<::openpni::io::ListmodeFileOutput>(std::move(header), std::move(ioOptions));
        }

        void Open(const std::string &path)
        {
            if (latestWriter_)
            {
                latestWriter_->Open(path);
                return;
            }
        }

        bool AppendSegment(std::span<const ::openpni::Listmode> listmodes,
                           uint64_t clockMs,
                           uint32_t durationMs)
        {
            if (listmodes.empty())
            {
                return true;
            }
            ::openpni::io::listmode::ListmodeFileSegment segment;
            segment.SetListmodes(listmodes);
            segment.SetClockMs(clockMs);
            segment.SetDurationMs(durationMs);
            latestWriter_->AppendSegment(std::move(segment));
            return (latestWriter_->GetStatus() & ::openpni::io::IOStatus_DiskSpaceNotEnough) == 0;
        }

        bool AppendSegment(std::vector<::openpni::Listmode> &&listmodes,
                           uint64_t clockMs,
                           uint32_t durationMs)
        {
            if (listmodes.empty())
            {
                return true;
            }
            if (!latestWriter_)
            {
                return false;
            }
            ::openpni::io::listmode::ListmodeFileSegment segment;
            segment.SetListmodes(std::move(listmodes));
            segment.SetClockMs(clockMs);
            segment.SetDurationMs(durationMs);
            latestWriter_->AppendSegment(std::move(segment));
            return (latestWriter_->GetStatus() & ::openpni::io::IOStatus_DiskSpaceNotEnough) == 0;
        }

        void FlushToDisk()
        {
            if (latestWriter_)
            {
                latestWriter_->FlushToDisk();
            }
        }

        ::openpni::io::IOStatus GetStatus() const
        {
            return latestWriter_ ? latestWriter_->GetStatus() : ::openpni::io::IOStatus_Success;
        }

        ::openpni::io::ListmodeFileOutput *RawHandle()
        {
            return latestWriter_.get();
        }

    private:
        ListmodeWriterOptions options_;
        std::unique_ptr<::openpni::io::ListmodeFileOutput> latestWriter_;
    };

    /**
     * @brief 通用的按文件大小分卷（滚动）写入包装类
     *
     * @tparam Writer 底层单文件写入器类型（RawDataFileWriter / SinglesFileWriter / ListmodeFileWriter 之一），
     *                要求其构造函数接受单个 OptionsT 参数。
     * @tparam OptionsT 对应的 Options 结构体类型（须内含 IOCommonOptions io 成员，用于读取 maxFileSizeBytes）。
     *
     * 设计说明：
     * - 底层三个 Writer 类本身保持"单文件"语义不变，不影响既有一次性读写调用方。
     * - 分卷仅在需要的调用方（采集/R2S/符合输出）按需叠加本包装类。
     * - baseOptions.io.maxFileSizeBytes == 0 时等价于普通单文件写入（不分卷）。
     */
    template <typename Writer, typename OptionsT>
    class RollingFileWriter
    {
    public:
        using FileReadyCallback = std::function<void(const std::string &)>;
        using OpenFn = std::function<void(Writer &, const std::string &)>;

        RollingFileWriter() = default;

        void SetFileReadyCallback(FileReadyCallback cb)
        {
            callback_ = std::move(cb);
        }

        /**
         * @brief 打开第一个分卷文件：{sessionDir}/{filePrefix}_{seq:04d}.{extension}
         *
         * @param openFn 用于调用底层 Writer::Open(path, ...) 的回调，允许携带 Open() 所需的额外参数
         *               （例如 SinglesFileWriter::Open 的 totalCrystals），在每次滚动重开文件时都会被复用。
         */
        bool Open(std::string sessionDir, std::string filePrefix, std::string extension,
                  OptionsT baseOptions, OpenFn openFn)
        {
            sessionDir_ = std::move(sessionDir);
            filePrefix_ = std::move(filePrefix);
            extension_ = std::move(extension);
            baseOptions_ = std::move(baseOptions);
            openFn_ = std::move(openFn);
            maxFileSizeBytes_ = baseOptions_.io.maxFileSizeBytes;
            fileSeq_ = 0;
            currentBytes_ = 0;
            return openNextFile();
        }

        /**
         * @brief 追加一段数据，若累计写入量超过 maxFileSizeBytes 则先滚动到下一个文件
         *
         * @param sizeEstimateBytes 本次写入的估算字节数（由调用方按数据类型计算）
         * @param args 转发给底层 Writer::AppendSegment(...) 的实际参数
         */
        template <typename... AppendArgs>
        bool AppendSegment(uint64_t sizeEstimateBytes, AppendArgs &&...args)
        {
            if (!writer_)
            {
                return false;
            }

            if (maxFileSizeBytes_ > 0 && currentBytes_ > 0 &&
                currentBytes_ + sizeEstimateBytes > maxFileSizeBytes_)
            {
                if (!rotate())
                {
                    return false;
                }
            }

            const bool ok = writer_->AppendSegment(std::forward<AppendArgs>(args)...);
            if (ok)
            {
                currentBytes_ += sizeEstimateBytes;
            }
            else if constexpr (requires { writer_->GetStatus(); })
            {
                lastStatus_ = writer_->GetStatus();
            }
            return ok;
        }

        void Stop()
        {
            closeCurrent();
        }

        Writer *RawHandle()
        {
            return writer_.get();
        }

        ::openpni::io::IOStatus GetStatus() const
        {
            if (writer_)
            {
                if constexpr (requires { writer_->GetStatus(); })
                {
                    return writer_->GetStatus();
                }
            }
            return lastStatus_;
        }

        const std::string &CurrentPath() const
        {
            return currentPath_;
        }

    private:
        bool openNextFile()
        {
            std::error_code ec;
            std::filesystem::create_directories(sessionDir_, ec);

            std::ostringstream oss;
            oss << sessionDir_ << "/" << filePrefix_;
            if (maxFileSizeBytes_ > 0)
            {
                // 仅在启用分卷时才追加序号后缀，未启用时保持与历史单文件命名完全一致
                // （例如 singles.lsingle / prompt.lmf），避免默认行为发生变化。
                oss << "_" << std::setfill('0') << std::setw(4) << fileSeq_;
            }
            oss << "." << extension_;
            ++fileSeq_;
            currentPath_ = oss.str();

            try
            {
                writer_ = std::make_unique<Writer>(baseOptions_);
                openFn_(*writer_, currentPath_);
                currentBytes_ = 0;
                return true;
            }
            catch (const std::exception &)
            {
                writer_.reset();
                currentPath_.clear();
                return false;
            }
        }

        bool rotate()
        {
            closeCurrent();
            return openNextFile();
        }

        void closeCurrent()
        {
            if (writer_)
            {
                if constexpr (requires { writer_->FlushToDisk(); })
                {
                    writer_->FlushToDisk();
                }
                if constexpr (requires { writer_->GetStatus(); })
                {
                    lastStatus_ = writer_->GetStatus();
                }
                writer_.reset();
            }
            if (!currentPath_.empty() && callback_)
            {
                callback_(currentPath_);
            }
            currentPath_.clear();
        }

        std::string sessionDir_;
        std::string filePrefix_;
        std::string extension_;
        OptionsT baseOptions_{};
        OpenFn openFn_;
        FileReadyCallback callback_;

        uint64_t maxFileSizeBytes_ = 0;
        uint64_t currentBytes_ = 0;
        size_t fileSeq_ = 0;

        std::unique_ptr<Writer> writer_;
        std::string currentPath_;
        ::openpni::io::IOStatus lastStatus_ = ::openpni::io::IOStatus_Success;
    };

} // namespace coreio
} // namespace distributed
} // namespace openpni
