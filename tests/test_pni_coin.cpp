// 在 PNI 头文件之前包含标准库头文件，避免 g++-13 命名空间冲突
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <string>
#include <vector>

#include <pni/PnI-Config.hpp>
#include <pni/io/IO.hpp>
#include <pni/io/ListmodeIO.hpp>

#include "src/core/merge-and-coin/MergeAndCoin.hpp"

namespace fs = std::filesystem;
using openpni::distributed::coin::CoincidenceProcessConfig;
using openpni::distributed::coin::merge_single_files;

namespace
{
    constexpr uint16_t kBdm50100ChannelNum = 48 * 3;
    constexpr uint32_t kBdm50100CrystalsPerChannel = 6 * 6 * 8;
    constexpr uint16_t kAnalyzeChannelNum = kBdm50100ChannelNum;
    constexpr int16_t kTimeWindowPs = 2000;
    constexpr float kEnergyLower_eV = 421.0f;
    constexpr float kEnergyUpper_eV = 1000.0f;
    constexpr int timeWindowPicosec = 2000;
    constexpr int delayTimePicosec = 100000;

    const std::string kSinglesFile = "/media/lenovo/新加卷/PNI_rawdata/NECR20260520/20260520001/PET-WB-2026_05_20_10_38_31/0/pni_singles_from_rs";
    const std::string kCoinOutputDir = "/media/lenovo/新加卷/PNI_rawdata/NECR20260520/20260520001/PET-WB-2026_05_20_10_38_31/0/rs_lm";
    const uint64_t kListmodeMaxBytes = 1300ull * 1024 * 1024; // 约 1.3G
    const std::string kPromptBaseName = "prompt";
    const std::string kDelayBaseName = "delay";
    struct EventSample
    {
        uint32_t globalCrystalIndex1 = 0;
        uint32_t globalCrystalIndex2 = 0;
        int16_t dt_pico = 0;
        uint16_t channelSep = 0;
    };

    struct LmfStats
    {
        std::string path;
        uint32_t segmentNum = 0;
        uint64_t totalEvents = 0;
        uint64_t checkedDtCount = 0;
        uint64_t negativeDtCount = 0;
        uint64_t overWindowDtCount = 0;
        uint64_t inAnalyzeChannelRangeCount = 0;
        uint64_t outOfAnalyzeChannelRangeCount = 0;
        int16_t minDt = std::numeric_limits<int16_t>::max();
        int16_t maxDt = std::numeric_limits<int16_t>::min();
        std::vector<uint64_t> channelSepHist;
        std::vector<uint64_t> channelPairHist12x12;
        std::vector<EventSample> samples;
    };

    struct EnergyStats
    {
        uint64_t total = 0;
        uint64_t inWindow_eV = 0;
        uint64_t inWindow_keV = 0;
        float minRaw = std::numeric_limits<float>::max();
        float maxRaw = std::numeric_limits<float>::lowest();
        float minScaled = std::numeric_limits<float>::max();
        float maxScaled = std::numeric_limits<float>::lowest();
    };

    struct CoinRunStats
    {
        uint64_t inputSingles = 0;
        uint64_t energySelectedSingles = 0;
    };

    uint16_t channelSeparation(uint32_t crystal1, uint32_t crystal2)
    {
        uint16_t ch1 = static_cast<uint16_t>(crystal1 / kBdm50100CrystalsPerChannel);
        uint16_t ch2 = static_cast<uint16_t>(crystal2 / kBdm50100CrystalsPerChannel);
        uint16_t diff = (ch1 > ch2) ? (ch1 - ch2) : (ch2 - ch1);
        uint16_t wrap = static_cast<uint16_t>(kBdm50100ChannelNum - diff);
        return std::min(diff, wrap);
    }

    EnergyStats analyzeSinglesEnergy(const std::string &singlePath)
    {
        EnergyStats stats;
        openpni::io::listmode::ListmodeFileInput input;
        input.Open(singlePath);

        const auto &header = input.Header();
        std::cout << "\n[Energy] " << singlePath << std::endl;
        {
            const auto typeName = std::string(header.FileTypeName().data());
            const int fields = static_cast<int>(header.FieldsInUse());
            auto has = [&](openpni::io::listmode::SupportedFields f)
            {
                return (fields & static_cast<int>(f)) != 0;
            };

            std::cout << "  header.fileType=" << typeName << std::endl;
            std::cout << "  header.segmentNum=" << input.SegmentNum() << std::endl;
            std::cout << "  header.fieldsInUse=0x" << std::hex << fields << std::dec << std::endl;
            if (has(openpni::io::listmode::SupportedFields::local_crystal_index1))
                std::cout << "  bits.local_crystal_index1=" << header.BitsForStorage(openpni::io::listmode::SupportedFields::local_crystal_index1) << std::endl;
            if (has(openpni::io::listmode::SupportedFields::channel_index1))
                std::cout << "  bits.channel_index1=" << header.BitsForStorage(openpni::io::listmode::SupportedFields::channel_index1) << std::endl;
            if (has(openpni::io::listmode::SupportedFields::energy1))
                std::cout << "  bits.energy1=" << header.BitsForStorage(openpni::io::listmode::SupportedFields::energy1) << std::endl;
            if (has(openpni::io::listmode::SupportedFields::absolute_timestamp1_100fs))
                std::cout << "  bits.absolute_timestamp1=" << header.BitsForStorage(openpni::io::listmode::SupportedFields::absolute_timestamp1_100fs) << std::endl;
        }

        if (header.FileTypeName() != openpni::io::listmode::fields::file_type_single_listmode)
        {
            std::cout << "  Not a single listmode file, skipping" << std::endl;
            return stats;
        }

        for (uint32_t segIdx = 0; segIdx < input.SegmentNum(); ++segIdx)
        {
            auto segment = input.ReadSegment(segIdx);
            const auto data = segment.GetHAnyData();
            const uint64_t count = data.count;
            stats.total += count;

            if (data.energy1)
            {
                for (uint64_t i = 0; i < count; ++i)
                {
                    const float raw = data.energy1[i];
                    const float scaled = raw;
                    stats.minRaw = std::min(stats.minRaw, raw);
                    stats.maxRaw = std::max(stats.maxRaw, raw);
                    stats.minScaled = std::min(stats.minScaled, scaled);
                    stats.maxScaled = std::max(stats.maxScaled, scaled);
                    if (scaled >= kEnergyLower_eV && scaled <= kEnergyUpper_eV)
                        stats.inWindow_eV++;
                    if (scaled >= kEnergyLower_eV / 1000.0f && scaled <= kEnergyUpper_eV / 1000.0f)
                        stats.inWindow_keV++;
                }
            }
            else
            {
                const float raw = 0.0f;
                const float scaled = 511.0f;
                stats.minRaw = std::min(stats.minRaw, raw);
                stats.maxRaw = std::max(stats.maxRaw, raw);
                stats.minScaled = std::min(stats.minScaled, scaled);
                stats.maxScaled = std::max(stats.maxScaled, scaled);
            }
        }

        std::cout << "  raw range: [" << stats.minRaw << ", " << stats.maxRaw << "]" << std::endl;
        std::cout << "  scaled range: [" << stats.minScaled << ", " << stats.maxScaled << "]" << std::endl;
        if (stats.total > 0)
        {
            const double ratioEv = 100.0 * static_cast<double>(stats.inWindow_eV) / static_cast<double>(stats.total);
            const double ratioKev = 100.0 * static_cast<double>(stats.inWindow_keV) / static_cast<double>(stats.total);
            std::cout << "  in window " << kEnergyLower_eV << "~" << kEnergyUpper_eV << " eV: "
                      << stats.inWindow_eV << " (" << ratioEv << "%)" << std::endl;
            std::cout << "  in window " << kEnergyLower_eV / 1000.0f << "~" << kEnergyUpper_eV / 1000.0f
                      << " keV: " << stats.inWindow_keV << " (" << ratioKev << "%)" << std::endl;
        }
        return stats;
    }

    bool computeCoincidenceByMergeAndCoin(CoinRunStats *statsOut)
    {
        std::vector<std::string> singleFiles;
        auto parse_part_number = [](const std::string &path) -> std::optional<uint64_t> {
            const std::string name = fs::path(path).filename().string();
            const std::string marker = "_part";
            const size_t pos = name.rfind(marker);
            if (pos == std::string::npos)
            {
                return std::nullopt;
            }
            const size_t start = pos + marker.size();
            const size_t end = name.find('.', start);
            if (end == std::string::npos || end <= start)
            {
                return std::nullopt;
            }
            try
            {
                return static_cast<uint64_t>(std::stoull(name.substr(start, end - start)));
            }
            catch (...)
            {
                return std::nullopt;
            }
        };
        if (fs::exists(kSinglesFile) && fs::is_directory(kSinglesFile))
        {
            for (const auto &entry : fs::directory_iterator(kSinglesFile))
            {
                if (!entry.is_regular_file())
                {
                    continue;
                }
                if (entry.path().extension() == ".lsingle")
                {
                    singleFiles.push_back(entry.path().string());
                }
            }
            std::sort(singleFiles.begin(), singleFiles.end(),
                      [&](const std::string &a, const std::string &b) {
                          const auto partA = parse_part_number(a);
                          const auto partB = parse_part_number(b);
                          if (partA && partB && partA != partB)
                          {
                              return partA.value() < partB.value();
                          }
                          return fs::path(a).filename().string() < fs::path(b).filename().string();
                      });
        }
        else
        {
            singleFiles.push_back(kSinglesFile);
        }

        if (singleFiles.empty())
        {
            std::cerr << "[Error] singles 文件不存在: " << kSinglesFile << std::endl;
            return false;
        }

        const auto energyStats = analyzeSinglesEnergy(singleFiles.front());
        if (statsOut)
        {
            statsOut->inputSingles = energyStats.total;
            statsOut->energySelectedSingles = energyStats.inWindow_eV;
        }

        fs::create_directories(kCoinOutputDir);

        CoincidenceProcessConfig cfg;
        cfg.enable = true;
        cfg.channelNum = kBdm50100ChannelNum;
        cfg.crystalsPerChannel = kBdm50100CrystalsPerChannel;
        cfg.outputDir = kCoinOutputDir;
        cfg.protocol.timeWindow_ps = timeWindowPicosec;
        cfg.protocol.delayTime_ps = delayTimePicosec;
        cfg.protocol.energyLower_eV = kEnergyLower_eV;
        cfg.protocol.energyUpper_eV = kEnergyUpper_eV;

        bool useKevWindow = false;
        if (energyStats.total > 0 && energyStats.inWindow_eV == 0 && energyStats.inWindow_keV > 0)
        {
            cfg.protocol.energyLower_eV = kEnergyLower_eV / 1000.0f;
            cfg.protocol.energyUpper_eV = kEnergyUpper_eV / 1000.0f;
            std::cout << "[Energy] Auto-switch window to keV scale: "
                      << cfg.protocol.energyLower_eV << "~" << cfg.protocol.energyUpper_eV << std::endl;
            useKevWindow = true;
        }

        if (statsOut)
        {
            statsOut->energySelectedSingles = useKevWindow ? energyStats.inWindow_keV : energyStats.inWindow_eV;
        }

        std::cout << "\n=== Step 1: MergeAndCoin 符合计算 ===" << std::endl;
        std::cout << "Input singles count: " << singleFiles.size() << std::endl;
        std::cout << "Output dir: " << kCoinOutputDir << std::endl;

        struct RotatingListmodeWriter
        {
            std::string outputDir;
            std::string baseName;
            uint64_t maxBytes = 0;
            uint64_t currentBytes = 0;
            size_t index = 0;
            std::unique_ptr<openpni::io::listmode::ListmodeFileOutput> writer;

            void openNew()
            {
                index++;
                currentBytes = 0;
                const std::string path = outputDir + "/" + baseName +
                                         "_" + (index < 10 ? "000" : index < 100 ? "00" : index < 1000 ? "0" : "") +
                                         std::to_string(index) + ".lmf";
                writer = std::make_unique<openpni::io::listmode::ListmodeFileOutput>(
                    openpni::distributed::coin::createCoinListmodeHeader());
                writer->Open(path);
            }

            void append(std::span<openpni::Listmode const> listmodes)
            {
                if (listmodes.empty())
                {
                    return;
                }
                if (!writer)
                {
                    openNew();
                }
                const uint64_t bytesNeeded = static_cast<uint64_t>(listmodes.size()) * sizeof(openpni::Listmode);
                if (maxBytes > 0 && currentBytes + bytesNeeded > maxBytes)
                {
                    openNew();
                }

                openpni::io::listmode::ListmodeFileSegment segment;
                segment.SetListmodes(listmodes);
                segment.SetClockMs(0);
                segment.SetDurationMs(0);
                writer->AppendSegment(std::move(segment));

                currentBytes += bytesNeeded;
            }
        };

        RotatingListmodeWriter promptWriter;
        RotatingListmodeWriter delayWriter;
        promptWriter.outputDir = kCoinOutputDir;
        delayWriter.outputDir = kCoinOutputDir;
        promptWriter.baseName = kPromptBaseName;
        delayWriter.baseName = kDelayBaseName;
        promptWriter.maxBytes = kListmodeMaxBytes;
        delayWriter.maxBytes = kListmodeMaxBytes;

        openpni::Coincidence coinNode;
        if (cfg.channelNum == 0 || cfg.crystalsPerChannel == 0)
        {
            std::cerr << "[Error] channelNum or crystalsPerChannel is zero." << std::endl;
            return false;
        }
        std::vector<uint32_t> crystalNumOfEachChannel(cfg.channelNum, cfg.crystalsPerChannel);
        coinNode.setTotalCrystalNumOfEachChannel(crystalNumOfEachChannel);

        uint64_t totalSegments = 0;
        uint64_t totalSingles = 0;
        uint64_t totalPromptEvents = 0;
        uint64_t totalDelayEvents = 0;
        uint64_t totalPromptBytes = 0;
        uint64_t totalDelayBytes = 0;

        for (const auto &singlePath : singleFiles)
        {
            openpni::io::listmode::ListmodeFileInput input;
            input.Open(singlePath);

            std::cout << "Processing singles: " << singlePath << std::endl;

            for (uint32_t segIdx = 0; segIdx < input.SegmentNum(); ++segIdx)
            {
                auto segment = input.ReadSegment(segIdx);
                auto singles = openpni::distributed::coin::readSinglesFromSegment(segment);
                if (singles.empty())
                {
                    continue;
                }

                totalSegments++;
                totalSingles += singles.size();

                std::span<const openpni::distributed::r2s::Single> hostSpan(singles.data(), singles.size());

                openpni::distributed::r2s::Single *d_singles_ptr = nullptr;
                const size_t bytes = singles.size() * sizeof(openpni::distributed::r2s::Single);
                cudaError_t err = cudaMalloc(&d_singles_ptr, bytes);
                if (err != cudaSuccess)
                {
                    std::cerr << "cudaMalloc failed: " << cudaGetErrorString(err) << std::endl;
                    return false;
                }

                err = cudaMemcpy(d_singles_ptr, hostSpan.data(), bytes, cudaMemcpyHostToDevice);
                if (err != cudaSuccess)
                {
                    std::cerr << "cudaMemcpy failed: " << cudaGetErrorString(err) << std::endl;
                    cudaFree(d_singles_ptr);
                    return false;
                }

                std::span<const openpni::distributed::r2s::Single> d_span(d_singles_ptr, singles.size());
                std::vector<std::span<openpni::distributed::r2s::Single const>> inputList;
                inputList.push_back(d_span);

                try
                {
                    auto [prompt, delay] = coinNode.getDListmode(inputList, cfg.protocol);

                    if (!prompt.empty())
                    {
                        std::vector<openpni::Listmode> promptHost;
                        const openpni::Listmode *promptPtr = prompt.data();
                        if (openpni::distributed::r2s::isDevicePointer(promptPtr))
                        {
                            promptHost.resize(prompt.size());
                            cudaError_t pErr = cudaMemcpy(promptHost.data(), promptPtr,
                                                          prompt.size() * sizeof(openpni::Listmode),
                                                          cudaMemcpyDeviceToHost);
                            if (pErr != cudaSuccess)
                            {
                                std::cerr << "cudaMemcpy prompt failed: " << cudaGetErrorString(pErr) << std::endl;
                                cudaFree(d_singles_ptr);
                                return false;
                            }
                            promptPtr = promptHost.data();
                        }
                        promptWriter.append(std::span<const openpni::Listmode>(promptPtr, prompt.size()));
                        totalPromptEvents += prompt.size();
                        totalPromptBytes += static_cast<uint64_t>(prompt.size()) * sizeof(openpni::Listmode);
                    }

                    if (!delay.empty())
                    {
                        std::vector<openpni::Listmode> delayHost;
                        const openpni::Listmode *delayPtr = delay.data();
                        if (openpni::distributed::r2s::isDevicePointer(delayPtr))
                        {
                            delayHost.resize(delay.size());
                            cudaError_t dErr = cudaMemcpy(delayHost.data(), delayPtr,
                                                          delay.size() * sizeof(openpni::Listmode),
                                                          cudaMemcpyDeviceToHost);
                            if (dErr != cudaSuccess)
                            {
                                std::cerr << "cudaMemcpy delay failed: " << cudaGetErrorString(dErr) << std::endl;
                                cudaFree(d_singles_ptr);
                                return false;
                            }
                            delayPtr = delayHost.data();
                        }
                        delayWriter.append(std::span<const openpni::Listmode>(delayPtr, delay.size()));
                        totalDelayEvents += delay.size();
                        totalDelayBytes += static_cast<uint64_t>(delay.size()) * sizeof(openpni::Listmode);
                    }

                    if (totalSegments % 10 == 0)
                    {
                        std::cout << "[Stats] segments=" << totalSegments
                                  << ", singles=" << totalSingles
                                  << ", prompt=" << totalPromptEvents
                                  << ", delay=" << totalDelayEvents
                                  << ", promptMB=" << (totalPromptBytes / (1024.0 * 1024.0))
                                  << ", delayMB=" << (totalDelayBytes / (1024.0 * 1024.0))
                                  << std::endl;
                    }
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Coincidence error: " << e.what() << std::endl;
                    cudaFree(d_singles_ptr);
                    return false;
                }

                cudaFree(d_singles_ptr);
            }
        }

        const std::string promptPath = kCoinOutputDir + "/" + kPromptBaseName + "_0001.lmf";
        const std::string delayPath = kCoinOutputDir + "/" + kDelayBaseName + "_0001.lmf";

        std::cout << "prompt exists: " << (fs::exists(promptPath) ? "yes" : "no");
        if (fs::exists(promptPath))
        {
            std::cout << ", size=" << fs::file_size(promptPath) << " bytes";
        }
        std::cout << std::endl;

        std::cout << "delay  exists: " << (fs::exists(delayPath) ? "yes" : "no");
        if (fs::exists(delayPath))
        {
            std::cout << ", size=" << fs::file_size(delayPath) << " bytes";
        }
        std::cout << std::endl;

        std::cout << "[Summary] segments=" << totalSegments
                  << ", singles=" << totalSingles
                  << ", prompt=" << totalPromptEvents
                  << ", delay=" << totalDelayEvents
                  << ", promptMB=" << (totalPromptBytes / (1024.0 * 1024.0))
                  << ", delayMB=" << (totalDelayBytes / (1024.0 * 1024.0))
                  << ", totalMB=" << ((totalPromptBytes + totalDelayBytes) / (1024.0 * 1024.0))
                  << std::endl;

        return fs::exists(promptPath) && fs::exists(delayPath);
    }

    LmfStats analyzeLmf(const std::string &lmfPath)
    {
        LmfStats stats;
        stats.path = lmfPath;
        stats.channelSepHist.assign(kBdm50100ChannelNum / 2 + 1, 0);
        stats.channelPairHist12x12.assign(kAnalyzeChannelNum * kAnalyzeChannelNum, 0);
        stats.samples.reserve(12);

        openpni::io::listmode::ListmodeFileInput input;
        input.Open(lmfPath);

        stats.segmentNum = input.SegmentNum();

        std::cout << "\n[LMF] " << lmfPath << std::endl;
        std::cout << "  segmentNum=" << stats.segmentNum << std::endl;

        for (uint32_t segIdx = 0; segIdx < stats.segmentNum; ++segIdx)
        {
            auto segment = input.ReadSegment(segIdx);
            const auto data = segment.GetHAnyData();
            if (!data.local_crystal_index1 || !data.local_crystal_index2 ||
                !data.channel_index1 || !data.channel_index2 || !data.time_of_flight_100fs)
            {
                std::cout << "  Segment " << segIdx << " missing listmode fields, skipping" << std::endl;
                continue;
            }

            stats.totalEvents += data.count;

            for (std::size_t i = 0; i < data.count; ++i)
            {
                const uint16_t ch1 = data.channel_index1[i];
                const uint16_t ch2 = data.channel_index2[i];
                const uint32_t g1 = static_cast<uint32_t>(ch1) * kBdm50100CrystalsPerChannel + data.local_crystal_index1[i];
                const uint32_t g2 = static_cast<uint32_t>(ch2) * kBdm50100CrystalsPerChannel + data.local_crystal_index2[i];
                const int16_t dt = static_cast<int16_t>(data.time_of_flight_100fs[i]);

                stats.checkedDtCount++;
                if (dt < 0)
                {
                    stats.negativeDtCount++;
                }
                else if (dt > kTimeWindowPs)
                {
                    stats.overWindowDtCount++;
                }

                stats.minDt = std::min(stats.minDt, dt);
                stats.maxDt = std::max(stats.maxDt, dt);

                const uint16_t sep = channelSeparation(g1, g2);
                stats.channelSepHist[sep]++;

                if (ch1 < kAnalyzeChannelNum && ch2 < kAnalyzeChannelNum)
                {
                    const uint16_t a = std::min(ch1, ch2);
                    const uint16_t b = std::max(ch1, ch2);
                    stats.channelPairHist12x12[a * kAnalyzeChannelNum + b]++;
                    stats.inAnalyzeChannelRangeCount++;
                }
                else
                {
                    stats.outOfAnalyzeChannelRangeCount++;
                }

                if (stats.samples.size() < 8)
                {
                    stats.samples.push_back(EventSample{
                        g1,
                        g2,
                        dt,
                        sep});
                }
            }
        }

        return stats;
    }

    void printChannelPairDistribution12(const LmfStats &stats, const std::string &name)
    {
        std::cout << "\n  " << name << " 通道对分布（0-" << (kAnalyzeChannelNum - 1)
              << "通道，i<=j）:" << std::endl;
        std::cout << "  in-range events=" << stats.inAnalyzeChannelRangeCount
                  << ", out-of-range events=" << stats.outOfAnalyzeChannelRangeCount << std::endl;

        std::cout << "  pair count matrix (upper triangle):" << std::endl;
        for (uint16_t i = 0; i < kAnalyzeChannelNum; ++i)
        {
            std::cout << "    ch" << std::setw(2) << i << ": ";
            for (uint16_t j = 0; j < kAnalyzeChannelNum; ++j)
            {
                if (j < i)
                {
                    std::cout << std::setw(8) << "-";
                }
                else
                {
                    std::cout << std::setw(8)
                              << stats.channelPairHist12x12[i * kAnalyzeChannelNum + j];
                }
            }
            std::cout << std::endl;
        }
    }

    void printTimeValueValidation(const LmfStats &stats, const std::string &name)
    {
        const uint64_t abnormal = stats.negativeDtCount + stats.overWindowDtCount;
        std::cout << "\n  " << name << " time1_2pico 校验:" << std::endl;
        std::cout << "    checked=" << stats.checkedDtCount
                  << ", negative=" << stats.negativeDtCount
                  << ", >window(" << kTimeWindowPs << " ps)=" << stats.overWindowDtCount
                  << ", abnormal_total=" << abnormal << std::endl;
        if (abnormal == 0)
        {
            std::cout << "    结果: 无负值或超时间窗异常。" << std::endl;
        }
        else
        {
            std::cout << "    结果: 存在异常值，请检查符合参数/解码或上游数据。" << std::endl;
        }
    }

    void printSampleCompare(const LmfStats &promptStats, const LmfStats &delayStats)
    {
        std::cout << "\n=== Step 2: listmodeIO 读取与样本对比 ===" << std::endl;
        std::cout << "Prompt sample (up to 8 events):" << std::endl;
        for (size_t i = 0; i < promptStats.samples.size(); ++i)
        {
            const auto &s = promptStats.samples[i];
            std::cout << "  [" << i << "] g1=" << s.globalCrystalIndex1
                      << ", g2=" << s.globalCrystalIndex2
                      << ", dt(ps)=" << s.dt_pico
                      << ", chSep=" << s.channelSep
                      << std::endl;
        }

        std::cout << "Delay sample (up to 8 events):" << std::endl;
        for (size_t i = 0; i < delayStats.samples.size(); ++i)
        {
            const auto &s = delayStats.samples[i];
            std::cout << "  [" << i << "] g1=" << s.globalCrystalIndex1
                      << ", g2=" << s.globalCrystalIndex2
                      << ", dt(ps)=" << s.dt_pico
                      << ", chSep=" << s.channelSep
                      << std::endl;
        }
    }

    void printTopBins(const std::vector<uint64_t> &hist, const std::string &name)
    {
        std::vector<std::pair<uint16_t, uint64_t>> bins;
        bins.reserve(hist.size());
        for (uint16_t i = 0; i < hist.size(); ++i)
        {
            bins.emplace_back(i, hist[i]);
        }
        std::sort(bins.begin(), bins.end(), [](const auto &a, const auto &b)
                  { return a.second > b.second; });

        std::cout << "  " << name << " top-5 channelSep bins:" << std::endl;
        for (size_t i = 0; i < std::min<size_t>(5, bins.size()); ++i)
        {
            std::cout << "    sep=" << std::setw(2) << bins[i].first
                      << ", count=" << bins[i].second << std::endl;
        }
    }

    void analyzeDistribution(const LmfStats &promptStats, const LmfStats &delayStats)
    {
        std::cout << "\n=== Step 3: 分布分析 ===" << std::endl;

        std::cout << "Prompt summary:" << std::endl;
        std::cout << "  totalEvents=" << promptStats.totalEvents
                  << ", dtRange=[" << promptStats.minDt << ", " << promptStats.maxDt << "] ps"
                  << std::endl;
        printTopBins(promptStats.channelSepHist, "prompt");

        std::cout << "Delay summary:" << std::endl;
        std::cout << "  totalEvents=" << delayStats.totalEvents
                  << ", dtRange=[" << delayStats.minDt << ", " << delayStats.maxDt << "] ps"
                  << std::endl;
        printTopBins(delayStats.channelSepHist, "delay");

        const uint64_t promptOpp = promptStats.channelSepHist[kBdm50100ChannelNum / 2];
        const double promptOppRatio = promptStats.totalEvents > 0
                                          ? static_cast<double>(promptOpp) / static_cast<double>(promptStats.totalEvents)
                                          : 0.0;

        const double delayMean = delayStats.totalEvents > 0
                                     ? static_cast<double>(delayStats.totalEvents) / static_cast<double>(delayStats.channelSepHist.size())
                                     : 0.0;

        double delayVar = 0.0;
        if (delayMean > 0.0)
        {
            for (const auto c : delayStats.channelSepHist)
            {
                const double d = static_cast<double>(c) - delayMean;
                delayVar += d * d;
            }
            delayVar /= static_cast<double>(delayStats.channelSepHist.size());
        }
        const double delayStd = std::sqrt(delayVar);
        const double delayCv = delayMean > 0.0 ? (delayStd / delayMean) : 0.0;

        std::cout << std::fixed << std::setprecision(4);
        std::cout << "\n判据输出:" << std::endl;
        std::cout << "  Prompt 对径通道间隔(sep=24)占比: " << promptOppRatio * 100.0 << "%" << std::endl;
        std::cout << "  Delay 通道间隔分布 CV: " << delayCv << " (越小越均匀)" << std::endl;
        std::cout << "  解释: prompt 应在对径探测器更集中；delay 作为随机符合应更平坦。" << std::endl;

        std::cout << "\n=== Step 4: 12通道对分布分析 ===" << std::endl;
        printChannelPairDistribution12(promptStats, "prompt");
        printChannelPairDistribution12(delayStats, "delay");

        std::cout << "\n=== Step 5: time1_2pico 异常值检查 ===" << std::endl;
        printTimeValueValidation(promptStats, "prompt");
        printTimeValueValidation(delayStats, "delay");
    }
}

int main()
{
    std::cout << "======================================" << std::endl;
    std::cout << "      PNI Coin Standalone Test" << std::endl;
    std::cout << "======================================" << std::endl;

    CoinRunStats runStats;
    const bool coinOk = computeCoincidenceByMergeAndCoin(&runStats);
    if (!coinOk)
    {
        std::cerr << "\n[FAIL] 符合计算失败，终止后续分析。" << std::endl;
        return 1;
    }

    const std::string promptPath = kCoinOutputDir + "/" + kPromptBaseName + "_0001.lmf";
    const std::string delayPath = kCoinOutputDir + "/" + kDelayBaseName + "_0001.lmf";

    if (!fs::exists(promptPath) || !fs::exists(delayPath))
    {
        std::cerr << "\n[FAIL] prompt.lmf 或 delay.lmf 不存在。" << std::endl;
        return 1;
    }

    // const auto promptStats = analyzeLmf(promptPath);
    // const auto delayStats = analyzeLmf(delayPath);

    // if (runStats.inputSingles > 0)
    // {
    //     const double energyRatio = 100.0 * static_cast<double>(runStats.energySelectedSingles) /
    //                                static_cast<double>(runStats.inputSingles);
    //     const double promptRatio = 100.0 * static_cast<double>(promptStats.totalEvents) /
    //                                static_cast<double>(runStats.energySelectedSingles);
    //     const double delayRatio = 100.0 * static_cast<double>(delayStats.totalEvents) /
    //                               static_cast<double>(runStats.energySelectedSingles);

    //     std::cout << std::fixed << std::setprecision(6);
    //     std::cout << "\n=== Coincidence Summary ===" << std::endl;
    //     std::cout << "Energy selected singles num / input singles num: " << energyRatio << "%" << std::endl;
    //     std::cout << "Prompt coins num: " << promptStats.totalEvents << std::endl;
    //     std::cout << "Prompt coins rate: " << promptRatio << "%" << std::endl;
    //     std::cout << "Delay coins num: " << delayStats.totalEvents << std::endl;
    //     std::cout << "Delay coins rate: " << delayRatio << "%" << std::endl;
    // }

    //printSampleCompare(promptStats, delayStats);
    //analyzeDistribution(promptStats, delayStats);

    std::cout << "\n[PASS] pniCoin 测试流程完成。" << std::endl;
    return 0;
}