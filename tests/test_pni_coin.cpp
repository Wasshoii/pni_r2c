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
#include <pni/io/v1/Decoding_v1.hpp>

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
    constexpr float kEnergyLower_eV = 350000.0f;
    constexpr float kEnergyUpper_eV = 650000.0f;

    const std::string kSinglesFile = "/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/pni_dis_r2c/data/res/singles_50100_test.single";
    const std::string kCoinOutputDir = "/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/pni_dis_r2c/data/res/coin_50100_full";
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

    inline uint16_t globalCrystalToChannel(uint32_t crystal)
    {
        return static_cast<uint16_t>(crystal / kBdm50100CrystalsPerChannel);
    }

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
        openpni::io::v1::single::SingleFileInput input;
        input.open(singlePath);

        const auto header = input.header();
        std::cout << "\n[Energy] " << singlePath << std::endl;
        std::cout << "  bytes4Energy=" << static_cast<int>(header.bytes4Energy)
                  << ", bytes4CrystalIndex=" << static_cast<int>(header.bytes4CrystalIndex)
                  << ", bytes4TimeValue=" << static_cast<int>(header.bytes4TimeValue) << std::endl;

        for (uint32_t segIdx = 0; segIdx < header.segmentNum; ++segIdx)
        {
            const auto segHeader = input.segmentHeader(segIdx);
            const auto segBytes = input.readSegment(segIdx);
            const uint64_t count = segHeader.count;
            stats.total += count;

            if (header.bytes4Energy == 4)
            {
                auto ptr = reinterpret_cast<const float *>(segBytes.energyBytes.get());
                for (uint64_t i = 0; i < count; ++i)
                {
                    const float raw = ptr[i];
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
            else if (header.bytes4Energy == 2)
            {
                auto ptr = reinterpret_cast<const uint16_t *>(segBytes.energyBytes.get());
                for (uint64_t i = 0; i < count; ++i)
                {
                    const float raw = static_cast<float>(ptr[i]);
                    const float scaled = raw * 0.01f;
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
            else if (header.bytes4Energy == 1)
            {
                auto ptr = reinterpret_cast<const uint8_t *>(segBytes.energyBytes.get());
                for (uint64_t i = 0; i < count; ++i)
                {
                    const float raw = static_cast<float>(ptr[i]);
                    const float scaled = raw * 4.0f;
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
        if (!fs::exists(kSinglesFile))
        {
            std::cerr << "[Error] singles 文件不存在: " << kSinglesFile << std::endl;
            return false;
        }

        const auto energyStats = analyzeSinglesEnergy(kSinglesFile);
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
        cfg.protocol.timeWindow_ps = 2000;
        cfg.protocol.delayTime_ps = 2000000;
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

        const std::vector<std::string> singleFiles = {kSinglesFile};
        const std::string mergedOutputPlaceholder = kCoinOutputDir + "/merged_placeholder.single";

        std::cout << "\n=== Step 1: MergeAndCoin 符合计算 ===" << std::endl;
        std::cout << "Input singles: " << kSinglesFile << std::endl;
        std::cout << "Output dir: " << kCoinOutputDir << std::endl;

        bool ok = merge_single_files(singleFiles, mergedOutputPlaceholder, cfg, false);
        if (!ok)
        {
            std::cerr << "[Error] merge_single_files 执行失败" << std::endl;
            return false;
        }

        const std::string promptPath = kCoinOutputDir + "/prompt.lmf";
        const std::string delayPath = kCoinOutputDir + "/delay.lmf";

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

        return fs::exists(promptPath) && fs::exists(delayPath);
    }

    LmfStats analyzeLmf(const std::string &lmfPath)
    {
        LmfStats stats;
        stats.path = lmfPath;
        stats.channelSepHist.assign(kBdm50100ChannelNum / 2 + 1, 0);
        stats.channelPairHist12x12.assign(kAnalyzeChannelNum * kAnalyzeChannelNum, 0);
        stats.samples.reserve(12);

        openpni::io::v1::listmode::ListmodeFileInput input;
        input.open(lmfPath);

        const auto header = input.header();
        stats.segmentNum = header.segmentNum;

        std::cout << "\n[LMF] " << lmfPath << std::endl;
        std::cout << "  segmentNum=" << header.segmentNum
                  << ", cystalNum=" << header.cystalNum
                  << ", bytes4CrystalIndex1=" << static_cast<int>(header.bytes4CrystalIndex1)
                  << ", bytes4CrystalIndex2=" << static_cast<int>(header.bytes4CrystalIndex2)
                  << ", bytes4TimeValue1_2=" << static_cast<int>(header.bytes4TimeValue1_2)
                  << std::endl;

        for (uint32_t segIdx = 0; segIdx < header.segmentNum; ++segIdx)
        {
            const auto segHeader = input.segmentHeader(segIdx);
            const auto segBytes = input.readSegment(segIdx);
            auto events = openpni::io::v1::listmode::decompress(header, segHeader, segBytes);

            stats.totalEvents += segHeader.count;

            for (uint64_t i = 0; i < segHeader.count; ++i)
            {
                const auto &ev = events[i];
                const uint16_t ch1 = globalCrystalToChannel(ev.globalCrystalIndex1);
                const uint16_t ch2 = globalCrystalToChannel(ev.globalCrystalIndex2);

                stats.checkedDtCount++;
                if (ev.time1_2pico < 0)
                {
                    stats.negativeDtCount++;
                }
                else if (ev.time1_2pico > kTimeWindowPs)
                {
                    stats.overWindowDtCount++;
                }

                stats.minDt = std::min(stats.minDt, ev.time1_2pico);
                stats.maxDt = std::max(stats.maxDt, ev.time1_2pico);

                const uint16_t sep = channelSeparation(ev.globalCrystalIndex1, ev.globalCrystalIndex2);
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
                        ev.globalCrystalIndex1,
                        ev.globalCrystalIndex2,
                        ev.time1_2pico,
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

    const std::string promptPath = kCoinOutputDir + "/prompt.lmf";
    const std::string delayPath = kCoinOutputDir + "/delay.lmf";

    if (!fs::exists(promptPath) || !fs::exists(delayPath))
    {
        std::cerr << "\n[FAIL] prompt.lmf 或 delay.lmf 不存在。" << std::endl;
        return 1;
    }

    const auto promptStats = analyzeLmf(promptPath);
    const auto delayStats = analyzeLmf(delayPath);

    if (runStats.inputSingles > 0)
    {
        const double energyRatio = 100.0 * static_cast<double>(runStats.energySelectedSingles) /
                                   static_cast<double>(runStats.inputSingles);
        const double promptRatio = 100.0 * static_cast<double>(promptStats.totalEvents) /
                                   static_cast<double>(runStats.energySelectedSingles);
        const double delayRatio = 100.0 * static_cast<double>(delayStats.totalEvents) /
                                  static_cast<double>(runStats.energySelectedSingles);

        std::cout << std::fixed << std::setprecision(6);
        std::cout << "\n=== Coincidence Summary ===" << std::endl;
        std::cout << "Energy selected singles num / input singles num: " << energyRatio << "%" << std::endl;
        std::cout << "Prompt coins num: " << promptStats.totalEvents << std::endl;
        std::cout << "Prompt coins rate: " << promptRatio << "%" << std::endl;
        std::cout << "Delay coins num: " << delayStats.totalEvents << std::endl;
        std::cout << "Delay coins rate: " << delayRatio << "%" << std::endl;
    }

    //printSampleCompare(promptStats, delayStats);
    //analyzeDistribution(promptStats, delayStats);

    std::cout << "\n[PASS] pniCoin 测试流程完成。" << std::endl;
    return 0;
}