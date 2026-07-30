#pragma once

/**
 * @file local_grpc_9120_common.hpp
 * @brief Shared 9120 dual-node constants, channel helpers, and LMF validators
 *        for streaming coincidence and gRPC R2S→coin tests.
 */

#include <pni/io/ListmodeIO.hpp>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace r2c_test_9120
{

constexpr const char *k9120DataRoot = "/media/lenovo/1TB/50100data/test_9120";
constexpr const char *k9120CalibrationDir = "/media/lenovo/1TB/50100data/pni_res/caliFile";
constexpr float k9120EnergyLower_eV = 421000.0f;
constexpr float k9120EnergyUpper_eV = 1000000.0f;
constexpr uint16_t k9120ChannelNum = 576;
constexpr uint32_t k9120CrystalsPerChannel = 6 * 6 * 8;
constexpr int16_t k9120TimeWindowPs = 2000;
constexpr int k9120DelayTimePs = 2000000;
constexpr uint16_t k9120ChannelsPerNode = 288;

inline uint16_t channelSeparation9120(uint32_t crystal1, uint32_t crystal2)
{
    const uint16_t ch1 = static_cast<uint16_t>(crystal1 / k9120CrystalsPerChannel);
    const uint16_t ch2 = static_cast<uint16_t>(crystal2 / k9120CrystalsPerChannel);
    const uint16_t diff = (ch1 > ch2) ? static_cast<uint16_t>(ch1 - ch2)
                                      : static_cast<uint16_t>(ch2 - ch1);
    const uint16_t wrap = static_cast<uint16_t>(k9120ChannelNum - diff);
    return std::min(diff, wrap);
}

struct LmfStats
{
    std::string path;
    uint64_t totalEvents = 0;
    uint64_t checkedDtCount = 0;
    uint64_t negativeDtCount = 0;
    uint64_t overWindowDtCount = 0;
    int16_t minDt = std::numeric_limits<int16_t>::max();
    int16_t maxDt = std::numeric_limits<int16_t>::min();
    std::vector<uint64_t> channelSepHist;
};

inline LmfStats analyzeLmfFile(const std::string &lmfPath, int16_t timeWindow100fs)
{
    LmfStats stats;
    stats.path = lmfPath;
    stats.channelSepHist.assign(k9120ChannelNum / 2 + 1, 0);

    openpni::io::listmode::ListmodeFileInput input;
    input.Open(lmfPath);

    std::cout << "\n[LMF] " << lmfPath << std::endl;
    std::cout << "  segmentNum=" << input.SegmentNum() << std::endl;

    for (uint32_t segIdx = 0; segIdx < input.SegmentNum(); ++segIdx)
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
            const uint32_t g1 =
                static_cast<uint32_t>(ch1) * k9120CrystalsPerChannel + data.local_crystal_index1[i];
            const uint32_t g2 =
                static_cast<uint32_t>(ch2) * k9120CrystalsPerChannel + data.local_crystal_index2[i];
            const int16_t dt = static_cast<int16_t>(data.time_of_flight_100fs[i]);
            const int16_t dtAbs = static_cast<int16_t>(std::abs(static_cast<int>(dt)));

            stats.checkedDtCount++;
            if (dt < 0)
            {
                stats.negativeDtCount++;
            }
            if (dtAbs > timeWindow100fs)
            {
                stats.overWindowDtCount++;
            }

            stats.minDt = std::min(stats.minDt, dt);
            stats.maxDt = std::max(stats.maxDt, dt);

            const uint16_t sep = channelSeparation9120(g1, g2);
            if (sep < stats.channelSepHist.size())
            {
                stats.channelSepHist[sep]++;
            }
        }
    }

    return stats;
}

inline bool validatePromptChannelSepPeak(
    const LmfStats &promptStats,
    uint16_t expectedSep,
    double minRatio)
{
    if (promptStats.totalEvents == 0 || expectedSep >= promptStats.channelSepHist.size())
    {
        return false;
    }

    std::vector<std::pair<uint16_t, uint64_t>> bins;
    bins.reserve(promptStats.channelSepHist.size());
    for (uint16_t sep = 0; sep < promptStats.channelSepHist.size(); ++sep)
    {
        if (promptStats.channelSepHist[sep] > 0)
        {
            bins.emplace_back(sep, promptStats.channelSepHist[sep]);
        }
    }
    if (bins.empty())
    {
        return false;
    }

    std::sort(bins.begin(), bins.end(),
              [](const auto &a, const auto &b) { return a.second > b.second; });

    const double sepRatio = static_cast<double>(promptStats.channelSepHist[expectedSep])
                            / static_cast<double>(promptStats.totalEvents);
    const bool inTop2 = (bins[0].first == expectedSep)
                        || (bins.size() > 1 && bins[1].first == expectedSep);

    std::cout << "  Prompt channelSep top bins:" << std::endl;
    for (size_t i = 0; i < std::min<size_t>(5, bins.size()); ++i)
    {
        std::cout << "    sep=" << bins[i].first << ", count=" << bins[i].second << std::endl;
    }
    std::cout << "  sep=" << expectedSep << " ratio=" << (sepRatio * 100.0) << "%" << std::endl;

    return inTop2 && sepRatio >= minRatio;
}

inline bool validate9120PromptChannelSep(const LmfStats &promptStats)
{
    // 双节点各 288 通道：对环主峰常在 sep=144（半机）或 sep=288（整机对环）
    constexpr uint16_t kSepPerNode = k9120ChannelNum / 4;   // 144
    constexpr uint16_t kSepFullRing = k9120ChannelNum / 2; // 288
    const bool perNodeOk = validatePromptChannelSepPeak(promptStats, kSepPerNode, 0.05);
    const bool fullRingOk = validatePromptChannelSepPeak(promptStats, kSepFullRing, 0.04);
    return perNodeOk || fullRingOk;
}

inline std::vector<uint16_t> makeChannelRange(uint16_t begin, uint16_t endExclusive)
{
    std::vector<uint16_t> channels;
    channels.reserve(static_cast<size_t>(endExclusive - begin));
    for (uint16_t ch = begin; ch < endExclusive; ++ch)
    {
        channels.push_back(ch);
    }
    return channels;
}

struct NodeInput
{
    uint32_t nodeId = 0;
    std::string rawdataPath;
    std::vector<uint16_t> channels;
};

inline std::vector<NodeInput> build9120DualNodeInputs(
    const std::string &node0RawDir,
    const std::string &node1RawDir)
{
    std::vector<NodeInput> nodes(2);
    nodes[0].nodeId = 0;
    nodes[0].rawdataPath = node0RawDir;
    nodes[0].channels = makeChannelRange(0, k9120ChannelsPerNode);

    nodes[1].nodeId = 1;
    nodes[1].rawdataPath = node1RawDir;
    nodes[1].channels = makeChannelRange(k9120ChannelsPerNode, k9120ChannelNum);
    return nodes;
}

} // namespace r2c_test_9120
