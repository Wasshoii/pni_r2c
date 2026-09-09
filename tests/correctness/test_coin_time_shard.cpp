/**
 * Dual-aligner PET epoch handoff vs single-batch gold (9120 .lsingle).
 */

#include <pni/PnI-Config.hpp>

#include "core/streaming/StreamingCoincidence.hpp"
#include "tests/correctness/data_9120_common.hpp"

#include <pni/io/IO.hpp>
#include <pni/io/ListmodeIO.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace openpni::distributed::streaming;
using namespace r2c_test_9120;
namespace fs = std::filesystem;

namespace
{
    std::vector<std::string> listLsingle(const std::string &dir)
    {
        std::vector<std::string> files;
        if (!fs::exists(dir))
        {
            return files;
        }
        for (const auto &entry : fs::directory_iterator(dir))
        {
            if (entry.path().extension() == ".lsingle")
            {
                files.push_back(entry.path().string());
            }
        }
        std::sort(files.begin(), files.end());
        return files;
    }

    std::vector<Single> readSingles(openpni::io::listmode::ListmodeFileSegment &segment)
    {
        const auto data = segment.GetHAnyData();
        if (!data.local_crystal_index1 || !data.channel_index1 || !data.absolute_timestamp1_100fs)
        {
            return {};
        }
        std::vector<Single> singles(data.count);
        for (std::size_t i = 0; i < data.count; ++i)
        {
            singles[i].channelIndex = data.channel_index1[i];
            singles[i].crystalIndex = data.local_crystal_index1[i];
            singles[i].timevalue_100fs = data.absolute_timestamp1_100fs[i];
            singles[i].energy_ev = data.energy1 ? data.energy1[i] : 0.0f;
        }
        return singles;
    }

    bool loadNodeChunks(std::array<std::vector<TimestampedSingleChunk>, 2> *out,
                        openpni::CoincidenceProtocol *proto)
    {
        const std::string node0Dir = std::string(k9120DataRoot) + "/pni_singles_node0";
        const std::string node1Dir = std::string(k9120DataRoot) + "/pni_singles_node1";
        const auto files0 = listLsingle(node0Dir);
        const auto files1 = listLsingle(node1Dir);
        if (files0.empty() || files1.empty())
        {
            std::cout << "  No .lsingle under " << node0Dir << " / " << node1Dir
                      << "; test skipped.\n";
            return false;
        }

        constexpr size_t kMaxSinglesPerNode = 100'000;
        constexpr size_t kChunkSingles = 10'000;
        const std::array<std::string, 2> firstFiles = {files0.front(), files1.front()};

        for (size_t node = 0; node < 2; ++node)
        {
            openpni::io::listmode::ListmodeFileInput input;
            input.Open(firstFiles[node]);
            size_t taken = 0;
            uint64_t chunkId = 0;
            for (uint32_t segIdx = 0; segIdx < input.SegmentNum() && taken < kMaxSinglesPerNode;
                 ++segIdx)
            {
                auto segment = input.ReadSegment(segIdx);
                auto singles = readSingles(segment);
                for (size_t off = 0; off < singles.size() && taken < kMaxSinglesPerNode;
                     off += kChunkSingles)
                {
                    const size_t end = std::min({off + kChunkSingles, singles.size(),
                                                 off + (kMaxSinglesPerNode - taken)});
                    TimestampedSingleChunk chunk;
                    chunk.nodeId = static_cast<uint16_t>(node);
                    chunk.chunkId = chunkId++;
                    chunk.computerClock_ms = segment.GetClockMs();
                    chunk.duration_ms = segment.GetDurationMs();
                    chunk.singles.assign(singles.begin() + static_cast<std::ptrdiff_t>(off),
                                         singles.begin() + static_cast<std::ptrdiff_t>(end));
                    chunk.updateTimeRange();
                    taken += chunk.singles.size();
                    (*out)[node].push_back(std::move(chunk));
                }
            }
            std::cout << "  Node " << node << ": " << taken << " singles\n";
            if (taken == 0)
            {
                return false;
            }
        }

        proto->timeWindow_ps = k9120TimeWindowPs;
        proto->delayTime_ps = k9120DelayTimePs;
        proto->energyLower_eV = k9120EnergyLower_eV;
        proto->energyUpper_eV = k9120EnergyUpper_eV;
        uint64_t inEv = 0;
        uint64_t inKev = 0;
        for (const auto &s : (*out)[0].front().singles)
        {
            if (s.energy_ev >= k9120EnergyLower_eV && s.energy_ev <= k9120EnergyUpper_eV)
            {
                ++inEv;
            }
            if (s.energy_ev >= k9120EnergyLower_eV / 1000.0f &&
                s.energy_ev <= k9120EnergyUpper_eV / 1000.0f)
            {
                ++inKev;
            }
        }
        if (inEv == 0 && inKev > 0)
        {
            proto->energyLower_eV = k9120EnergyLower_eV / 1000.0f;
            proto->energyUpper_eV = k9120EnergyUpper_eV / 1000.0f;
        }
        return true;
    }

    TimeAlignerConfig makeCfg(const std::string &outDir, const openpni::CoincidenceProtocol &proto)
    {
        TimeAlignerConfig cfg = createBDM50100_9120AlignerConfig(outDir, proto);
        cfg.processingIntervalMs = 0;
        cfg.minSegmentSingles = 0;
        cfg.minSegmentOverlapFactor = 1;
        cfg.maxProcessLatencyMs = 0;
        return cfg;
    }

    bool pushAll(StreamingTimeAligner *aligner,
                 const std::array<std::vector<TimestampedSingleChunk>, 2> &nodeChunks)
    {
        for (size_t node = 0; node < nodeChunks.size(); ++node)
        {
            auto *buf = aligner->getNodeBuffer(static_cast<uint16_t>(node));
            for (const auto &chunk : nodeChunks[node])
            {
                TimestampedSingleChunk copy = chunk;
                if (!buf->push(std::move(copy), 60'000))
                {
                    return false;
                }
            }
        }
        return true;
    }

    bool waitWatermark(StreamingTimeAligner *aligner, uint64_t W, int timeoutMs)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (aligner->publishedWatermark() >= W)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return false;
    }

    struct MergedLmfHit
    {
        uint32_t ts100us = 0;
        uint64_t epochId = 0;
        uint64_t index = 0;
    };

    void collectLmfHits(const std::string &dir, const std::string &stemPrefix,
                        std::vector<MergedLmfHit> *out)
    {
        if (!out || !fs::exists(dir))
        {
            return;
        }
        std::vector<fs::path> files;
        for (const auto &entry : fs::directory_iterator(dir))
        {
            const auto name = entry.path().filename().string();
            if (name.rfind(stemPrefix, 0) != 0 || entry.path().extension() != ".lmf")
            {
                continue;
            }
            files.push_back(entry.path());
        }
        std::sort(files.begin(), files.end());
        for (const auto &path : files)
        {
            openpni::io::listmode::ListmodeFileInput input;
            input.Open(path.string());
            for (uint32_t s = 0; s < input.SegmentNum(); ++s)
            {
                auto seg = input.ReadSegment(s);
                const auto data = seg.GetHAnyData();
                const uint64_t epochId = seg.GetClockMs();
                for (std::size_t i = 0; i < data.count; ++i)
                {
                    MergedLmfHit hit;
                    hit.epochId = epochId;
                    hit.index = out->size();
                    if (data.coincidence_timestamp_100us)
                    {
                        hit.ts100us = data.coincidence_timestamp_100us[i];
                    }
                    out->push_back(hit);
                }
            }
        }
    }

    uint64_t mergeLmfByTime(const std::string &dirA, const std::string &dirB,
                            const std::string &stemPrefix)
    {
        std::vector<MergedLmfHit> hits;
        collectLmfHits(dirA, stemPrefix, &hits);
        collectLmfHits(dirB, stemPrefix, &hits);
        std::sort(hits.begin(), hits.end(),
                  [](const MergedLmfHit &a, const MergedLmfHit &b)
                  {
                      if (a.ts100us != b.ts100us)
                      {
                          return a.ts100us < b.ts100us;
                      }
                      if (a.epochId != b.epochId)
                      {
                          return a.epochId < b.epochId;
                      }
                      return a.index < b.index;
                  });
        return hits.size();
    }

    uint64_t pickCutWatermark(const std::array<std::vector<TimestampedSingleChunk>, 2> &nodeChunks,
                              const TimeAlignerConfig &cfg)
    {
        uint64_t tMin = UINT64_MAX;
        uint64_t nodeMax[2] = {0, 0};
        for (size_t node = 0; node < 2; ++node)
        {
            for (const auto &c : nodeChunks[node])
            {
                tMin = std::min(tMin, c.minTime_pico);
                nodeMax[node] = std::max(nodeMax[node], c.maxTime_pico);
            }
        }
        const uint64_t wm = std::min(nodeMax[0], nodeMax[1]) > cfg.getTotalSafetyMargin()
                                ? std::min(nodeMax[0], nodeMax[1]) - cfg.getTotalSafetyMargin()
                                : 0;
        if (wm <= tMin || tMin == UINT64_MAX)
        {
            return 0;
        }
        uint64_t W = tMin + (wm - tMin) / 2;
        const uint64_t span = cfg.minSegmentSpan_100fs();
        if (W < tMin + span)
        {
            W = tMin + span;
        }
        if (W >= wm)
        {
            W = tMin + (wm - tMin) / 2;
        }
        return W;
    }
} // namespace

int main()
{
    std::array<std::vector<TimestampedSingleChunk>, 2> nodeChunks;
    openpni::CoincidenceProtocol proto;
    if (!loadNodeChunks(&nodeChunks, &proto))
    {
        return 0;
    }

    std::vector<Single> all;
    for (const auto &chunks : nodeChunks)
    {
        for (const auto &c : chunks)
        {
            all.insert(all.end(), c.singles.begin(), c.singles.end());
        }
    }
    std::sort(all.begin(), all.end(),
              [](const Single &a, const Single &b)
              { return a.timevalue_100fs < b.timevalue_100fs; });

    openpni::Coincidence coin;
    coin.setTotalCrystalNumOfEachChannel(
        std::vector<uint32_t>(k9120ChannelNum, k9120CrystalsPerChannel));
    openpni::tools::UniPtr<Single> devAll{"time_shard_gold"};
    devAll.CopyFromHost(std::span<const Single>(all));
    const auto gold = coin.getDListmode(std::vector<std::span<Single const>>{devAll.CudaRStdSpan()},
                                        proto, 0);
    const uint64_t goldPrompt = gold.prompt.size();
    const uint64_t goldDelay = gold.delay.size();
    std::cout << "gold prompt=" << goldPrompt << " delay=" << goldDelay << "\n";

    const std::string dirA = "/tmp/r2c_time_shard_A";
    const std::string dirB = "/tmp/r2c_time_shard_B";
    const std::string dirOne = "/tmp/r2c_time_shard_one";
    fs::remove_all(dirA);
    fs::remove_all(dirB);
    fs::remove_all(dirOne);

    auto cfgA = makeCfg(dirA, proto);
    const uint64_t W = pickCutWatermark(nodeChunks, cfgA);
    if (W == 0)
    {
        std::cerr << "could not pick cut watermark\n";
        return 1;
    }
    std::cout << "cut W=" << W << "\n";

    StreamingTimeAligner one(makeCfg(dirOne, proto), 2);
    one.start();
    if (!pushAll(&one, nodeChunks))
    {
        std::cerr << "push one failed\n";
        return 1;
    }
    one.stop(true);
    if (one.hadError())
    {
        std::cerr << "single-aligner stop path failed\n";
        return 1;
    }
    const auto &stOne = one.getStatistics();
    const uint64_t oneDelay = stOne.totalDelayPairs.load();
    const uint64_t onePrompt = stOne.totalPromptPairs.load();
    const uint64_t oneCarry = stOne.carrySinglesTotal.load();
    std::cout << "one-aligner prompt=" << onePrompt << " delay=" << oneDelay
              << " carry=" << oneCarry << "\n";
    if (oneDelay != goldDelay)
    {
        std::cerr << "single-aligner delay " << oneDelay << " != gold " << goldDelay << "\n";
        return 1;
    }

    cfgA.epochId = 1;
    cfgA.coinId = 0;
    cfgA.epochT0_100fs = 0;
    cfgA.epochT1_100fs = W;
    cfgA.epochCut_100fs = W;
    StreamingTimeAligner alignerA(cfgA, 2);
    alignerA.start();
    if (!pushAll(&alignerA, nodeChunks))
    {
        std::cerr << "push A failed\n";
        return 1;
    }
    if (!waitWatermark(&alignerA, W, 60'000))
    {
        std::cerr << "watermark did not reach W (got " << alignerA.publishedWatermark() << ")\n";
        alignerA.stop(true);
        return 1;
    }
    if (!alignerA.completeEpoch(W, /*epochId=*/1))
    {
        std::cerr << "completeEpoch failed\n";
        alignerA.stop(true);
        return 1;
    }
    EpochHandoff handoff = alignerA.takeHandoff();
    alignerA.stop(true);
    if (alignerA.hadError())
    {
        std::cerr << "aligner A error\n";
        return 1;
    }
    if (W > 0 && handoff.carry.empty() && handoff.cutWatermark_100fs == 0)
    {
        std::cerr << "empty handoff\n";
        return 1;
    }
    std::cout << "handoff carry=" << handoff.carry.size() << " tailChunks=" << handoff.tail.size()
              << "\n";

    auto cfgB = makeCfg(dirB, proto);
    cfgB.coinId = 1;
    cfgB.epochId = 2;
    cfgB.epochT0_100fs = W;
    StreamingTimeAligner alignerB(cfgB, 2);
    if (!alignerB.applyHandoff(std::move(handoff)))
    {
        std::cerr << "applyHandoff failed\n";
        return 1;
    }
    if (alignerB.lastExtractedWatermark() != W)
    {
        std::cerr << "B lastWatermark " << alignerB.lastExtractedWatermark() << " != W\n";
        return 1;
    }
    alignerB.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    alignerB.stop(true);
    if (alignerB.hadError())
    {
        std::cerr << "aligner B error\n";
        return 1;
    }

    const auto &stA = alignerA.getStatistics();
    const auto &stB = alignerB.getStatistics();
    const uint64_t splitDelay = stA.totalDelayPairs.load() + stB.totalDelayPairs.load();
    const uint64_t splitPrompt = stA.totalPromptPairs.load() + stB.totalPromptPairs.load();
    const uint64_t splitCarry = stA.carrySinglesTotal.load() + stB.carrySinglesTotal.load();
    std::cout << "split prompt=" << splitPrompt << " delay=" << splitDelay
              << " carry=" << splitCarry << "\n";

    if (splitDelay != goldDelay)
    {
        std::cerr << "split delay " << splitDelay << " != gold " << goldDelay << "\n";
        return 1;
    }
    if (splitPrompt > goldPrompt + splitCarry)
    {
        std::cerr << "split prompt " << splitPrompt << " exceeds gold+carry " << goldPrompt << "+"
                  << splitCarry << "\n";
        return 1;
    }

    const uint64_t lmfPrompt = mergeLmfByTime(dirA, dirB, "prompt");
    const uint64_t lmfDelay = mergeLmfByTime(dirA, dirB, "delay");
    if (lmfDelay != splitDelay)
    {
        std::cerr << "merged LMF delay count " << lmfDelay << " != stats " << splitDelay << "\n";
        return 1;
    }
    if (lmfPrompt != splitPrompt)
    {
        std::cerr << "merged LMF prompt count " << lmfPrompt << " != stats " << splitPrompt << "\n";
        return 1;
    }

    bool sawEpochName = false;
    if (fs::exists(dirA))
    {
        for (const auto &entry : fs::directory_iterator(dirA))
        {
            const auto name = entry.path().filename().string();
            if (name.find("epoch") != std::string::npos)
            {
                sawEpochName = true;
                break;
            }
        }
    }
    if (!sawEpochName)
    {
        std::cerr << "expected epoch metadata in A listmode names under " << dirA << "\n";
        return 1;
    }

    std::cout << "PASS: time-shard dual aligner (delay exact, prompt within carry contract)\n";
    return 0;
}
