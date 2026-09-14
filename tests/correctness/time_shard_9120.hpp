#pragma once

/**
 * Shared 9120 load / cut-watermark / LMF-merge helpers for time-shard tests.
 * Used by test_coin_time_shard (memcpy gold) and test_coin_time_shard_ship
 * (production ship QP + GPU).
 */

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
#include <limits>
#include <string>
#include <thread>
#include <vector>

namespace r2c_time_shard_9120
{
    using openpni::distributed::streaming::EpochHandoff;
    using openpni::distributed::streaming::Single;
    using openpni::distributed::streaming::StreamingTimeAligner;
    using openpni::distributed::streaming::TimeAlignerConfig;
    using openpni::distributed::streaming::TimestampedSingleChunk;
    using openpni::distributed::streaming::createBDM50100_9120AlignerConfig;
    using r2c_test_9120::k9120DataRoot;
    using r2c_test_9120::k9120DelayTimePs;
    using r2c_test_9120::k9120EnergyLower_eV;
    using r2c_test_9120::k9120EnergyUpper_eV;
    using r2c_test_9120::k9120TimeWindowPs;
    namespace fs = std::filesystem;

    inline std::vector<std::string> listLsingle(const std::string &dir)
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

    inline std::vector<Single> readSingles(openpni::io::listmode::ListmodeFileSegment &segment)
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

    inline bool loadNodeChunks(std::array<std::vector<TimestampedSingleChunk>, 2> *out,
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

    inline TimeAlignerConfig makeCfg(const std::string &outDir, const openpni::CoincidenceProtocol &proto)
    {
        TimeAlignerConfig cfg = createBDM50100_9120AlignerConfig(outDir, proto);
        cfg.processingIntervalMs = 0;
        cfg.minSegmentSingles = 0;
        cfg.minSegmentOverlapFactor = 1;
        cfg.maxProcessLatencyMs = 0;
        return cfg;
    }

    inline bool pushAll(StreamingTimeAligner *aligner,
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

    inline bool waitWatermark(StreamingTimeAligner *aligner, uint64_t W, int timeoutMs)
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

    inline void collectLmfHits(const std::string &dir, const std::string &stemPrefix,
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

    inline uint64_t mergeLmfByTime(const std::string &dirA, const std::string &dirB,
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

    inline uint64_t pickCutWatermark(const std::array<std::vector<TimestampedSingleChunk>, 2> &nodeChunks,
                                     const TimeAlignerConfig &cfg)
    {
        uint64_t tMin = std::numeric_limits<uint64_t>::max();
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
        if (wm <= tMin || tMin == std::numeric_limits<uint64_t>::max())
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

    inline std::vector<Single> flattenSingles(
        const std::array<std::vector<TimestampedSingleChunk>, 2> &nodeChunks)
    {
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
        return all;
    }
} // namespace r2c_time_shard_9120
