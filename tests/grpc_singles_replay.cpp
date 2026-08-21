/**
 * @file grpc_singles_replay.cpp
 * @brief .lsingle → RDMA packed replay for L2/L3 tests (gRPC control plane only).
 */

#include "tests/grpc_singles_replay.hpp"
#include "core/streaming/PackedSingle.hpp"
#include "grpcService/CoincidenceClient.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <pni/io/IO.hpp>
#include <pni/io/ListmodeIO.hpp>

namespace fs = std::filesystem;
namespace coincidence = openpni::distributed::coincidence;
using Single = openpni::Single;
using openpni::distributed::streaming::CoincidenceClient;
using openpni::distributed::streaming::CoincidenceClientConfig;

namespace grpc_singles_replay
{

namespace
{
    struct ChunkSpec
    {
        size_t offset = 0;
        size_t count = 0;
        uint64_t chunkId = 0;
    };

    std::optional<int> parseSinglesPartNumber(const std::string &path)
    {
        const std::string name = fs::path(path).stem().string();
        const auto pos = name.rfind("_part");
        if (pos == std::string::npos)
            return std::nullopt;
        try { return std::stoi(name.substr(pos + 5)); }
        catch (...) { return std::nullopt; }
    }

    std::vector<Single> readSinglesFromSegment(openpni::io::listmode::ListmodeFileSegment &segment)
    {
        const auto data = segment.GetHAnyData();
        if (!data.local_crystal_index1 || !data.channel_index1 || !data.absolute_timestamp1_100fs)
            return {};

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

    void throttleForRate(uint64_t singlesInBatch, uint64_t singlesPerSec,
                         double jitterFraction, std::mt19937 &rng)
    {
        if (singlesPerSec == 0 || singlesInBatch == 0)
            return;
        double effectiveRate = static_cast<double>(singlesPerSec);
        if (jitterFraction > 0.0)
        {
            std::uniform_real_distribution<double> dist(
                std::max(0.05, 1.0 - jitterFraction), 1.0 + jitterFraction);
            effectiveRate *= dist(rng);
        }
        const double sleepSec = static_cast<double>(singlesInBatch) / effectiveRate;
        if (sleepSec > 0.0)
            std::this_thread::sleep_for(std::chrono::duration<double>(sleepSec));
    }

    bool sendViaClient(
        CoincidenceClient &client,
        const ReplayOptions &opts,
        std::span<const Single> singles,
        const ChunkSpec &spec,
        std::mt19937 &rng,
        uint64_t &chunksSent,
        uint64_t &singlesSent,
        bool borrow)
    {
        const uint64_t clockMs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
        const std::span<const Single> view = singles.subspan(spec.offset, spec.count);
        const bool ok = borrow
                            ? client.sendSinglesView(view, clockMs, 0)
                            : client.sendSingles(view, clockMs, 0);
        if (!ok)
        {
            return false;
        }
        singlesSent += spec.count;
        chunksSent += 1;
        throttleForRate(spec.count, opts.singlesPerSec, opts.rateJitterFraction, rng);
        return true;
    }

    CoincidenceClientConfig clientConfigFromOpts(const ReplayOptions &opts)
    {
        CoincidenceClientConfig cfg;
        cfg.serverAddress = opts.serverAddress;
        cfg.nodeId = opts.nodeId;
        cfg.nodeAddress = opts.nodeAddress.empty() ? "127.0.0.1" : opts.nodeAddress;
        cfg.channelCount = opts.channelCount;
        cfg.detectorType = opts.detectorType;
        cfg.waitForStartSignal = opts.waitForStartSignal;
        cfg.waitForStartTimeoutMs = opts.waitForStartTimeoutMs;
        cfg.requireRoce = opts.requireRoce;
        cfg.forceInProcess = opts.forceInProcess;
        cfg.rdmaDeviceName = opts.rdmaDeviceName;
        cfg.gidIndex = opts.gidIndex;
        return cfg;
    }

    std::vector<ChunkSpec> buildChunkPlan(
        size_t totalSingles,
        size_t chunkSize,
        uint32_t sendRounds,
        uint64_t &nextChunkId)
    {
        std::vector<ChunkSpec> plan;
        chunkSize = std::max<size_t>(1, chunkSize);

        for (uint32_t round = 0; round < sendRounds; ++round)
        {
            for (size_t off = 0; off < totalSingles; off += chunkSize)
            {
                const size_t count = std::min(chunkSize, totalSingles - off);
                plan.push_back(ChunkSpec{off, count, nextChunkId++});
            }
        }
        return plan;
    }

    bool sendPreloaded(
        CoincidenceClient &client,
        const ReplayOptions &opts,
        const std::vector<Single> &preloadedSingles,
        uint64_t &outSinglesSent,
        uint64_t &outChunksSent)
    {
        const uint32_t roundCount = std::max<uint32_t>(1, opts.sendRounds);
        uint64_t nextChunkId = 0;
        const auto plan = buildChunkPlan(
            preloadedSingles.size(), opts.pushChunkSingles, roundCount, nextChunkId);

        std::mt19937 rng(opts.nodeId * 7919U + 42U);
        uint64_t chunksSent = 0;
        uint64_t singlesSent = 0;

        for (const auto &spec : plan)
        {
            if (!sendViaClient(client, opts, preloadedSingles, spec, rng, chunksSent, singlesSent,
                               /*borrow=*/true))
            {
                std::cerr << "[Replay] Node " << opts.nodeId
                          << " RDMA write failed at chunk " << spec.chunkId << std::endl;
                return false;
            }
        }

        outSinglesSent = singlesSent;
        outChunksSent = chunksSent;
        return true;
    }

    bool sendStreaming(
        CoincidenceClient &client,
        const ReplayOptions &opts,
        const std::vector<std::string> &files,
        uint64_t &outSinglesSent,
        uint64_t &outChunksSent)
    {
        std::mt19937 rng(opts.nodeId * 7919U + 42U);
        uint64_t chunkId = 0;
        uint64_t singlesSent = 0;
        uint64_t chunksSent = 0;
        const uint32_t roundCount = std::max<uint32_t>(1, opts.sendRounds);

        for (uint32_t round = 0; round < roundCount; ++round)
        {
            for (const auto &filePath : files)
            {
                openpni::io::listmode::ListmodeFileInput input;
                input.Open(filePath);

                for (uint32_t segIdx = 0; segIdx < input.SegmentNum(); ++segIdx)
                {
                    auto segment = input.ReadSegment(segIdx);
                    auto allSingles = readSinglesFromSegment(segment);
                    if (allSingles.empty())
                        continue;

                    const size_t chunkSize = std::max<size_t>(1, opts.pushChunkSingles);
                    for (size_t off = 0; off < allSingles.size(); off += chunkSize)
                    {
                        const size_t end = std::min(off + chunkSize, allSingles.size());
                        const ChunkSpec spec{off, end - off, chunkId++};
                        if (!sendViaClient(client, opts, allSingles, spec, rng, chunksSent, singlesSent,
                                           /*borrow=*/false))
                            return false;
                    }
                }
            }
        }

        outSinglesSent = singlesSent;
        outChunksSent = chunksSent;
        return true;
    }

    bool preloadSingles(
        const std::vector<std::string> &files,
        const ReplayOptions &opts,
        std::vector<Single> &outSingles)
    {
        const size_t memCap = opts.maxMemoryBytes > 0
                                  ? opts.maxMemoryBytes
                                  : static_cast<size_t>(30ULL * 1024 * 1024 * 1024);
        const size_t maxSingles = memCap / sizeof(Single);

        std::cout << "[Replay] Node " << opts.nodeId << " preloading (cap "
                  << memCap / (1024 * 1024) << " MiB)..." << std::endl;
        const auto tLoad0 = std::chrono::steady_clock::now();

        for (const auto &filePath : files)
        {
            if (outSingles.size() >= maxSingles)
                break;

            openpni::io::listmode::ListmodeFileInput input;
            input.Open(filePath);

            for (uint32_t segIdx = 0; segIdx < input.SegmentNum(); ++segIdx)
            {
                if (outSingles.size() >= maxSingles)
                    break;

                auto segment = input.ReadSegment(segIdx);
                auto segSingles = readSinglesFromSegment(segment);
                if (segSingles.empty())
                    continue;

                const size_t room = maxSingles - outSingles.size();
                const size_t take = std::min(room, segSingles.size());
                outSingles.insert(outSingles.end(),
                                  segSingles.begin(),
                                  segSingles.begin() + static_cast<std::ptrdiff_t>(take));
            }
        }

        const auto tLoad1 = std::chrono::steady_clock::now();
        const auto loadMs = std::chrono::duration_cast<std::chrono::milliseconds>(tLoad1 - tLoad0).count();
        const double loadGB = static_cast<double>(outSingles.size()) * sizeof(Single) / (1024.0 * 1024.0 * 1024.0);
        std::cout << "[Replay] Node " << opts.nodeId << " preloaded "
                  << outSingles.size() << " singles (" << loadGB << " GiB) in "
                  << loadMs << " ms" << std::endl;
        return !outSingles.empty();
    }

} // anonymous namespace

std::vector<std::string> collectSinglesFiles(const std::string &dirOrFile)
{
    std::vector<std::string> files;
    if (!fs::exists(dirOrFile))
        return files;
    if (fs::is_regular_file(dirOrFile))
    {
        files.push_back(dirOrFile);
        return files;
    }
    if (!fs::is_directory(dirOrFile))
        return files;

    for (const auto &entry : fs::directory_iterator(dirOrFile))
    {
        if (entry.is_regular_file() && entry.path().extension() == ".lsingle")
            files.push_back(entry.path().string());
    }

    std::sort(files.begin(), files.end(),
              [](const std::string &a, const std::string &b)
              {
                  const auto partA = parseSinglesPartNumber(a);
                  const auto partB = parseSinglesPartNumber(b);
                  if (partA && partB && partA != partB)
                      return partA.value() < partB.value();
                  return fs::path(a).filename().string() < fs::path(b).filename().string();
              });
    return files;
}

uint64_t countSinglesInFiles(const std::vector<std::string> &filePaths)
{
    uint64_t total = 0;
    for (const auto &path : filePaths)
    {
        openpni::io::listmode::ListmodeFileInput input;
        input.Open(path);
        for (uint32_t segIdx = 0; segIdx < input.SegmentNum(); ++segIdx)
        {
            auto segment = input.ReadSegment(segIdx);
            const auto data = segment.GetHAnyData();
            total += data.count;
        }
    }
    return total;
}

ReplayStats runNodeReplay(const std::vector<std::string> &filePaths, const ReplayOptions &opts)
{
    ReplayStats stats;
    stats.sendRounds = std::max<uint32_t>(1, opts.sendRounds);

    if (filePaths.empty())
    {
        std::cerr << "[Replay] Node " << opts.nodeId << " no input files" << std::endl;
        return stats;
    }

    std::vector<std::string> files = filePaths;
    if (opts.maxFiles > 0 && files.size() > opts.maxFiles)
        files.resize(opts.maxFiles);

    CoincidenceClient client(clientConfigFromOpts(opts));
    if (!client.start())
    {
        std::cerr << "[Replay] Node " << opts.nodeId << " CoincidenceClient start failed" << std::endl;
        return stats;
    }

    std::vector<Single> preloadedSingles;
    if (opts.preload)
    {
        if (!preloadSingles(files, opts, preloadedSingles))
        {
            std::cerr << "[Replay] Node " << opts.nodeId << " preload produced no singles" << std::endl;
            client.stop();
            return stats;
        }
    }

    const auto t0 = std::chrono::steady_clock::now();
    bool ok = false;

    if (opts.preload)
    {
        ok = sendPreloaded(
            client, opts, preloadedSingles, stats.singlesSent, stats.chunksSent);
    }
    else
    {
        ok = sendStreaming(
            client, opts, files, stats.singlesSent, stats.chunksSent);
    }

    if (ok)
    {
        ok = client.waitUntilIdle();
    }

    const auto t1 = std::chrono::steady_clock::now();
    stats.elapsedMs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());

    if (ok)
    {
        (void)client.notifyProducerComplete();
    }
    client.stop();
    stats.success = ok;
    return stats;
}

} // namespace grpc_singles_replay
