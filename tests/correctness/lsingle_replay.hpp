#pragma once

/**
 * @file lsingle_replay.hpp
 * @brief Load .lsingle files and replay them over the RDMA singles data plane
 *        (gRPC control: Register / WaitForStart / OpenDataPlane).
 */

#include <cstdint>
#include <string>
#include <vector>

namespace lsingle_replay
{

struct ReplayOptions
{
    std::string serverAddress = "127.0.0.1:50061";
    uint32_t nodeId = 0;
    std::string nodeAddress = "127.0.0.1";
    uint32_t channelCount = 288;
    std::string detectorType = "BDM50100";

    uint64_t singlesPerSec = 0;        // 0 = unlimited (burst mode)
    size_t pushChunkSingles = 2000000; // Singles per logical RDMA chunk
    double rateJitterFraction = 0.0;

    size_t maxFiles = 0;               // 0 = all files
    size_t maxMemoryBytes = 0;         // 0 = use 30 GiB default cap when preload

    uint32_t sendRounds = 3;           // Repeat full preload send N times (benchmark)

    bool waitForStartSignal = true;
    uint32_t waitForStartTimeoutMs = 60000;
    bool preload = false;

    bool requireRoce = false;
    bool forceInProcess = false;
    std::string rdmaDeviceName;
    int gidIndex = -1;
};

struct ReplayStats
{
    bool success = false;
    uint64_t singlesSent = 0;
    uint64_t chunksSent = 0;
    uint64_t elapsedMs = 0;
    uint32_t sendRounds = 0;
};

std::vector<std::string> collectSinglesFiles(const std::string &dirOrFile);
uint64_t countSinglesInFiles(const std::vector<std::string> &filePaths);
ReplayStats runNodeReplay(const std::vector<std::string> &filePaths, const ReplayOptions &opts);

} // namespace lsingle_replay
