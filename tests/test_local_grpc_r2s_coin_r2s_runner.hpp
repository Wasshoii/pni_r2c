#pragma once

/**
 * @file test_local_grpc_r2s_coin_r2s_runner.hpp
 * @brief R2S gRPC node runner for the E2E test, isolated from coin headers
 *        (pni/node/Coincidence.hpp vs pni/node/misc/Coincidence.hpp conflict).
 */

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace r2s_coin_test
{

struct R2SRunnerOptions
{
    std::string address;
    std::string calibrationDir;
    std::string resultDir;
    size_t maxPendingSegments = 64;
    uint32_t batchSegmentsPerMessage = 1;
    uint32_t waitForStartTimeoutMs = 30000;
    float energyCutLow = 421000.0f;
    float energyCutHigh = 1000000.0f;
};

struct R2SRunnerNodeInput
{
    uint32_t nodeId = 0;
    std::string rawdataPath;
    std::vector<uint16_t> channels;
};

struct R2SRunnerStats
{
    bool success = false;
    uint64_t callbackCount = 0;
    uint64_t singlesSent = 0;
    uint64_t grpcMessagesSent = 0;
};

R2SRunnerStats run9120R2SGrpcNode(
    const R2SRunnerOptions &opts,
    const R2SRunnerNodeInput &node);

} // namespace r2s_coin_test
