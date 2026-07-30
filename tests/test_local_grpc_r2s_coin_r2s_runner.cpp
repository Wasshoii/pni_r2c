/**
 * @file test_local_grpc_r2s_coin_r2s_runner.cpp
 * @brief Implements 9120 R2S→gRPC node run without including streaming/coin headers.
 */

#include "tests/test_local_grpc_r2s_coin_r2s_runner.hpp"

#include <any>
#include <valarray>
#include <type_traits>
#include <utility>
#include <iostream>

#include <pni/PnI-Config.hpp>

#include "core/r2s/R2S.hpp"
#include "grpcNode/r2sNode.hpp"

namespace r2s = openpni::distributed::r2s;
namespace grpcnode = openpni::distributed::grpcnode;

namespace r2s_coin_test
{

R2SRunnerStats run9120R2SGrpcNode(
    const R2SRunnerOptions &opts,
    const R2SRunnerNodeInput &node)
{
    auto config = r2s::createBDM50100_9120Config(
        node.rawdataPath,
        opts.resultDir,
        {opts.calibrationDir, opts.calibrationDir},
        "node_" + std::to_string(node.nodeId),
        node.channels,
        4);

    config.sortDataByTime = true;
    config.saveData2SingleFile = false;
    config.asyncFileWrite = false;
    config.useEnergyCut = true;
    config.energyCutLow = opts.energyCutLow;
    config.energyCutHigh = opts.energyCutHigh;

    grpcnode::R2SGrpcNode nodeRunner(
        config,
        opts.address,
        node.nodeId,
        static_cast<uint32_t>(node.channels.size()),
        opts.maxPendingSegments,
        "127.0.0.1",
        "BDM50100",
        50,
        true,
        0,
        opts.waitForStartTimeoutMs,
        1000,
        opts.batchSegmentsPerMessage);

    std::cout << "[Node " << node.nodeId << "] R2S start, dir=" << node.rawdataPath;
    if (!node.channels.empty())
    {
        std::cout << " channels=[" << node.channels.front() << ".." << node.channels.back() << "]";
    }
    std::cout << std::endl;

    nodeRunner.run();
    const auto &s = nodeRunner.stats();

    std::cout << "[Node " << node.nodeId << "] R2S done, success="
              << (s.success ? "true" : "false")
              << " callbacks=" << s.callbackCount
              << " grpcMessages=" << s.grpcMessagesSent
              << " singlesSent=" << s.singlesSent << std::endl;

    R2SRunnerStats out;
    out.success = s.success;
    out.callbackCount = s.callbackCount;
    out.singlesSent = s.singlesSent;
    out.grpcMessagesSent = s.grpcMessagesSent;
    return out;
}

} // namespace r2s_coin_test
