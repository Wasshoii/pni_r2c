/**
 * @file test_r2s_rdma_coin.cpp
 * @brief 9120 dual-node end-to-end: R2S → RDMA singles → StreamingTimeAligner
 *
 * Single-process harness: CoinGrpcNode + two parallel R2SGrpcNode instances.
 * R2S node logic lives in test_r2s_rdma_coin_runner.cpp to avoid
 * conflicting pni Coincidence.hpp includes in one TU.
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <pni/PnI-Config.hpp>
#include <pni/io/IO.hpp>

#include <grpcpp/grpcpp.h>

#include "protos/coincidence.grpc.pb.h"
#include "core/io/IOAdapter.hpp"
#include "core/streaming/StreamingCoincidence.hpp"
#include "grpcNode/coinNode.hpp"
#include "tests/correctness/data_9120_common.hpp"
#include "tests/correctness/test_r2s_rdma_coin_runner.hpp"

namespace fs = std::filesystem;
namespace coincidence = openpni::distributed::coincidence;
namespace grpcnode = openpni::distributed::grpcnode;
namespace streaming = openpni::distributed::streaming;

using r2c_test_9120::NodeInput;
using r2c_test_9120::analyzeLmfFile;
using r2c_test_9120::build9120DualNodeInputs;
using r2c_test_9120::k9120CalibrationDir;
using r2c_test_9120::k9120ChannelNum;
using r2c_test_9120::k9120DataRoot;
using r2c_test_9120::k9120DelayTimePs;
using r2c_test_9120::k9120EnergyLower_eV;
using r2c_test_9120::k9120EnergyUpper_eV;
using r2c_test_9120::k9120TimeWindowPs;
using r2c_test_9120::validate9120PromptChannelSep;

namespace
{
    struct ProgramOptions
    {
        std::string address = "127.0.0.1:50061";
        std::string dataRoot = k9120DataRoot;
        std::string node0RawDir;
        std::string node1RawDir;
        std::string calibrationDir = k9120CalibrationDir;
        std::string resultDir;
        std::string coinOutputDir;
        size_t maxPendingSegments = 64;
        uint32_t startLeadTimeMs = 1000;
        uint32_t waitForStartTimeoutMs = 30000;
        uint64_t networkLatencyMarginPs = 5'000'000; // 5 ms
        uint32_t processingIntervalMs = 100;
        bool disableMultiGpu = false;
        bool skipLmfAnalysis = false;
        bool strictChannelSep = false;
        bool helpOnly = false;

        void resolveDerivedPaths()
        {
            if (node0RawDir.empty())
            {
                node0RawDir = (fs::path(dataRoot) / "pni_raw_node0").string();
            }
            if (node1RawDir.empty())
            {
                node1RawDir = (fs::path(dataRoot) / "pni_raw_node1").string();
            }
            if (resultDir.empty())
            {
                resultDir = (fs::path(dataRoot) / "pni_singles_grpc_coin").string();
            }
            if (coinOutputDir.empty())
            {
                coinOutputDir = (fs::path(dataRoot) / "coin_9120_grpc_stream").string();
            }
        }
    };

    void printUsage(const char *argv0)
    {
        std::cerr
            << "Usage: " << argv0 << " [options]\n"
            << "  9120 dual-node R2S → RDMA → streaming coincidence (single process)\n\n"
            << "Options:\n"
            << "  --address <host:port>           Coin gRPC listen address (default 127.0.0.1:50061)\n"
            << "  --data-root <dir>               9120 data root\n"
            << "  --node0-raw-dir <dir>           Override node0 merged raw dir\n"
            << "  --node1-raw-dir <dir>           Override node1 merged raw dir\n"
            << "  --calibration-dir <dir>         Calibration directory\n"
            << "  --result-dir <dir>              R2S side output dir\n"
            << "  --coin-output-dir <dir>         Coincidence LMF output dir\n"
            << "  --max-pending-segments <n>     R2S gRPC send queue depth (default 64)\n"
            << "  --network-latency-margin-ps <n> Aligner network margin in ps (default 5e6)\n"
            << "  --processing-interval-ms <n>   Aligner poll interval (default 100)\n"
            << "  --disable-multi-gpu            Disable multi-GPU coincidence engine\n"
            << "  --skip-lmf-analysis            Skip prompt/delay LMF content checks\n"
            << "  --strict-channel-sep           Fail if channelSep peak not at 144/288\n"
            << "  --help                         Show this help\n";
    }

    bool parseArgs(int argc, char **argv, ProgramOptions &opts)
    {
        for (int i = 1; i < argc; ++i)
        {
            const std::string arg = argv[i];
            auto needValue = [&](const char *name) -> std::string
            {
                if (i + 1 >= argc)
                {
                    throw std::runtime_error(std::string("Missing value for ") + name);
                }
                return argv[++i];
            };

            if (arg == "--help" || arg == "-h")
            {
                opts.helpOnly = true;
                printUsage(argv[0]);
                return true;
            }
            if (arg == "--address")
            {
                opts.address = needValue("--address");
                continue;
            }
            if (arg == "--data-root")
            {
                opts.dataRoot = needValue("--data-root");
                continue;
            }
            if (arg == "--node0-raw-dir")
            {
                opts.node0RawDir = needValue("--node0-raw-dir");
                continue;
            }
            if (arg == "--node1-raw-dir")
            {
                opts.node1RawDir = needValue("--node1-raw-dir");
                continue;
            }
            if (arg == "--calibration-dir")
            {
                opts.calibrationDir = needValue("--calibration-dir");
                continue;
            }
            if (arg == "--result-dir")
            {
                opts.resultDir = needValue("--result-dir");
                continue;
            }
            if (arg == "--coin-output-dir")
            {
                opts.coinOutputDir = needValue("--coin-output-dir");
                continue;
            }
            if (arg == "--max-pending-segments")
            {
                opts.maxPendingSegments =
                    static_cast<size_t>(std::stoul(needValue("--max-pending-segments")));
                continue;
            }
            if (arg == "--network-latency-margin-ps")
            {
                opts.networkLatencyMarginPs = std::stoull(needValue("--network-latency-margin-ps"));
                continue;
            }
            if (arg == "--processing-interval-ms")
            {
                opts.processingIntervalMs =
                    static_cast<uint32_t>(std::stoul(needValue("--processing-interval-ms")));
                continue;
            }
            if (arg == "--disable-multi-gpu")
            {
                opts.disableMultiGpu = true;
                continue;
            }
            if (arg == "--skip-lmf-analysis")
            {
                opts.skipLmfAnalysis = true;
                continue;
            }
            if (arg == "--strict-channel-sep")
            {
                opts.strictChannelSep = true;
                continue;
            }

            std::cerr << "Unknown argument: " << arg << std::endl;
            printUsage(argv[0]);
            return false;
        }

        opts.resolveDerivedPaths();
        return true;
    }

    std::string findFirstRawFile(const std::string &rawDir)
    {
        std::vector<std::string> matches;
        for (const auto &entry : fs::directory_iterator(rawDir))
        {
            if (!entry.is_regular_file())
            {
                continue;
            }
            const std::string name = entry.path().filename().string();
            if (name.rfind("pniRaw-", 0) == 0 && name.size() >= 4 &&
                name.compare(name.size() - 4, 4, ".bin") == 0)
            {
                matches.push_back(entry.path().string());
            }
        }
        std::sort(matches.begin(), matches.end());
        return matches.empty() ? std::string() : matches.front();
    }

    bool validateRawdataHeaders(const std::vector<NodeInput> &nodes)
    {
        bool ok = true;
        for (const auto &n : nodes)
        {
            try
            {
                const std::string samplePath = findFirstRawFile(n.rawdataPath);
                if (samplePath.empty())
                {
                    std::cerr << "[Input] No pniRaw-*.bin in directory: " << n.rawdataPath << std::endl;
                    ok = false;
                    continue;
                }

                openpni::distributed::coreio::RawDataFileReader reader;
                reader.Open(samplePath);
                const auto &info = reader.Info();
                if (info.channelNum != k9120ChannelNum)
                {
                    std::cerr << "[Input] Rawdata channelNum mismatch: " << samplePath
                              << " channelNum=" << info.channelNum
                              << " expected=" << k9120ChannelNum << std::endl;
                    ok = false;
                }
                if (info.segmentNum == 0)
                {
                    std::cerr << "[Input] Rawdata has no segments: " << samplePath << std::endl;
                    ok = false;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "[Input] Failed to read rawdata header for node " << n.nodeId
                          << " dir=" << n.rawdataPath << " error=" << e.what() << std::endl;
                ok = false;
            }
        }
        return ok;
    }

    bool validateInputs(
        const std::vector<NodeInput> &nodes,
        const std::string &calibrationDir,
        const std::string &resultDir,
        const std::string &coinOutputDir)
    {
        bool ok = true;

        for (const auto &n : nodes)
        {
            if (!fs::exists(n.rawdataPath) || !fs::is_directory(n.rawdataPath))
            {
                std::cerr << "[Input] Missing merged rawdata directory: " << n.rawdataPath << std::endl;
                ok = false;
            }
        }

        if (!fs::exists(calibrationDir) || !fs::is_directory(calibrationDir))
        {
            std::cerr << "[Input] Missing calibration directory: " << calibrationDir << std::endl;
            ok = false;
        }

        std::error_code ec;
        fs::create_directories(resultDir, ec);
        if (ec)
        {
            std::cerr << "[Input] Failed to create result directory: " << resultDir
                      << " error=" << ec.message() << std::endl;
            ok = false;
        }
        fs::create_directories(coinOutputDir, ec);
        if (ec)
        {
            std::cerr << "[Input] Failed to create coin output directory: " << coinOutputDir
                      << " error=" << ec.message() << std::endl;
            ok = false;
        }

        if (!validateRawdataHeaders(nodes))
        {
            ok = false;
        }

        return ok;
    }

    void printServerStatus(const std::string &address)
    {
        auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        auto stub = coincidence::CoincidenceService::NewStub(channel);

        grpc::ClientContext context;
        coincidence::StatusRequest request;
        request.set_include_node_stats(true);
        coincidence::StatusResponse status;

        grpc::Status rpcStatus = stub->GetStatus(&context, request, &status);
        if (!rpcStatus.ok())
        {
            std::cerr << "[Status] failed to query coin status: "
                      << rpcStatus.error_message() << std::endl;
            return;
        }

        std::cout << "\n========== Coin Status ==========" << std::endl;
        std::cout << "Total singles received: " << status.total_singles_received() << std::endl;
        std::cout << "Total singles processed: " << status.total_singles_processed() << std::endl;
        std::cout << "Prompt pairs: " << status.total_prompt_pairs() << std::endl;
        std::cout << "Delay pairs: " << status.total_delay_pairs() << std::endl;
        std::cout << "Connected nodes: " << status.connected_node_count()
                  << "/" << status.expected_node_count() << std::endl;
        std::cout << "Start signal issued: " << (status.start_signal_issued() ? "true" : "false")
                  << std::endl;

        for (const auto &node : status.node_stats())
        {
            std::cout << "  Node " << node.node_id()
                      << " chunks=" << node.chunks_received()
                      << " singles=" << node.singles_received()
                      << " connected=" << (node.connected() ? "true" : "false")
                      << std::endl;
        }
        std::cout << "=================================\n" << std::endl;
    }

} // namespace

int main(int argc, char **argv)
{
    ProgramOptions opts;
    try
    {
        if (!parseArgs(argc, argv, opts))
        {
            return 1;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Argument error: " << e.what() << std::endl;
        return 1;
    }

    if (opts.helpOnly)
    {
        return 0;
    }

    std::cout << "====================================================" << std::endl;
    std::cout << "  Local gRPC R2S → Coincidence (9120 dual-node)" << std::endl;
    std::cout << "====================================================" << std::endl;
    std::cout << "address           : " << opts.address << std::endl;
    std::cout << "dataRoot          : " << opts.dataRoot << std::endl;
    std::cout << "node0RawDir       : " << opts.node0RawDir << std::endl;
    std::cout << "node1RawDir       : " << opts.node1RawDir << std::endl;
    std::cout << "calibrationDir    : " << opts.calibrationDir << std::endl;
    std::cout << "resultDir         : " << opts.resultDir << std::endl;
    std::cout << "coinOutputDir     : " << opts.coinOutputDir << std::endl;
    std::cout << "maxPendingSegments: " << opts.maxPendingSegments << std::endl;
    std::cout << "disableMultiGpu   : " << (opts.disableMultiGpu ? "true" : "false") << std::endl;
    std::cout << "skipLmfAnalysis   : " << (opts.skipLmfAnalysis ? "true" : "false") << std::endl;

    const auto nodeInputs = build9120DualNodeInputs(opts.node0RawDir, opts.node1RawDir);
    if (!validateInputs(nodeInputs, opts.calibrationDir, opts.resultDir, opts.coinOutputDir))
    {
        std::cerr << "Input validation failed." << std::endl;
        return 2;
    }

    openpni::CoincidenceProtocol coinProtocol;
    coinProtocol.timeWindow_ps = k9120TimeWindowPs;
    coinProtocol.delayTime_ps = k9120DelayTimePs;
    coinProtocol.energyLower_eV = k9120EnergyLower_eV;
    coinProtocol.energyUpper_eV = k9120EnergyUpper_eV;

    streaming::TimeAlignerConfig alignerConfig =
        streaming::createBDM50100_9120AlignerConfig(opts.coinOutputDir, coinProtocol);
    alignerConfig.networkLatencyMargin_pico = opts.networkLatencyMarginPs;
    alignerConfig.processingIntervalMs = opts.processingIntervalMs;
    if (opts.disableMultiGpu)
    {
        alignerConfig.enableMultiGpu = false;
    }

    grpcnode::CoinGrpcNode::InitOptions coinInit;
    coinInit.alignerConfig = alignerConfig;
    coinInit.listenAddress = opts.address;
    coinInit.expectedNodeCount = 2;
    coinInit.autoStartWhenAllRegistered = true;
    coinInit.startLeadTimeMs = opts.startLeadTimeMs;
    coinInit.waitForStartDefaultTimeoutMs = opts.waitForStartTimeoutMs;
    coinInit.rejectStreamBeforeStart = true;

    grpcnode::CoinGrpcNode coinNode(coinInit);
    if (!coinNode.start())
    {
        std::cerr << "[Coin] Failed to start CoinGrpcNode on " << opts.address << std::endl;
        return 3;
    }
    std::cout << "[Coin] CoinGrpcNode listening on " << opts.address
              << " expectedNodeCount=2" << std::endl;

    r2s_coin_test::R2SRunnerOptions r2sOpts;
    r2sOpts.address = opts.address;
    r2sOpts.calibrationDir = opts.calibrationDir;
    r2sOpts.resultDir = opts.resultDir;
    r2sOpts.maxPendingSegments = opts.maxPendingSegments;
    r2sOpts.waitForStartTimeoutMs = opts.waitForStartTimeoutMs;
    r2sOpts.energyCutLow = k9120EnergyLower_eV;
    r2sOpts.energyCutHigh = k9120EnergyUpper_eV;

    const auto t0 = std::chrono::steady_clock::now();

    // Real coin host requires both nodes to Register before start — must run in parallel.
    std::vector<r2s_coin_test::R2SRunnerStats> stats(nodeInputs.size());
    std::vector<std::thread> workers;
    workers.reserve(nodeInputs.size());
    for (size_t i = 0; i < nodeInputs.size(); ++i)
    {
        workers.emplace_back(
            [&, i]()
            {
                r2s_coin_test::R2SRunnerNodeInput n;
                n.nodeId = nodeInputs[i].nodeId;
                n.rawdataPath = nodeInputs[i].rawdataPath;
                n.channels = nodeInputs[i].channels;
                stats[i] = r2s_coin_test::run9120R2SGrpcNode(r2sOpts, n);
            });
    }
    for (auto &w : workers)
    {
        w.join();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    printServerStatus(opts.address);

    const bool startIssued = coinNode.startSignalIssued();
    std::cout << "[Coin] Stopping (flush aligner)..." << std::endl;
    coinNode.stop();

    const auto &coinStats = coinNode.statistics();
    const uint64_t processed = coinStats.totalSinglesProcessed.load();
    const uint64_t promptPairs = coinStats.totalPromptPairs.load();
    const uint64_t delayPairs = coinStats.totalDelayPairs.load();
    const uint64_t watermarkBatches = coinStats.chunksProcessed.load();

    uint64_t totalSinglesSent = 0;
    bool allSuccess = true;
    for (const auto &s : stats)
    {
        totalSinglesSent += s.singlesSent;
        allSuccess = allSuccess && s.success;
    }

    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - t0)
                               .count();

    std::cout << "\n========== Pipeline Summary ==========" << std::endl;
    std::cout << "R2S all success        : " << (allSuccess ? "true" : "false") << std::endl;
    std::cout << "Start signal issued    : " << (startIssued ? "true" : "false") << std::endl;
    std::cout << "Total singles sent     : " << totalSinglesSent << std::endl;
    std::cout << "Singles processed      : " << processed << std::endl;
    std::cout << "Prompt pairs           : " << promptPairs << std::endl;
    std::cout << "Delay pairs            : " << delayPairs << std::endl;
    std::cout << "Watermark batches      : " << watermarkBatches << std::endl;
    std::cout << "Elapsed                : " << elapsedMs << " ms" << std::endl;
    std::cout << "======================================" << std::endl;

    bool ok = true;
    if (!allSuccess)
    {
        std::cerr << "FAIL: One or more R2S nodes failed" << std::endl;
        ok = false;
    }
    if (!startIssued)
    {
        std::cerr << "FAIL: Start signal was never issued" << std::endl;
        ok = false;
    }
    if (totalSinglesSent == 0 || processed == 0)
    {
        std::cerr << "FAIL: No singles sent/processed" << std::endl;
        ok = false;
    }
    if (promptPairs == 0 || delayPairs == 0)
    {
        std::cerr << "FAIL: Expected both prompt and delay pairs > 0" << std::endl;
        ok = false;
    }

    const std::string promptFile = opts.coinOutputDir + "/prompt.lmf";
    const std::string delayFile = opts.coinOutputDir + "/delay.lmf";
    if (!fs::exists(promptFile) || !fs::exists(delayFile))
    {
        std::cerr << "FAIL: Output LMF files missing under " << opts.coinOutputDir << std::endl;
        ok = false;
    }
    else if (fs::file_size(promptFile) == 0 || fs::file_size(delayFile) == 0)
    {
        std::cerr << "FAIL: Output LMF files are empty" << std::endl;
        ok = false;
    }
    else
    {
        std::cout << "\n  Output Files:" << std::endl;
        std::cout << "    Prompt: " << promptFile << " (" << fs::file_size(promptFile) << " bytes)"
                  << std::endl;
        std::cout << "    Delay: " << delayFile << " (" << fs::file_size(delayFile) << " bytes)"
                  << std::endl;
    }

    if (ok && !opts.skipLmfAnalysis && fs::exists(promptFile))
    {
        const int16_t timeWindow100fs = static_cast<int16_t>(k9120TimeWindowPs * 10);
        const auto promptStats = analyzeLmfFile(promptFile, timeWindow100fs);
        const auto delayStats = analyzeLmfFile(delayFile, timeWindow100fs);

        if (promptStats.overWindowDtCount > 0)
        {
            std::cerr << "FAIL: Prompt LMF has " << promptStats.overWindowDtCount
                      << " events over time window" << std::endl;
            ok = false;
        }

        if (!validate9120PromptChannelSep(promptStats))
        {
            if (opts.strictChannelSep)
            {
                std::cerr << "FAIL: Prompt channelSep peak not at 144 or 288" << std::endl;
                ok = false;
            }
            else
            {
                std::cerr << "WARN: Prompt channelSep peak not at 144 or 288 "
                             "(soft check; use --strict-channel-sep to fail)"
                          << std::endl;
            }
        }

        std::cout << "  Delay events: " << delayStats.totalEvents << std::endl;
    }

    std::cout << "\n====================================================" << std::endl;
    std::cout << "  R2S→Coin gRPC test " << (ok ? "PASSED" : "FAILED") << std::endl;
    std::cout << "====================================================" << std::endl;

    return ok ? 0 : 4;
}

/*
Build:
  cmake --preset linux-release-tests-cuda
  cmake --build --preset build-tests-cuda --target test_r2s_rdma_coin

Run:
./bin/test/test_r2s_rdma_coin \
    --address 127.0.0.1:50061 \
    --data-root /media/lenovo/1TB/50100data/test_9120

Lower VRAM pressure:
./bin/test/test_r2s_rdma_coin --max-pending-segments 16 --disable-multi-gpu

Protocol-only (no real coin) two-terminal debug still uses:
  ./bin/test/test_rdma_recv_stub --expected-node-count 2 --address 127.0.0.1:50061
  ./bin/test/test_r2s_rdma_send --no-local-receiver --parallel --address 127.0.0.1:50061
*/
