/**
 * @file test_coin_lsingle_stream.cpp
 * @brief L3 gRPC coincidence stream test: replay .lsingle → CoinGrpcNode → LMF.
 *
 * Uses lsingle_replay to stream pre-computed singles from both 9120 nodes
 * to a real CoinGrpcNode (StreamingTimeAligner + coincidence engine). Validates
 * that prompt/delay LMF files are produced and contain meaningful data.
 *
 * No R2S CUDA workload — safe for single-GPU environments.
 *
 * Build:
 *   cmake --build --preset build-tests-pni --target test_coin_lsingle_stream
 *
 * Run:
 *   ./bin/test/test_coin_lsingle_stream \
 *     --data-root /media/lenovo/1TB/50100data/test_9120 --disable-multi-gpu
 */

#include <chrono>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <pni/PnI-Config.hpp>

#include "core/streaming/StreamingCoincidence.hpp"
#include "grpcNode/coinNode.hpp"
#include "tests/correctness/lsingle_replay.hpp"
#include "tests/correctness/data_9120_common.hpp"

namespace fs = std::filesystem;
namespace streaming = openpni::distributed::streaming;
namespace grpcnode = openpni::distributed::grpcnode;

using r2c_test_9120::analyzeLmfFile;
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
        std::string node0Dir;
        std::string node1Dir;
        std::string coinOutputDir;
        uint64_t singlesPerSec = 0;
        size_t pushChunkSingles = 2000000;
        size_t maxFiles = 0;
        uint64_t networkLatencyMarginPs = 5'000'000;
        uint32_t processingIntervalMs = 100;
        bool disableMultiGpu = true; // default true for single-GPU safety
        bool skipLmfAnalysis = false;
        bool strictChannelSep = false;
        bool helpOnly = false;

        void resolveDerivedPaths()
        {
            if (node0Dir.empty())
                node0Dir = (fs::path(dataRoot) / "pni_singles_node0").string();
            if (node1Dir.empty())
                node1Dir = (fs::path(dataRoot) / "pni_singles_node1").string();
            if (coinOutputDir.empty())
                coinOutputDir = (fs::path(dataRoot) / "coin_9120_grpc_stream_l3").string();
        }
    };

    bool parseArgs(int argc, char **argv, ProgramOptions &opts)
    {
        for (int i = 1; i < argc; ++i)
        {
            std::string arg = argv[i];
            auto val = [&]() -> const char * {
                if (i + 1 >= argc) { std::cerr << "Missing value for " << arg << std::endl; return nullptr; }
                return argv[++i];
            };

            if (arg == "--help" || arg == "-h") { opts.helpOnly = true; return true; }
            if (arg == "--data-root") { auto v = val(); if (!v) return false; opts.dataRoot = v; continue; }
            if (arg == "--node0-dir") { auto v = val(); if (!v) return false; opts.node0Dir = v; continue; }
            if (arg == "--node1-dir") { auto v = val(); if (!v) return false; opts.node1Dir = v; continue; }
            if (arg == "--coin-output-dir") { auto v = val(); if (!v) return false; opts.coinOutputDir = v; continue; }
            if (arg == "--address") { auto v = val(); if (!v) return false; opts.address = v; continue; }
            if (arg == "--singles-per-sec") { auto v = val(); if (!v) return false; opts.singlesPerSec = std::stoull(v); continue; }
            if (arg == "--push-chunk") { auto v = val(); if (!v) return false; opts.pushChunkSingles = std::stoull(v); continue; }
            if (arg == "--max-files") { auto v = val(); if (!v) return false; opts.maxFiles = std::stoull(v); continue; }
            if (arg == "--network-latency-margin-ps") { auto v = val(); if (!v) return false; opts.networkLatencyMarginPs = std::stoull(v); continue; }
            if (arg == "--processing-interval-ms") { auto v = val(); if (!v) return false; opts.processingIntervalMs = std::stoul(v); continue; }
            if (arg == "--disable-multi-gpu") { opts.disableMultiGpu = true; continue; }
            if (arg == "--enable-multi-gpu") { opts.disableMultiGpu = false; continue; }
            if (arg == "--skip-lmf-analysis") { opts.skipLmfAnalysis = true; continue; }
            if (arg == "--strict-channel-sep") { opts.strictChannelSep = true; continue; }

            std::cerr << "Unknown argument: " << arg << std::endl;
            return false;
        }
        return true;
    }
} // namespace

int main(int argc, char **argv)
{
    ProgramOptions opts;
    if (!parseArgs(argc, argv, opts)) return 1;
    if (opts.helpOnly)
    {
        std::cout << "Usage: test_coin_lsingle_stream [options]\n"
                  << "  --data-root <dir>                 Base data directory\n"
                  << "  --node0-dir / --node1-dir <dir>   Singles directories\n"
                  << "  --coin-output-dir <dir>           LMF output directory\n"
                  << "  --address <host:port>             gRPC address\n"
                  << "  --disable-multi-gpu               Disable multi-GPU (default)\n"
                  << "  --enable-multi-gpu                Enable multi-GPU\n"
                  << "  --singles-per-sec <n>             Rate limit (0=burst)\n"
                  << "  --push-chunk <n>                  Singles per RDMA chunk"
                     " (e.g. 2000000/4000000)\n"
                  << "  --max-files <n>                   Max files per node\n"
                  << "  --skip-lmf-analysis               Skip LMF content checks\n"
                  << "  --strict-channel-sep              Fail on channel-sep validation\n";
        return 0;
    }
    opts.resolveDerivedPaths();

    std::cout << "====================================================" << std::endl;
    std::cout << "  L3 gRPC Coincidence Stream Test (9120 dual-node)" << std::endl;
    std::cout << "====================================================" << std::endl;
    std::cout << "address           : " << opts.address << std::endl;
    std::cout << "node0Dir          : " << opts.node0Dir << std::endl;
    std::cout << "node1Dir          : " << opts.node1Dir << std::endl;
    std::cout << "coinOutputDir     : " << opts.coinOutputDir << std::endl;
    std::cout << "disableMultiGpu   : " << (opts.disableMultiGpu ? "true" : "false") << std::endl;
    std::cout << "pushChunk         : " << opts.pushChunkSingles
              << " (" << (opts.pushChunkSingles * 16ULL) / (1024 * 1024) << " MiB/chunk)" << std::endl;
    std::cout << "singlesPerSec     : " << opts.singlesPerSec << std::endl;

    // Validate inputs
    auto files0 = lsingle_replay::collectSinglesFiles(opts.node0Dir);
    auto files1 = lsingle_replay::collectSinglesFiles(opts.node1Dir);
    if (files0.empty() || files1.empty())
    {
        std::cerr << "[CoinStream] ERROR: missing .lsingle files (node0=" << files0.size()
                  << " node1=" << files1.size() << ")" << std::endl;
        std::cerr << "  Run L1 offline R2S first to produce singles." << std::endl;
        return 2;
    }

    std::error_code ec;
    fs::create_directories(opts.coinOutputDir, ec);
    if (ec)
    {
        std::cerr << "[CoinStream] Failed to create output directory: " << ec.message() << std::endl;
        return 2;
    }

    // Configure and start CoinGrpcNode
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
    coinInit.startLeadTimeMs = 1000;
    coinInit.waitForStartDefaultTimeoutMs = 30000;
    coinInit.rejectStreamBeforeStart = true;

    grpcnode::CoinGrpcNode coinNode(coinInit);
    if (!coinNode.start())
    {
        std::cerr << "[CoinStream] Failed to start CoinGrpcNode on " << opts.address << std::endl;
        return 3;
    }
    std::cout << "[CoinStream] CoinGrpcNode listening on " << opts.address << std::endl;

    // Launch replay threads
    const auto t0 = std::chrono::steady_clock::now();

    lsingle_replay::ReplayStats stats0, stats1;
    auto makeOpts = [&](uint32_t nodeId) {
        lsingle_replay::ReplayOptions ro;
        ro.serverAddress = opts.address;
        ro.nodeId = nodeId;
        ro.channelCount = 288;
        ro.singlesPerSec = opts.singlesPerSec;
        ro.pushChunkSingles = opts.pushChunkSingles;
        ro.maxFiles = opts.maxFiles;
        ro.waitForStartSignal = true;
        ro.sendRounds = 1;
        return ro;
    };

    std::thread t0Thread([&] { stats0 = lsingle_replay::runNodeReplay(files0, makeOpts(0)); });
    std::thread t1Thread([&] { stats1 = lsingle_replay::runNodeReplay(files1, makeOpts(1)); });

    t0Thread.join();
    t1Thread.join();

    // Allow aligner to flush remaining data
    std::this_thread::sleep_for(std::chrono::seconds(3));
    std::cout << "[CoinStream] Stopping CoinGrpcNode (flushing aligner)..." << std::endl;
    coinNode.stop();

    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - t0).count();

    // Gather statistics
    const auto &coinStats = coinNode.statistics();
    const uint64_t processed = coinStats.totalSinglesProcessed.load();
    const uint64_t promptPairs = coinStats.totalPromptPairs.load();
    const uint64_t delayPairs = coinStats.totalDelayPairs.load();
    const uint64_t totalSent = stats0.singlesSent + stats1.singlesSent;

    std::cout << "\n========== Pipeline Summary ==========" << std::endl;
    std::cout << "Replay success   : node0=" << stats0.success << " node1=" << stats1.success << std::endl;
    std::cout << "Total sent       : " << totalSent << std::endl;
    std::cout << "Singles processed: " << processed << std::endl;
    std::cout << "Prompt pairs     : " << promptPairs << std::endl;
    std::cout << "Delay pairs      : " << delayPairs << std::endl;
    std::cout << "Elapsed          : " << elapsedMs << " ms" << std::endl;
    std::cout << "======================================\n" << std::endl;

    // Assertions
    bool ok = true;

    if (!stats0.success || !stats1.success)
    {
        std::cerr << "[CoinStream][FAIL] Replay did not complete" << std::endl;
        ok = false;
    }
    if (processed == 0)
    {
        std::cerr << "[CoinStream][FAIL] No singles processed by aligner" << std::endl;
        ok = false;
    }
    if (promptPairs == 0)
    {
        std::cerr << "[CoinStream][FAIL] Zero prompt pairs" << std::endl;
        ok = false;
    }
    if (delayPairs == 0)
    {
        std::cerr << "[CoinStream][FAIL] Zero delay pairs" << std::endl;
        ok = false;
    }

    // LMF assertions
    if (!opts.skipLmfAnalysis)
    {
        const std::string promptLmf = (fs::path(opts.coinOutputDir) / "prompt.lmf").string();
        const std::string delayLmf = (fs::path(opts.coinOutputDir) / "delay.lmf").string();

        if (!fs::exists(promptLmf))
        {
            std::cerr << "[CoinStream][FAIL] prompt.lmf not found: " << promptLmf << std::endl;
            ok = false;
        }
        else
        {
            auto pStats = analyzeLmfFile(promptLmf, k9120TimeWindowPs);
            std::cout << "[LMF] prompt events=" << pStats.totalEvents
                      << " overWindow=" << pStats.overWindowDtCount << std::endl;
            if (pStats.totalEvents == 0)
            {
                std::cerr << "[CoinStream][FAIL] prompt.lmf has zero events" << std::endl;
                ok = false;
            }
            if (pStats.overWindowDtCount > 0)
            {
                std::cerr << "[CoinStream][FAIL] prompt.lmf has " << pStats.overWindowDtCount
                          << " events exceeding time window" << std::endl;
                ok = false;
            }

            if (opts.strictChannelSep)
            {
                if (!validate9120PromptChannelSep(pStats))
                {
                    std::cerr << "[CoinStream][FAIL] channel separation validation failed" << std::endl;
                    ok = false;
                }
            }
            else
            {
                validate9120PromptChannelSep(pStats); // print only
            }
        }

        if (!fs::exists(delayLmf))
        {
            std::cerr << "[CoinStream][FAIL] delay.lmf not found: " << delayLmf << std::endl;
            ok = false;
        }
        else
        {
            auto dStats = analyzeLmfFile(delayLmf, k9120TimeWindowPs);
            std::cout << "[LMF] delay events=" << dStats.totalEvents << std::endl;
            if (dStats.totalEvents == 0)
            {
                std::cerr << "[CoinStream][FAIL] delay.lmf has zero events" << std::endl;
                ok = false;
            }
        }
    }

    std::cout << "====================================================" << std::endl;
    std::cout << "  L3 Coin Stream Test " << (ok ? "PASSED" : "FAILED") << std::endl;
    std::cout << "====================================================" << std::endl;

    return ok ? 0 : 4;
}
