#include <pni/PnI-Config.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include <sched.h>

#include "app/common/AppConfig.hpp"
#include "core/r2s/R2S.hpp"
#include "grpcNode/acquisitionNode.hpp"
#include "grpcService/CoincidenceClient.hpp"

namespace
{
    namespace fs = std::filesystem;
    namespace appcfg = openpni::distributed::app;
    namespace grpcnode = openpni::distributed::grpcnode;
    namespace r2s = openpni::distributed::r2s;
    namespace streaming = openpni::distributed::streaming;

    std::atomic<bool> g_stopRequested{false};

    void onSignal(int /*sig*/)
    {
        g_stopRequested.store(true, std::memory_order_relaxed);
    }

    std::vector<std::string> buildCalibrationFiles(const std::string &calibrationPath)
    {
        std::vector<std::string> files;
        files.reserve(48);

        const fs::path path(calibrationPath);
        if (fs::is_regular_file(path))
        {
            for (int i = 0; i < 48; ++i)
            {
                files.push_back(path.string());
            }
            return files;
        }

        for (int i = 0; i < 48; ++i)
        {
            std::ostringstream oss;
            oss << "channel_" << std::setw(2) << std::setfill('0') << i << ".data";
            files.push_back((path / oss.str()).string());
        }

        return files;
    }

    r2s::DetectorType parseDetectorType(const std::string &value)
    {
        if (value == "BDM2")
        {
            return r2s::DetectorType::BDM2;
        }
        if (value == "BDM50100")
        {
            return r2s::DetectorType::BDM50100;
        }
        return r2s::DetectorType::Unknown;
    }

    void printUsage(const char *prog)
    {
        std::cout << "Usage: " << prog << " --config <path> [--dry-run]\n"
                  << "Options:\n"
                  << "  --config <path>      JSON config file path (required)\n"
                  << "  --dry-run            Parse and print config, then exit\n"
                  << "  --help               Print this message\n"
                  << std::endl;
    }

    bool parseArgs(int argc, char **argv, std::string *configPath, bool *dryRun)
    {
        for (int i = 1; i < argc; ++i)
        {
            const std::string arg = argv[i];
            if (arg == "--help")
            {
                printUsage(argv[0]);
                return false;
            }

            if (arg == "--dry-run")
            {
                *dryRun = true;
                continue;
            }

            if (arg == "--config")
            {
                if (i + 1 >= argc)
                {
                    std::cerr << "Missing value for --config" << std::endl;
                    return false;
                }
                *configPath = argv[++i];
                continue;
            }

            std::cerr << "Unknown argument: " << arg << std::endl;
            return false;
        }

        if (configPath->empty())
        {
            std::cerr << "--config is required" << std::endl;
            return false;
        }

        return true;
    }

    bool applyProcessCpuAffinity(const appcfg::RuntimeSection &runtime, std::string *errorMessage)
    {
        if (!runtime.enableCpuAffinity)
        {
            return true;
        }

        cpu_set_t cpuSet;
        CPU_ZERO(&cpuSet);

        for (uint16_t core : runtime.cpuAffinityCores)
        {
            if (core >= CPU_SETSIZE)
            {
                if (errorMessage)
                {
                    *errorMessage = "runtime.cpuAffinityCores contains core >= CPU_SETSIZE: " + std::to_string(core);
                }
                return false;
            }
            CPU_SET(core, &cpuSet);
        }

        if (::sched_setaffinity(0, sizeof(cpuSet), &cpuSet) != 0)
        {
            if (errorMessage)
            {
                *errorMessage = "sched_setaffinity failed";
            }
            return false;
        }

        std::ostringstream oss;
        oss << "[AcqR2SNode] CPU affinity enabled: ";
        for (size_t i = 0; i < runtime.cpuAffinityCores.size(); ++i)
        {
            if (i > 0)
            {
                oss << ",";
            }
            oss << runtime.cpuAffinityCores[i];
        }
        std::cout << oss.str() << std::endl;

        return true;
    }

} // namespace

int main(int argc, char **argv)
{
    google::InitGoogleLogging(argv[0]);
    std::atexit([]()
                { google::ShutdownGoogleLogging(); });

    std::string configPath;
    bool dryRun = false;
    if (!parseArgs(argc, argv, &configPath, &dryRun))
    {
        return 1;
    }

    appcfg::AcqR2SNodeConfig cfg;
    std::string err;
    if (!appcfg::loadAcqR2SNodeConfig(configPath, &cfg, &err))
    {
        std::cerr << "[AcqR2SNode] config load failed: " << err << std::endl;
        return 2;
    }

    if (!applyProcessCpuAffinity(cfg.runtime, &err))
    {
        std::cerr << "[AcqR2SNode] runtime setup failed: " << err << std::endl;
        return 2;
    }

    std::cout << "===========================================" << std::endl;
    std::cout << "  App: Acquisition + R2S Node" << std::endl;
    std::cout << "===========================================" << std::endl;
    std::cout << "configPath            : " << configPath << std::endl;
    std::cout << "acq.masterAddress     : " << cfg.acqNode.masterAddress << std::endl;
    std::cout << "acq.nodeId            : " << cfg.acqNode.nodeId << std::endl;
    std::cout << "acq.nodeAddress       : " << cfg.acqNode.nodeAddress << std::endl;
    std::cout << "acq.outputRoot        : " << cfg.acqNode.outputRoot << std::endl;
    std::cout << "r2s.calibrationDir    : " << cfg.r2s.calibrationDir << std::endl;
    std::cout << "r2s.resultDir         : " << cfg.r2s.resultDir << std::endl;
    std::cout << "bridge.enabled        : " << (cfg.bridge.enabled ? "true" : "false") << std::endl;
    std::cout << "coinClient.enabled    : " << (cfg.coinClient.enabled ? "true" : "false") << std::endl;
    std::cout << "coinClient.remapGlobal: " << (cfg.coinClient.remapLocalToGlobalChannels ? "true" : "false") << std::endl;
    std::cout << "coinClient.globalOffset: " << cfg.coinClient.globalChannelOffset << std::endl;
    std::cout << "coinClient.cpc        : " << cfg.coinClient.crystalsPerChannel << std::endl;
    std::cout << "runtime.enableCpuAffinity: " << (cfg.runtime.enableCpuAffinity ? "true" : "false") << std::endl;
    std::cout << "runtime.strictBindIpsOwnershipCheck: " << (cfg.runtime.strictBindIpsOwnershipCheck ? "true" : "false") << std::endl;
    std::cout << "runtime.strictNumaTopologyCheck: " << (cfg.runtime.strictNumaTopologyCheck ? "true" : "false") << std::endl;

    if (dryRun)
    {
        std::cout << "[AcqR2SNode] dry-run mode, exit." << std::endl;
        return 0;
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    const auto calibrationFiles = buildCalibrationFiles(cfg.r2s.calibrationDir);

    streaming::CoincidenceClientConfig coinClientConfig;
    coinClientConfig.serverAddress = cfg.coinClient.serverAddress;
    coinClientConfig.nodeId = cfg.coinClient.nodeId;
    coinClientConfig.nodeAddress = cfg.coinClient.nodeAddress;
    coinClientConfig.channelCount = cfg.coinClient.channelCount;
    coinClientConfig.detectorType = cfg.coinClient.detectorType;
    coinClientConfig.remapLocalToGlobalChannels = cfg.coinClient.remapLocalToGlobalChannels;
    coinClientConfig.globalChannelOffset = cfg.coinClient.globalChannelOffset;
    coinClientConfig.crystalsPerChannel = cfg.coinClient.crystalsPerChannel;
    coinClientConfig.maxPendingChunks = cfg.coinClient.maxPendingChunks;
    coinClientConfig.batchSize = cfg.coinClient.batchSize;
    coinClientConfig.heartbeatIntervalMs = cfg.coinClient.heartbeatIntervalMs;
    coinClientConfig.waitForStartSignal = cfg.coinClient.waitForStartSignal;
    coinClientConfig.waitForStartTimeoutMs = cfg.coinClient.waitForStartTimeoutMs;
    coinClientConfig.waitForStartRpcTimeoutMs = cfg.coinClient.waitForStartRpcTimeoutMs;
    coinClientConfig.waitForStartRetryIntervalMs = cfg.coinClient.waitForStartRetryIntervalMs;

    streaming::CoincidenceClient coinClient(coinClientConfig);

    const auto detectorType = parseDetectorType(cfg.coinClient.detectorType);
    r2s::R2SProcessConfig r2sConfig;
    switch (detectorType)
    {
    case r2s::DetectorType::BDM2:
        r2sConfig = r2s::createBDM2Config(
            "",
            cfg.r2s.resultDir,
            calibrationFiles,
            cfg.acqNode.nodeId,
            cfg.r2s.channelIndices);
        break;
    case r2s::DetectorType::BDM50100:
        r2sConfig = r2s::createBDM50100Config(
            "",
            cfg.r2s.resultDir,
            calibrationFiles,
            cfg.acqNode.nodeId,
            cfg.r2s.channelIndices);
        break;
    default:
        std::cerr << "[AcqR2SNode] unsupported detectorType for R2S: " << cfg.coinClient.detectorType << std::endl;
        return 2;
    }

    r2sConfig.sortDataByTime = cfg.r2s.sortDataByTime;
    r2sConfig.saveData2SingleFile = cfg.r2s.saveData2SingleFile;
    r2sConfig.asyncFileWrite = cfg.r2s.asyncFileWrite;
    r2sConfig.progressLogInterval = 0;

    if (cfg.coinClient.enabled)
    {
        r2sConfig.onSinglesReady = [&coinClient](std::vector<r2s::Single> &&singles, uint64_t clockMs, uint32_t durationMs) -> bool
        {
            return coinClient.sendSingles(singles, clockMs, durationMs);
        };
    }

    r2s::AsyncRawDataToR2SBridge::Config bridgeConfig;
    bridgeConfig.queue.capacity = cfg.bridge.queueCapacity;
    bridgeConfig.queue.reservePacketsPerSlot = cfg.bridge.reservePacketsPerSlot;
    bridgeConfig.queue.reserveBytesPerSlot = cfg.bridge.reserveBytesPerSlot;
    bridgeConfig.blockWhenQueueFull = cfg.bridge.blockWhenQueueFull;
    bridgeConfig.queueFullWarnEvery = cfg.bridge.queueFullWarnEvery;

    r2s::AsyncRawDataToR2SBridge bridge(r2sConfig, bridgeConfig);

    if (cfg.coinClient.enabled)
    {
        if (!coinClient.start())
        {
            std::cerr << "[AcqR2SNode] failed to start CoincidenceClient" << std::endl;
            return 3;
        }
    }

    if (cfg.bridge.enabled)
    {
        if (!bridge.start(cfg.bridge.inputChannelCount))
        {
            std::cerr << "[AcqR2SNode] failed to start AsyncRawDataToR2SBridge" << std::endl;
            coinClient.stop();
            return 4;
        }
    }

    grpcnode::AcquisitionGrpcNode::InitOptions nodeOpt;
    nodeOpt.masterAddress = cfg.acqNode.masterAddress;
    nodeOpt.nodeId = cfg.acqNode.nodeId;
    nodeOpt.nodeAddress = cfg.acqNode.nodeAddress;
    nodeOpt.outputRoot = cfg.acqNode.outputRoot;
    nodeOpt.outputRoots = cfg.acqNode.outputRoots;
    nodeOpt.shardStrategy = cfg.acqNode.shardStrategy;
    nodeOpt.manifestFilename = cfg.acqNode.manifestFilename;
    nodeOpt.sessionNamePrefix = cfg.acqNode.sessionNamePrefix;
    nodeOpt.maxFileSizeMb = cfg.acqNode.maxFileSizeMb;
    nodeOpt.reservedStorageGiB = cfg.acqNode.reservedStorageGiB;
    nodeOpt.statusIntervalMs = cfg.acqNode.statusIntervalMs;
    nodeOpt.enableRawFileWrite = cfg.acqNode.enableRawFileWrite;
    nodeOpt.asyncQueueDepth = cfg.acqNode.asyncQueueDepth;
    nodeOpt.writerThreadsPerShard = cfg.acqNode.writerThreadsPerShard;
    nodeOpt.useSpillToDisk = cfg.acqNode.useSpillToDisk;
    nodeOpt.failOnQueueFull = cfg.acqNode.failOnQueueFull;
    nodeOpt.fsyncEachSegment = cfg.acqNode.fsyncEachSegment;
    nodeOpt.strictBindIpsOwnershipCheck = cfg.runtime.strictBindIpsOwnershipCheck;
    nodeOpt.strictNumaTopologyCheck = cfg.runtime.strictNumaTopologyCheck;
    nodeOpt.requireBindIpsSingleNuma = cfg.runtime.requireBindIpsSingleNuma;
    nodeOpt.requireCpuAffinityOnNuma = cfg.runtime.requireCpuAffinityOnNuma;
    nodeOpt.expectedNumaNode = cfg.runtime.expectedNumaNode;
    nodeOpt.cpuAffinityCores.assign(cfg.runtime.cpuAffinityCores.begin(), cfg.runtime.cpuAffinityCores.end());

    grpcnode::AcquisitionGrpcNode node(nodeOpt);
    if (cfg.bridge.enabled)
    {
        node.setRawDataReadyCallback(bridge.makeRawDataCallback());
    }

    std::atomic<bool> nodeRunOk{false};
    std::atomic<bool> nodeDone{false};

    std::thread nodeThread([&]()
                           {
                               nodeRunOk.store(node.run(), std::memory_order_relaxed);
                               nodeDone.store(true, std::memory_order_relaxed); });

    auto perfLastTick = std::chrono::steady_clock::now();
    uint64_t perfLastEnqueuedSeg = 0;
    uint64_t perfLastProcessedSeg = 0;
    uint64_t perfLastDroppedSeg = 0;
    uint64_t perfLastSentSingles = 0;
    auto perfLastAcqStatus = perfLastTick;

    while (!g_stopRequested.load(std::memory_order_relaxed) && !nodeDone.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        const auto now = std::chrono::steady_clock::now();
        if (now - perfLastAcqStatus >= std::chrono::milliseconds(cfg.acqNode.statusIntervalMs))
        {
            const double dtSec = std::max(
                1e-6,
                std::chrono::duration<double>(now - perfLastTick).count());

            const auto bridgeStats = bridge.stats();
            const uint64_t sentSingles = cfg.coinClient.enabled ? coinClient.getTotalSinglesSent() : 0;

            const double enqueueSegRate = static_cast<double>(bridgeStats.enqueuedSegments - perfLastEnqueuedSeg) / dtSec;
            const double processSegRate = static_cast<double>(bridgeStats.processedSegments - perfLastProcessedSeg) / dtSec;
            const double dropSegRate = static_cast<double>(bridgeStats.droppedSegments - perfLastDroppedSeg) / dtSec;
            const double sendSinglesRateM = static_cast<double>(sentSingles - perfLastSentSingles) / dtSec / 1e6;

            std::cout << "[AcqR2SNode/Perf] enqueueSeg/s=" << std::fixed << std::setprecision(2) << enqueueSegRate
                      << " processSeg/s=" << processSegRate
                      << " dropSeg/s=" << dropSegRate
                      << " queuePeak=" << bridgeStats.queuePeakDepth
                      << " enqueueFullHits=" << bridgeStats.enqueueFullHits
                      << " sentSingles=" << sentSingles
                      << " sendRateMSingles=" << std::setprecision(4) << sendSinglesRateM
                      << std::defaultfloat
                      << std::endl;

            perfLastTick = now;
            perfLastEnqueuedSeg = bridgeStats.enqueuedSegments;
            perfLastProcessedSeg = bridgeStats.processedSegments;
            perfLastDroppedSeg = bridgeStats.droppedSegments;
            perfLastSentSingles = sentSingles;
            perfLastAcqStatus = now;
        }
    }

    if (g_stopRequested.load(std::memory_order_relaxed))
    {
        std::cout << "[AcqR2SNode] stop signal received, stopping node..." << std::endl;
        node.stop();
    }

    if (nodeThread.joinable())
    {
        nodeThread.join();
    }

    bool bridgeStopOk = true;
    if (cfg.bridge.enabled)
    {
        bridgeStopOk = bridge.stop();
    }

    if (cfg.coinClient.enabled)
    {
        coinClient.stop();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(cfg.runtime.shutdownGraceMs));

    std::cout << "[AcqR2SNode] nodeRunOk=" << (nodeRunOk.load() ? "true" : "false")
              << " bridgeStopOk=" << (bridgeStopOk ? "true" : "false") << std::endl;

    return (nodeRunOk.load() && bridgeStopOk) ? 0 : 5;
}
