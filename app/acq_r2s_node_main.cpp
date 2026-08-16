#include <pni/PnI-Config.hpp>

#include <algorithm>
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
#include "core/acquisition/RawIngress.hpp"
#include "core/r2s/R2S.hpp"
#include "core/streaming/SyntheticSingles.hpp"
#include "grpcNode/acquisitionNode.hpp"
#include "grpcService/CoincidenceClient.hpp"

#include <pni/io/IO.hpp>
#include <pni/io/ListmodeIO.hpp>

namespace
{
    namespace fs = std::filesystem;
    namespace appcfg = openpni::distributed::app;
    namespace grpcnode = openpni::distributed::grpcnode;
    namespace r2s = openpni::distributed::r2s;
    namespace streaming = openpni::distributed::streaming;
    namespace acq = openpni::distributed::acquisition;

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

    const char *sourceStateName(openpni::distributed::coincidence::SourceState s)
    {
        using S = openpni::distributed::coincidence::SourceState;
        switch (s)
        {
        case S::SOURCE_STATE_IDLE:
            return "idle";
        case S::SOURCE_STATE_RUNNING:
            return "run";
        case S::SOURCE_STATE_PAUSED:
            return "pause";
        case S::SOURCE_STATE_COMPLETE:
            return "done";
        case S::SOURCE_STATE_ERROR:
            return "err";
        default:
            return "?";
        }
    }

    void printWorkerStatus(streaming::CoincidenceClient &client, size_t pendingCap)
    {
        std::cout << "[AcqR2SNode status]"
                  << " state=" << sourceStateName(client.sourceState())
                  << " pend=" << client.getPendingMessageCount() << "/" << pendingCap
                  << " rdma=" << client.rdmaCreditRemaining() << "/" << client.rdmaSlotCount()
                  << " sent=" << client.getTotalSinglesSent()
                  << " rtt=" << client.lastRttMs()
                  << std::endl;
    }

    bool sendSinglesChunked(
        streaming::CoincidenceClient &client,
        const std::vector<streaming::Single> &singles,
        size_t chunkSize,
        uint64_t singlesPerSec)
    {
        if (singles.empty())
        {
            return true;
        }
        chunkSize = std::max<size_t>(1, chunkSize);
        for (size_t off = 0; off < singles.size(); off += chunkSize)
        {
            if (g_stopRequested.load(std::memory_order_relaxed) ||
                client.stopProduceRequested())
            {
                return false;
            }
            const size_t n = std::min(chunkSize, singles.size() - off);
            std::vector<streaming::Single> slice(singles.begin() + static_cast<std::ptrdiff_t>(off),
                                                singles.begin() + static_cast<std::ptrdiff_t>(off + n));
            const uint64_t clockMs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
            if (!client.sendSingles(slice, clockMs, 0))
            {
                return false;
            }
            if (singlesPerSec > 0)
            {
                const double sleepSec = static_cast<double>(n) / static_cast<double>(singlesPerSec);
                std::this_thread::sleep_for(std::chrono::duration<double>(sleepSec));
            }
        }
        return true;
    }

    std::vector<streaming::Single> loadLsingles(const std::string &path)
    {
        std::vector<streaming::Single> out;
        std::vector<std::string> files;
        const fs::path p(path);
        if (fs::is_regular_file(p))
        {
            files.push_back(path);
        }
        else if (fs::is_directory(p))
        {
            for (const auto &entry : fs::directory_iterator(p))
            {
                if (entry.is_regular_file() && entry.path().extension() == ".lsingle")
                {
                    files.push_back(entry.path().string());
                }
            }
            std::sort(files.begin(), files.end());
        }
        for (const auto &filePath : files)
        {
            openpni::io::listmode::ListmodeFileInput input;
            input.Open(filePath);
            for (uint32_t segIdx = 0; segIdx < input.SegmentNum(); ++segIdx)
            {
                auto segment = input.ReadSegment(segIdx);
                const auto data = segment.GetHAnyData();
                if (!data.local_crystal_index1 || !data.channel_index1 || !data.absolute_timestamp1_100fs)
                {
                    continue;
                }
                const size_t base = out.size();
                out.resize(base + data.count);
                for (std::size_t i = 0; i < data.count; ++i)
                {
                    out[base + i].channelIndex = data.channel_index1[i];
                    out[base + i].crystalIndex = data.local_crystal_index1[i];
                    out[base + i].timevalue_100fs = data.absolute_timestamp1_100fs[i];
                    out[base + i].energy_ev = data.energy1 ? data.energy1[i] : 0.0f;
                }
            }
        }
        return out;
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

    const char *sourceType = "synthetic";
    if (cfg.source.type == appcfg::WorkerSourceType::LsingleReplay)
    {
        sourceType = "lsingle_replay";
    }
    else if (cfg.source.type == appcfg::WorkerSourceType::Acquisition)
    {
        sourceType = "acquisition";
    }

    std::cout << "===========================================" << std::endl;
    std::cout << "  App: Acquisition + R2S Node (worker)" << std::endl;
    std::cout << "===========================================" << std::endl;
    std::cout << "configPath            : " << configPath << std::endl;
    std::cout << "cluster.serverAddress : " << cfg.coinClient.serverAddress << std::endl;
    std::cout << "cluster.nodeId        : " << cfg.coinClient.nodeId << std::endl;
    std::cout << "cluster.nodeAddress   : " << cfg.coinClient.nodeAddress << std::endl;
    std::cout << "dataplane.requireRoce : " << (cfg.coinClient.dataplane.requireRoce ? "true" : "false") << std::endl;
    std::cout << "dataplane.deviceName  : "
              << (cfg.coinClient.dataplane.deviceName.empty() ? "(auto)" : cfg.coinClient.dataplane.deviceName)
              << std::endl;
    std::cout << "dataplane.gidIndex    : " << cfg.coinClient.dataplane.gidIndex << std::endl;
    std::cout << "source.type           : " << sourceType << std::endl;
    std::cout << "source.lsinglePath    : " << cfg.source.lsinglePath << std::endl;
    std::cout << "source.promptPairs    : " << cfg.source.promptPairs << std::endl;
    std::cout << "source.delayPairs     : " << cfg.source.delayPairs << std::endl;
    std::cout << "rawIngress.enabled    : " << (cfg.rawIngress.enabled ? "true" : "false") << std::endl;
    std::cout << "acq.masterAddress     : " << cfg.acqNode.masterAddress << std::endl;
    std::cout << "acq.nodeId            : " << cfg.acqNode.nodeId << std::endl;
    std::cout << "r2s.calibrationDir    : " << cfg.r2s.calibrationDir << std::endl;
    std::cout << "bridge.enabled        : " << (cfg.bridge.enabled ? "true" : "false") << std::endl;
    std::cout << "coinClient.enabled    : " << (cfg.coinClient.enabled ? "true" : "false") << std::endl;
    std::cout << "runtime.enableCpuAffinity: " << (cfg.runtime.enableCpuAffinity ? "true" : "false") << std::endl;

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
    coinClientConfig.requireRoce = cfg.coinClient.dataplane.requireRoce;
    coinClientConfig.forceInProcess = cfg.coinClient.dataplane.forceInProcess;
    coinClientConfig.rdmaDeviceName = cfg.coinClient.dataplane.deviceName;
    coinClientConfig.gidIndex = cfg.coinClient.dataplane.gidIndex;
    coinClientConfig.txSlotCount = cfg.coinClient.dataplane.txSlotCount;

    streaming::CoincidenceClient coinClient(coinClientConfig);

    const bool singlesSource =
        cfg.source.type == appcfg::WorkerSourceType::Synthetic ||
        cfg.source.type == appcfg::WorkerSourceType::LsingleReplay;

    if (singlesSource)
    {
        if (!cfg.coinClient.enabled)
        {
            std::cerr << "[AcqR2SNode] coinClient must be enabled for synthetic/replay" << std::endl;
            return 2;
        }
        if (cfg.source.type == appcfg::WorkerSourceType::Acquisition)
        {
            std::cerr << "[AcqR2SNode] acquisition source is not implemented; use synthetic or lsingle_replay" << std::endl;
            return 2;
        }
        if (cfg.rawIngress.enabled)
        {
            std::cout << "[AcqR2SNode] rawIngress.enabled ignored for synthetic/replay" << std::endl;
        }
        if (!coinClient.start())
        {
            std::cerr << "[AcqR2SNode] failed to start CoincidenceClient" << std::endl;
            return 3;
        }
        std::cout << "[AcqR2SNode] dataplane kind="
                  << (coinClient.dataPlaneKind() ==
                              openpni::distributed::dataplane::rdma::DataPlaneKind::RdmaRoceV2
                          ? "RoCEv2"
                          : "InProcess")
                  << std::endl;

        std::vector<streaming::Single> singles;
        if (cfg.source.type == appcfg::WorkerSourceType::Synthetic)
        {
            streaming::SyntheticSpec spec;
            spec.nodeId = cfg.coinClient.nodeId;
            spec.peerNodeId = cfg.source.peerNodeId;
            spec.localChannel = cfg.source.localChannel;
            spec.peerChannel = cfg.source.peerChannel;
            spec.promptPairs = cfg.source.promptPairs;
            spec.delayPairs = cfg.source.delayPairs;
            spec.delayTimePs = cfg.source.delayTimePs;
            singles = streaming::generateSyntheticSingles(spec);
            std::cout << "[AcqR2SNode] synthetic singles=" << singles.size()
                      << " promptPairs=" << spec.promptPairs
                      << " delayPairs=" << spec.delayPairs << std::endl;
        }
        else
        {
            if (cfg.source.lsinglePath.empty())
            {
                std::cerr << "[AcqR2SNode] source.lsinglePath is required for replay" << std::endl;
                coinClient.stop();
                return 2;
            }
            singles = loadLsingles(cfg.source.lsinglePath);
            std::cout << "[AcqR2SNode] replay loaded singles=" << singles.size()
                      << " from " << cfg.source.lsinglePath << std::endl;
        }

        const uint32_t statusIntervalMs = cfg.coinClient.heartbeatIntervalMs == 0
                                              ? 1000u
                                              : cfg.coinClient.heartbeatIntervalMs;
        std::atomic<bool> statusStop{false};
        std::thread statusThread(
            [&coinClient, pendingCap = cfg.coinClient.maxPendingChunks, statusIntervalMs, &statusStop]()
            {
                auto lastPrint = std::chrono::steady_clock::now();
                printWorkerStatus(coinClient, pendingCap);
                while (!statusStop.load(std::memory_order_relaxed) &&
                       !g_stopRequested.load(std::memory_order_relaxed))
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
                    const auto now = std::chrono::steady_clock::now();
                    if (now - lastPrint >= std::chrono::milliseconds(statusIntervalMs))
                    {
                        printWorkerStatus(coinClient, pendingCap);
                        lastPrint = now;
                    }
                }
            });

        const bool sent = sendSinglesChunked(
            coinClient, singles, cfg.source.pushChunkSingles, cfg.source.singlesPerSec);
        statusStop.store(true, std::memory_order_relaxed);
        if (statusThread.joinable())
        {
            statusThread.join();
        }
        printWorkerStatus(coinClient, cfg.coinClient.maxPendingChunks);
        if (sent)
        {
            (void)coinClient.notifyProducerComplete();
        }
        coinClient.stop();
        std::cout << "[AcqR2SNode] source finished sent=" << (sent ? "true" : "false")
                  << " total=" << coinClient.getTotalSinglesSent() << std::endl;
        return sent ? 0 : 5;
    }

    if (cfg.source.type == appcfg::WorkerSourceType::Acquisition)
    {
        std::cerr << "[AcqR2SNode] source.type=acquisition is reserved; "
                     "this phase only provides StubRawIngress. Use synthetic or lsingle_replay."
                  << std::endl;
        if (cfg.rawIngress.enabled)
        {
            auto stub = acq::makeStubRawIngress();
            stub->start();
            std::cout << "[AcqR2SNode] StubRawIngress started (no raw data)" << std::endl;
            while (!g_stopRequested.load(std::memory_order_relaxed))
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            stub->stop();
            return 0;
        }
        return 2;
    }

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
    r2sConfig.singlesMaxFileSizeBytes = cfg.r2s.maxFileSizeMb * 1024ull * 1024ull;
    r2sConfig.singlesOverwriteExisting = cfg.r2s.overwriteExisting;
    r2sConfig.progressLogInterval = 0;

    if (cfg.coinClient.enabled)
    {
        r2sConfig.onSinglesReady = [&coinClient](std::vector<r2s::Single> &&singles, uint64_t clockMs, uint32_t durationMs) -> bool
        {
            return coinClient.sendSingles(singles, clockMs, durationMs);
        };
    }

    r2s::AsyncRawDataToR2SBridge::Config bridgeConfig;
    bridgeConfig.leaseQueueCapacity = cfg.bridge.leaseQueueCapacity;
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
    nodeOpt.overwriteExisting = cfg.acqNode.overwriteExisting;
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
        node.setDeferRawDataRelease(true);
        bridge.setReleaseFn(node.makeRawDataReleaseFn());
        if (!bridge.start(cfg.bridge.inputChannelCount))
        {
            std::cerr << "[AcqR2SNode] failed to start AsyncRawDataToR2SBridge" << std::endl;
            coinClient.stop();
            return 4;
        }
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
