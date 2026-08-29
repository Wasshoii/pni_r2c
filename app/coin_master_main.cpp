#include <pni/PnI-Config.hpp>

#include <arpa/inet.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>

#include <glog/logging.h>

#include "app/common/AppConfig.hpp"
#include "grpcNode/coinNode.hpp"
#include "grpcService/AcquisitionMaster.hpp"

namespace
{
    namespace appcfg = openpni::distributed::app;
    namespace acq = openpni::distributed::acquisition;
    namespace grpcnode = openpni::distributed::grpcnode;
    namespace streaming = openpni::distributed::streaming;

    std::atomic<bool> g_stopRequested{false};

    void onSignal(int /*sig*/)
    {
        g_stopRequested.store(true, std::memory_order_relaxed);
    }

    uint32_t ipToInt(const std::string &ip)
    {
        in_addr addr{};
        if (::inet_pton(AF_INET, ip.c_str(), &addr) != 1)
        {
            return 0;
        }
        return ntohl(addr.s_addr);
    }

    bool isValidIpv4(const std::string &ip)
    {
        in_addr addr{};
        return ::inet_pton(AF_INET, ip.c_str(), &addr) == 1;
    }

    const appcfg::AcqControlSection::NodeOverride *findNodeOverride(
        const appcfg::AcqControlSection &control,
        const std::string &nodeId)
    {
        for (const auto &entry : control.nodeOverrides)
        {
            if (entry.nodeId == nodeId)
            {
                return &entry;
            }
        }
        return nullptr;
    }

    bool fillAcquisitionTask(acq::AcquisitionTask *task, const appcfg::AcqControlSection &c, std::string *err)
    {
        switch (c.acquisitionAlgorithm)
        {
        case appcfg::AcqControlSection::AcquisitionAlgorithm::Dpdk:
            task->set_algorithm_type(acq::ALGORITHM_TYPE_DPDK);
            break;
        case appcfg::AcqControlSection::AcquisitionAlgorithm::Socket:
        default:
            task->set_algorithm_type(acq::ALGORITHM_TYPE_SOCKET);
            break;
        }

        if (c.acquisitionAlgorithm == appcfg::AcqControlSection::AcquisitionAlgorithm::Dpdk && c.dpdkBindIps.empty())
        {
            if (err)
            {
                *err = "acquisitionControl.dpdkBindIps must be configured when acquisitionAlgorithm=dpdk";
            }
            return false;
        }

        if (c.acquisitionAlgorithm == appcfg::AcqControlSection::AcquisitionAlgorithm::Dpdk)
        {
            for (const auto &ip : c.dpdkBindIps)
            {
                if (!isValidIpv4(ip))
                {
                    if (err)
                    {
                        *err = "acquisitionControl.dpdkBindIps contains invalid IPv4: " + ip;
                    }
                    return false;
                }
            }
        }

        task->set_storage_unit_size(c.storageUnitSize);
        task->set_min_packet_size(c.minPacketSize);
        task->set_max_buffer_size(c.maxBufferSize);
        task->set_time_switch_buffer_ms(c.timeSwitchBufferMs);
        task->set_session_name(c.sessionName);
        task->set_reserved_storage_gib(c.reservedStorageGiB);
        task->set_max_file_size_mb(c.maxFileSizeMb);

        task->clear_detector_sources();
        if (!c.detectorSources.empty())
        {
            for (size_t i = 0; i < c.detectorSources.size(); ++i)
            {
                const auto &det = c.detectorSources[i];
                auto *source = task->add_detector_sources();
                source->set_detector_id(det.detectorId.empty() ? ("detector-" + std::to_string(i)) : det.detectorId);
                source->set_ip_source(ipToInt(det.sourceIp));
                source->set_port_source(det.sourcePort);
            }
        }
        else
        {
            for (uint16_t i = 0; i < c.channelCount; ++i)
            {
                auto *source = task->add_detector_sources();
                source->set_detector_id("detector-" + std::to_string(i));
                source->set_ip_source(ipToInt(c.sourceIp));
                source->set_port_source(static_cast<uint32_t>(c.sourcePortBase + i));
            }
        }

        auto *dest = task->mutable_destination_rule();
        dest->set_ip_destination(ipToInt(c.destinationIp));
        dest->set_port_destination_base(c.destinationPortBase);
        dest->set_port_destination_stride(1);

        auto *dpdk = task->mutable_dpdk_options();
        dpdk->set_copy_thread_num(c.dpdkCopyThreadNum);
        dpdk->set_rx_rings_per_port(c.dpdkRxRingsPerPort);
        dpdk->set_rte_mbuf_double_pointer_size_multiply(c.dpdkMbufDoublePointerSizeMultiply);
        dpdk->set_rte_mbuf_double_pointer_num_multiply(c.dpdkMbufDoublePointerNumMultiply);

        dpdk->clear_bind_ips();
        for (const auto &ip : c.dpdkBindIps)
        {
            dpdk->add_bind_ips(ip);
        }

        return true;
    }

    bool applyNodeOverrideToTask(
        const std::string &nodeId,
        const appcfg::AcqControlSection &control,
        acq::AcquisitionTask *task,
        std::string *err)
    {
        const auto *overrideCfg = findNodeOverride(control, nodeId);
        if (!overrideCfg)
        {
            return true;
        }

        switch (overrideCfg->acquisitionAlgorithm)
        {
        case appcfg::AcqControlSection::NodeOverride::AlgorithmOverride::Socket:
            task->set_algorithm_type(acq::ALGORITHM_TYPE_SOCKET);
            break;
        case appcfg::AcqControlSection::NodeOverride::AlgorithmOverride::Dpdk:
            task->set_algorithm_type(acq::ALGORITHM_TYPE_DPDK);
            break;
        case appcfg::AcqControlSection::NodeOverride::AlgorithmOverride::Inherit:
        default:
            break;
        }

        const bool hasDpdkNumericOverride =
            overrideCfg->dpdkCopyThreadNum > 0 ||
            overrideCfg->dpdkRxRingsPerPort > 0 ||
            overrideCfg->dpdkMbufDoublePointerSizeMultiply > 0 ||
            overrideCfg->dpdkMbufDoublePointerNumMultiply > 0;
        const bool hasDpdkIpOverride = !overrideCfg->dpdkBindIps.empty();

        if (hasDpdkNumericOverride || hasDpdkIpOverride)
        {
            auto *dpdk = task->mutable_dpdk_options();
            if (overrideCfg->dpdkCopyThreadNum > 0)
            {
                dpdk->set_copy_thread_num(overrideCfg->dpdkCopyThreadNum);
            }
            if (overrideCfg->dpdkRxRingsPerPort > 0)
            {
                dpdk->set_rx_rings_per_port(overrideCfg->dpdkRxRingsPerPort);
            }
            if (overrideCfg->dpdkMbufDoublePointerSizeMultiply > 0)
            {
                dpdk->set_rte_mbuf_double_pointer_size_multiply(overrideCfg->dpdkMbufDoublePointerSizeMultiply);
            }
            if (overrideCfg->dpdkMbufDoublePointerNumMultiply > 0)
            {
                dpdk->set_rte_mbuf_double_pointer_num_multiply(overrideCfg->dpdkMbufDoublePointerNumMultiply);
            }
            if (hasDpdkIpOverride)
            {
                dpdk->clear_bind_ips();
                for (const auto &ip : overrideCfg->dpdkBindIps)
                {
                    dpdk->add_bind_ips(ip);
                }
            }
        }

        if (task->algorithm_type() == acq::ALGORITHM_TYPE_DPDK)
        {
            if (!task->has_dpdk_options() || task->dpdk_options().bind_ips_size() == 0)
            {
                if (err)
                {
                    *err = "node " + nodeId + " requires non-empty dpdk bind_ips";
                }
                return false;
            }

            const auto &dpdk = task->dpdk_options();
            for (int i = 0; i < dpdk.bind_ips_size(); ++i)
            {
                if (!isValidIpv4(dpdk.bind_ips(i)))
                {
                    if (err)
                    {
                        *err = "node " + nodeId + " has invalid dpdk bind ip: " + dpdk.bind_ips(i);
                    }
                    return false;
                }
            }
        }

        return true;
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

} // namespace

namespace coincidence = openpni::distributed::coincidence;

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

    appcfg::CoinMasterConfig cfg;
    std::string err;
    if (!appcfg::loadCoinMasterConfig(configPath, &cfg, &err))
    {
        std::cerr << "[CoinMaster] config load failed: " << err << std::endl;
        return 2;
    }

    std::cout << "===========================================" << std::endl;
    std::cout << "  App: Coin Master (coincidence-only)" << std::endl;
    std::cout << "===========================================" << std::endl;
    std::cout << "configPath               : " << configPath << std::endl;
    std::cout << "coin.listenAddress       : " << cfg.coinMaster.listenAddress << std::endl;
    std::cout << "coin.expectedNodeCount   : " << cfg.coinMaster.expectedNodeCount << std::endl;
    std::cout << "coin.detectorProfile     : " << cfg.coinMaster.detectorProfile << std::endl;
    std::cout << "dataplane.requireRoce    : " << (cfg.coinMaster.dataplane.requireRoce ? "true" : "false") << std::endl;
    std::cout << "dataplane.deviceName     : " << (cfg.coinMaster.dataplane.deviceName.empty() ? "(auto)" : cfg.coinMaster.dataplane.deviceName) << std::endl;
    std::cout << "dataplane.gidIndex       : " << cfg.coinMaster.dataplane.gidIndex << std::endl;
    std::cout << "aligner.outputDir        : " << cfg.aligner.outputDir << std::endl;
    std::cout << "acqControl.enabled       : " << (cfg.acquisitionControl.enabled ? "true (IGNORED this phase)" : "false")
              << std::endl;
    std::cout << "acqControl.algorithm     : "
              << (cfg.acquisitionControl.acquisitionAlgorithm == appcfg::AcqControlSection::AcquisitionAlgorithm::Dpdk ? "dpdk" : "socket")
              << std::endl;
    std::cout << "acqControl.dpdkBindIps   : " << cfg.acquisitionControl.dpdkBindIps.size() << std::endl;
    std::cout << "acqControl.nodeOverrides : " << cfg.acquisitionControl.nodeOverrides.size() << std::endl;
    std::cout << "acqControl.detectorSources: " << cfg.acquisitionControl.detectorSources.size() << std::endl;

    if (dryRun)
    {
        std::cout << "[CoinMaster] dry-run mode, exit." << std::endl;
        return 0;
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    streaming::TimeAlignerConfig alignerConfig;
    if (cfg.coinMaster.detectorProfile == "BDM50100_9120" ||
        cfg.coinMaster.detectorProfile == "BDM50100")
    {
        openpni::CoincidenceProtocol proto;
        proto.timeWindow_ps = cfg.aligner.coinProtocol.timeWindowPs;
        proto.delayTime_ps = cfg.aligner.coinProtocol.delayTimePs;
        proto.energyLower_eV = cfg.aligner.coinProtocol.energyLowerEV;
        proto.energyUpper_eV = cfg.aligner.coinProtocol.energyUpperEV;
        alignerConfig = streaming::createBDM50100_9120AlignerConfig(cfg.aligner.outputDir, proto);
    }
    else
    {
        openpni::CoincidenceProtocol proto;
        proto.timeWindow_ps = cfg.aligner.coinProtocol.timeWindowPs;
        proto.delayTime_ps = cfg.aligner.coinProtocol.delayTimePs;
        proto.energyLower_eV = cfg.aligner.coinProtocol.energyLowerEV;
        proto.energyUpper_eV = cfg.aligner.coinProtocol.energyUpperEV;
        alignerConfig = streaming::createBDM2AlignerConfig(cfg.aligner.outputDir, proto);
    }
    if (!cfg.aligner.outputDir.empty())
    {
        alignerConfig.outputDir = cfg.aligner.outputDir;
    }
    if (cfg.aligner.channelNum > 0)
    {
        alignerConfig.channelNum = cfg.aligner.channelNum;
    }
    alignerConfig.crystalsPerChannel = cfg.aligner.crystalsPerChannel;
    alignerConfig.networkLatencyMargin_pico = cfg.aligner.networkLatencyMarginPico;
    alignerConfig.processingIntervalMs = cfg.aligner.processingIntervalMs;
    alignerConfig.maxChunksPerNode = cfg.aligner.maxChunksPerNode;
    alignerConfig.maxSegmentSingles = cfg.aligner.maxSegmentSingles;
    alignerConfig.minSegmentSingles = cfg.aligner.minSegmentSingles;
    alignerConfig.minSegmentOverlapFactor = cfg.aligner.minSegmentOverlapFactor;
    alignerConfig.bufferHighWaterRatio = cfg.aligner.bufferHighWaterRatio;
    alignerConfig.maxProcessLatencyMs = cfg.aligner.maxProcessLatencyMs;
    alignerConfig.allowStalledNodeBypass = cfg.aligner.allowStalledNodeBypass;
    alignerConfig.nodeStallTimeoutMs = cfg.aligner.nodeStallTimeoutMs;
    alignerConfig.coinPipelineDepth = cfg.aligner.coinPipelineDepth;
    alignerConfig.maxTotalMemoryBytes = cfg.aligner.maxTotalMemoryBytes;
    alignerConfig.useMemoryPool = cfg.aligner.useMemoryPool;
    alignerConfig.savePrompt = cfg.aligner.savePrompt;
    alignerConfig.saveDelay = cfg.aligner.saveDelay;
    alignerConfig.listmodeMaxFileSizeBytes = cfg.aligner.maxFileSizeMb * 1024ull * 1024ull;
    alignerConfig.listmodeOverwriteExisting = cfg.aligner.overwriteExisting;
    alignerConfig.coinProtocol.timeWindow_ps = cfg.aligner.coinProtocol.timeWindowPs;
    alignerConfig.coinProtocol.delayTime_ps = cfg.aligner.coinProtocol.delayTimePs;
    alignerConfig.coinProtocol.energyLower_eV = cfg.aligner.coinProtocol.energyLowerEV;
    alignerConfig.coinProtocol.energyUpper_eV = cfg.aligner.coinProtocol.energyUpperEV;

    if (cfg.acquisitionControl.enabled)
    {
        std::cerr << "[CoinMaster] acquisitionControl.enabled is ignored in this phase "
                     "(coin node is coincidence-only). Unset acquisitionControl.enabled."
                  << std::endl;
        cfg.acquisitionControl.enabled = false;
    }

    grpcnode::CoinGrpcNode::InitOptions coinInit;
    coinInit.alignerConfig = alignerConfig;
    coinInit.listenAddress = cfg.coinMaster.listenAddress;
    coinInit.expectedNodeCount = cfg.coinMaster.expectedNodeCount;
    coinInit.autoStartWhenAllRegistered = cfg.coinMaster.autoStartWhenAllRegistered;
    coinInit.startLeadTimeMs = cfg.coinMaster.startLeadTimeMs;
    coinInit.waitForStartDefaultTimeoutMs = cfg.coinMaster.waitForStartDefaultTimeoutMs;
    coinInit.rejectStreamBeforeStart = cfg.coinMaster.rejectStreamBeforeStart;
    coinInit.requireRoce = cfg.coinMaster.dataplane.requireRoce;
    coinInit.forceInProcess = cfg.coinMaster.dataplane.forceInProcess;
    coinInit.rdmaDeviceName = cfg.coinMaster.dataplane.deviceName;
    coinInit.gidIndex = cfg.coinMaster.dataplane.gidIndex;
    coinInit.slotCount = cfg.coinMaster.dataplane.slotCount;
    coinInit.slotBytes = cfg.coinMaster.dataplane.slotBytes;

    grpcnode::CoinGrpcNode coinNode(coinInit);
    if (!coinNode.start())
    {
        std::cerr << "[CoinMaster] failed to start CoinGrpcNode" << std::endl;
        return 3;
    }

    std::cout << "[CoinMaster] Coin service listening on " << cfg.coinMaster.listenAddress << std::endl;

    acq::AcquisitionMaster acqMaster;
    bool acqMasterStarted = false;
    bool taskDistributed = false;
    bool startSent = false;

    if (cfg.acquisitionControl.enabled)
    {
        acq::AcquisitionTask globalTask;
        std::string taskError;
        if (!fillAcquisitionTask(&globalTask, cfg.acquisitionControl, &taskError))
        {
            std::cerr << "[CoinMaster] invalid acquisition task config: " << taskError << std::endl;
            coinNode.stop();
            return 3;
        }

        std::cout << "[CoinMaster] Acquisition mapping prepared: detector_sources="
                  << globalTask.detector_sources_size()
                  << " algorithm="
                  << (globalTask.algorithm_type() == acq::ALGORITHM_TYPE_DPDK ? "DPDK" : "SOCKET")
                  << std::endl;

        acqMaster.Initialize(globalTask);
        acqMaster.StartServer(cfg.acquisitionControl.masterAddress);
        acqMaster.SetConfigureTaskOverrideFn(
            [&cfg](const std::string &nodeId, acq::AcquisitionTask *task, std::string *overrideErr)
            {
                return applyNodeOverrideToTask(nodeId, cfg.acquisitionControl, task, overrideErr);
            });
        acqMasterStarted = true;

        std::cout << "[CoinMaster] AcquisitionMaster started at " << cfg.acquisitionControl.masterAddress << std::endl;
    }

    const auto t0 = std::chrono::steady_clock::now();
    auto lastTick = std::chrono::steady_clock::now();
    uint64_t lastSinglesReceived = 0;
    uint64_t lastSinglesProcessed = 0;
    uint64_t lastPromptPairs = 0;
    uint64_t lastDelayPairs = 0;
    uint64_t lastAcqRxPackets = 0;
    std::unordered_map<uint32_t, uint64_t> lastNodeSent;
    std::unordered_map<uint32_t, uint64_t> lastNodeR2s;
    std::unordered_map<uint32_t, uint64_t> lastNodeAcq;

    while (!g_stopRequested.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.coinMaster.statusPrintIntervalMs));

        const auto &stats = coinNode.statistics();
        const uint64_t totalSinglesReceived = stats.totalSinglesReceived.load(std::memory_order_relaxed);
        const uint64_t totalSinglesProcessed = stats.totalSinglesProcessed.load(std::memory_order_relaxed);
        const uint64_t totalPromptPairs = stats.totalPromptPairs.load(std::memory_order_relaxed);
        const uint64_t totalDelayPairs = stats.totalDelayPairs.load(std::memory_order_relaxed);
        const auto memStatus = coinNode.aligner().getMemoryStatus();
        const double memUsedMB = static_cast<double>(memStatus.usedBytes) / (1024.0 * 1024.0);
        const double memMaxMB = static_cast<double>(memStatus.maxBytes) / (1024.0 * 1024.0);
        const double memUsagePct = memStatus.usageRatio * 100.0;

        const auto nowTick = std::chrono::steady_clock::now();
        const double dtSec = std::max(
            1e-6,
            std::chrono::duration<double>(nowTick - lastTick).count());

        double acqSpeedMpps = 0.0;      // 所有采集子节点当前瞬时包速率之和，单位 Mpps
        double acqBandwidthMbps = 0.0;  // 所有采集子节点当前瞬时带宽之和，单位 Mbps
        uint64_t acqTotalRxPackets = 0; // 所有采集子节点累计接收包数总和
        if (acqMasterStarted)
        {
            for (const auto &node : acqMaster.SnapshotNodes())
            {
                acqSpeedMpps += node.lastStatus.current_speed_mpps();
                acqBandwidthMbps += node.lastStatus.current_bandwidth_mbps();
                acqTotalRxPackets += node.lastStatus.total_rx_packets();
            }
        }

        const double recvRateMps = static_cast<double>(totalSinglesReceived - lastSinglesReceived) / dtSec / 1e6;
        const double procRateMps = static_cast<double>(totalSinglesProcessed - lastSinglesProcessed) / dtSec / 1e6;
        const double promptRateMps = static_cast<double>(totalPromptPairs - lastPromptPairs) / dtSec / 1e6;
        const double delayRateMps = static_cast<double>(totalDelayPairs - lastDelayPairs) / dtSec / 1e6;
        const double acqRxRateMpps = static_cast<double>(acqTotalRxPackets - lastAcqRxPackets) / dtSec / 1e6;
        const uint64_t windowsProcessed = stats.chunksProcessed.load(std::memory_order_relaxed);
        const double avgProcMs = stats.avgProcessingTime_ms.load(std::memory_order_relaxed);

        coincidence::StatusResponse status;
        const bool haveNodeStatus = coinNode.copyStatus(&status);
        const uint32_t producersCompleteCount =
            haveNodeStatus ? status.producers_complete_count() : 0;

        std::cout << "[CoinMaster status] connected=" << coinNode.connectedNodeCount() << "/" << coinNode.expectedNodeCount()
                  << " dataplaneOpen=" << coinNode.dataplaneOpenCount()
                  << " startIssued=" << (coinNode.startSignalIssued() ? "true" : "false")
                  << " complete=" << producersCompleteCount << "/" << coinNode.expectedNodeCount()
                  << " producersComplete=" << (coinNode.allProducersComplete() ? "true" : "false")
                  << " windows=" << windowsProcessed
                  << " totalSinglesReceived=" << totalSinglesReceived
                  << " singlesProcessed=" << totalSinglesProcessed
                  << " promptPairs=" << totalPromptPairs
                  << " delayPairs=" << totalDelayPairs
                  << std::fixed << std::setprecision(3)
                  << std::endl;
        if (acqMasterStarted)
        {
            std::cout << "[CoinMaster Total_acq_msg] acqRxRateMpps=" << acqRxRateMpps
                      << " acqSpeedMpps=" << acqSpeedMpps
                      << " acqBandwidthMbps=" << acqBandwidthMbps
                      << std::fixed << std::setprecision(3)
                      << std::endl;
        }
        std::cout << "[CoinMaster Stream_coin] recvRateMSingles=" << recvRateMps
                  << " procRateMSingles=" << procRateMps
                  << " promptRateMPairs=" << promptRateMps
                  << " delayRateMPairs=" << delayRateMps
                  << " avgProcMs=" << avgProcMs
                  << " memUsedMB=(" << memUsedMB
                  << " / " << memMaxMB << ")"
                  << " memUsagePct=" << memUsagePct << "% "
                  << std::fixed << std::setprecision(3)
                  << std::endl;

        if (haveNodeStatus)
        {
            std::cout << "[CoinMaster nodes] id state live rtt_ms age_ms sent_k/s pending credit/slots buf lag r2s_k/s acq_k/s"
                      << std::endl;
            for (const auto &node : status.node_stats())
            {
                const uint64_t prevSent = lastNodeSent[node.node_id()];
                const uint64_t prevR2s = lastNodeR2s[node.node_id()];
                const uint64_t prevAcq = lastNodeAcq[node.node_id()];
                const double sentRate = static_cast<double>(node.singles_sent_heartbeat() - prevSent) / dtSec / 1e3;
                const double r2sRate = static_cast<double>(node.r2s_singles_out() - prevR2s) / dtSec / 1e3;
                const double acqRate = static_cast<double>(node.acq_packets_total() - prevAcq) / dtSec / 1e3;
                lastNodeSent[node.node_id()] = node.singles_sent_heartbeat();
                lastNodeR2s[node.node_id()] = node.r2s_singles_out();
                lastNodeAcq[node.node_id()] = node.acq_packets_total();
                const int64_t lag = static_cast<int64_t>(node.singles_sent_heartbeat()) -
                                    static_cast<int64_t>(node.singles_received());
                std::cout << "  n" << node.node_id()
                          << " " << sourceStateName(node.source_state())
                          << " live=" << (node.connected() ? "y" : "n")
                          << " rtt=" << node.last_rtt_ms()
                          << " age=" << node.last_heartbeat_age_ms()
                          << std::fixed << std::setprecision(2)
                          << " sent=" << sentRate
                          << " pend=" << node.chunks_pending() << "/" << node.chunks_pending_cap()
                          << " rdma=" << node.rdma_credit_remaining() << "/" << node.rdma_slot_count()
                          << " buf=" << node.buffer_size() << "/" << cfg.aligner.maxChunksPerNode
                          << " lag=" << lag
                          << " r2s=" << r2sRate
                          << " acq=" << acqRate
                          << (node.acq_running() ? " acqOn" : "");
                if (node.r2s_lease_cap() > 0)
                {
                    std::cout << " lease=" << node.r2s_lease_used() << "/" << node.r2s_lease_cap();
                }
                std::cout << std::endl;
            }
        }

        lastTick = nowTick;
        lastSinglesReceived = totalSinglesReceived;
        lastSinglesProcessed = totalSinglesProcessed;
        lastPromptPairs = totalPromptPairs;
        lastDelayPairs = totalDelayPairs;
        lastAcqRxPackets = acqTotalRxPackets;

        if (acqMasterStarted)
        {
            const size_t acqConnected = acqMaster.ConnectedNodeCount();
            if (!taskDistributed &&
                cfg.acquisitionControl.autoDistributeWhenAllConnected &&
                acqConnected >= cfg.coinMaster.expectedNodeCount)
            {
                acqMaster.DistributeTasks();
                taskDistributed = true;
                std::cout << "[CoinMaster] Acquisition tasks distributed" << std::endl;
            }

            if (!startSent &&
                cfg.acquisitionControl.autoStartOnCoinStartSignal &&
                taskDistributed &&
                coinNode.startSignalIssued())
            {
                acqMaster.SendStart(coinNode.plannedStartTimeMs(), cfg.acquisitionControl.startDurationMs);
                startSent = true;
                std::cout << "[CoinMaster] Acquisition START sent at plannedStartMs=" << coinNode.plannedStartTimeMs() << std::endl;
            }
        }

        if (coinNode.allProducersComplete())
        {
            std::cout << "[CoinMaster] all producers complete, stopping..." << std::endl;
            break;
        }

        if (cfg.coinMaster.runSeconds > 0)
        {
            const auto elapsedSec = std::chrono::duration_cast<std::chrono::seconds>(
                                        std::chrono::steady_clock::now() - t0)
                                        .count();
            if (elapsedSec >= cfg.coinMaster.runSeconds)
            {
                std::cout << "[CoinMaster] runSeconds reached, stopping..." << std::endl;
                break;
            }
        }
    }

    if (acqMasterStarted)
    {
        acqMaster.SendStop("coin-master-stopping");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        acqMaster.SendShutdown("coin-master-shutdown");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        acqMaster.StopServer();
    }

    coinNode.stop();

    const auto &stats = coinNode.statistics();
    std::cout << "[CoinMaster] Final statistics:"
              << " singles_received=" << stats.totalSinglesReceived.load(std::memory_order_relaxed)
              << " singles_processed=" << stats.totalSinglesProcessed.load(std::memory_order_relaxed)
              << " prompt_pairs=" << stats.totalPromptPairs.load(std::memory_order_relaxed)
              << " delay_pairs=" << stats.totalDelayPairs.load(std::memory_order_relaxed)
              << std::endl;

    return 0;
}
