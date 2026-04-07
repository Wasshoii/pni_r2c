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

    void fillAcquisitionTask(acq::AcquisitionTask *task, const appcfg::AcqControlSection &c)
    {
        task->set_algorithm_type(acq::ALGORITHM_TYPE_SOCKET); // 若需要使用DPDK，则改为ALGORITHM_TYPE_DPDK，并设置dpdk_options，后续计划改为可调配置
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

    appcfg::CoinMasterConfig cfg;
    std::string err;
    if (!appcfg::loadCoinMasterConfig(configPath, &cfg, &err))
    {
        std::cerr << "[CoinMaster] config load failed: " << err << std::endl;
        return 2;
    }

    std::cout << "===========================================" << std::endl;
    std::cout << "  App: Coin Master Node" << std::endl;
    std::cout << "===========================================" << std::endl;
    std::cout << "configPath               : " << configPath << std::endl;
    std::cout << "coin.listenAddress       : " << cfg.coinMaster.listenAddress << std::endl;
    std::cout << "coin.expectedNodeCount   : " << cfg.coinMaster.expectedNodeCount << std::endl;
    std::cout << "aligner.outputDir        : " << cfg.aligner.outputDir << std::endl;
    std::cout << "acqControl.enabled       : " << (cfg.acquisitionControl.enabled ? "true" : "false") << std::endl;
    std::cout << "acqControl.detectorSources: " << cfg.acquisitionControl.detectorSources.size() << std::endl;

    if (dryRun)
    {
        std::cout << "[CoinMaster] dry-run mode, exit." << std::endl;
        return 0;
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    streaming::TimeAlignerConfig alignerConfig;
    alignerConfig.outputDir = cfg.aligner.outputDir;
    alignerConfig.channelNum = cfg.aligner.channelNum;
    alignerConfig.crystalsPerChannel = cfg.aligner.crystalsPerChannel;
    alignerConfig.networkLatencyMargin_pico = cfg.aligner.networkLatencyMarginPico;
    alignerConfig.processingIntervalMs = cfg.aligner.processingIntervalMs;
    alignerConfig.maxChunksPerNode = cfg.aligner.maxChunksPerNode;
    alignerConfig.maxTotalMemoryBytes = cfg.aligner.maxTotalMemoryBytes;
    alignerConfig.useMemoryPool = cfg.aligner.useMemoryPool;
    alignerConfig.savePrompt = cfg.aligner.savePrompt;
    alignerConfig.saveDelay = cfg.aligner.saveDelay;

    alignerConfig.coinProtocol.timeWindow_ps = cfg.aligner.coinProtocol.timeWindowPs;
    alignerConfig.coinProtocol.delayTime_ps = cfg.aligner.coinProtocol.delayTimePs;
    alignerConfig.coinProtocol.energyLower_eV = cfg.aligner.coinProtocol.energyLowerEV;
    alignerConfig.coinProtocol.energyUpper_eV = cfg.aligner.coinProtocol.energyUpperEV;

    if (cfg.acquisitionControl.enabled)
    {
        const size_t effectiveMappedChannels =
            !cfg.acquisitionControl.detectorSources.empty()
                ? cfg.acquisitionControl.detectorSources.size()
                : static_cast<size_t>(cfg.acquisitionControl.channelCount);

        if (effectiveMappedChannels == 0)
        {
            std::cerr << "[CoinMaster] invalid acquisitionControl mapping: no detector sources configured" << std::endl;
            return 3;
        }

        if (effectiveMappedChannels > alignerConfig.channelNum)
        {
            std::cerr << "[CoinMaster] invalid mapping: detector source count=" << effectiveMappedChannels
                      << " exceeds aligner.channelNum=" << alignerConfig.channelNum
                      << ". Increase aligner.channelNum or reduce detectorSources." << std::endl;
            return 3;
        }
    }

    grpcnode::CoinGrpcNode::InitOptions coinInit;
    coinInit.alignerConfig = alignerConfig;
    coinInit.listenAddress = cfg.coinMaster.listenAddress;
    coinInit.expectedNodeCount = cfg.coinMaster.expectedNodeCount;
    coinInit.autoStartWhenAllRegistered = cfg.coinMaster.autoStartWhenAllRegistered;
    coinInit.startLeadTimeMs = cfg.coinMaster.startLeadTimeMs;
    coinInit.waitForStartDefaultTimeoutMs = cfg.coinMaster.waitForStartDefaultTimeoutMs;
    coinInit.rejectStreamBeforeStart = cfg.coinMaster.rejectStreamBeforeStart;

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
        fillAcquisitionTask(&globalTask, cfg.acquisitionControl);

        std::cout << "[CoinMaster] Acquisition mapping prepared: detector_sources="
                  << globalTask.detector_sources_size() << std::endl;

        acqMaster.Initialize(globalTask);
        acqMaster.StartServer(cfg.acquisitionControl.masterAddress);
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

        std::cout << "[CoinMaster status] connected=" << coinNode.connectedNodeCount() << "/" << coinNode.expectedNodeCount()
                  << " startIssued=" << (coinNode.startSignalIssued() ? "true" : "false")
                  << " totalSinglesReceived=" << totalSinglesReceived
                  << " singlesProcessed=" << totalSinglesProcessed
                  << " promptPairs=" << totalPromptPairs
                  << " delayPairs=" << totalDelayPairs
                  << std::fixed << std::setprecision(3)
                  << std::endl;
        std::cout << "[CoinMaster Total_acq_msg] acqRxRateMpps=" << acqRxRateMpps
                  << " acqSpeedMpps=" << acqSpeedMpps
                  << " acqBandwidthMbps=" << acqBandwidthMbps
                  << std::fixed << std::setprecision(3)
                  << std::endl;
        std::cout << "[CoinMaster Stream_coin] recvRateMSingles=" << recvRateMps
                  << " procRateMSingles=" << procRateMps
                  << " promptRateMPairs=" << promptRateMps
                  << " delayRateMPairs=" << delayRateMps
                  << " memUsedMB=(" << memUsedMB
                  << " / " << memMaxMB << ")"
                  << " memUsagePct=" << memUsagePct << "% "
                  << std::fixed << std::setprecision(3)
                  << std::endl;

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
