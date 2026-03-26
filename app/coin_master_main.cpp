#include <pni/PnI-Config.hpp>

#include <arpa/inet.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

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
        task->set_algorithm_type(acq::ALGORITHM_TYPE_SOCKET);
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

    while (!g_stopRequested.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.coinMaster.statusPrintIntervalMs));

        const auto &stats = coinNode.statistics();
        std::cout << "[CoinMaster] connected=" << coinNode.connectedNodeCount() << "/" << coinNode.expectedNodeCount()
                  << " startIssued=" << (coinNode.startSignalIssued() ? "true" : "false")
                  << " plannedStartMs=" << coinNode.plannedStartTimeMs()
                  << " totalSinglesReceived=" << stats.totalSinglesReceived.load(std::memory_order_relaxed)
                  << " promptPairs=" << stats.totalPromptPairs.load(std::memory_order_relaxed)
                  << " delayPairs=" << stats.totalDelayPairs.load(std::memory_order_relaxed)
                  << std::endl;

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
