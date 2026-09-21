#include <arpa/inet.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>

#include <glog/logging.h>

#include "grpcService/AcquisitionMaster.hpp"
#include "tests/performance/dpdk_acq/DpdkAcqTestConfig.hpp"

namespace
{
    namespace acq = openpni::distributed::acquisition;
    namespace testcfg = openpni::distributed::test::dpdk_acq;

    std::atomic<bool> g_stop{false};

    void onSignal(int)
    {
        g_stop.store(true);
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

    const char *stateName(acq::NodeState state)
    {
        switch (state)
        {
        case acq::STATE_IDLE:
            return "idle";
        case acq::STATE_CONFIGURED:
            return "configured";
        case acq::STATE_RUNNING:
            return "running";
        case acq::STATE_ERROR:
            return "error";
        default:
            return "?";
        }
    }

    void printUsage(const char *prog)
    {
        std::cout
            << "Usage: " << prog << " --config <path> [--duration-sec N]\n"
            << "Slim AcquisitionMaster for DPDK-only acquisition tests.\n"
            << "Without --config, prints SKIP and exits 0 (ctest placeholder).\n";
    }

    bool fillTask(const testcfg::AcquisitionSection &c, acq::AcquisitionTask *task, std::string *err)
    {
        task->set_algorithm_type(acq::ALGORITHM_TYPE_DPDK);
        task->set_storage_unit_size(c.storageUnitSize);
        task->set_min_packet_size(c.minPacketSize);
        task->set_max_buffer_size(c.maxBufferSize);
        task->set_time_switch_buffer_ms(c.timeSwitchBufferMs);
        task->set_session_name(c.sessionName);

        task->clear_detector_sources();
        if (!c.detectorSources.empty())
        {
            for (size_t i = 0; i < c.detectorSources.size(); ++i)
            {
                const auto &det = c.detectorSources[i];
                if (ipToInt(det.sourceIp) == 0)
                {
                    if (err)
                    {
                        *err = "invalid detectorSources.sourceIp: " + det.sourceIp;
                    }
                    return false;
                }
                auto *source = task->add_detector_sources();
                source->set_detector_id(det.detectorId.empty() ? ("detector-" + std::to_string(i)) : det.detectorId);
                source->set_ip_source(ipToInt(det.sourceIp));
                source->set_port_source(det.sourcePort);
            }
        }
        else
        {
            if (ipToInt(c.sourceIp) == 0)
            {
                if (err)
                {
                    *err = "invalid acquisition.sourceIp: " + c.sourceIp;
                }
                return false;
            }
            for (uint16_t i = 0; i < c.channelCount; ++i)
            {
                auto *source = task->add_detector_sources();
                source->set_detector_id("detector-" + std::to_string(i));
                source->set_ip_source(ipToInt(c.sourceIp));
                source->set_port_source(static_cast<uint32_t>(c.sourcePortBase + i));
            }
        }

        if (ipToInt(c.destinationIp) == 0)
        {
            if (err)
            {
                *err = "invalid acquisition.destinationIp: " + c.destinationIp;
            }
            return false;
        }
        auto *dest = task->mutable_destination_rule();
        dest->set_ip_destination(ipToInt(c.destinationIp));
        dest->set_port_destination_base(c.destinationPortBase);
        dest->set_port_destination_stride(1);

        auto *dpdk = task->mutable_dpdk_options();
        dpdk->set_rx_rings_per_port(c.dpdkRxRingsPerPort);
        dpdk->set_mbuf_pool_size(c.dpdkMbufPoolSize);
        dpdk->set_mbuf_cache_size(c.dpdkMbufCacheSize);
        dpdk->set_local_loopback_iface(c.dpdkLocalLoopbackIface);
        dpdk->clear_bind_ips();
        for (const auto &ip : c.dpdkBindIps)
        {
            dpdk->add_bind_ips(ip);
        }
        dpdk->clear_extra_eal_args();
        for (const auto &arg : c.dpdkExtraEalArgs)
        {
            dpdk->add_extra_eal_args(arg);
        }
        return true;
    }
} // namespace

int main(int argc, char **argv)
{
    if (argc <= 1)
    {
        std::cout << "SKIP: dual-machine DPDK acquisition test; use tests/performance/dpdk_acq/run_rx.sh"
                  << std::endl;
        return 0;
    }

    google::InitGoogleLogging(argv[0]);
    std::atexit([]()
                { google::ShutdownGoogleLogging(); });

    std::string configPath;
    uint32_t durationSec = 0;
    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        if (arg == "--help")
        {
            printUsage(argv[0]);
            return 0;
        }
        if (arg == "--config")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "Missing value for --config" << std::endl;
                return 1;
            }
            configPath = argv[++i];
            continue;
        }
        if (arg == "--duration-sec")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "Missing value for --duration-sec" << std::endl;
                return 1;
            }
            durationSec = static_cast<uint32_t>(std::stoul(argv[++i]));
            continue;
        }
        std::cerr << "Unknown argument: " << arg << std::endl;
        return 1;
    }

    if (configPath.empty())
    {
        printUsage(argv[0]);
        return 1;
    }

    testcfg::DpdkAcqTestConfig cfg;
    std::string err;
    if (!testcfg::loadDpdkAcqTestConfig(configPath, &cfg, &err))
    {
        std::cerr << "[DPDK-ACQ-MASTER] config load failed: " << err << std::endl;
        return 2;
    }

    acq::AcquisitionTask globalTask;
    if (!fillTask(cfg.acquisition, &globalTask, &err))
    {
        std::cerr << "[DPDK-ACQ-MASTER] invalid acquisition task: " << err << std::endl;
        return 2;
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    acq::AcquisitionMaster master;
    master.Initialize(globalTask);
    master.StartServer(cfg.master.listenAddress);

    std::cout << "[DPDK-ACQ-MASTER] listen=" << cfg.master.listenAddress
              << " expectedNodes=" << cfg.master.expectedNodeCount
              << " channels=" << globalTask.detector_sources_size()
              << " rings=" << cfg.acquisition.dpdkRxRingsPerPort
              << std::endl;

    if (!master.WaitForConnectedNodes(cfg.master.expectedNodeCount, cfg.master.waitConnectTimeoutMs))
    {
        std::cerr << "[DPDK-ACQ-MASTER] wait for nodes timed out, connected="
                  << master.ConnectedNodeCount() << std::endl;
        master.SendShutdown("wait-timeout");
        master.StopServer();
        return 3;
    }

    master.DistributeTasks();
    std::cout << "[DPDK-ACQ-MASTER] tasks distributed" << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    master.SendStart(0, cfg.master.startDurationMs);
    std::cout << "[DPDK-ACQ-MASTER] START sent durationMs=" << cfg.master.startDurationMs << std::endl;

    const auto t0 = std::chrono::steady_clock::now();
    auto lastTick = t0;
    uint64_t lastRxPackets = 0;

    while (!g_stop.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.master.statusIntervalMs));
        const auto now = std::chrono::steady_clock::now();
        const double dtSec = std::max(1e-6, std::chrono::duration<double>(now - lastTick).count());

        double speedMpps = 0.0;
        double bandwidthMbps = 0.0;
        uint64_t totalRx = 0;
        uint64_t unknown = 0;
        uint64_t imissed = 0;
        uint64_t ierrors = 0;
        uint64_t bufferUsed = 0;
        uint64_t bufferVolume = 0;
        float bufferPct = 0.0f;

        const auto nodes = master.SnapshotNodes();
        for (const auto &node : nodes)
        {
            speedMpps += node.lastStatus.current_speed_mpps();
            bandwidthMbps += node.lastStatus.current_bandwidth_mbps();
            totalRx += node.lastStatus.total_rx_packets();
            unknown += node.lastStatus.error_packets();
            imissed += node.lastStatus.imissed_packets();
            ierrors += node.lastStatus.ierrors_packets();
            bufferUsed += node.lastStatus.buffer_used();
            bufferVolume += node.lastStatus.buffer_volume();
            bufferPct = std::max(bufferPct, node.lastStatus.buffer_usage_percent());

            std::cout << "[DPDK-ACQ-MASTER] node=" << node.info.node_id()
                      << " state=" << stateName(node.lastStatus.state())
                      << " speed_mpps=" << std::fixed << std::setprecision(4) << node.lastStatus.current_speed_mpps()
                      << " bandwidth_MBps=" << std::setprecision(2) << node.lastStatus.current_bandwidth_mbps()
                      << " total_rx_packets=" << node.lastStatus.total_rx_packets()
                      << " unknown=" << node.lastStatus.error_packets()
                      << " imissed=" << node.lastStatus.imissed_packets()
                      << " ierrors=" << node.lastStatus.ierrors_packets()
                      << " buffer_used=" << node.lastStatus.buffer_used()
                      << " buffer_pct=" << std::setprecision(1) << node.lastStatus.buffer_usage_percent()
                      << std::defaultfloat
                      << std::endl;
        }

        const double rxRateMpps = static_cast<double>(totalRx - lastRxPackets) / dtSec / 1e6;
        std::cout << "[DPDK-ACQ-MASTER] sum speed_mpps=" << std::fixed << std::setprecision(4) << speedMpps
                  << " rxRateMpps=" << rxRateMpps
                  << " bandwidth_MBps=" << std::setprecision(2) << bandwidthMbps
                  << " total_rx_packets=" << totalRx
                  << " unknown=" << unknown
                  << " imissed=" << imissed
                  << " ierrors=" << ierrors
                  << " buffer_used=" << bufferUsed
                  << "/" << bufferVolume
                  << " buffer_pct=" << std::setprecision(1) << bufferPct
                  << std::defaultfloat
                  << std::endl;

        lastTick = now;
        lastRxPackets = totalRx;

        if (durationSec > 0)
        {
            const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - t0).count();
            if (elapsed >= static_cast<int64_t>(durationSec))
            {
                break;
            }
        }
    }

    master.SendStop("dpdk-acq-test-done");
    std::this_thread::sleep_for(std::chrono::milliseconds(cfg.master.shutdownGraceMs));
    master.SendShutdown("dpdk-acq-test-done");
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    master.StopServer();
    std::cout << "[DPDK-ACQ-MASTER] stopped total_rx_packets=" << lastRxPackets << std::endl;
    return 0;
}
