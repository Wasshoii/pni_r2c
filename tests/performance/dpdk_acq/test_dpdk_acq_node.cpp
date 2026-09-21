#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#include <glog/logging.h>
#include <sched.h>

#include "grpcNode/acquisitionNode.hpp"
#include "tests/performance/dpdk_acq/DpdkAcqTestConfig.hpp"

namespace
{
    namespace grpcnode = openpni::distributed::grpcnode;
    namespace acqproto = openpni::distributed::acquisition;
    namespace testcfg = openpni::distributed::test::dpdk_acq;

    std::atomic<bool> g_stop{false};

    void onSignal(int)
    {
        g_stop.store(true);
    }

    const char *stateName(acqproto::NodeState state)
    {
        switch (state)
        {
        case acqproto::STATE_IDLE:
            return "idle";
        case acqproto::STATE_CONFIGURED:
            return "configured";
        case acqproto::STATE_RUNNING:
            return "running";
        case acqproto::STATE_ERROR:
            return "error";
        default:
            return "?";
        }
    }

    void printUsage(const char *prog)
    {
        std::cout
            << "Usage: " << prog << " --config <path> [--write-raw]\n"
            << "Slim AcquisitionGrpcNode for DPDK-only acquisition tests.\n"
            << "Without --config, prints SKIP and exits 0 (ctest placeholder).\n";
    }

    bool applyCpuAffinity(const testcfg::NodeSection &node, std::string *errorMessage)
    {
        if (!node.enableCpuAffinity)
        {
            return true;
        }

        cpu_set_t cpuSet;
        CPU_ZERO(&cpuSet);
        for (uint16_t core : node.cpuAffinityCores)
        {
            if (core >= CPU_SETSIZE)
            {
                if (errorMessage)
                {
                    *errorMessage = "cpuAffinityCores contains core >= CPU_SETSIZE";
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
        oss << "[DPDK-ACQ-NODE] CPU affinity: ";
        for (size_t i = 0; i < node.cpuAffinityCores.size(); ++i)
        {
            if (i > 0)
            {
                oss << ",";
            }
            oss << node.cpuAffinityCores[i];
        }
        std::cout << oss.str() << std::endl;
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
    bool writeRawOverride = false;
    bool haveWriteRaw = false;
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
        if (arg == "--write-raw")
        {
            writeRawOverride = true;
            haveWriteRaw = true;
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
        std::cerr << "[DPDK-ACQ-NODE] config load failed: " << err << std::endl;
        return 2;
    }

    if (!applyCpuAffinity(cfg.node, &err))
    {
        std::cerr << "[DPDK-ACQ-NODE] " << err << std::endl;
        return 2;
    }

    grpcnode::AcquisitionGrpcNode::InitOptions opt;
    opt.masterAddress = cfg.node.masterAddress;
    opt.nodeId = cfg.node.nodeId;
    opt.nodeAddress = cfg.node.nodeAddress;
    opt.outputRoot = cfg.node.outputRoot;
    opt.sessionNamePrefix = cfg.node.sessionNamePrefix;
    opt.maxFileSizeMb = cfg.node.maxFileSizeMb;
    opt.overwriteExisting = cfg.node.overwriteExisting;
    opt.reservedStorageGiB = cfg.node.reservedStorageGiB;
    opt.enableRawFileWrite = haveWriteRaw ? writeRawOverride : cfg.node.enableRawFileWrite;
    opt.fsyncEachSegment = cfg.node.fsyncEachSegment;
    opt.strictBindIpsOwnershipCheck = cfg.node.strictBindIpsOwnershipCheck;
    opt.strictNumaTopologyCheck = cfg.node.strictNumaTopologyCheck;
    opt.requireBindIpsSingleNuma = cfg.node.requireBindIpsSingleNuma;
    opt.requireCpuAffinityOnNuma = cfg.node.requireCpuAffinityOnNuma;
    opt.expectedNumaNode = cfg.node.expectedNumaNode;
    opt.cpuAffinityCores.assign(cfg.node.cpuAffinityCores.begin(), cfg.node.cpuAffinityCores.end());
    opt.statusIntervalMs = cfg.master.statusIntervalMs == 0 ? 1000 : cfg.master.statusIntervalMs;

    std::cout << "[DPDK-ACQ-NODE] master=" << opt.masterAddress
              << " nodeId=" << opt.nodeId
              << " writeRaw=" << (opt.enableRawFileWrite ? "true" : "false")
              << " outputRoot=" << opt.outputRoot
              << std::endl;

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    grpcnode::AcquisitionGrpcNode node(opt);
    std::atomic<bool> runOk{false};
    std::atomic<bool> done{false};
    std::thread runner(
        [&]()
        {
            runOk.store(node.run(), std::memory_order_relaxed);
            done.store(true, std::memory_order_relaxed);
        });

    const auto statusInterval = std::chrono::milliseconds(opt.statusIntervalMs);
    auto lastPrint = std::chrono::steady_clock::now();
    while (!g_stop.load(std::memory_order_relaxed) && !done.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        const auto now = std::chrono::steady_clock::now();
        if (now - lastPrint < statusInterval)
        {
            continue;
        }
        lastPrint = now;
        const auto st = node.lastStatus();
        std::cout << "[DPDK-ACQ-NODE] state=" << stateName(st.state())
                  << " speed_mpps=" << std::fixed << std::setprecision(4) << st.current_speed_mpps()
                  << " bandwidth_MBps=" << std::setprecision(2) << st.current_bandwidth_mbps()
                  << " total_rx_packets=" << st.total_rx_packets()
                  << " unknown=" << st.error_packets()
                  << " imissed=" << st.imissed_packets()
                  << " ierrors=" << st.ierrors_packets()
                  << " buffer_used=" << st.buffer_used()
                  << " buffer_pct=" << std::setprecision(1) << st.buffer_usage_percent()
                  << std::defaultfloat
                  << std::endl;
    }

    if (g_stop.load(std::memory_order_relaxed) && !done.load(std::memory_order_relaxed))
    {
        std::cout << "[DPDK-ACQ-NODE] stop signal, requesting node.stop()" << std::endl;
        node.stop();
    }

    if (runner.joinable())
    {
        runner.join();
    }

    std::cout << "[DPDK-ACQ-NODE] exit ok=" << (runOk.load() ? "true" : "false") << std::endl;
    return runOk.load() ? 0 : 5;
}
