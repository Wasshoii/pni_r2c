#include <pni/PnI-Config.hpp>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "distributed/AcquisitionMaster.hpp"
#include "grpcNode/acquisitionNode.hpp"

namespace
{
    namespace fs = std::filesystem;
    namespace acq = openpni::distributed::acquisition;
    namespace grpcnode = openpni::distributed::grpcnode;

    struct UdpChannel
    {
        uint16_t sourcePort = 0;
        uint16_t destinationPort = 0;
    };

    uint64_t nowMs()
    {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
    }

    uint32_t ipToInt(const std::string &ip)
    {
        std::stringstream ss(ip);
        std::string token;
        uint32_t result = 0;

        for (int i = 0; i < 4; ++i)
        {
            if (!std::getline(ss, token, '.'))
            {
                return 0;
            }
            const uint32_t octet = static_cast<uint32_t>(std::stoul(token));
            if (octet > 255)
            {
                return 0;
            }
            result = (result << 8) | octet;
        }

        return result;
    }

    std::string stateToString(acq::NodeState state)
    {
        switch (state)
        {
        case acq::STATE_IDLE:
            return "IDLE";
        case acq::STATE_CONFIGURED:
            return "CONFIGURED";
        case acq::STATE_RUNNING:
            return "RUNNING";
        case acq::STATE_ERROR:
            return "ERROR";
        default:
            return "UNKNOWN";
        }
    }

    class UdpSimulator
    {
    public:
        UdpSimulator(
            std::string sourceIp,
            std::string destinationIp,
            std::vector<UdpChannel> channels,
            size_t payloadBytes,
            uint32_t sendIntervalUs)
            : m_sourceIp(std::move(sourceIp)),
              m_destinationIp(std::move(destinationIp)),
              m_channels(std::move(channels)),
              m_payloadBytes(payloadBytes),
              m_sendIntervalUs(sendIntervalUs)
        {
        }

        ~UdpSimulator()
        {
            stop();
        }

        void start()
        {
            if (m_running.exchange(true, std::memory_order_acq_rel))
            {
                return;
            }

            m_threads.clear();
            m_threads.reserve(m_channels.size());
            for (size_t i = 0; i < m_channels.size(); ++i)
            {
                m_threads.emplace_back([this, i]()
                                       { senderLoop(i); });
            }
        }

        void stop()
        {
            if (!m_running.exchange(false, std::memory_order_acq_rel))
            {
                return;
            }

            for (auto &t : m_threads)
            {
                if (t.joinable())
                {
                    t.join();
                }
            }
            m_threads.clear();
        }

        uint64_t sentPackets() const
        {
            return m_sentPackets.load(std::memory_order_relaxed);
        }

        uint64_t sentBytes() const
        {
            return m_sentBytes.load(std::memory_order_relaxed);
        }

    private:
        void senderLoop(size_t channelIndex)
        {
            const auto &ch = m_channels[channelIndex];

            const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
            if (fd < 0)
            {
                std::cerr << "[UdpSimulator] failed to create socket for channel " << channelIndex << std::endl;
                return;
            }

            sockaddr_in srcAddr{};
            srcAddr.sin_family = AF_INET;
            srcAddr.sin_port = htons(ch.sourcePort);
            if (::inet_pton(AF_INET, m_sourceIp.c_str(), &srcAddr.sin_addr) != 1)
            {
                std::cerr << "[UdpSimulator] invalid source ip: " << m_sourceIp << std::endl;
                ::close(fd);
                return;
            }

            if (::bind(fd, reinterpret_cast<const sockaddr *>(&srcAddr), sizeof(srcAddr)) != 0)
            {
                std::cerr << "[UdpSimulator] bind failed on source port " << ch.sourcePort << std::endl;
                ::close(fd);
                return;
            }

            sockaddr_in dstAddr{};
            dstAddr.sin_family = AF_INET;
            dstAddr.sin_port = htons(ch.destinationPort);
            if (::inet_pton(AF_INET, m_destinationIp.c_str(), &dstAddr.sin_addr) != 1)
            {
                std::cerr << "[UdpSimulator] invalid destination ip: " << m_destinationIp << std::endl;
                ::close(fd);
                return;
            }

            std::vector<uint8_t> payload(m_payloadBytes, 0);
            uint64_t seq = 0;

            while (m_running.load(std::memory_order_acquire))
            {
                // Keep payload deterministic while changing some bytes to emulate stream.
                if (!payload.empty())
                {
                    payload[0] = static_cast<uint8_t>(channelIndex & 0xFF);
                    if (payload.size() > 1)
                    {
                        payload[1] = static_cast<uint8_t>(seq & 0xFF);
                    }
                }

                const ssize_t written = ::sendto(
                    fd,
                    payload.data(),
                    payload.size(),
                    0,
                    reinterpret_cast<const sockaddr *>(&dstAddr),
                    sizeof(dstAddr));

                if (written > 0)
                {
                    m_sentPackets.fetch_add(1, std::memory_order_relaxed);
                    m_sentBytes.fetch_add(static_cast<uint64_t>(written), std::memory_order_relaxed);
                }

                ++seq;
                if (m_sendIntervalUs > 0)
                {
                    std::this_thread::sleep_for(std::chrono::microseconds(m_sendIntervalUs));
                }
            }

            ::close(fd);
        }

        std::string m_sourceIp;
        std::string m_destinationIp;
        std::vector<UdpChannel> m_channels;
        size_t m_payloadBytes = 1024;
        uint32_t m_sendIntervalUs = 0;

        std::atomic<bool> m_running{false};
        std::vector<std::thread> m_threads;

        std::atomic<uint64_t> m_sentPackets{0};
        std::atomic<uint64_t> m_sentBytes{0};
    };

    bool waitNodeState(
        acq::AcquisitionMaster &master,
        const std::string &nodeId,
        acq::NodeState targetState,
        uint32_t timeoutMs)
    {
        const auto begin = std::chrono::steady_clock::now();
        while (true)
        {
            const auto snapshot = master.SnapshotNodes();
            for (const auto &node : snapshot)
            {
                if (node.info.node_id() == nodeId)
                {
                    if (node.lastStatus.state() == targetState)
                    {
                        return true;
                    }
                }
            }

            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now() - begin)
                                     .count();
            if (elapsed >= timeoutMs)
            {
                return false;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    void fillAcquisitionTask(
        acq::AcquisitionTask &task,
        const std::string &sessionName,
        const std::string &sourceIp,
        const std::string &destinationIp,
        uint16_t sourcePortBase,
        uint16_t destinationPortBase,
        uint16_t channelCount)
    {
        task.set_algorithm_type(acq::ALGORITHM_TYPE_SOCKET);
        task.set_storage_unit_size(1024);
        task.set_min_packet_size(1024);
        task.set_max_buffer_size(4ull * 1024ull * 1024ull * 1024ull);
        task.set_time_switch_buffer_ms(1000);
        task.set_session_name(sessionName);
        task.set_reserved_storage_gib(20);
        task.set_max_file_size_mb(256);

        for (uint16_t i = 0; i < channelCount; ++i)
        {
            auto *source = task.add_detector_sources();
            source->set_detector_id("detector-" + std::to_string(i));
            source->set_ip_source(ipToInt(sourceIp));
            source->set_port_source(static_cast<uint32_t>(sourcePortBase + i));
        }

        auto *dest = task.mutable_destination_rule();
        dest->set_ip_destination(ipToInt(destinationIp));
        dest->set_port_destination_base(destinationPortBase);
        dest->set_port_destination_stride(1);

        // DPDK fields are filled for protocol completeness even in socket mode.
        auto *dpdk = task.mutable_dpdk_options();
        dpdk->set_copy_thread_num(8);
        dpdk->set_rx_rings_per_port(1);
        dpdk->set_rte_mbuf_double_pointer_size_multiply(32);
        dpdk->set_rte_mbuf_double_pointer_num_multiply(2);
    }

    size_t countRawFiles(const fs::path &sessionDir)
    {
        if (!fs::exists(sessionDir) || !fs::is_directory(sessionDir))
        {
            return 0;
        }

        size_t count = 0;
        for (const auto &entry : fs::directory_iterator(sessionDir))
        {
            if (!entry.is_regular_file())
            {
                continue;
            }

            if (entry.path().extension() == ".raw")
            {
                ++count;
            }
        }
        return count;
    }

} // namespace

int main()
{
    const std::string masterAddress = "127.0.0.1:50092";
    const std::string nodeId = "acq-node-udp-1";

    const std::string sourceIp = "127.0.0.1";
    const std::string destinationIp = "127.0.0.1";
    const uint16_t sourcePortBase = 17000;
    const uint16_t destinationPortBase = 18000;
    const uint16_t channelCount = 4;

    const std::string sessionName = "acq_udp_datapath_" + std::to_string(nowMs());

    acq::AcquisitionTask globalTask;
    fillAcquisitionTask(
        globalTask,
        sessionName,
        sourceIp,
        destinationIp,
        sourcePortBase,
        destinationPortBase,
        channelCount);

    std::cout << "[DatapathTest] Session=" << sessionName
              << " channels=" << channelCount
              << " payload_bytes=1024"
              << std::endl;

    acq::AcquisitionMaster master;
    master.Initialize(globalTask);
    master.StartServer(masterAddress);

    grpcnode::AcquisitionGrpcNode::InitOptions nodeOpt;
    nodeOpt.masterAddress = masterAddress;
    nodeOpt.nodeId = nodeId;
    nodeOpt.nodeAddress = "127.0.0.1";
    nodeOpt.outputRoot = "Data/raw_data";
    nodeOpt.sessionNamePrefix = "datapath_node";
    nodeOpt.statusIntervalMs = 500;

    grpcnode::AcquisitionGrpcNode node(nodeOpt);
    std::thread nodeThread([&]()
                           {
                               const bool ok = node.run();
                               std::cout << "[DatapathTest] Node thread exit, ok=" << (ok ? "true" : "false") << std::endl; });

    const bool connected = master.WaitForConnectedNodes(1, 10000);
    std::cout << "[DatapathTest] Connected=" << (connected ? "true" : "false")
              << " count=" << master.ConnectedNodeCount() << std::endl;

    if (!connected)
    {
        master.SendShutdown("no-node-connected");
        master.StopServer();
        if (nodeThread.joinable())
        {
            nodeThread.join();
        }
        return 1;
    }

    master.DistributeTasks();
    const bool configured = waitNodeState(master, nodeId, acq::STATE_CONFIGURED, 8000);
    std::cout << "[DatapathTest] Node configured=" << (configured ? "true" : "false") << std::endl;

    std::vector<UdpChannel> channels;
    channels.reserve(channelCount);
    for (uint16_t i = 0; i < channelCount; ++i)
    {
        UdpChannel ch;
        ch.sourcePort = static_cast<uint16_t>(sourcePortBase + i);
        ch.destinationPort = static_cast<uint16_t>(destinationPortBase + i);
        channels.push_back(ch);
    }

    UdpSimulator simulator(sourceIp, destinationIp, channels, 1024, 1000);
    uint64_t observedRxPackets = 0;
    uint64_t observedRxBytes = 0;

    // Start packet generation, then start acquisition.
    simulator.start();
    const size_t startSentCount = master.SendStart(nowMs() + 300, 0);
    std::cout << "[DatapathTest] Start command sent to " << startSentCount << " node(s)" << std::endl;

    constexpr uint32_t sampleIntervalMs = 500;
    constexpr uint32_t realtimeLogIntervalMs = 2000;
    constexpr uint32_t runDurationMs = 8000;

    for (uint32_t elapsedMs = sampleIntervalMs; elapsedMs <= runDurationMs; elapsedMs += sampleIntervalMs)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(sampleIntervalMs));

        const auto runningSnapshot = master.SnapshotNodes();
        bool foundNode = false;
        acq::NodeState realtimeState = acq::STATE_UNKNOWN;
        double realtimeMpps = 0.0;
        double realtimeMbps = 0.0;
        float realtimeBufferPercent = 0.0f;
        uint64_t realtimeErrorPackets = 0;
        uint64_t realtimePackets = 0;
        uint64_t realtimeBytes = 0;

        for (const auto &nodeSnap : runningSnapshot)
        {
            if (nodeSnap.info.node_id() != nodeId)
            {
                continue;
            }

            foundNode = true;
            realtimeState = nodeSnap.lastStatus.state();
            realtimeMpps = nodeSnap.lastStatus.current_speed_mpps();
            realtimeMbps = nodeSnap.lastStatus.current_bandwidth_mbps();
            realtimeBufferPercent = nodeSnap.lastStatus.buffer_usage_percent();
            realtimeErrorPackets = nodeSnap.lastStatus.error_packets();
            realtimePackets = nodeSnap.lastStatus.total_rx_packets();
            realtimeBytes = nodeSnap.lastStatus.total_rx_bytes();

            observedRxPackets = std::max<uint64_t>(observedRxPackets, nodeSnap.lastStatus.total_rx_packets());
            observedRxBytes = std::max<uint64_t>(observedRxBytes, nodeSnap.lastStatus.total_rx_bytes());
        }

        if (elapsedMs % realtimeLogIntervalMs == 0)
        {
            if (foundNode)
            {
                std::ostringstream oss;
                oss << std::fixed << std::setprecision(3)
                    << "[DatapathTest/Realtime] t=" << (elapsedMs / 1000.0) << "s"
                    << " state=" << stateToString(realtimeState)
                    << " speed_mpps=" << realtimeMpps
                    << " bandwidth_mbps=" << realtimeMbps
                    << " rx_packets=" << realtimePackets
                    << " rx_bytes=" << realtimeBytes
                    << " buffer_usage_pct=" << realtimeBufferPercent
                    << " error_packets=" << realtimeErrorPackets;
                std::cout << oss.str() << std::endl;
            }
            else
            {
                std::cout << "[DatapathTest/Realtime] t=" << (elapsedMs / 1000.0)
                          << "s node status not found" << std::endl;
            }
        }
    }

    simulator.stop();
    const uint64_t sentPackets = simulator.sentPackets();
    const uint64_t sentBytes = simulator.sentBytes();

    const size_t stopSentCount = master.SendStop("udp-simulation-finished");
    std::cout << "[DatapathTest] Stop command sent to " << stopSentCount << " node(s)" << std::endl;

    const bool backToConfigured = waitNodeState(master, nodeId, acq::STATE_CONFIGURED, 8000);
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    uint64_t rxPackets = 0;
    uint64_t rxBytes = 0;
    std::string errorMessage;
    acq::NodeState finalState = acq::STATE_UNKNOWN;

    const auto snapshot = master.SnapshotNodes();
    for (const auto &nodeSnap : snapshot)
    {
        if (nodeSnap.info.node_id() != nodeId)
        {
            continue;
        }

        finalState = nodeSnap.lastStatus.state();
        rxPackets = nodeSnap.lastStatus.total_rx_packets();
        rxBytes = nodeSnap.lastStatus.total_rx_bytes();
        errorMessage = nodeSnap.lastStatus.error_message();
    }

    const fs::path sessionDir = fs::path("Data/raw_data") / sessionName;
    const size_t rawFileCount = countRawFiles(sessionDir);

    std::cout << "[DatapathTest] Sent packets=" << sentPackets
              << " bytes=" << sentBytes << std::endl;
    std::cout << "[DatapathTest] Observed while running: rx_packets=" << observedRxPackets
              << " rx_bytes=" << observedRxBytes << std::endl;
    std::cout << "[DatapathTest] Node state=" << stateToString(finalState)
              << " rx_packets=" << rxPackets
              << " rx_bytes=" << rxBytes
              << " err='" << errorMessage << "'"
              << std::endl;
    std::cout << "[DatapathTest] Raw session dir=" << sessionDir
              << " raw_file_count=" << rawFileCount << std::endl;

    master.SendShutdown("datapath-test-done");
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    master.StopServer();

    if (nodeThread.joinable())
    {
        nodeThread.join();
    }

    const bool success = connected && configured && backToConfigured &&
                         sentPackets > 0 && observedRxPackets > 0 && observedRxBytes > 0 && rawFileCount > 0;

    std::cout << "[DatapathTest] RESULT=" << (success ? "SUCCESS" : "FAILED") << std::endl;
    return success ? 0 : 1;
}
