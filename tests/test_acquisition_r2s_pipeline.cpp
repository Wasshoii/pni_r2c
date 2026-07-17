#include <pni/PnI-Config.hpp>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <pni/io/IO.hpp>

#include "core/r2s/R2S.hpp"
#include "grpcService/AcquisitionMaster.hpp"
#include "grpcNode/acquisitionNode.hpp"

namespace
{
    namespace fs = std::filesystem;
    namespace acq = openpni::distributed::acquisition;
    namespace grpcnode = openpni::distributed::grpcnode;
    namespace r2s = openpni::distributed::r2s;

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

    struct SinglesDigest
    {
        uint64_t callbacks = 0;
        uint64_t totalSingles = 0;
        uint64_t checksumXor = 0;
    };

    class SinglesAccumulator
    {
    public:
        bool onSingles(std::span<r2s::Single const> singles)
        {
            std::vector<r2s::Single> hostSingles;
            try
            {
                hostSingles = r2s::materializeSinglesOnHost(singles);
            }
            catch (const std::exception &e)
            {
                std::cerr << "[SinglesAccumulator] materialize failed: " << e.what() << std::endl;
                return false;
            }

            uint64_t segmentXor = 0;
            for (const auto &s : hostSingles)
            {
                segmentXor ^= hashSingle(s);
            }

            std::lock_guard<std::mutex> lock(m_mutex);
            m_digest.callbacks += 1;
            m_digest.totalSingles += static_cast<uint64_t>(hostSingles.size());
            m_digest.checksumXor ^= segmentXor;
            return true;
        }

        SinglesDigest snapshot() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_digest;
        }

    private:
        static uint64_t hashSingle(const r2s::Single &s)
        {
            uint32_t energyBits = 0;
            std::memcpy(&energyBits, &s.energy, sizeof(energyBits));

            uint64_t h = 1469598103934665603ULL;
            auto mix = [&h](uint64_t v)
            {
                h ^= v;
                h *= 1099511628211ULL;
            };

            mix(static_cast<uint64_t>(s.channelIndex));
            mix(static_cast<uint64_t>(s.crystalIndex));
            mix(static_cast<uint64_t>(s.timevalue_100fs));
            mix(static_cast<uint64_t>(energyBits));
            return h;
        }

        mutable std::mutex m_mutex;
        SinglesDigest m_digest;
    };

    struct ReplayResult
    {
        bool success = false;
        uint64_t sentPackets = 0;
        uint64_t sentBytes = 0;
        uint64_t skippedPackets = 0;
        uint32_t replayedSegments = 0;
    };

    class RawFileUdpReplayer
    {
    public:
        RawFileUdpReplayer(
            std::string rawPath,
            std::string sourceIp,
            std::string destinationIp,
            uint16_t sourcePortBase,
            uint16_t destinationPortBase,
            uint16_t channelCount)
            : m_rawPath(std::move(rawPath)),
              m_sourceIp(std::move(sourceIp)),
              m_destinationIp(std::move(destinationIp)),
              m_sourcePortBase(sourcePortBase),
              m_destinationPortBase(destinationPortBase),
              m_channelCount(channelCount)
        {
        }

        ReplayResult replay(uint32_t maxSegments, uint32_t interPacketUs)
        {
            ReplayResult result;

            try
            {
                openpni::io::v1::RawFileInput input;
                input.open(m_rawPath);

                const auto header = input.header();
                const uint32_t segmentsToReplay = std::min<uint32_t>(header.segmentNum, maxSegments);

                std::vector<int> sockets(m_channelCount, -1);
                std::vector<sockaddr_in> destinations(m_channelCount);

                if (!createSockets(sockets, destinations))
                {
                    closeSockets(sockets);
                    return result;
                }

                for (uint32_t seg = 0; seg < segmentsToReplay; ++seg)
                {
                    auto segment = input.readSegment(seg, seg + 1);
                    auto segHeader = input.segmentHeader(seg);
                    auto view = segment.view(header, segHeader);

                    if (!view.data || !view.length || !view.offset || !view.channel)
                    {
                        continue;
                    }

                    for (uint64_t i = 0; i < view.count; ++i)
                    {
                        const uint16_t ch = view.channel[i];
                        if (ch >= m_channelCount)
                        {
                            result.skippedPackets += 1;
                            continue;
                        }

                        const uint16_t packetLength = view.length[i];
                        const uint64_t packetOffset = view.offset[i];
                        const uint8_t *payload = view.data + packetOffset;

                        const ssize_t written = ::sendto(
                            sockets[ch],
                            payload,
                            packetLength,
                            0,
                            reinterpret_cast<const sockaddr *>(&destinations[ch]),
                            sizeof(sockaddr_in));

                        if (written > 0)
                        {
                            result.sentPackets += 1;
                            result.sentBytes += static_cast<uint64_t>(written);
                        }
                        else
                        {
                            std::cerr << "[RawFileUdpReplayer] sendto failed at segment=" << seg
                                      << " packet=" << i << " channel=" << ch << std::endl;
                            closeSockets(sockets);
                            return result;
                        }

                        if (interPacketUs > 0)
                        {
                            std::this_thread::sleep_for(std::chrono::microseconds(interPacketUs));
                        }
                    }

                    result.replayedSegments += 1;
                }

                closeSockets(sockets);
                result.success = true;
                return result;
            }
            catch (const std::exception &e)
            {
                std::cerr << "[RawFileUdpReplayer] exception: " << e.what() << std::endl;
                return result;
            }
        }

    private:
        bool createSockets(std::vector<int> &sockets, std::vector<sockaddr_in> &destinations)
        {
            for (uint16_t ch = 0; ch < m_channelCount; ++ch)
            {
                const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
                if (fd < 0)
                {
                    std::cerr << "[RawFileUdpReplayer] socket create failed, channel=" << ch << std::endl;
                    return false;
                }

                sockaddr_in srcAddr{};
                srcAddr.sin_family = AF_INET;
                srcAddr.sin_port = htons(static_cast<uint16_t>(m_sourcePortBase + ch));
                if (::inet_pton(AF_INET, m_sourceIp.c_str(), &srcAddr.sin_addr) != 1)
                {
                    std::cerr << "[RawFileUdpReplayer] invalid source ip: " << m_sourceIp << std::endl;
                    ::close(fd);
                    return false;
                }

                if (::bind(fd, reinterpret_cast<const sockaddr *>(&srcAddr), sizeof(srcAddr)) != 0)
                {
                    std::cerr << "[RawFileUdpReplayer] bind failed on source port=" << (m_sourcePortBase + ch) << std::endl;
                    ::close(fd);
                    return false;
                }

                sockaddr_in dstAddr{};
                dstAddr.sin_family = AF_INET;
                dstAddr.sin_port = htons(static_cast<uint16_t>(m_destinationPortBase + ch));
                if (::inet_pton(AF_INET, m_destinationIp.c_str(), &dstAddr.sin_addr) != 1)
                {
                    std::cerr << "[RawFileUdpReplayer] invalid destination ip: " << m_destinationIp << std::endl;
                    ::close(fd);
                    return false;
                }

                sockets[ch] = fd;
                destinations[ch] = dstAddr;
            }

            return true;
        }

        static void closeSockets(std::vector<int> &sockets)
        {
            for (int &fd : sockets)
            {
                if (fd >= 0)
                {
                    ::close(fd);
                    fd = -1;
                }
            }
        }

        std::string m_rawPath;
        std::string m_sourceIp;
        std::string m_destinationIp;
        uint16_t m_sourcePortBase = 0;
        uint16_t m_destinationPortBase = 0;
        uint16_t m_channelCount = 0;
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

        // 为回放测试放宽包长过滤范围，避免因固定 1024 过滤导致误丢包。
        task.set_storage_unit_size(2048);
        task.set_min_packet_size(1);

        task.set_max_buffer_size(4ull * 1024ull * 1024ull * 1024ull);
        task.set_time_switch_buffer_ms(200);
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

        auto *dpdk = task.mutable_dpdk_options();
        dpdk->set_copy_thread_num(8);
        dpdk->set_rx_rings_per_port(1);
        dpdk->set_rte_mbuf_double_pointer_size_multiply(32);
        dpdk->set_rte_mbuf_double_pointer_num_multiply(2);
    }

    std::vector<std::string> buildCalibrationFiles(const std::string &calibrationDir)
    {
        std::vector<std::string> files;
        files.reserve(48);

        for (int i = 0; i < 48; ++i)
        {
            std::ostringstream oss;
            oss << "channel_" << std::setw(2) << std::setfill('0') << i << ".data";
            files.push_back((fs::path(calibrationDir) / oss.str()).string());
        }

        return files;
    }

    bool validateInputFiles(const std::string &rawPath, const std::vector<std::string> &calibrationFiles)
    {
        bool ok = true;

        if (!fs::exists(rawPath))
        {
            std::cerr << "[Input] raw file not found: " << rawPath << std::endl;
            ok = false;
        }

        for (const auto &file : calibrationFiles)
        {
            if (!fs::exists(file))
            {
                std::cerr << "[Input] calibration file not found: " << file << std::endl;
                ok = false;
            }
        }

        return ok;
    }

    struct BaselineResult
    {
        bool success = false;
        uint32_t processedSegments = 0;
        uint64_t processedPackets = 0;
        SinglesDigest digest;
    };

    BaselineResult runOfflineBaseline(
        const std::string &rawPath,
        const std::vector<std::string> &calibrationFiles,
        const std::vector<uint16_t> &channelIndices,
        uint32_t segmentLimit)
    {
        BaselineResult result;

        try
        {
            openpni::io::v1::RawFileInput input;
            input.open(rawPath);
            auto header = input.header();

            SinglesAccumulator accumulator;

            auto config = r2s::createBDM2Config(
                rawPath,
                "Data/result/Bdm2/split",
                calibrationFiles,
                "baseline_unused",
                channelIndices);

            config.saveData2SingleFile = false;
            config.sortDataByTime = true;
            config.progressLogInterval = 0;
            config.onSinglesReady = nullptr;
            config.onSinglesSpanReady = [&accumulator](
                                            std::span<r2s::Single const> singles,
                                            uint64_t,
                                            uint32_t) -> bool
            {
                return accumulator.onSingles(singles);
            };

            r2s::R2SStreamProcessor processor(config);
            if (!processor.initialize(header.channelNum))
            {
                return result;
            }

            const uint32_t targetSegments = std::min<uint32_t>(segmentLimit, header.segmentNum);
            for (uint32_t i = 0; i < targetSegments; ++i)
            {
                auto segment = input.readSegment(i, i + 1);
                auto segHeader = input.segmentHeader(i);
                auto view = segment.view(header, segHeader);
                view.clock_ms = segHeader.clock;
                view.duration_ms = segHeader.duration;

                result.processedPackets += view.count;
                if (!processor.processSegment(view))
                {
                    return result;
                }

                result.processedSegments += 1;
            }

            result.success = processor.finalize();
            result.digest = accumulator.snapshot();
            return result;
        }
        catch (const std::exception &e)
        {
            std::cerr << "[Baseline] exception: " << e.what() << std::endl;
            return result;
        }
    }

} // namespace

int main()
{
    const std::string masterAddress = "127.0.0.1:50093";
    const std::string nodeId = "acq-r2s-pipeline-node-1";

    const std::string sourceIp = "127.0.0.1";
    const std::string destinationIp = "127.0.0.1";
    const uint16_t sourcePortBase = 17100;
    const uint16_t destinationPortBase = 18100;
    const uint16_t channelCount = 4;

    const std::string rawPath = "Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw";
    const std::string calibrationDir = "Data/bdm2/calibration";

    const uint32_t replaySegments = 80;
    const uint32_t interPacketUs = 2;

    const std::vector<uint16_t> channelIndices = {0, 1, 2, 3};
    const auto calibrationFiles = buildCalibrationFiles(calibrationDir);

    if (!validateInputFiles(rawPath, calibrationFiles))
    {
        return 1;
    }

    std::cout << "[AcqR2STest] Running offline baseline on " << replaySegments << " segment(s)..." << std::endl;
    BaselineResult baseline = runOfflineBaseline(rawPath, calibrationFiles, channelIndices, replaySegments);
    if (!baseline.success)
    {
        std::cerr << "[AcqR2STest] Baseline R2S failed" << std::endl;
        return 1;
    }

    std::cout << "[AcqR2STest] Baseline done: segments=" << baseline.processedSegments
              << " packets=" << baseline.processedPackets
              << " singles=" << baseline.digest.totalSingles
              << " checksum_xor=" << baseline.digest.checksumXor << std::endl;

    const std::string sessionName = "acq_r2s_pipeline_" + std::to_string(nowMs());

    acq::AcquisitionTask globalTask;
    fillAcquisitionTask(
        globalTask,
        sessionName,
        sourceIp,
        destinationIp,
        sourcePortBase,
        destinationPortBase,
        channelCount);

    acq::AcquisitionMaster master;
    master.Initialize(globalTask);
    master.StartServer(masterAddress);

    auto r2sConfig = r2s::createBDM2Config(
        "",
        "Data/result/Bdm2/split",
        calibrationFiles,
        "acq_pipeline_unused",
        channelIndices);
    r2sConfig.saveData2SingleFile = false;
    r2sConfig.sortDataByTime = true;
    r2sConfig.progressLogInterval = 0;

    SinglesAccumulator onlineAccumulator;
    r2sConfig.onSinglesReady = nullptr;
    r2sConfig.onSinglesSpanReady = [&onlineAccumulator](
                                       std::span<r2s::Single const> singles,
                                       uint64_t,
                                       uint32_t) -> bool
    {
        return onlineAccumulator.onSingles(singles);
    };

    r2s::AsyncRawDataToR2SBridge::Config bridgeConfig;
    bridgeConfig.queue.capacity = 256;
    bridgeConfig.queue.reservePacketsPerSlot = 4096;
    bridgeConfig.queue.reserveBytesPerSlot = 4 * 1024 * 1024;
    bridgeConfig.blockWhenQueueFull = true;
    bridgeConfig.queueFullWarnEvery = 5000;

    r2s::AsyncRawDataToR2SBridge bridge(r2sConfig, bridgeConfig);
    if (!bridge.start(channelCount))
    {
        std::cerr << "[AcqR2STest] Failed to start AsyncRawDataToR2SBridge" << std::endl;
        master.StopServer();
        return 1;
    }

    grpcnode::AcquisitionGrpcNode::InitOptions nodeOpt;
    nodeOpt.masterAddress = masterAddress;
    nodeOpt.nodeId = nodeId;
    nodeOpt.nodeAddress = "127.0.0.1";
    nodeOpt.outputRoot = "Data/raw_data";
    nodeOpt.sessionNamePrefix = "acq_r2s_pipeline_node";
    nodeOpt.statusIntervalMs = 300;
    nodeOpt.enableRawFileWrite = false;

    grpcnode::AcquisitionGrpcNode node(nodeOpt);
    node.setRawDataReadyCallback(bridge.makeRawDataCallback());

    std::thread nodeThread([&]()
                           {
                               const bool ok = node.run();
                               std::cout << "[AcqR2STest] Node thread exit, ok=" << (ok ? "true" : "false") << std::endl; });

    bool connected = false;
    bool configured = false;
    bool running = false;
    bool backToConfigured = false;

    connected = master.WaitForConnectedNodes(1, 10000);
    std::cout << "[AcqR2STest] Connected=" << (connected ? "true" : "false")
              << " count=" << master.ConnectedNodeCount() << std::endl;

    ReplayResult replay;
    uint64_t rxPackets = 0;
    uint64_t rxBytes = 0;
    uint64_t observedRxPackets = 0;
    uint64_t observedRxBytes = 0;
    acq::NodeState finalState = acq::STATE_UNKNOWN;
    std::string errorMessage;

    if (connected)
    {
        master.DistributeTasks();
        configured = waitNodeState(master, nodeId, acq::STATE_CONFIGURED, 8000);
        std::cout << "[AcqR2STest] Configured=" << (configured ? "true" : "false") << std::endl;
    }

    if (configured)
    {
        master.SendStart(nowMs() + 300, 0);
        running = waitNodeState(master, nodeId, acq::STATE_RUNNING, 8000);
        std::cout << "[AcqR2STest] Running=" << (running ? "true" : "false") << std::endl;
    }

    if (running)
    {
        RawFileUdpReplayer replayer(
            rawPath,
            sourceIp,
            destinationIp,
            sourcePortBase,
            destinationPortBase,
            channelCount);
        replay = replayer.replay(replaySegments, interPacketUs);
        std::cout << "[AcqR2STest] Replay success=" << (replay.success ? "true" : "false")
                  << " segments=" << replay.replayedSegments
                  << " sent_packets=" << replay.sentPackets
                  << " sent_bytes=" << replay.sentBytes
                  << " skipped_packets=" << replay.skippedPackets
                  << std::endl;

        // 采集停止后节点可能清零累计计数，因此在 RUNNING 阶段采样最大 RX 计数作为验收依据。
        for (int i = 0; i < 8; ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            const auto runningSnapshot = master.SnapshotNodes();
            for (const auto &nodeSnap : runningSnapshot)
            {
                if (nodeSnap.info.node_id() != nodeId)
                {
                    continue;
                }

                observedRxPackets = std::max<uint64_t>(observedRxPackets, nodeSnap.lastStatus.total_rx_packets());
                observedRxBytes = std::max<uint64_t>(observedRxBytes, nodeSnap.lastStatus.total_rx_bytes());
            }
        }

        master.SendStop("acq-r2s-pipeline-finished");
        backToConfigured = waitNodeState(master, nodeId, acq::STATE_CONFIGURED, 8000);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    const bool bridgeStopOk = bridge.stop();
    const auto bridgeStats = bridge.stats();
    const auto onlineDigest = onlineAccumulator.snapshot();

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

    master.SendShutdown("acq-r2s-pipeline-done");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    master.StopServer();

    if (nodeThread.joinable())
    {
        nodeThread.join();
    }

    const uint64_t effectiveRxPackets = observedRxPackets;
    const double packetLossRatio =
        replay.sentPackets > 0
            ? static_cast<double>(replay.sentPackets - std::min<uint64_t>(effectiveRxPackets, replay.sentPackets)) / static_cast<double>(replay.sentPackets)
            : 1.0;

    const uint64_t expectedSingles = baseline.digest.totalSingles;
    const uint64_t actualSingles = onlineDigest.totalSingles;
    const uint64_t singlesDiff = expectedSingles > actualSingles ? (expectedSingles - actualSingles) : (actualSingles - expectedSingles);
    const double singlesDiffRatio =
        expectedSingles > 0 ? static_cast<double>(singlesDiff) / static_cast<double>(expectedSingles) : 1.0;

    const bool checksumMatch = (baseline.digest.checksumXor == onlineDigest.checksumXor);

    std::cout << "[AcqR2STest] Node final state=" << stateToString(finalState)
              << " rx_packets=" << rxPackets
              << " rx_bytes=" << rxBytes
              << " observed_rx_packets=" << observedRxPackets
              << " observed_rx_bytes=" << observedRxBytes
              << " err='" << errorMessage << "'"
              << std::endl;

    std::cout << "[AcqR2STest] Bridge stats: healthy=" << (bridgeStats.healthy ? "true" : "false")
              << " enqueued=" << bridgeStats.enqueuedSegments
              << " processed=" << bridgeStats.processedSegments
              << " dropped=" << bridgeStats.droppedSegments
              << " full_hits=" << bridgeStats.enqueueFullHits
              << " peak_depth=" << bridgeStats.queuePeakDepth
              << std::endl;

    std::cout << "[AcqR2STest] Expected singles=" << expectedSingles
              << " actual singles=" << actualSingles
              << " singles_diff=" << singlesDiff
              << " singles_diff_ratio=" << singlesDiffRatio
              << std::endl;

    std::cout << "[AcqR2STest] Expected checksum_xor=" << baseline.digest.checksumXor
              << " actual checksum_xor=" << onlineDigest.checksumXor
              << " checksum_match=" << (checksumMatch ? "true" : "false")
              << std::endl;

    std::cout << "[AcqR2STest] packet_loss_ratio=" << packetLossRatio
              << " bridge_stop_ok=" << (bridgeStopOk ? "true" : "false")
              << " connected=" << (connected ? "true" : "false")
              << " configured=" << (configured ? "true" : "false")
              << " running=" << (running ? "true" : "false")
              << " back_to_configured=" << (backToConfigured ? "true" : "false")
              << std::endl;

    const bool success =
        connected &&
        configured &&
        running &&
        replay.success &&
        replay.sentPackets > 0 &&
        backToConfigured &&
        bridgeStopOk &&
        bridgeStats.healthy &&
        bridgeStats.droppedSegments == 0 &&
        packetLossRatio <= 0.01 &&
        expectedSingles > 0 &&
        actualSingles > 0 &&
        singlesDiffRatio <= 0.01 &&
        checksumMatch;

    std::cout << "[AcqR2STest] RESULT=" << (success ? "SUCCESS" : "FAILED") << std::endl;
    return success ? 0 : 1;
}
