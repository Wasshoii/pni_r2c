// Include standard library headers before PNI headers to avoid g++-13 namespace conflicts.
#include <any>
#include <valarray>
#include <type_traits>
#include <utility>
#include <memory>
#include <string>
#include <cstdint>
#include <algorithm>
#include <vector>
#include <iostream>
#include <thread>
#include <atomic>
#include <mutex>
#include <deque>
#include <condition_variable>
#include <unordered_map>
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <chrono>

// PnI-Config.hpp must be included before other PNI headers.
#include <pni/PnI-Config.hpp>
#include <pni/io/IO.hpp>

#include <grpcpp/grpcpp.h>

#include "protos/coincidence.grpc.pb.h"
#include "../src/core/aquisition-and-r2s/R2S.hpp"
#include "grpcNode/r2sNode.hpp"

namespace fs = std::filesystem;
namespace coincidence = openpni::distributed::coincidence;
namespace r2s = openpni::distributed::r2s;
namespace grpcnode = openpni::distributed::grpcnode;

namespace
{
    struct ProgramOptions
    {
        std::string address = "127.0.0.1:50061";
        std::string splitDir = "Data/bdm2/split_Data";
        std::string calibrationDir = "Data/bdm2/calibration";
        std::string resultDir = "Data/result/Bdm2/split";
        std::string rawPrefix = "2_PET_2Bed pet 600s-bed0";
        uint32_t nodeCount = 3;
        size_t maxPendingSegments = 64;
        uint32_t batchSegmentsPerMessage = 1;
        bool parallelNodes = false;
        bool noLocalReceiver = false;
        bool helpOnly = false;
    };

    struct NodeInput
    {
        uint32_t nodeId = 0;
        std::string rawdataPath;
        std::vector<uint16_t> channels;
    };

    using NodeRunStats = grpcnode::NodeRunStats;

    struct ReceiverNodeStats
    {
        uint64_t chunksReceived = 0;
        uint64_t singlesReceived = 0;
        bool connected = false;
        uint32_t channelCount = 0;
        std::string detectorType;
        std::string nodeAddress;
        uint64_t lastHeartbeatMs = 0;
    };

    class ReceiverOnlyCoincidenceService final : public coincidence::CoincidenceService::Service
    {
    public:
        grpc::Status StreamSingles(
            grpc::ServerContext * /*context*/,
            grpc::ServerReader<coincidence::SingleChunkMessage> *reader,
            coincidence::StreamResponse *response) override
        {
            coincidence::SingleChunkMessage msg;
            uint64_t totalReceivedInRpc = 0;

            while (reader->Read(&msg))
            {
                const uint32_t nodeId = msg.node_id();
                const uint64_t singlesCount = static_cast<uint64_t>(msg.singles_size());
                const uint64_t segmentCount =
                    msg.segment_metas_size() > 0 ? static_cast<uint64_t>(msg.segment_metas_size()) : 1ULL;

                totalReceivedInRpc += singlesCount;
                m_totalChunksReceived.fetch_add(segmentCount, std::memory_order_relaxed);
                m_totalSinglesReceived.fetch_add(singlesCount, std::memory_order_relaxed);

                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    auto &node = m_nodes[nodeId];
                    node.connected = true;

                    if (msg.segment_metas_size() > 0)
                    {
                        uint64_t singlesFromMeta = 0;
                        for (const auto &meta : msg.segment_metas())
                        {
                            const uint64_t segmentSingles = static_cast<uint64_t>(meta.singles_count());
                            node.chunksReceived += 1;
                            node.singlesReceived += segmentSingles;
                            singlesFromMeta += segmentSingles;

                            if (meta.chunk_id() % 500 == 0)
                            {
                                std::cout << "[Receiver] node=" << nodeId
                                          << " chunk=" << meta.chunk_id()
                                          << " singles=" << segmentSingles
                                          << " totalSingles=" << m_totalSinglesReceived.load(std::memory_order_relaxed)
                                          << std::endl;
                            }
                        }

                        if (singlesFromMeta < singlesCount)
                        {
                            node.singlesReceived += (singlesCount - singlesFromMeta);
                        }
                    }
                    else
                    {
                        node.chunksReceived += 1;
                        node.singlesReceived += singlesCount;

                        if (msg.chunk_id() % 500 == 0)
                        {
                            std::cout << "[Receiver] node=" << nodeId
                                      << " chunk=" << msg.chunk_id()
                                      << " singles=" << singlesCount
                                      << " totalSingles=" << m_totalSinglesReceived.load(std::memory_order_relaxed)
                                      << std::endl;
                        }
                    }
                }
            }

            response->set_success(true);
            response->set_singles_received(totalReceivedInRpc);
            response->set_message("Receiver-only mode: chunk stream accepted");
            return grpc::Status::OK;
        }

        grpc::Status GetStatus(
            grpc::ServerContext * /*context*/,
            const coincidence::StatusRequest *request,
            coincidence::StatusResponse *response) override
        {
            response->set_total_singles_received(m_totalSinglesReceived.load(std::memory_order_relaxed));
            response->set_total_singles_processed(0);
            response->set_total_prompt_pairs(0);
            response->set_total_delay_pairs(0);
            response->set_alignment_windows_processed(0);
            response->set_avg_processing_time_ms(0.0);
            response->set_current_time_boundary_pico(0);
            response->set_is_running(true);
            response->set_expected_node_count(static_cast<uint32_t>(m_nodes.size()));
            response->set_connected_node_count(static_cast<uint32_t>(m_nodes.size()));
            response->set_start_signal_issued(true);

            if (request->include_node_stats())
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                for (const auto &[nodeId, info] : m_nodes)
                {
                    auto *nodeStatus = response->add_node_stats();
                    nodeStatus->set_node_id(nodeId);
                    nodeStatus->set_chunks_received(info.chunksReceived);
                    nodeStatus->set_singles_received(info.singlesReceived);
                    nodeStatus->set_buffer_size(0);
                    nodeStatus->set_connected(info.connected);
                }
            }

            return grpc::Status::OK;
        }

        grpc::Status WaitForStart(
            grpc::ServerContext * /*context*/,
            const coincidence::WaitForStartRequest *request,
            coincidence::WaitForStartResponse *response) override
        {
            response->set_success(true);
            response->set_start_signal_issued(true);
            response->set_start_time_ms(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
            response->set_expected_node_count(static_cast<uint32_t>(m_nodes.size()));
            response->set_connected_node_count(static_cast<uint32_t>(m_nodes.size()));
            response->set_message("Receiver-only mode: start signal is always ready");
            (void)request;
            return grpc::Status::OK;
        }

        grpc::Status Control(
            grpc::ServerContext * /*context*/,
            const coincidence::ControlRequest * /*request*/,
            coincidence::ControlResponse *response) override
        {
            response->set_success(true);
            response->set_message("Receiver-only mode: control command ignored");
            return grpc::Status::OK;
        }

        grpc::Status UpdateConfig(
            grpc::ServerContext * /*context*/,
            const coincidence::ConfigUpdateRequest * /*request*/,
            coincidence::ConfigUpdateResponse *response) override
        {
            response->set_success(true);
            response->set_message("Receiver-only mode: config update ignored");
            return grpc::Status::OK;
        }

        grpc::Status RegisterNode(
            grpc::ServerContext * /*context*/,
            const coincidence::RegisterNodeRequest *request,
            coincidence::RegisterNodeResponse *response) override
        {
            std::lock_guard<std::mutex> lock(m_mutex);

            auto &node = m_nodes[request->node_id()];
            node.connected = true;
            node.channelCount = request->channel_count();
            node.detectorType = request->detector_type();
            node.nodeAddress = request->node_address();

            response->set_success(true);
            response->set_assigned_node_id(request->node_id());
            response->set_expected_node_count(static_cast<uint32_t>(m_nodes.size()));
            response->set_connected_node_count(static_cast<uint32_t>(m_nodes.size()));
            response->set_start_signal_issued(true);
            response->set_planned_start_time_ms(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
            response->set_message("Node registered (receiver-only mode)");

            std::cout << "[Receiver] Register node=" << request->node_id()
                      << " addr=" << request->node_address()
                      << " channels=" << request->channel_count()
                      << " detector=" << request->detector_type()
                      << std::endl;

            return grpc::Status::OK;
        }

        grpc::Status Heartbeat(
            grpc::ServerContext * /*context*/,
            const coincidence::HeartbeatRequest *request,
            coincidence::HeartbeatResponse *response) override
        {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                auto &node = m_nodes[request->node_id()];
                node.connected = true;
                node.lastHeartbeatMs = request->timestamp_ms();
            }

            response->set_acknowledged(true);
            response->set_server_timestamp_ms(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
            return grpc::Status::OK;
        }

    private:
        std::mutex m_mutex;
        std::unordered_map<uint32_t, ReceiverNodeStats> m_nodes;
        std::atomic<uint64_t> m_totalSinglesReceived{0};
        std::atomic<uint64_t> m_totalChunksReceived{0};
    };

    class LocalReceiverServer
    {
    public:
        explicit LocalReceiverServer(std::string address)
            : m_address(std::move(address))
        {
        }

        bool start()
        {
            grpc::ServerBuilder builder;
            builder.AddListeningPort(m_address, grpc::InsecureServerCredentials());
            builder.RegisterService(&m_service);
            builder.SetMaxReceiveMessageSize(100 * 1024 * 1024);
            builder.SetMaxSendMessageSize(10 * 1024 * 1024);

            m_server = builder.BuildAndStart();
            if (!m_server)
            {
                std::cerr << "[Receiver] Failed to start server at " << m_address << std::endl;
                return false;
            }

            std::cout << "[Receiver] Listening on " << m_address << std::endl;
            return true;
        }

        void stop()
        {
            if (m_server)
            {
                m_server->Shutdown();
                m_server.reset();
                std::cout << "[Receiver] Stopped" << std::endl;
            }
        }

    private:
        std::string m_address;
        ReceiverOnlyCoincidenceService m_service;
        std::unique_ptr<grpc::Server> m_server;
    };

    void printUsage(const char *prog)
    {
        std::cout << "Usage: " << prog << " [options]\n"
                  << "Options:\n"
                  << "  --address <host:port>         gRPC local address (default: 127.0.0.1:50061)\n"
                  << "  --split-dir <path>            BDM2 split rawdata directory\n"
                  << "  --calibration-dir <path>      BDM2 calibration directory\n"
                  << "  --result-dir <path>           Output directory for R2S config\n"
                  << "  --raw-prefix <name>           Split rawdata file prefix\n"
                  << "  --node-count <N>              Number of nodes/files to process (default: 3)\n"
                  << "  --max-pending-segments <N>    Async sender queue length in segments (default: 64)\n"
                  << "  --batch-segments <N>          Segments packed into one gRPC message (default: 1)\n"
                  << "  --no-local-receiver           Do not start built-in receiver; use external coin host\n"
                  << "  --parallel                    Run node conversion in parallel\n"
                  << "  --help                        Print this message\n"
                  << std::endl;
    }

    bool parseArgs(int argc, char **argv, ProgramOptions &opts)
    {
        for (int i = 1; i < argc; ++i)
        {
            const std::string arg = argv[i];

            auto requireValue = [&](const std::string &name) -> const char *
            {
                if (i + 1 >= argc)
                {
                    std::cerr << "Missing value for " << name << std::endl;
                    return nullptr;
                }
                return argv[++i];
            };

            if (arg == "--help")
            {
                printUsage(argv[0]);
                opts.helpOnly = true;
                return true;
            }
            if (arg == "--address")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.address = v;
                continue;
            }
            if (arg == "--split-dir")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.splitDir = v;
                continue;
            }
            if (arg == "--calibration-dir")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.calibrationDir = v;
                continue;
            }
            if (arg == "--result-dir")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.resultDir = v;
                continue;
            }
            if (arg == "--raw-prefix")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.rawPrefix = v;
                continue;
            }
            if (arg == "--parallel")
            {
                opts.parallelNodes = true;
                continue;
            }
            if (arg == "--no-local-receiver")
            {
                opts.noLocalReceiver = true;
                continue;
            }
            if (arg == "--node-count")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }

                try
                {
                    const int parsed = std::stoi(v);
                    if (parsed <= 0)
                    {
                        std::cerr << "--node-count must be positive" << std::endl;
                        return false;
                    }
                    opts.nodeCount = static_cast<uint32_t>(parsed);
                }
                catch (const std::exception &)
                {
                    std::cerr << "Invalid --node-count value: " << v << std::endl;
                    return false;
                }
                continue;
            }
            if (arg == "--max-pending-segments")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }

                try
                {
                    const uint64_t parsed = std::stoull(v);
                    if (parsed == 0)
                    {
                        std::cerr << "--max-pending-segments must be positive" << std::endl;
                        return false;
                    }
                    opts.maxPendingSegments = static_cast<size_t>(parsed);
                }
                catch (const std::exception &)
                {
                    std::cerr << "Invalid --max-pending-segments value: " << v << std::endl;
                    return false;
                }
                continue;
            }

            if (arg == "--batch-segments")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }

                try
                {
                    const uint64_t parsed = std::stoull(v);
                    if (parsed == 0)
                    {
                        std::cerr << "--batch-segments must be positive" << std::endl;
                        return false;
                    }
                    opts.batchSegmentsPerMessage = static_cast<uint32_t>(parsed);
                }
                catch (const std::exception &)
                {
                    std::cerr << "Invalid --batch-segments value: " << v << std::endl;
                    return false;
                }
                continue;
            }

            std::cerr << "Unknown argument: " << arg << std::endl;
            printUsage(argv[0]);
            return false;
        }

        return true;
    }

    std::string makeSplitRawPath(
        const std::string &splitDir,
        const std::string &rawPrefix,
        const std::vector<uint16_t> &channels)
    {
        std::ostringstream oss;
        oss << rawPrefix;
        for (auto ch : channels)
        {
            oss << "_ch" << ch;
        }
        oss << ".raw";
        return (fs::path(splitDir) / oss.str()).string();
    }

    std::vector<NodeInput> buildNodeInputs(const ProgramOptions &opts)
    {
        std::vector<NodeInput> nodes;
        nodes.reserve(opts.nodeCount);

        for (uint32_t i = 0; i < opts.nodeCount; ++i)
        {
            NodeInput n;
            n.nodeId = i;
            n.channels = {
                static_cast<uint16_t>(i * 4),
                static_cast<uint16_t>(i * 4 + 1),
                static_cast<uint16_t>(i * 4 + 2),
                static_cast<uint16_t>(i * 4 + 3)};
            n.rawdataPath = makeSplitRawPath(opts.splitDir, opts.rawPrefix, n.channels);
            nodes.push_back(std::move(n));
        }

        return nodes;
    }

    std::vector<std::string> buildCalibrationFiles(const std::string &calibrationDir)
    {
        std::vector<std::string> files;
        files.reserve(48);

        for (int i = 0; i < 48; ++i)
        {
            std::ostringstream fileName;
            fileName << "channel_" << std::setw(2) << std::setfill('0') << i << ".data";
            files.push_back((fs::path(calibrationDir) / fileName.str()).string());
        }

        return files;
    }

    bool validateInputs(
        const std::vector<NodeInput> &nodes,
        const std::vector<std::string> &calibrationFiles,
        const std::string &resultDir)
    {
        bool ok = true;

        for (const auto &n : nodes)
        {
            if (!fs::exists(n.rawdataPath))
            {
                std::cerr << "[Input] Missing split rawdata file: " << n.rawdataPath << std::endl;
                ok = false;
            }
        }

        for (const auto &path : calibrationFiles)
        {
            if (!fs::exists(path))
            {
                std::cerr << "[Input] Missing calibration file: " << path << std::endl;
                ok = false;
            }
        }

        std::error_code ec;
        fs::create_directories(resultDir, ec);
        if (ec)
        {
            std::cerr << "[Input] Failed to create result directory: " << resultDir
                      << " error=" << ec.message() << std::endl;
            ok = false;
        }

        return ok;
    }

    NodeRunStats runSingleNode(
        const ProgramOptions &opts,
        const NodeInput &node,
        const std::vector<std::string> &calibrationFiles)
    {
        auto config = r2s::createBDM2Config(
            node.rawdataPath,
            opts.resultDir,
            calibrationFiles,
            "node_" + std::to_string(node.nodeId),
            node.channels);

        // R2S guarantees segment-level time ordering when sortDataByTime is enabled.
        config.sortDataByTime = true;
        config.saveData2SingleFile = false;
        config.asyncFileWrite = false;

        grpcnode::R2SGrpcNode nodeRunner(
            config,
            opts.address,
            node.nodeId,
            static_cast<uint32_t>(node.channels.size()),
            opts.maxPendingSegments,
            "127.0.0.1",
            "BDM2",
            50,
            true,
            0,
            15000,
            1000,
            opts.batchSegmentsPerMessage);

        std::cout << "[Node " << node.nodeId << "] R2S start, file=" << node.rawdataPath << std::endl;
        nodeRunner.run();
        NodeRunStats stats = nodeRunner.stats();

        std::cout << "[Node " << node.nodeId << "] R2S done, success="
                  << (stats.success ? "true" : "false")
                  << " callbacks=" << stats.callbackCount
                  << " grpcMessages=" << stats.grpcMessagesSent
                  << " singlesSent=" << stats.singlesSent << std::endl;

#ifdef DEBUG
        const double enqueueAvgUs = stats.enqueueCalls > 0 ? (double)stats.enqueueTotalNs / stats.enqueueCalls / 1e3 : 0.0;
        const double enqueueWaitAvgUs = stats.enqueueCalls > 0 ? (double)stats.enqueueWaitNs / stats.enqueueCalls / 1e3 : 0.0;
        const double serializeAvgUs = stats.grpcMessagesSent > 0 ? (double)stats.serializeBuildNs / stats.grpcMessagesSent / 1e3 : 0.0;
        const double writeAvgUs = stats.grpcMessagesSent > 0 ? (double)stats.writeNs / stats.grpcMessagesSent / 1e3 : 0.0;

        std::cout << "[Node " << node.nodeId << "] Perf enqueue(avg/wait avg/max wait)="
                  << enqueueAvgUs << "/" << enqueueWaitAvgUs << "/" << (double)stats.maxEnqueueWaitNs / 1e3
                  << " us, serialize avg=" << serializeAvgUs
                  << " us, write(avg/max)=" << writeAvgUs << "/" << (double)stats.maxWriteNs / 1e3
                  << " us" << std::endl;
        std::cout << "[Node " << node.nodeId << "] Memory queuePeak=" << stats.peakQueueSegments
                  << " segments, " << stats.peakQueueSingles << " singles, "
                  << (double)stats.peakQueueBytes / (1024.0 * 1024.0) << " MiB, processRSS="
                  << (double)stats.processRssBytes / (1024.0 * 1024.0) << " MiB" << std::endl;
#endif

        return stats;
    }

    void printServerStatus(const std::string &address)
    {
        auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        auto stub = coincidence::CoincidenceService::NewStub(channel);

        grpc::ClientContext context;
        coincidence::StatusRequest request;
        request.set_include_node_stats(true);
        coincidence::StatusResponse status;

        grpc::Status rpcStatus = stub->GetStatus(&context, request, &status);
        if (!rpcStatus.ok())
        {
            std::cerr << "[Status] failed to query receiver status: "
                      << rpcStatus.error_message() << std::endl;
            return;
        }

        std::cout << "\n========== Receiver Status ==========" << std::endl;
        std::cout << "Total singles received: " << status.total_singles_received() << std::endl;
        std::cout << "Total singles processed: " << status.total_singles_processed() << std::endl;
        std::cout << "Registered nodes in status: " << status.node_stats_size() << std::endl;

        for (const auto &node : status.node_stats())
        {
            std::cout << "  Node " << node.node_id()
                      << " chunks=" << node.chunks_received()
                      << " singles=" << node.singles_received()
                      << " connected=" << (node.connected() ? "true" : "false")
                      << std::endl;
        }

        std::cout << "=====================================\n"
                  << std::endl;
    }

} // namespace

int main(int argc, char **argv)
{
    ProgramOptions opts;
    if (!parseArgs(argc, argv, opts))
    {
        return 1;
    }
    if (opts.helpOnly)
    {
        return 0;
    }

    std::cout << "============================================" << std::endl;
    std::cout << "  Local gRPC R2S Streaming Test (BDM2)" << std::endl;
    std::cout << "============================================" << std::endl;
    std::cout << "address      : " << opts.address << std::endl;
    std::cout << "splitDir     : " << opts.splitDir << std::endl;
    std::cout << "calibrationDir: " << opts.calibrationDir << std::endl;
    std::cout << "resultDir    : " << opts.resultDir << std::endl;
    std::cout << "rawPrefix    : " << opts.rawPrefix << std::endl;
    std::cout << "nodeCount    : " << opts.nodeCount << std::endl;
    std::cout << "maxPendingSegments: " << opts.maxPendingSegments << std::endl;
    std::cout << "batchSegmentsPerMessage: " << opts.batchSegmentsPerMessage << std::endl;
    std::cout << "parallelNodes: " << (opts.parallelNodes ? "true" : "false") << std::endl;
    std::cout << "noLocalReceiver: " << (opts.noLocalReceiver ? "true" : "false") << std::endl;

    if (opts.nodeCount > 12)
    {
        std::cerr << "For BDM2 4-channel groups, nodeCount must be <= 12" << std::endl;
        return 1;
    }

    const auto nodeInputs = buildNodeInputs(opts);
    const auto calibrationFiles = buildCalibrationFiles(opts.calibrationDir);

    if (!validateInputs(nodeInputs, calibrationFiles, opts.resultDir))
    {
        std::cerr << "Input validation failed. Please verify rawdata/calibration files." << std::endl;
        return 2;
    }

    std::unique_ptr<LocalReceiverServer> server;
    if (!opts.noLocalReceiver)
    {
        server = std::make_unique<LocalReceiverServer>(opts.address);
        if (!server->start())
        {
            return 3;
        }
    }
    else
    {
        std::cout << "[Test] Using external receiver at " << opts.address << std::endl;
    }

    const auto t0 = std::chrono::steady_clock::now();

    std::vector<NodeRunStats> stats(nodeInputs.size());

    if (opts.parallelNodes)
    {
        std::vector<std::thread> workers;
        workers.reserve(nodeInputs.size());

        for (size_t i = 0; i < nodeInputs.size(); ++i)
        {
            workers.emplace_back([&, i]()
                                 { stats[i] = runSingleNode(opts, nodeInputs[i], calibrationFiles); });
        }

        for (auto &w : workers)
        {
            w.join();
        }
    }
    else
    {
        for (size_t i = 0; i < nodeInputs.size(); ++i)
        {
            stats[i] = runSingleNode(opts, nodeInputs[i], calibrationFiles);
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    printServerStatus(opts.address);

    if (server)
    {
        server->stop();
    }

    uint64_t totalCallbacks = 0;
    uint64_t totalSinglesSent = 0;
    uint64_t totalGrpcMessages = 0;
#ifdef DEBUG
    uint64_t totalEnqueueCalls = 0;
    uint64_t totalEnqueueTotalNs = 0;
    uint64_t totalEnqueueWaitNs = 0;
    uint64_t totalSerializeBuildNs = 0;
    uint64_t totalWriteNs = 0;
    uint64_t totalEstimatedWireBytes = 0;
    uint64_t maxEnqueueWaitNsAll = 0;
    uint64_t maxWriteNsAll = 0;
    uint64_t maxPeakQueueBytes = 0;
    uint64_t maxPeakQueueSegments = 0;
    uint64_t maxPeakQueueSingles = 0;
    uint64_t maxProcessRssBytes = 0;
#endif
    bool allSuccess = true;

    for (const auto &s : stats)
    {
        totalCallbacks += s.callbackCount;
        totalSinglesSent += s.singlesSent;
        totalGrpcMessages += s.grpcMessagesSent;
#ifdef DEBUG
        totalEnqueueCalls += s.enqueueCalls;
        totalEnqueueTotalNs += s.enqueueTotalNs;
        totalEnqueueWaitNs += s.enqueueWaitNs;
        totalSerializeBuildNs += s.serializeBuildNs;
        totalWriteNs += s.writeNs;
        totalEstimatedWireBytes += s.estimatedWireBytes;
        maxEnqueueWaitNsAll = std::max(maxEnqueueWaitNsAll, s.maxEnqueueWaitNs);
        maxWriteNsAll = std::max(maxWriteNsAll, s.maxWriteNs);
        maxPeakQueueBytes = std::max(maxPeakQueueBytes, s.peakQueueBytes);
        maxPeakQueueSegments = std::max(maxPeakQueueSegments, s.peakQueueSegments);
        maxPeakQueueSingles = std::max(maxPeakQueueSingles, s.peakQueueSingles);
        maxProcessRssBytes = std::max(maxProcessRssBytes, s.processRssBytes);
#endif
        allSuccess = allSuccess && s.success;
    }

    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - t0)
                               .count();

    std::cout << "========== Test Summary ==========" << std::endl;
    std::cout << "All node conversions success: " << (allSuccess ? "true" : "false") << std::endl;
    std::cout << "Total callbacks: " << totalCallbacks << std::endl;
    std::cout << "Total gRPC messages: " << totalGrpcMessages << std::endl;
    std::cout << "Total singles sent: " << totalSinglesSent << std::endl;
#ifdef DEBUG
    const double enqueueAvgUs = totalEnqueueCalls > 0 ? (double)totalEnqueueTotalNs / totalEnqueueCalls / 1e3 : 0.0;
    const double enqueueWaitAvgUs = totalEnqueueCalls > 0 ? (double)totalEnqueueWaitNs / totalEnqueueCalls / 1e3 : 0.0;
    const double serializeAvgUs = totalGrpcMessages > 0 ? (double)totalSerializeBuildNs / totalGrpcMessages / 1e3 : 0.0;
    const double writeAvgUs = totalGrpcMessages > 0 ? (double)totalWriteNs / totalGrpcMessages / 1e3 : 0.0;
    const double wireMiB = (double)totalEstimatedWireBytes / (1024.0 * 1024.0);
    const double wireThroughputMiBs = elapsedMs > 0 ? wireMiB / ((double)elapsedMs / 1000.0) : 0.0;

    std::cout << "Perf enqueue(avg/wait avg/max wait): "
              << enqueueAvgUs << "/" << enqueueWaitAvgUs << "/" << (double)maxEnqueueWaitNsAll / 1e3
              << " us" << std::endl;
    std::cout << "Perf serialize avg per message: " << serializeAvgUs
              << " us, write(avg/max): " << writeAvgUs << "/" << (double)maxWriteNsAll / 1e3
              << " us" << std::endl;
    std::cout << "Wire bytes (estimated proto): " << totalEstimatedWireBytes
              << " bytes (" << wireMiB << " MiB), throughput=" << wireThroughputMiBs
              << " MiB/s" << std::endl;
    std::cout << "Memory peak queue: " << maxPeakQueueSegments << " segments, "
              << maxPeakQueueSingles << " singles, " << (double)maxPeakQueueBytes / (1024.0 * 1024.0)
              << " MiB" << std::endl;
    std::cout << "Memory max process RSS: "
              << (double)maxProcessRssBytes / (1024.0 * 1024.0) << " MiB" << std::endl;
#endif
    std::cout << "Elapsed time: " << elapsedMs << " ms" << std::endl;
    std::cout << "==================================" << std::endl;

    return allSuccess ? 0 : 4;
}

/*
Build example:
  make test-local-grpc-r2s

运行（连接外部 coin，3 节点并行注册并等待开始信号）
./bin/test_local_grpc_r2s \
  --address 127.0.0.1:50061 \
  --split-dir Data/bdm2/split_Data \
  --calibration-dir Data/bdm2/calibration \
  --result-dir Data/result/Bdm2/split \
  --raw-prefix "2_PET_2Bed pet 600s-bed0" \
  --node-count 3 \
  --max-pending-segments 128 \
    --batch-segments 3 \
  --parallel \
  --no-local-receiver
*/
