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

#include "dataplane/rdma/ProtoConvert.hpp"
#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "protos/coincidence.grpc.pb.h"
#include "core/io/IOAdapter.hpp"
#include "core/r2s/R2S.hpp"
#include "grpcNode/r2sNode.hpp"

namespace fs = std::filesystem;
namespace coincidence = openpni::distributed::coincidence;
namespace r2s = openpni::distributed::r2s;
namespace grpcnode = openpni::distributed::grpcnode;
namespace rdma = openpni::distributed::dataplane::rdma;

namespace
{
    struct ProgramOptions
    {
        std::string address = "127.0.0.1:50061";
        std::string dataRoot = "/media/lenovo/1TB/50100data/test_9120";
        std::string node0RawDir;
        std::string node1RawDir;
        std::string calibrationDir = "/media/lenovo/1TB/50100data/pni_res/caliFile";
        std::string resultDir;
        size_t maxPendingSegments = 64;
        bool parallelNodes = false;
        bool noLocalReceiver = false;
        bool helpOnly = false;

        void resolveDerivedPaths()
        {
            if (node0RawDir.empty())
            {
                node0RawDir = (fs::path(dataRoot) / "pni_raw_node0").string();
            }
            if (node1RawDir.empty())
            {
                node1RawDir = (fs::path(dataRoot) / "pni_raw_node1").string();
            }
            if (resultDir.empty())
            {
                resultDir = (fs::path(dataRoot) / "pni_singles_grpc").string();
            }
        }
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
        ReceiverOnlyCoincidenceService()
        {
            m_rdma = std::make_unique<rdma::RdmaRecvServer>(rdma::RdmaRecvServer::Config{});
            m_rdma->setIngest([this](const rdma::SlotChunkView &view) {
                m_totalChunksReceived.fetch_add(1, std::memory_order_relaxed);
                m_totalSinglesReceived.fetch_add(view.singlesCount, std::memory_order_relaxed);
                std::lock_guard<std::mutex> lock(m_mutex);
                auto &node = m_nodes[view.nodeId];
                node.connected = true;
                node.chunksReceived += 1;
                node.singlesReceived += view.singlesCount;
                return true;
            });
            m_rdma->start();
        }
        ~ReceiverOnlyCoincidenceService() override
        {
            if (m_rdma) m_rdma->stop();
        }


        grpc::Status StreamSingles(
            grpc::ServerContext *,
            grpc::ServerReader<coincidence::SingleChunkMessage> *,
            coincidence::StreamResponse *) override
        {
            return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "use OpenDataPlane");
        }

        grpc::Status OpenDataPlane(
            grpc::ServerContext *,
            const coincidence::OpenDataPlaneRequest *request,
            coincidence::OpenDataPlaneResponse *response) override
        {
            if (!m_rdma)
            {
                response->set_success(false);
                response->set_message("rdma server missing");
                return grpc::Status::OK;
            }
            auto session = m_rdma->ensureSession(request->node_id());
            if (!session)
            {
                response->set_success(false);
                response->set_message("ensureSession failed");
                return grpc::Status::OK;
            }
            if (request->has_node_endpoint() && session->kind() == rdma::DataPlaneKind::RdmaRoceV2)
            {
                if (!session->acceptRemote(rdma::fromProtoEndpoint(request->node_endpoint())))
                {
                    response->set_success(false);
                    response->set_message("acceptRemote failed");
                    return grpc::Status::OK;
                }
            }
            const auto local = session->localEndpoint();
            response->set_success(true);
            response->set_data_plane_kind(rdma::toProto(local.kind));
            rdma::fillProtoEndpoint(local, response->mutable_coin_endpoint());
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

        grpc::Status WaitForStart(
            grpc::ServerContext *,
            const coincidence::WaitForStartRequest *,
            coincidence::WaitForStartResponse *response) override
        {
            response->set_success(true);
            response->set_start_signal_issued(true);
            response->set_start_time_ms(0);
            response->set_message("receiver-only start");
            return grpc::Status::OK;
        }

        grpc::Status NotifyProducerComplete(
            grpc::ServerContext *,
            const coincidence::NotifyProducerCompleteRequest *,
            coincidence::NotifyProducerCompleteResponse *response) override
        {
            response->set_success(true);
            response->set_all_complete(true);
            response->set_message("ok");
            return grpc::Status::OK;
        }

    private:
        std::mutex m_mutex;
        std::unordered_map<uint32_t, ReceiverNodeStats> m_nodes;
        std::atomic<uint64_t> m_totalSinglesReceived{0};
        std::unique_ptr<rdma::RdmaRecvServer> m_rdma;
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
                  << "9120 dual-node (2 rings/node) local gRPC R2S streaming test.\n"
                  << "Options:\n"
                  << "  --address <host:port>         gRPC local address (default: 127.0.0.1:50061)\n"
                  << "  --data-root <path>            Root with pni_raw_node0/node1 (default: test_9120)\n"
                  << "  --node0-raw <path>            Node0 merged raw directory (channels 0..287)\n"
                  << "  --node1-raw <path>            Node1 merged raw directory (channels 288..575)\n"
                  << "  --calibration-dir <path>      Per-ring calibration directory (reused for both rings)\n"
                  << "  --result-dir <path>           Output directory for R2S config\n"
                  << "  --max-pending-segments <N>    Async sender queue length in segments (default: 64)\n"
                  << "  --no-local-receiver           Do not start built-in receiver; use external coin host\n"
                  << "  --serial                      Run nodes sequentially (default; safer on single GPU)\n"
                  << "  --parallel                    Run nodes in parallel (needs enough GPU VRAM for 2x 288ch)\n"
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
            if (arg == "--data-root")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.dataRoot = v;
                continue;
            }
            if (arg == "--node0-raw")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.node0RawDir = v;
                continue;
            }
            if (arg == "--node1-raw")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.node1RawDir = v;
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
            if (arg == "--parallel")
            {
                opts.parallelNodes = true;
                continue;
            }
            if (arg == "--serial")
            {
                opts.parallelNodes = false;
                continue;
            }
            if (arg == "--no-local-receiver")
            {
                opts.noLocalReceiver = true;
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


            std::cerr << "Unknown argument: " << arg << std::endl;
            printUsage(argv[0]);
            return false;
        }

        opts.resolveDerivedPaths();
        return true;
    }

    std::vector<uint16_t> makeChannelRange(uint16_t begin, uint16_t endExclusive)
    {
        std::vector<uint16_t> channels;
        channels.reserve(static_cast<size_t>(endExclusive - begin));
        for (uint16_t ch = begin; ch < endExclusive; ++ch)
        {
            channels.push_back(ch);
        }
        return channels;
    }

    std::vector<NodeInput> buildNodeInputs(const ProgramOptions &opts)
    {
        std::vector<NodeInput> nodes(2);
        nodes[0].nodeId = 0;
        nodes[0].rawdataPath = opts.node0RawDir;
        nodes[0].channels = makeChannelRange(0, 288);

        nodes[1].nodeId = 1;
        nodes[1].rawdataPath = opts.node1RawDir;
        nodes[1].channels = makeChannelRange(288, 576);
        return nodes;
    }

    std::string findFirstRawFile(const std::string &rawDir)
    {
        std::vector<std::string> matches;
        for (const auto &entry : fs::directory_iterator(rawDir))
        {
            if (!entry.is_regular_file())
            {
                continue;
            }
            const std::string name = entry.path().filename().string();
            if (name.rfind("pniRaw-", 0) == 0 && name.size() >= 4 &&
                name.compare(name.size() - 4, 4, ".bin") == 0)
            {
                matches.push_back(entry.path().string());
            }
        }
        std::sort(matches.begin(), matches.end());
        return matches.empty() ? std::string() : matches.front();
    }

    bool validateRawdataHeaders(const std::vector<NodeInput> &nodes)
    {
        constexpr uint16_t kExpectedChannelNum = 576;
        bool ok = true;
        for (const auto &n : nodes)
        {
            try
            {
                const std::string samplePath = findFirstRawFile(n.rawdataPath);
                if (samplePath.empty())
                {
                    std::cerr << "[Input] No pniRaw-*.bin in directory: " << n.rawdataPath << std::endl;
                    ok = false;
                    continue;
                }

                openpni::distributed::coreio::RawDataFileReader reader;
                reader.Open(samplePath);
                const auto &info = reader.Info();
                if (info.channelNum != kExpectedChannelNum)
                {
                    std::cerr << "[Input] Rawdata channelNum mismatch: " << samplePath
                              << " channelNum=" << info.channelNum
                              << " expected=" << kExpectedChannelNum << std::endl;
                    ok = false;
                }
                if (info.segmentNum == 0)
                {
                    std::cerr << "[Input] Rawdata has no segments: " << samplePath << std::endl;
                    ok = false;
                }

                if (info.segmentNum > 0 && !n.channels.empty())
                {
                    const auto minIt = std::min_element(n.channels.begin(), n.channels.end());
                    const auto maxIt = std::max_element(n.channels.begin(), n.channels.end());
                    const uint16_t minChannel = *minIt;
                    const uint16_t maxChannel = *maxIt;

                    auto segment = reader.ReadSegment(0, 1);
                    auto view = segment.View();
                    for (uint64_t i = 0; i < view.count; ++i)
                    {
                        const uint16_t ch = view.channel[i];
                        if (ch >= info.channelNum)
                        {
                            std::cerr << "[Input] Rawdata channel out of header range: " << samplePath
                                      << " channel=" << ch << " header.channelNum=" << info.channelNum << std::endl;
                            ok = false;
                            break;
                        }
                        if (ch < minChannel || ch > maxChannel)
                        {
                            std::cerr << "[Input] Warning: channel outside node range (ignored): " << samplePath
                                      << " channel=" << ch << " expected=[" << minChannel << "-" << maxChannel << "]"
                                      << std::endl;
                            // Soft warning only — merged files may contain both rings' neighbors
                            // when sampling first packets; filtering happens in R2S.
                            break;
                        }
                    }
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "[Input] Failed to read rawdata header for node " << n.nodeId
                          << " dir=" << n.rawdataPath << " error=" << e.what() << std::endl;
                ok = false;
            }
        }
        return ok;
    }

    bool validateInputs(
        const std::vector<NodeInput> &nodes,
        const std::string &calibrationDir,
        const std::string &resultDir)
    {
        bool ok = true;

        for (const auto &n : nodes)
        {
            if (!fs::exists(n.rawdataPath) || !fs::is_directory(n.rawdataPath))
            {
                std::cerr << "[Input] Missing merged rawdata directory: " << n.rawdataPath << std::endl;
                ok = false;
            }
        }

        if (!fs::exists(calibrationDir) || !fs::is_directory(calibrationDir))
        {
            std::cerr << "[Input] Missing calibration directory: " << calibrationDir << std::endl;
            ok = false;
        }

        std::error_code ec;
        fs::create_directories(resultDir, ec);
        if (ec)
        {
            std::cerr << "[Input] Failed to create result directory: " << resultDir
                      << " error=" << ec.message() << std::endl;
            ok = false;
        }

        if (!validateRawdataHeaders(nodes))
        {
            ok = false;
        }

        return ok;
    }

    NodeRunStats runSingleNode(
        const ProgramOptions &opts,
        const NodeInput &node)
    {
        auto config = r2s::createBDM50100_9120Config(
            node.rawdataPath,
            opts.resultDir,
            {opts.calibrationDir, opts.calibrationDir},
            "node_" + std::to_string(node.nodeId),
            node.channels,
            4);

        config.sortDataByTime = true;
        config.saveData2SingleFile = false;
        config.asyncFileWrite = false;
        config.useEnergyCut = true;
        config.energyCutLow = 421000.0f;
        config.energyCutHigh = 1000000.0f;

        grpcnode::R2SGrpcNode nodeRunner(
            config,
            opts.address,
            node.nodeId,
            static_cast<uint32_t>(node.channels.size()),
            opts.maxPendingSegments,
            "127.0.0.1",
            "BDM50100",
            50,
            true,
            0,
            15000,
            1000);

        std::cout << "[Node " << node.nodeId << "] R2S start, dir=" << node.rawdataPath
                  << " channels=[" << node.channels.front() << ".." << node.channels.back() << "]"
                  << std::endl;
        nodeRunner.run();
        NodeRunStats stats = nodeRunner.stats();

        std::cout << "[Node " << node.nodeId << "] R2S done, success="
                  << (stats.success ? "true" : "false")
                  << " callbacks=" << stats.callbackCount
                  << " rdmaChunks=" << stats.rdmaChunksSent
                  << " singlesSent=" << stats.singlesSent << std::endl;

#ifdef DEBUG
        const double enqueueAvgUs = stats.enqueueCalls > 0 ? (double)stats.enqueueTotalNs / stats.enqueueCalls / 1e3 : 0.0;
        const double enqueueWaitAvgUs = stats.enqueueCalls > 0 ? (double)stats.enqueueWaitNs / stats.enqueueCalls / 1e3 : 0.0;
        const double writeAvgUs = stats.rdmaChunksSent > 0 ? (double)stats.writeNs / stats.rdmaChunksSent / 1e3 : 0.0;

        std::cout << "[Node " << node.nodeId << "] Perf enqueue(avg/wait avg/max wait)="
                  << enqueueAvgUs << "/" << enqueueWaitAvgUs << "/" << (double)stats.maxEnqueueWaitNs / 1e3
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
    std::cout << "  Local gRPC R2S Streaming Test (9120 2-node)" << std::endl;
    std::cout << "============================================" << std::endl;
    std::cout << "address      : " << opts.address << std::endl;
    std::cout << "dataRoot     : " << opts.dataRoot << std::endl;
    std::cout << "node0RawDir  : " << opts.node0RawDir << std::endl;
    std::cout << "node1RawDir  : " << opts.node1RawDir << std::endl;
    std::cout << "calibrationDir: " << opts.calibrationDir << std::endl;
    std::cout << "resultDir    : " << opts.resultDir << std::endl;
    std::cout << "maxPendingSegments: " << opts.maxPendingSegments << std::endl;
    std::cout << "parallelNodes: " << (opts.parallelNodes ? "true" : "false") << std::endl;
    std::cout << "noLocalReceiver: " << (opts.noLocalReceiver ? "true" : "false") << std::endl;

    const auto nodeInputs = buildNodeInputs(opts);

    if (!validateInputs(nodeInputs, opts.calibrationDir, opts.resultDir))
    {
        std::cerr << "Input validation failed. Please verify merged raw dirs / calibration." << std::endl;
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
                                 { stats[i] = runSingleNode(opts, nodeInputs[i]); });
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
            stats[i] = runSingleNode(opts, nodeInputs[i]);
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
    uint64_t totalRdmaChunks = 0;
#ifdef DEBUG
    uint64_t totalEnqueueCalls = 0;
    uint64_t totalEnqueueTotalNs = 0;
    uint64_t totalEnqueueWaitNs = 0;
    uint64_t totalWriteNs = 0;
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
        totalRdmaChunks += s.rdmaChunksSent;
#ifdef DEBUG
        totalEnqueueCalls += s.enqueueCalls;
        totalEnqueueTotalNs += s.enqueueTotalNs;
        totalEnqueueWaitNs += s.enqueueWaitNs;
        totalWriteNs += s.writeNs;
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
    std::cout << "Total RDMA chunks: " << totalRdmaChunks << std::endl;
    std::cout << "Total singles sent: " << totalSinglesSent << std::endl;
#ifdef DEBUG
    const double enqueueAvgUs = totalEnqueueCalls > 0 ? (double)totalEnqueueTotalNs / totalEnqueueCalls / 1e3 : 0.0;
    const double enqueueWaitAvgUs = totalEnqueueCalls > 0 ? (double)totalEnqueueWaitNs / totalEnqueueCalls / 1e3 : 0.0;
    const double writeAvgUs = totalRdmaChunks > 0 ? (double)totalWriteNs / totalRdmaChunks / 1e3 : 0.0;

    std::cout << "Perf enqueue(avg/wait avg/max wait): "
              << enqueueAvgUs << "/" << enqueueWaitAvgUs << "/" << (double)maxEnqueueWaitNsAll / 1e3
              << " us" << std::endl;
    std::cout << "Perf write(avg/max): " << writeAvgUs << "/" << (double)maxWriteNsAll / 1e3
              << " us" << std::endl;
    std::cout << "Memory peak queue: " << maxPeakQueueSegments << " segments, "
              << maxPeakQueueSingles << " singles, " << (double)maxPeakQueueBytes / (1024.0 * 1024.0)
              << " MiB" << std::endl;
    std::cout << "Memory max process RSS: "
              << (double)maxProcessRssBytes / (1024.0 * 1024.0) << " MiB" << std::endl;
#endif
    std::cout << "Elapsed time: " << elapsedMs << " ms" << std::endl;
    std::cout << "==================================" << std::endl;

    return allSuccess && totalSinglesSent > 0 ? 0 : 4;
}

/*
Build example:
    cmake --build --preset build-tests-cuda --target test_local_grpc_r2s

Run (9120 dual-node; default serial to avoid dual-GPU OOM on one card):
./bin/test/test_local_grpc_r2s \
    --address 127.0.0.1:50061 \
    --data-root /media/lenovo/1TB/50100data/test_9120 \
    --calibration-dir /media/lenovo/1TB/50100data/pni_res/caliFile \
    --result-dir /media/lenovo/1TB/50100data/test_9120/pni_singles_grpc \
    --max-pending-segments 32 \
    
Parallel (requires enough free GPU VRAM for 2x 288ch R2S):
./bin/test/test_local_grpc_r2s --parallel

External coincidence host (protocol-only receiver):
./bin/test/test_local_grpc_coin --expected-node-count 2 --address 127.0.0.1:50061
./bin/test/test_local_grpc_r2s --no-local-receiver --parallel --address 127.0.0.1:50061

Real R2S→streaming coincidence E2E (CoinGrpcNode + dual R2S in one process, multi-GPU only):
./bin/test/test_local_grpc_r2s_coin

L2 RDMA ingress (no CUDA, replays .lsingle files):
./bin/test/test_local_grpc_singles_ingress --data-root /media/lenovo/1TB/50100data/test_9120

L3 gRPC + streaming coincidence (single-GPU safe, replays .lsingle files):
./bin/test/test_local_grpc_coin_stream --data-root /media/lenovo/1TB/50100data/test_9120 --disable-multi-gpu

NOTE: L2/L3 require pre-computed singles from L1 offline R2S (pni_singles_node0/1).
*/
