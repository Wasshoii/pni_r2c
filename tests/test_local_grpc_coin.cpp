#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "dataplane/rdma/ProtoConvert.hpp"
#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "protos/coincidence.grpc.pb.h"

namespace coincidence = openpni::distributed::coincidence;
namespace rdma = openpni::distributed::dataplane::rdma;

namespace
{
    std::atomic<bool> g_stopRequested{false};

    void onSignal(int /*sig*/)
    {
        g_stopRequested.store(true, std::memory_order_relaxed);
    }

    struct ProgramOptions
    {
        std::string address = "127.0.0.1:50061";
        uint32_t expectedNodeCount = 2;
        uint32_t startLeadTimeMs = 1000;
        uint32_t defaultWaitForStartTimeoutMs = 30000;
        uint32_t statusPrintIntervalMs = 2000;
        uint32_t idleExitSeconds = 15;
        uint32_t runSeconds = 0; // 0 = run until Ctrl+C or idle-exit
        bool autoStartWhenAllRegistered = true;
        bool requireDataFromAllNodes = true;
        bool helpOnly = false;
    };

    struct NodeStats
    {
        bool registered = false;
        bool connected = false;
        uint32_t channelCount = 0;
        std::string detectorType;
        std::string nodeAddress;
        uint64_t singlesReceived = 0;
        uint64_t chunksReceived = 0;
        uint64_t lastChunkId = 0;
        bool hasLastChunk = false;
        uint64_t chunkGapCount = 0;
        uint64_t lastHeartbeatMs = 0;
    };

    class LocalCoincidenceReceiverService final : public coincidence::CoincidenceService::Service
    {
    public:
        explicit LocalCoincidenceReceiverService(ProgramOptions opts)
            : m_opts(std::move(opts))
        {
            m_lastActivityNs.store(nowNs(), std::memory_order_relaxed);
            m_rdma = std::make_unique<rdma::RdmaRecvServer>(rdma::RdmaRecvServer::Config{});
            m_rdma->setIngest([this](const rdma::SlotChunkView &view) {
                m_totalChunksReceived.fetch_add(1, std::memory_order_relaxed);
                m_totalSinglesReceived.fetch_add(view.singlesCount, std::memory_order_relaxed);
                m_lastActivityNs.store(nowNs(), std::memory_order_relaxed);
                std::lock_guard<std::mutex> lock(m_mutex);
                auto &node = m_nodes[view.nodeId];
                node.connected = true;
                node.registered = true;
                if (node.hasLastChunk && view.chunkId != node.lastChunkId + 1)
                    node.chunkGapCount++;
                node.lastChunkId = view.chunkId;
                node.hasLastChunk = true;
                node.chunksReceived += 1;
                node.singlesReceived += view.singlesCount;
                return true;
            });
            m_rdma->start();
        }
        ~LocalCoincidenceReceiverService() override
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

        grpc::Status WaitForStart(
            grpc::ServerContext * /*context*/,
            const coincidence::WaitForStartRequest *request,
            coincidence::WaitForStartResponse *response) override
        {
            uint32_t timeoutMs = request->timeout_ms();
            if (timeoutMs == 0)
            {
                timeoutMs = m_opts.defaultWaitForStartTimeoutMs;
            }

            std::unique_lock<std::mutex> lock(m_mutex);
            auto nodeIt = m_nodes.find(request->node_id());
            if (nodeIt == m_nodes.end() || !nodeIt->second.registered)
            {
                fillOrchestrationFields(*response);
                response->set_success(false);
                response->set_start_signal_issued(false);
                response->set_start_time_ms(0);
                response->set_message("node not registered yet");
                return grpc::Status::OK;
            }

            const auto ready = [this]
            {
                return m_startSignalIssued || m_stopService;
            };

            bool ok = ready();
            if (!ok)
            {
                if (timeoutMs == 0)
                {
                    m_cv.wait(lock, ready);
                    ok = ready();
                }
                else
                {
                    ok = m_cv.wait_for(lock, std::chrono::milliseconds(timeoutMs), ready);
                }
            }

            fillOrchestrationFields(*response);
            if (m_stopService)
            {
                response->set_success(false);
                response->set_start_signal_issued(false);
                response->set_start_time_ms(0);
                response->set_message("receiver stopping");
                return grpc::Status::OK;
            }

            if (!ok || !m_startSignalIssued)
            {
                response->set_success(false);
                response->set_start_signal_issued(false);
                response->set_start_time_ms(0);
                response->set_message("timed out waiting for start signal");
                return grpc::Status::OK;
            }

            response->set_success(true);
            response->set_start_signal_issued(true);
            response->set_start_time_ms(m_plannedStartTimeMs);
            response->set_message("start signal issued");
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
            response->set_is_running(!m_stopService.load(std::memory_order_relaxed));

            {
                std::lock_guard<std::mutex> lock(m_mutex);
                fillOrchestrationFields(*response);

                if (request->include_node_stats())
                {
                    for (const auto &[nodeId, node] : m_nodes)
                    {
                        auto *nodeStatus = response->add_node_stats();
                        nodeStatus->set_node_id(nodeId);
                        nodeStatus->set_chunks_received(node.chunksReceived);
                        nodeStatus->set_singles_received(node.singlesReceived);
                        nodeStatus->set_buffer_size(0);
                        nodeStatus->set_connected(node.connected);
                    }
                }
            }

            return grpc::Status::OK;
        }

        grpc::Status Control(
            grpc::ServerContext * /*context*/,
            const coincidence::ControlRequest *request,
            coincidence::ControlResponse *response) override
        {
            if (request->command() == coincidence::ControlRequest::START)
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_startSignalIssued)
                {
                    response->set_success(false);
                    response->set_message("start already issued");
                }
                else
                {
                    issueStartSignalLocked("manual control start");
                    response->set_success(true);
                    response->set_message("start issued");
                }
                return grpc::Status::OK;
            }

            if (request->command() == coincidence::ControlRequest::STOP)
            {
                requestStop();
                response->set_success(true);
                response->set_message("stop requested");
                return grpc::Status::OK;
            }

            response->set_success(false);
            response->set_message("unsupported control command in receiver-only test");
            return grpc::Status::OK;
        }

        grpc::Status UpdateConfig(
            grpc::ServerContext * /*context*/,
            const coincidence::ConfigUpdateRequest * /*request*/,
            coincidence::ConfigUpdateResponse *response) override
        {
            response->set_success(false);
            response->set_message("not supported in receiver-only test");
            return grpc::Status::OK;
        }

        grpc::Status RegisterNode(
            grpc::ServerContext * /*context*/,
            const coincidence::RegisterNodeRequest *request,
            coincidence::RegisterNodeResponse *response) override
        {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                auto &node = m_nodes[request->node_id()];
                node.registered = true;
                node.connected = true;
                node.channelCount = request->channel_count();
                node.detectorType = request->detector_type();
                node.nodeAddress = request->node_address();

                std::cout << "[CoinReceiver] Register node=" << request->node_id()
                          << " addr=" << request->node_address()
                          << " channels=" << request->channel_count()
                          << " detector=" << request->detector_type()
                          << " connected=" << connectedNodeCountLocked()
                          << "/" << m_opts.expectedNodeCount << std::endl;

                if (m_opts.autoStartWhenAllRegistered &&
                    connectedNodeCountLocked() >= m_opts.expectedNodeCount)
                {
                    issueStartSignalLocked("all expected nodes registered");
                }

                fillOrchestrationFields(*response);
            }

            response->set_success(true);
            response->set_assigned_node_id(request->node_id());
            response->set_message("node registered");
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
                node.registered = true;
                node.lastHeartbeatMs = request->timestamp_ms();
            }

            response->set_acknowledged(true);
            response->set_server_timestamp_ms(nowMs());
            return grpc::Status::OK;
        }

        void printStatusSnapshot() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            std::cout << "\n[CoinReceiver] Status" << std::endl;
            std::cout << "  startIssued=" << (m_startSignalIssued ? "true" : "false")
                      << " plannedStartMs=" << m_plannedStartTimeMs << std::endl;
            std::cout << "  connected=" << connectedNodeCountLocked() << "/" << m_opts.expectedNodeCount << std::endl;
            std::cout << "  totalChunks=" << m_totalChunksReceived.load(std::memory_order_relaxed)
                      << " totalSingles=" << m_totalSinglesReceived.load(std::memory_order_relaxed)
                      << std::endl;

            for (const auto &[nodeId, node] : m_nodes)
            {
                std::cout << "  node=" << nodeId
                          << " chunks=" << node.chunksReceived
                          << " singles=" << node.singlesReceived
                          << " chunkGaps=" << node.chunkGapCount
                          << " connected=" << (node.connected ? "true" : "false")
                          << std::endl;
            }
            std::cout << std::endl;
        }

        bool basicValidation() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);

            bool ok = true;
            const uint32_t connected = connectedNodeCountLocked();

            if (connected < m_opts.expectedNodeCount)
            {
                std::cerr << "[CoinReceiver][CHECK] expected " << m_opts.expectedNodeCount
                          << " nodes, but registered " << connected << std::endl;
                ok = false;
            }

            if (m_totalSinglesReceived.load(std::memory_order_relaxed) == 0)
            {
                std::cerr << "[CoinReceiver][CHECK] no singles received" << std::endl;
                ok = false;
            }

            for (const auto &[nodeId, node] : m_nodes)
            {
                if (m_opts.requireDataFromAllNodes && node.singlesReceived == 0)
                {
                    std::cerr << "[CoinReceiver][CHECK] node " << nodeId << " has zero singles" << std::endl;
                    ok = false;
                }

                if (node.chunkGapCount > 0)
                {
                    std::cerr << "[CoinReceiver][CHECK] node " << nodeId
                              << " has chunk gaps=" << node.chunkGapCount << std::endl;
                    ok = false;
                }
            }

            if (ok)
            {
                std::cout << "[CoinReceiver][CHECK] PASS" << std::endl;
            }
            else
            {
                std::cout << "[CoinReceiver][CHECK] FAIL" << std::endl;
            }

            return ok;
        }

        void requestStop()
        {
            m_stopService.store(true, std::memory_order_relaxed);
            m_cv.notify_all();
        }

        bool shouldStop() const
        {
            return m_stopService.load(std::memory_order_relaxed);
        }

        bool startIssued() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_startSignalIssued;
        }

        uint64_t totalChunks() const
        {
            return m_totalChunksReceived.load(std::memory_order_relaxed);
        }

        uint64_t lastActivityNs() const
        {
            return m_lastActivityNs.load(std::memory_order_relaxed);
        }

    private:
        static uint64_t nowMs()
        {
            return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
        }

        static uint64_t nowNs()
        {
            return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count());
        }

        uint32_t connectedNodeCountLocked() const
        {
            uint32_t count = 0;
            for (const auto &[_, node] : m_nodes)
            {
                if (node.registered)
                {
                    ++count;
                }
            }
            return count;
        }

        void issueStartSignalLocked(const std::string &reason)
        {
            if (m_startSignalIssued)
            {
                return;
            }

            m_startSignalIssued = true;
            m_plannedStartTimeMs = nowMs() + m_opts.startLeadTimeMs;
            m_cv.notify_all();

            std::cout << "[CoinReceiver] start signal issued"
                      << " reason=" << reason
                      << " plannedStartMs=" << m_plannedStartTimeMs
                      << " connected=" << connectedNodeCountLocked()
                      << "/" << m_opts.expectedNodeCount
                      << std::endl;
        }

        void fillOrchestrationFields(coincidence::StatusResponse &response) const
        {
            response.set_expected_node_count(m_opts.expectedNodeCount);
            response.set_connected_node_count(connectedNodeCountLocked());
            response.set_start_signal_issued(m_startSignalIssued);
        }

        void fillOrchestrationFields(coincidence::RegisterNodeResponse &response) const
        {
            response.set_expected_node_count(m_opts.expectedNodeCount);
            response.set_connected_node_count(connectedNodeCountLocked());
            response.set_start_signal_issued(m_startSignalIssued);
            response.set_planned_start_time_ms(m_plannedStartTimeMs);
        }

        void fillOrchestrationFields(coincidence::WaitForStartResponse &response) const
        {
            response.set_expected_node_count(m_opts.expectedNodeCount);
            response.set_connected_node_count(connectedNodeCountLocked());
        }

        ProgramOptions m_opts;

        mutable std::mutex m_mutex;
        mutable std::condition_variable m_cv;
        std::unordered_map<uint32_t, NodeStats> m_nodes;

        std::atomic<bool> m_stopService{false};
        bool m_startSignalIssued = false;
        uint64_t m_plannedStartTimeMs = 0;

        std::atomic<uint64_t> m_totalSinglesReceived{0};
        std::unique_ptr<rdma::RdmaRecvServer> m_rdma;
        std::atomic<uint64_t> m_totalChunksReceived{0};
        std::atomic<uint64_t> m_lastActivityNs{0};
    };

    void printUsage(const char *prog)
    {
        std::cout << "Usage: " << prog << " [options]\n"
                  << "Options:\n"
                  << "  --address <host:port>              gRPC listen address (default: 127.0.0.1:50061)\n"
                  << "  --expected-node-count <N>          expected node count before auto-start (default: 3)\n"
                  << "  --start-lead-ms <ms>               delay between start signal and start time (default: 1000)\n"
                  << "  --wait-start-timeout-ms <ms>       default WaitForStart timeout (default: 30000)\n"
                  << "  --status-interval-ms <ms>          periodic status print interval (default: 2000)\n"
                  << "  --idle-exit-seconds <sec>          auto-exit when idle after data started (default: 15)\n"
                  << "  --run-seconds <sec>                force exit after N seconds (0=infinite)\n"
                  << "  --manual-start                     disable auto-start when all nodes registered\n"
                  << "  --allow-empty-node-data            do not require every node to send singles\n"
                  << "  --help                             print this message\n"
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
            if (arg == "--manual-start")
            {
                opts.autoStartWhenAllRegistered = false;
                continue;
            }
            if (arg == "--allow-empty-node-data")
            {
                opts.requireDataFromAllNodes = false;
                continue;
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
            if (arg == "--expected-node-count")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.expectedNodeCount = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--start-lead-ms")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.startLeadTimeMs = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--wait-start-timeout-ms")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.defaultWaitForStartTimeoutMs = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--status-interval-ms")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.statusPrintIntervalMs = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--idle-exit-seconds")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.idleExitSeconds = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--run-seconds")
            {
                const char *v = requireValue(arg);
                if (!v)
                {
                    return false;
                }
                opts.runSeconds = static_cast<uint32_t>(std::stoul(v));
                continue;
            }

            std::cerr << "Unknown argument: " << arg << std::endl;
            printUsage(argv[0]);
            return false;
        }

        if (opts.expectedNodeCount == 0)
        {
            std::cerr << "--expected-node-count must be positive" << std::endl;
            return false;
        }

        return true;
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

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    std::cout << "===========================================" << std::endl;
    std::cout << "  Local gRPC Coin Receiver Test (No Coin)" << std::endl;
    std::cout << "===========================================" << std::endl;
    std::cout << "address             : " << opts.address << std::endl;
    std::cout << "expectedNodeCount   : " << opts.expectedNodeCount << std::endl;
    std::cout << "autoStart           : " << (opts.autoStartWhenAllRegistered ? "true" : "false") << std::endl;
    std::cout << "startLeadTimeMs     : " << opts.startLeadTimeMs << std::endl;
    std::cout << "waitStartTimeoutMs  : " << opts.defaultWaitForStartTimeoutMs << std::endl;
    std::cout << "statusIntervalMs    : " << opts.statusPrintIntervalMs << std::endl;
    std::cout << "idleExitSeconds     : " << opts.idleExitSeconds << std::endl;
    std::cout << "runSeconds          : " << opts.runSeconds << std::endl;

    LocalCoincidenceReceiverService service(opts);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(opts.address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    if (!server)
    {
        std::cerr << "[CoinReceiver] failed to start on " << opts.address << std::endl;
        return 2;
    }

    std::cout << "[CoinReceiver] listening on " << opts.address << std::endl;
    std::cout << "[CoinReceiver] waiting for nodes..." << std::endl;

    const auto t0 = std::chrono::steady_clock::now();

    while (!g_stopRequested.load(std::memory_order_relaxed) && !service.shouldStop())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(opts.statusPrintIntervalMs));
        service.printStatusSnapshot();

        const auto now = std::chrono::steady_clock::now();
        if (opts.runSeconds > 0)
        {
            const auto elapsedSec = std::chrono::duration_cast<std::chrono::seconds>(now - t0).count();
            if (elapsedSec >= opts.runSeconds)
            {
                std::cout << "[CoinReceiver] run-seconds reached, stopping" << std::endl;
                break;
            }
        }

        if (opts.idleExitSeconds > 0 && service.startIssued() && service.totalChunks() > 0)
        {
            const uint64_t nowNs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count());
            const uint64_t idleNs = nowNs - service.lastActivityNs();
            if (idleNs >= static_cast<uint64_t>(opts.idleExitSeconds) * 1000000000ULL)
            {
                std::cout << "[CoinReceiver] idle timeout reached, stopping" << std::endl;
                break;
            }
        }
    }

    service.requestStop();
    server->Shutdown();

    service.printStatusSnapshot();
    const bool ok = service.basicValidation();

    std::cout << "===========================================" << std::endl;
    std::cout << "  Coin receiver test " << (ok ? "PASSED" : "FAILED") << std::endl;
    std::cout << "===========================================" << std::endl;

    return ok ? 0 : 4;
}

/*
Build:
  cmake --build --preset build-tests-pni --target test_local_grpc_coin

Terminal 1 (coin receiver, orchestration only — no real StreamingTimeAligner):
  ./bin/test/test_local_grpc_coin --address 127.0.0.1:50061 --expected-node-count 2

Terminal 2 (9120 dual-node R2S; parallel required so both Register before start):
  ./bin/test/test_local_grpc_r2s --no-local-receiver --parallel --address 127.0.0.1:50061

For real R2S→coincidence E2E (multi-GPU only, single process):
  ./bin/test/test_local_grpc_r2s_coin

Preferred single-GPU alternatives (replay .lsingle, no R2S CUDA):
  L2 ingress:  ./bin/test/test_local_grpc_singles_ingress --data-root /media/lenovo/1TB/50100data/test_9120
  L3 coin:     ./bin/test/test_local_grpc_coin_stream --data-root /media/lenovo/1TB/50100data/test_9120 --disable-multi-gpu
*/
