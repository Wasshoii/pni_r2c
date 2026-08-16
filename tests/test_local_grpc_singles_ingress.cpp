/**
 * @file test_local_grpc_singles_ingress.cpp
 * @brief L2 RDMA ingress test: replay .lsingle files to a receive-only host.
 *
 * Validates Register / WaitForStart / OpenDataPlane(RDMA) / chunk continuity without
 * any CUDA workload.
 *
 * Build:
 *   cmake --build --preset build-tests-pni --target test_local_grpc_singles_ingress
 *
 * Run:
 *   ./bin/test/test_local_grpc_singles_ingress \
 *     --data-root /media/lenovo/1TB/50100data/test_9120
 */

#include <atomic>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include <fstream>
#include <sstream>

#include "dataplane/rdma/ProtoConvert.hpp"
#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"
#include "protos/coincidence.grpc.pb.h"
#include "tests/grpc_singles_replay.hpp"

namespace fs = std::filesystem;
namespace coincidence = openpni::distributed::coincidence;
namespace rdma = openpni::distributed::dataplane::rdma;

namespace
{
    struct ProgramOptions
    {
        std::string address = "127.0.0.1:50061";
        std::string dataRoot = "/media/lenovo/1TB/50100data/test_9120";
        std::string node0Dir;
        std::string node1Dir;
        /** "0" | "1" | "both" — which sender nodes to launch. */
        std::string nodes = "0";
        uint32_t expectedNodeCount = 0; // 0 = derived from nodes
        uint64_t singlesPerSec = 0;        // 0 = burst
        size_t pushChunkSingles = 2000000;
        size_t maxFiles = 0;
        uint32_t idleExitSeconds = 10;
        bool preload = true;
        size_t maxMemoryGB = 30; // per-node cap in GiB
        uint32_t sendRounds = 3;
        bool sendOnly = false;        // black-hole receiver (send benchmark)
        bool helpOnly = false;

        bool enableNode0() const { return nodes == "0" || nodes == "both"; }
        bool enableNode1() const { return nodes == "1" || nodes == "both"; }
        uint32_t activeNodeCount() const
        {
            return (enableNode0() ? 1u : 0u) + (enableNode1() ? 1u : 0u);
        }

        void resolveDerivedPaths()
        {
            if (node0Dir.empty())
                node0Dir = (fs::path(dataRoot) / "pni_singles_node0").string();
            if (node1Dir.empty())
                node1Dir = (fs::path(dataRoot) / "pni_singles_node1").string();
            if (expectedNodeCount == 0)
                expectedNodeCount = activeNodeCount();
        }
    };

    /** Read MemAvailable from /proc/meminfo (bytes). Returns 0 on failure. */
    uint64_t readMemAvailableBytes()
    {
        std::ifstream meminfo("/proc/meminfo");
        if (!meminfo)
            return 0;
        std::string line;
        while (std::getline(meminfo, line))
        {
            if (line.rfind("MemAvailable:", 0) == 0)
            {
                std::istringstream iss(line.substr(13));
                uint64_t kb = 0;
                iss >> kb;
                return kb * 1024ULL;
            }
        }
        return 0;
    }

    /** Per-node preload cap respecting CLI and available RAM (80% shared across nodes). */
    size_t computePerNodeMemoryCapBytes(const ProgramOptions &opts)
    {
        constexpr size_t kBytesPerGiB = 1024ULL * 1024ULL * 1024ULL;
        size_t capBytes = opts.maxMemoryGB * kBytesPerGiB;

        const uint64_t memAvail = readMemAvailableBytes();
        if (memAvail > 0 && opts.expectedNodeCount > 0)
        {
            // Leave ~20% headroom; split remainder across concurrent preload threads.
            const uint64_t budget = static_cast<uint64_t>(static_cast<double>(memAvail) * 0.80);
            const uint64_t perNodeFromRam = budget / opts.expectedNodeCount;
            if (perNodeFromRam < capBytes)
            {
                std::cout << "[Ingress] Reducing per-node preload cap to "
                          << (perNodeFromRam / kBytesPerGiB) << " GiB (MemAvailable="
                          << (memAvail / kBytesPerGiB) << " GiB)" << std::endl;
                capBytes = static_cast<size_t>(perNodeFromRam);
            }
        }

        return capBytes;
    }

    struct NodeStats
    {
        bool registered = false;
        uint64_t singlesReceived = 0;
        uint64_t chunksReceived = 0;
        uint64_t lastChunkId = 0;
        bool hasLastChunk = false;
        uint64_t chunkGapCount = 0;
    };

    // Orchestration shared by full and black-hole receivers
    struct ReceiverOrchestration
    {
        uint32_t expectedNodeCount = 2;
        mutable std::mutex mu;
        std::condition_variable cv;
        std::unordered_map<uint32_t, bool> registered;
        std::atomic<bool> startIssued{false};
        uint64_t startTimeMs = 0;

        void onRegister(uint32_t nodeId)
        {
            std::lock_guard<std::mutex> lk(mu);
            registered[nodeId] = true;
            if (registered.size() >= expectedNodeCount && !startIssued.load())
            {
                startIssued.store(true);
                startTimeMs = nowMs() + 1000;
                cv.notify_all();
                std::cout << "[Ingress] All nodes registered, start signal issued" << std::endl;
            }
        }

        uint32_t connectedCountLocked() const
        {
            return static_cast<uint32_t>(registered.size());
        }

        static uint64_t nowMs()
        {
            return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
        }
    };

    // Minimal receiver: discard payload, atomic counter only (send-only benchmark)
    class BlackHoleReceiverService final : public coincidence::CoincidenceService::Service
    {
    public:
        explicit BlackHoleReceiverService(std::shared_ptr<ReceiverOrchestration> orch)
            : m_orch(std::move(orch))
        {
            m_rdma = std::make_unique<rdma::RdmaRecvServer>(rdma::RdmaRecvServer::Config{});
            // Diagnostic: return credit without reading payload (not a production path).
            m_rdma->setIngestMode(rdma::IngestMode::CreditOnly);
            m_rdma->setCreditOnly([this](uint32_t /*nodeId*/, uint32_t singlesCount) {
                m_totalSingles.fetch_add(singlesCount, std::memory_order_relaxed);
            });
            m_rdma->start();
        }

        ~BlackHoleReceiverService() override
        {
            if (m_rdma) m_rdma->stop();
        }

        grpc::Status RegisterNode(
            grpc::ServerContext *,
            const coincidence::RegisterNodeRequest *request,
            coincidence::RegisterNodeResponse *response) override
        {
            std::cout << "[BlackHole] Register node=" << request->node_id() << std::endl;
            m_orch->onRegister(request->node_id());

            response->set_success(true);
            response->set_assigned_node_id(request->node_id());
            response->set_expected_node_count(m_orch->expectedNodeCount);
            response->set_connected_node_count(m_orch->connectedCountLocked());
            response->set_start_signal_issued(m_orch->startIssued.load());
            response->set_planned_start_time_ms(m_orch->startTimeMs);
            return grpc::Status::OK;
        }

        grpc::Status WaitForStart(
            grpc::ServerContext *,
            const coincidence::WaitForStartRequest *request,
            coincidence::WaitForStartResponse *response) override
        {
            uint32_t timeoutMs = request->timeout_ms();
            if (timeoutMs == 0) timeoutMs = 30000;

            std::unique_lock<std::mutex> lk(m_orch->mu);
            if (!m_orch->startIssued.load())
            {
                m_orch->cv.wait_for(lk, std::chrono::milliseconds(timeoutMs),
                                   [this] { return m_orch->startIssued.load(); });
            }

            response->set_success(m_orch->startIssued.load());
            response->set_start_signal_issued(m_orch->startIssued.load());
            response->set_start_time_ms(m_orch->startTimeMs);
            response->set_expected_node_count(m_orch->expectedNodeCount);
            response->set_connected_node_count(m_orch->connectedCountLocked());
            return grpc::Status::OK;
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
            if (!m_loggedKind.exchange(true))
            {
                std::cout << "[BlackHole] dataPlaneKind="
                          << (local.kind == rdma::DataPlaneKind::RdmaRoceV2 ? "RoCEv2" : "InProcess")
                          << " slots=" << local.slotCount
                          << " stride=" << local.slotStride << std::endl;
            }
            return grpc::Status::OK;
        }

        grpc::Status GetStatus(
            grpc::ServerContext *,
            const coincidence::StatusRequest *,
            coincidence::StatusResponse *response) override
        {
            response->set_total_singles_received(m_totalSingles.load(std::memory_order_relaxed));
            response->set_is_running(true);
            return grpc::Status::OK;
        }

        grpc::Status Heartbeat(
            grpc::ServerContext *,
            const coincidence::HeartbeatRequest *,
            coincidence::HeartbeatResponse *response) override
        {
            response->set_acknowledged(true);
            response->set_server_timestamp_ms(ReceiverOrchestration::nowMs());
            return grpc::Status::OK;
        }

        grpc::Status NotifyProducerComplete(
            grpc::ServerContext *,
            const coincidence::NotifyProducerCompleteRequest *,
            coincidence::NotifyProducerCompleteResponse *response) override
        {
            response->set_success(true);
            response->set_all_complete(true);
            return grpc::Status::OK;
        }

        grpc::Status Control(
            grpc::ServerContext *,
            const coincidence::ControlRequest *,
            coincidence::ControlResponse *response) override
        {
            response->set_success(true);
            return grpc::Status::OK;
        }

        grpc::Status UpdateConfig(
            grpc::ServerContext *,
            const coincidence::ConfigUpdateRequest *,
            coincidence::ConfigUpdateResponse *response) override
        {
            response->set_success(false);
            return grpc::Status::OK;
        }

        bool validate(uint64_t expectedTotal) const
        {
            const uint64_t received = m_totalSingles.load(std::memory_order_relaxed);
            const bool ok = (expectedTotal == 0 || received == expectedTotal);
            if (ok)
                std::cout << "[BlackHole] PASS total_singles=" << received << std::endl;
            else
                std::cerr << "[BlackHole] FAIL sent=" << expectedTotal
                          << " received=" << received << std::endl;
            return ok;
        }

    private:
        std::shared_ptr<ReceiverOrchestration> m_orch;
        std::atomic<uint64_t> m_totalSingles{0};
        std::atomic<bool> m_loggedKind{false};
        std::unique_ptr<rdma::RdmaRecvServer> m_rdma;
    };

    // Full receive-only service with per-node stats (correctness path)
    class ReceiverService final : public coincidence::CoincidenceService::Service
    {
    public:
        ReceiverService(uint32_t expectedNodes)
            : m_expectedNodeCount(expectedNodes)
        {
            m_rdma = std::make_unique<rdma::RdmaRecvServer>(rdma::RdmaRecvServer::Config{});
            m_rdma->setIngestMode(rdma::IngestMode::Full);
            m_rdma->setIngest([this](const rdma::SlotChunkView &view) {
                m_totalSingles.fetch_add(view.singlesCount, std::memory_order_relaxed);
                m_lastActivityNs.store(nowNs(), std::memory_order_relaxed);
                std::lock_guard<std::mutex> lk(m_mu);
                auto &node = m_nodes[view.nodeId];
                node.registered = true;
                node.singlesReceived += view.singlesCount;
                // Logical chunk continuity: only commit on EOF (multi-slot shares chunkId).
                const bool isEof = (view.flags & rdma::kSlotFlagEof) != 0;
                if (!isEof)
                    return true;
                if (node.hasLastChunk && view.chunkId != node.lastChunkId + 1)
                    node.chunkGapCount++;
                node.lastChunkId = view.chunkId;
                node.hasLastChunk = true;
                node.chunksReceived++;
                return true;
            });
            m_rdma->start();
        }

        ~ReceiverService() override
        {
            if (m_rdma) m_rdma->stop();
        }

        grpc::Status RegisterNode(
            grpc::ServerContext *,
            const coincidence::RegisterNodeRequest *request,
            coincidence::RegisterNodeResponse *response) override
        {
            std::lock_guard<std::mutex> lk(m_mu);
            auto &node = m_nodes[request->node_id()];
            node.registered = true;

            std::cout << "[Ingress] Register node=" << request->node_id()
                      << " channels=" << request->channel_count()
                      << " connected=" << registeredCountLocked() << "/" << m_expectedNodeCount << std::endl;

            if (registeredCountLocked() >= m_expectedNodeCount && !m_startIssued)
            {
                m_startIssued = true;
                m_startTimeMs = nowMs() + 1000;
                m_cv.notify_all();
                std::cout << "[Ingress] All nodes registered, start signal issued" << std::endl;
            }

            response->set_success(true);
            response->set_assigned_node_id(request->node_id());
            response->set_expected_node_count(m_expectedNodeCount);
            response->set_connected_node_count(registeredCountLocked());
            response->set_start_signal_issued(m_startIssued);
            response->set_planned_start_time_ms(m_startTimeMs);
            return grpc::Status::OK;
        }

        grpc::Status WaitForStart(
            grpc::ServerContext *,
            const coincidence::WaitForStartRequest *request,
            coincidence::WaitForStartResponse *response) override
        {
            uint32_t timeoutMs = request->timeout_ms();
            if (timeoutMs == 0) timeoutMs = 30000;

            std::unique_lock<std::mutex> lk(m_mu);
            if (!m_startIssued)
            {
                m_cv.wait_for(lk, std::chrono::milliseconds(timeoutMs),
                              [this] { return m_startIssued.load(); });
            }

            response->set_success(m_startIssued.load());
            response->set_start_signal_issued(m_startIssued.load());
            response->set_start_time_ms(m_startTimeMs);
            response->set_expected_node_count(m_expectedNodeCount);
            response->set_connected_node_count(registeredCountLocked());
            return grpc::Status::OK;
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
            if (!m_loggedKind.exchange(true))
            {
                std::cout << "[Ingress] dataPlaneKind="
                          << (local.kind == rdma::DataPlaneKind::RdmaRoceV2 ? "RoCEv2" : "InProcess")
                          << " slots=" << local.slotCount
                          << " stride=" << local.slotStride << std::endl;
            }
            return grpc::Status::OK;
        }

        grpc::Status GetStatus(
            grpc::ServerContext *,
            const coincidence::StatusRequest *,
            coincidence::StatusResponse *response) override
        {
            response->set_total_singles_received(m_totalSingles.load(std::memory_order_relaxed));
            response->set_is_running(true);
            return grpc::Status::OK;
        }

        grpc::Status Heartbeat(
            grpc::ServerContext *,
            const coincidence::HeartbeatRequest *,
            coincidence::HeartbeatResponse *response) override
        {
            response->set_acknowledged(true);
            response->set_server_timestamp_ms(nowMs());
            return grpc::Status::OK;
        }

        grpc::Status NotifyProducerComplete(
            grpc::ServerContext *,
            const coincidence::NotifyProducerCompleteRequest *,
            coincidence::NotifyProducerCompleteResponse *response) override
        {
            response->set_success(true);
            response->set_all_complete(true);
            return grpc::Status::OK;
        }

        grpc::Status Control(
            grpc::ServerContext *,
            const coincidence::ControlRequest *,
            coincidence::ControlResponse *response) override
        {
            response->set_success(true);
            return grpc::Status::OK;
        }

        grpc::Status UpdateConfig(
            grpc::ServerContext *,
            const coincidence::ConfigUpdateRequest *,
            coincidence::ConfigUpdateResponse *response) override
        {
            response->set_success(false);
            response->set_message("not supported");
            return grpc::Status::OK;
        }

        uint64_t totalSingles() const { return m_totalSingles.load(std::memory_order_relaxed); }
        uint64_t lastActivityNs() const { return m_lastActivityNs.load(std::memory_order_relaxed); }

        bool validate(uint64_t expectedTotal) const
        {
            std::lock_guard<std::mutex> lk(m_mu);
            bool ok = true;

            uint64_t receivedSum = 0;
            for (const auto &[id, node] : m_nodes)
            {
                receivedSum += node.singlesReceived;
                if (node.singlesReceived == 0)
                {
                    std::cerr << "[Ingress][FAIL] node " << id << " received 0 singles" << std::endl;
                    ok = false;
                }
                if (node.chunkGapCount > 0)
                {
                    std::cerr << "[Ingress][FAIL] node " << id
                              << " chunk gaps=" << node.chunkGapCount << std::endl;
                    ok = false;
                }
            }

            if (expectedTotal > 0 && receivedSum != expectedTotal)
            {
                std::cerr << "[Ingress][FAIL] sent=" << expectedTotal
                          << " received=" << receivedSum << std::endl;
                ok = false;
            }

            if (ok)
                std::cout << "[Ingress] PASS  total_singles=" << receivedSum << std::endl;
            else
                std::cout << "[Ingress] FAIL" << std::endl;

            return ok;
        }

    private:
        static uint64_t nowMs()
        {
            return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
        }
        static uint64_t nowNs()
        {
            return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count());
        }
        uint32_t registeredCountLocked() const
        {
            uint32_t c = 0;
            for (const auto &[_, n] : m_nodes)
                if (n.registered) ++c;
            return c;
        }

        uint32_t m_expectedNodeCount;
        mutable std::mutex m_mu;
        std::condition_variable m_cv;
        std::unordered_map<uint32_t, NodeStats> m_nodes;
        std::atomic<bool> m_startIssued{false};
        uint64_t m_startTimeMs = 0;
        std::atomic<uint64_t> m_totalSingles{0};
        std::atomic<uint64_t> m_lastActivityNs{0};
        std::atomic<bool> m_loggedKind{false};
        std::unique_ptr<rdma::RdmaRecvServer> m_rdma;
    };

    bool parseArgs(int argc, char **argv, ProgramOptions &opts)
    {
        for (int i = 1; i < argc; ++i)
        {
            std::string arg = argv[i];
            auto val = [&]() -> const char * {
                if (i + 1 >= argc) { std::cerr << "Missing value for " << arg << std::endl; return nullptr; }
                return argv[++i];
            };

            if (arg == "--help" || arg == "-h") { opts.helpOnly = true; return true; }
            if (arg == "--data-root") { auto v = val(); if (!v) return false; opts.dataRoot = v; continue; }
            if (arg == "--node0-dir") { auto v = val(); if (!v) return false; opts.node0Dir = v; continue; }
            if (arg == "--node1-dir") { auto v = val(); if (!v) return false; opts.node1Dir = v; continue; }
            if (arg == "--address") { auto v = val(); if (!v) return false; opts.address = v; continue; }
            if (arg == "--nodes")
            {
                auto v = val();
                if (!v) return false;
                opts.nodes = v;
                if (opts.nodes != "0" && opts.nodes != "1" && opts.nodes != "both")
                {
                    std::cerr << "--nodes must be 0, 1, or both" << std::endl;
                    return false;
                }
                continue;
            }
            if (arg == "--singles-per-sec") { auto v = val(); if (!v) return false; opts.singlesPerSec = std::stoull(v); continue; }
            if (arg == "--push-chunk") { auto v = val(); if (!v) return false; opts.pushChunkSingles = std::stoull(v); continue; }
            if (arg == "--max-files") { auto v = val(); if (!v) return false; opts.maxFiles = std::stoull(v); continue; }
            if (arg == "--idle-exit-seconds") { auto v = val(); if (!v) return false; opts.idleExitSeconds = std::stoul(v); continue; }
            if (arg == "--preload") { opts.preload = true; continue; }
            if (arg == "--no-preload") { opts.preload = false; continue; }
            if (arg == "--max-memory-gb") { auto v = val(); if (!v) return false; opts.maxMemoryGB = std::stoull(v); continue; }
            if (arg == "--send-rounds") { auto v = val(); if (!v) return false; opts.sendRounds = std::stoul(v); continue; }
            if (arg == "--send-only") { opts.sendOnly = true; continue; }
            if (arg == "--no-send-only" || arg == "--full-recv") { opts.sendOnly = false; continue; }

            std::cerr << "Unknown argument: " << arg << std::endl;
            return false;
        }
        return true;
    }

} // namespace

int main(int argc, char **argv)
{
    ProgramOptions opts;
    if (!parseArgs(argc, argv, opts)) return 1;
    if (opts.helpOnly)
    {
        std::cout << "Usage: test_local_grpc_singles_ingress [options]\n"
                  << "  --data-root <dir>       Base data directory\n"
                  << "  --node0-dir <dir>       Node0 singles directory\n"
                  << "  --node1-dir <dir>       Node1 singles directory\n"
                  << "  --nodes 0|1|both        Which senders to launch (default both)\n"
                  << "  --address <host:port>   gRPC listen address\n"
                  << "  --singles-per-sec <n>   Rate limit (0=burst)\n"
                  << "  --push-chunk <n>        Singles per RDMA chunk (e.g. 2e6/4e6/8e6)\n"
                  << "  --max-files <n>         Limit files per node\n"
                  << "  --idle-exit-seconds <n> Idle timeout\n"
                  << "  --preload               Preload singles into RAM (default)\n"
                  << "  --no-preload            Stream from disk\n"
                  << "  --max-memory-gb <n>     Per-node preload cap in GiB (default 30)\n"
                  << "  --send-rounds <n>       Repeat preload send N times (default 3)\n"
                  << "  --send-only             Black-hole CreditOnly receiver (send benchmark)\n"
                  << "  --no-send-only          Full per-slot receiver + chunk gap check\n";
        return 0;
    }
    opts.resolveDerivedPaths();

    if (opts.activeNodeCount() == 0)
    {
        std::cerr << "[Ingress] ERROR: no sender nodes selected" << std::endl;
        return 1;
    }

    std::cout << "===========================================" << std::endl;
    std::cout << "  L2 RDMA Singles Ingress Test" << std::endl;
    std::cout << "===========================================" << std::endl;
    std::cout << "address       : " << opts.address << std::endl;
    std::cout << "wireFormat    : binary_packed (16B/single)" << std::endl;
    std::cout << "nodes         : " << opts.nodes
              << " (active=" << opts.activeNodeCount() << ")" << std::endl;
    std::cout << "node0Dir      : " << opts.node0Dir << std::endl;
    std::cout << "node1Dir      : " << opts.node1Dir << std::endl;
    std::cout << "pushChunk     : " << opts.pushChunkSingles
              << " (" << (opts.pushChunkSingles * 16ULL) / (1024 * 1024) << " MiB/chunk)" << std::endl;
    {
        const size_t maxPerSlot = rdma::maxSinglesPerSlot(rdma::kDefaultSlotBytes);
        const size_t slotsPerChunk = maxPerSlot == 0
            ? 0
            : (opts.pushChunkSingles + maxPerSlot - 1) / maxPerSlot;
        std::cout << "slotBytes     : " << rdma::kDefaultSlotBytes
                  << " (maxSingles/slot=" << maxPerSlot << ")" << std::endl;
        std::cout << "slotsPerChunk : ~" << slotsPerChunk;
        if (slotsPerChunk > 1)
            std::cout << " (multi-slot; Full=per-slot zero-copy, no host reassembly)";
        else
            std::cout << " (fits in one slot)";
        std::cout << std::endl;
        std::cout << "recvPath      : " << (opts.sendOnly
            ? "CreditOnly (diagnostic; no payload read)"
            : "Full per-slot zero-copy into ring") << std::endl;
        std::cout << "note          : InProcess localhost is diagnostic only; "
                     "30 GiB/s needs dual-host RoCE" << std::endl;
    }
    std::cout << "singlesPerSec : " << opts.singlesPerSec << std::endl;
    std::cout << "preload       : " << (opts.preload ? "true" : "false") << std::endl;
    std::cout << "sendRounds    : " << opts.sendRounds << std::endl;
    std::cout << "sendOnly      : " << (opts.sendOnly ? "true" : "false") << std::endl;
    if (opts.preload)
        std::cout << "maxMemoryGB   : " << opts.maxMemoryGB << " (per node, may be reduced by RAM)" << std::endl;

    // Collect files for enabled nodes
    std::vector<std::string> files0, files1;
    if (opts.enableNode0())
    {
        files0 = grpc_singles_replay::collectSinglesFiles(opts.node0Dir);
        if (files0.empty())
        {
            std::cerr << "[Ingress] ERROR: missing .lsingle files in node0Dir="
                      << opts.node0Dir << std::endl;
            return 2;
        }
        std::cout << "node0 files   : " << files0.size() << std::endl;
    }
    if (opts.enableNode1())
    {
        files1 = grpc_singles_replay::collectSinglesFiles(opts.node1Dir);
        if (files1.empty())
        {
            std::cerr << "[Ingress] ERROR: missing .lsingle files in node1Dir="
                      << opts.node1Dir << std::endl;
            return 2;
        }
        std::cout << "node1 files   : " << files1.size() << std::endl;
    }

    if (!opts.preload)
    {
        uint64_t expectedTotal = 0;
        if (opts.enableNode0())
            expectedTotal += grpc_singles_replay::countSinglesInFiles(files0);
        if (opts.enableNode1())
            expectedTotal += grpc_singles_replay::countSinglesInFiles(files1);
        std::cout << "expectedTotal : " << expectedTotal << std::endl;
    }
    else
    {
        std::cout << "expectedTotal : (determined after preload send)" << std::endl;
    }

    const size_t perNodeMemCap = opts.preload ? computePerNodeMemoryCapBytes(opts) : 0;

    // Start server (full receiver or black-hole for send-only benchmark)
    std::unique_ptr<ReceiverService> fullReceiver;
    std::unique_ptr<BlackHoleReceiverService> blackHoleReceiver;
    grpc::Service *servicePtr = nullptr;

    if (opts.sendOnly)
    {
        auto orch = std::make_shared<ReceiverOrchestration>();
        orch->expectedNodeCount = opts.expectedNodeCount;
        blackHoleReceiver = std::make_unique<BlackHoleReceiverService>(std::move(orch));
        servicePtr = blackHoleReceiver.get();
        std::cout << "[Ingress] Black-hole receiver (send-only benchmark)" << std::endl;
    }
    else
    {
        fullReceiver = std::make_unique<ReceiverService>(opts.expectedNodeCount);
        servicePtr = fullReceiver.get();
        std::cout << "[Ingress] Full receiver with chunk gap check" << std::endl;
    }

    grpc::ServerBuilder builder;
    builder.AddListeningPort(opts.address, grpc::InsecureServerCredentials());
    builder.RegisterService(servicePtr);

    auto server = builder.BuildAndStart();
    if (!server)
    {
        std::cerr << "[Ingress] Failed to start server on " << opts.address << std::endl;
        return 3;
    }
    std::cout << "[Ingress] Server listening on " << opts.address << std::endl;

    grpc_singles_replay::ReplayStats stats0, stats1;
    stats0.success = true; // unused node treated as success
    stats1.success = true;

    auto makeOpts = [&](uint32_t nodeId) {
        grpc_singles_replay::ReplayOptions ro;
        ro.serverAddress = opts.address;
        ro.nodeId = nodeId;
        ro.channelCount = 288;
        ro.singlesPerSec = opts.singlesPerSec;
        ro.pushChunkSingles = opts.pushChunkSingles;
        ro.maxFiles = opts.maxFiles;
        ro.waitForStartSignal = true;
        ro.preload = opts.preload;
        ro.sendRounds = opts.sendRounds;
        if (opts.preload)
            ro.maxMemoryBytes = perNodeMemCap;
        return ro;
    };

    std::thread t0, t1;
    if (opts.enableNode0())
        t0 = std::thread([&] { stats0 = grpc_singles_replay::runNodeReplay(files0, makeOpts(0)); });
    if (opts.enableNode1())
        t1 = std::thread([&] { stats1 = grpc_singles_replay::runNodeReplay(files1, makeOpts(1)); });

    if (t0.joinable()) t0.join();
    if (t1.joinable()) t1.join();

    // Wait for server to flush
    std::this_thread::sleep_for(std::chrono::seconds(2));
    server->Shutdown();

    // Results
    std::cout << "\n--- Replay Stats ---" << std::endl;
    auto printNodeStats = [](const char *label, const grpc_singles_replay::ReplayStats &s, bool enabled) {
        if (!enabled)
        {
            std::cout << label << ": (disabled)" << std::endl;
            return;
        }
        std::cout << label << ": sent=" << s.singlesSent << " chunks=" << s.chunksSent
                  << " elapsed=" << s.elapsedMs << "ms"
                  << " rounds=" << s.sendRounds << std::endl;
        if (s.chunksSent > 0)
        {
            const double avgSingles = static_cast<double>(s.singlesSent) / static_cast<double>(s.chunksSent);
            const double avgBytes = avgSingles * 16.0;
            std::cout << "       avg=" << avgSingles << " singles/chunk ("
                      << (avgBytes / (1024.0 * 1024.0)) << " MiB/chunk)" << std::endl;
        }
    };
    printNodeStats("Node0", stats0, opts.enableNode0());
    printNodeStats("Node1", stats1, opts.enableNode1());

    const bool replayOk =
        (!opts.enableNode0() || stats0.success) &&
        (!opts.enableNode1() || stats1.success);
    if (!replayOk)
    {
        std::cerr << "[Ingress] FAIL: replay did not complete successfully" << std::endl;
        return 4;
    }

    const uint64_t totalSent =
        (opts.enableNode0() ? stats0.singlesSent : 0) +
        (opts.enableNode1() ? stats1.singlesSent : 0);
    const uint64_t totalChunks =
        (opts.enableNode0() ? stats0.chunksSent : 0) +
        (opts.enableNode1() ? stats1.chunksSent : 0);

    uint64_t maxElapsed = 0;
    if (opts.enableNode0()) maxElapsed = std::max(maxElapsed, stats0.elapsedMs);
    if (opts.enableNode1()) maxElapsed = std::max(maxElapsed, stats1.elapsedMs);

    if (maxElapsed > 0 && totalSent > 0)
    {
        const double totalBytes = static_cast<double>(totalSent) * 16.0;
        const double gbPerSec = totalBytes / (static_cast<double>(maxElapsed) / 1000.0) / (1024.0 * 1024.0 * 1024.0);
        const double singlesPerSec = static_cast<double>(totalSent) / (static_cast<double>(maxElapsed) / 1000.0);
        const uint32_t nNodes = opts.activeNodeCount();
        std::cout << "\n--- Benchmark ---" << std::endl;
        if (opts.preload)
            std::cout << "Mode          : preload (send timing excludes file I/O)" << std::endl;
        if (opts.sendOnly)
            std::cout << "Receiver      : black-hole (minimal recv overhead)" << std::endl;
        std::cout << "Nodes         : " << opts.nodes << std::endl;
        std::cout << "Send rounds   : " << opts.sendRounds << std::endl;
        std::cout << "Push chunk    : " << opts.pushChunkSingles << " singles" << std::endl;
        std::cout << "Chunks        : " << totalChunks << std::endl;
        if (totalChunks > 0)
        {
            std::cout << "Avg chunk size: "
                      << (static_cast<double>(totalSent) / static_cast<double>(totalChunks))
                      << " singles ("
                      << (static_cast<double>(totalSent) * 16.0 / static_cast<double>(totalChunks) / (1024.0 * 1024.0))
                      << " MiB)" << std::endl;
        }
        std::cout << "Throughput    : " << gbPerSec << " GB/s (binary packed, aggregate)" << std::endl;
        std::cout << "Singles/s     : " << singlesPerSec / 1e6 << " M singles/s" << std::endl;
        if (nNodes > 0)
            std::cout << "Per-node      : " << gbPerSec / static_cast<double>(nNodes) << " GB/s/node" << std::endl;
    }

    const bool ok = opts.sendOnly
                        ? blackHoleReceiver->validate(totalSent)
                        : fullReceiver->validate(totalSent);

    std::cout << "===========================================" << std::endl;
    std::cout << "  L2 Ingress Test " << (ok ? "PASSED" : "FAILED") << std::endl;
    std::cout << "===========================================" << std::endl;

    return ok ? 0 : 5;
}
