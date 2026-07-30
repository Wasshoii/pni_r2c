#pragma once

#include "core/streaming/StreamingCoincidence.hpp"
#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "protos/coincidence.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace openpni::distributed::streaming
{

    struct NodeConnectionInfo
    {
        uint32_t nodeId = 0;
        std::string address;
        uint32_t channelCount = 0;
        std::string detectorType;
        std::chrono::steady_clock::time_point lastHeartbeat;
        std::atomic<uint64_t> singlesReceived{0};
        std::atomic<uint64_t> chunksReceived{0};
        std::atomic<bool> connected{false};
    };

    class CoincidenceServiceImpl final : public coincidence::CoincidenceService::Service
    {
    public:
        struct OrchestrationConfig
        {
            uint32_t expectedNodeCount = 0;
            bool autoStartWhenAllRegistered = true;
            uint32_t startLeadTimeMs = 1000;
            uint32_t waitForStartDefaultTimeoutMs = 30000;
            bool rejectStreamBeforeStart = true;
        };

        explicit CoincidenceServiceImpl(StreamingTimeAligner &aligner);
        CoincidenceServiceImpl(StreamingTimeAligner &aligner, OrchestrationConfig orchestration);

        /** Shared ingest used by RDMA receive path (and legacy helpers). */
        bool ingestPackedSinglesChunk(
            uint32_t nodeId,
            uint64_t chunkId,
            uint64_t computerClockMs,
            uint32_t durationMs,
            const void *singlesPacked,
            uint32_t singlesCount,
            std::string *errorMessage = nullptr);

        grpc::Status StreamSingles(
            grpc::ServerContext *context,
            grpc::ServerReader<coincidence::SingleChunkMessage> *reader,
            coincidence::StreamResponse *response) override;

        grpc::Status OpenDataPlane(
            grpc::ServerContext *context,
            const coincidence::OpenDataPlaneRequest *request,
            coincidence::OpenDataPlaneResponse *response) override;

        grpc::Status WaitForStart(
            grpc::ServerContext *context,
            const coincidence::WaitForStartRequest *request,
            coincidence::WaitForStartResponse *response) override;

        grpc::Status GetStatus(
            grpc::ServerContext *context,
            const coincidence::StatusRequest *request,
            coincidence::StatusResponse *response) override;

        grpc::Status Control(
            grpc::ServerContext *context,
            const coincidence::ControlRequest *request,
            coincidence::ControlResponse *response) override;

        grpc::Status UpdateConfig(
            grpc::ServerContext *context,
            const coincidence::ConfigUpdateRequest *request,
            coincidence::ConfigUpdateResponse *response) override;

        grpc::Status RegisterNode(
            grpc::ServerContext *context,
            const coincidence::RegisterNodeRequest *request,
            coincidence::RegisterNodeResponse *response) override;

        grpc::Status Heartbeat(
            grpc::ServerContext *context,
            const coincidence::HeartbeatRequest *request,
            coincidence::HeartbeatResponse *response) override;

        bool waitForAllNodes(uint32_t timeoutMs = 0) const;
        bool waitForStartSignal(uint32_t timeoutMs = 0) const;

        uint32_t expectedNodeCount() const;
        uint32_t connectedNodeCount() const;
        bool startSignalIssued() const;
        uint64_t plannedStartTimeMs() const;

        void notifyServerStopping();
        void clearServerStoppingState();

        openpni::distributed::dataplane::rdma::RdmaRecvServer &rdmaServer() { return *m_rdmaServer; }

    private:
        static uint64_t nowMs();

        void fillOrchestrationStatusUnlocked(coincidence::StatusResponse *response) const;
        void fillOrchestrationStatusUnlocked(coincidence::RegisterNodeResponse *response) const;
        void fillOrchestrationStatusUnlocked(coincidence::WaitForStartResponse *response) const;

        bool issueStartSignalLocked(uint64_t startTimeMs, const std::string &reason);
        void maybeAutoStartAfterRegister();
        void updateNodeStats(uint32_t nodeId, uint64_t singlesCount);
        void startRdmaIngest();

        StreamingTimeAligner &m_aligner;
        OrchestrationConfig m_orchestration;

        std::unordered_map<uint32_t, std::shared_ptr<NodeConnectionInfo>> m_nodeInfos;
        mutable std::shared_mutex m_nodeInfosMutex;

        mutable std::mutex m_orchestrationMutex;
        mutable std::condition_variable m_orchestrationCv;
        std::unordered_set<uint32_t> m_registeredNodes;

        std::atomic<bool> m_startSignalIssued{false};
        std::atomic<uint64_t> m_plannedStartTimeMs{0};
        std::atomic<bool> m_serverStopping{false};

        std::unique_ptr<openpni::distributed::dataplane::rdma::RdmaRecvServer> m_rdmaServer;
    };

    std::unique_ptr<grpc::Server> createCoincidenceServer(
        CoincidenceServiceImpl &service,
        const std::string &address);

    class CoincidenceServer
    {
    public:
        CoincidenceServer(
            const TimeAlignerConfig &config,
            size_t nodeCount,
            const std::string &address);

        CoincidenceServer(
            const TimeAlignerConfig &config,
            size_t nodeCount,
            const std::string &address,
            CoincidenceServiceImpl::OrchestrationConfig orchestration);

        void start();
        void stop();
        void wait();

        bool waitForAllNodes(uint32_t timeoutMs = 0) const;
        bool waitForStartSignal(uint32_t timeoutMs = 0) const;

        StreamingTimeAligner &getAligner();
        const StreamingTimeAligner &getAligner() const;

        const ProcessingStatistics &getStatistics() const;

        const CoincidenceServiceImpl &getService() const;
        CoincidenceServiceImpl &getService();

        bool isRunning() const;

    private:
        static CoincidenceServiceImpl::OrchestrationConfig normalizeOrchestration(
            CoincidenceServiceImpl::OrchestrationConfig orchestration,
            size_t nodeCount);

        StreamingTimeAligner m_aligner;
        CoincidenceServiceImpl m_service;
        std::string m_address;
        std::unique_ptr<grpc::Server> m_server;
        std::atomic<bool> m_running{false};
    };

} // namespace openpni::distributed::streaming
