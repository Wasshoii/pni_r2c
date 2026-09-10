#pragma once

#include "core/streaming/StreamingCoincidence.hpp"
#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "dataplane/rdma/RdmaWriteSender.hpp"
#include "grpcService/EpochHandoffShip.hpp"
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
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace openpni::distributed::streaming
{

    struct NodeConnectionInfo
    {
        uint32_t nodeId = 0;
        std::string address;
        uint32_t channelCount = 0;
        std::string detectorType;
        std::chrono::steady_clock::time_point lastHeartbeat{};
        std::atomic<uint64_t> singlesReceived{0};
        std::atomic<uint64_t> chunksReceived{0};
        std::atomic<uint64_t> singlesSentHeartbeat{0};
        std::atomic<bool> connected{false};
        std::atomic<bool> dataplaneOpen{false};
        std::atomic<bool> producerComplete{false};
        std::atomic<uint32_t> dataPlaneKind{0};

        coincidence::SourceState sourceState = coincidence::SOURCE_STATE_UNSPECIFIED;
        uint64_t chunksPending = 0;
        uint64_t chunksPendingCap = 0;
        uint64_t rdmaSlotsInFlight = 0;
        uint32_t rdmaSlotCount = 0;
        uint64_t rdmaCreditRemaining = 0;
        uint64_t r2sSinglesOut = 0;
        uint64_t r2sLeaseUsed = 0;
        uint64_t r2sLeaseCap = 0;
        uint64_t acqPacketsTotal = 0;
        uint64_t acqBytesTotal = 0;
        bool acqRunning = false;
        uint64_t lastRttMs = 0;
    };

    class CoincidenceServiceImpl final : public coincidence::CoincidenceService::Service
    {
    public:
        struct OrchestrationConfig
        {
            uint32_t expectedNodeCount = 0;
            /** Start only after N nodes are registered AND all have OpenDataPlane. */
            bool autoStartWhenAllRegistered = true;
            uint32_t startLeadTimeMs = 0;
            uint32_t waitForStartDefaultTimeoutMs = 30000;
            bool rejectStreamBeforeStart = true;

            bool requireRoce = false;
            bool forceInProcess = false;
            std::string deviceName;
            int gidIndex = -1;
            uint32_t slotCount = 0;
            size_t slotBytes = 0;
            uint32_t heartbeatTimeoutMs = 3000;

            uint32_t coinId = 0;
            bool enableTimeShard = false;
            uint64_t plannedLeaseSpan_100fs = 0;
            uint64_t minLease_100fs = 0;
            uint32_t nextCoinId = 1;
            std::string nextCoinAddress;
        };

        enum class TimeLeasePhase : uint32_t
        {
            LeaseActive = 0,
            PrepareNext = 1,
            Cutting = 2,
            Shipping = 3,
            Redirected = 4,
            DrainPrev = 5
        };

        struct TimeLease
        {
            uint64_t epochId = 0;
            uint32_t coinId = 0;
            uint64_t t0_100fs = 0;
            uint64_t t1_100fs = 0;
            uint64_t minLease_100fs = 0;
            TimeLeasePhase phase = TimeLeasePhase::LeaseActive;
            uint32_t activeCoinId = 0;
            bool nextPrepared = false;
        };

        explicit CoincidenceServiceImpl(StreamingTimeAligner &aligner);
        CoincidenceServiceImpl(StreamingTimeAligner &aligner, OrchestrationConfig orchestration);
        ~CoincidenceServiceImpl();

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

        grpc::Status NotifyProducerComplete(
            grpc::ServerContext *context,
            const coincidence::NotifyProducerCompleteRequest *request,
            coincidence::NotifyProducerCompleteResponse *response) override;

        grpc::Status OpenShipPlane(
            grpc::ServerContext *context,
            const coincidence::OpenShipPlaneRequest *request,
            coincidence::OpenShipPlaneResponse *response) override;

        grpc::Status WaitForShipApplied(
            grpc::ServerContext *context,
            const coincidence::WaitForShipAppliedRequest *request,
            coincidence::WaitForShipAppliedResponse *response) override;

        grpc::Status RegisterCoin(
            grpc::ServerContext *context,
            const coincidence::RegisterCoinRequest *request,
            coincidence::RegisterCoinResponse *response) override;

        bool waitForAllNodes(uint32_t timeoutMs = 0) const;
        bool waitForStartSignal(uint32_t timeoutMs = 0) const;

        uint32_t expectedNodeCount() const;
        uint32_t connectedNodeCount() const;
        uint32_t dataplaneOpenCount() const;
        uint32_t producersCompleteCount() const;
        bool startSignalIssued() const;
        uint64_t plannedStartTimeMs() const;
        bool allProducersComplete() const;
        coincidence::DataPlaneKind dataPlaneKind() const;

        void notifyServerStopping();
        void clearServerStoppingState();

        TimeLease timeLease() const;
        void markNextCoinPrepared(bool prepared);
        void requestSetActiveCoin(uint32_t coinId);
        uint32_t activeCoinId() const;
        bool cutEpochAtWatermark();
        EpochHandoff takeEpochHandoff();
        bool applyEpochHandoff(EpochHandoff handoff);
        bool maybePreemptLease();
        bool connectShipTo(const std::string &nextListenAddress, uint32_t nextCoinId);
        bool shipEpochTo();
        bool shipEpochTo(CoincidenceServiceImpl &next);
        bool sendEpochHandoffAndWait(const EpochHandoff &handoff);
        bool tickLease();
        bool shipPlaneReady() const;
        std::string registeredCoinListenAddress(uint32_t coinId) const;

        openpni::distributed::dataplane::rdma::RdmaRecvServer &rdmaServer() { return *m_rdmaServer; }

    private:
        static uint64_t nowMs();

        void fillOrchestrationStatusUnlocked(coincidence::StatusResponse *response) const;
        void fillOrchestrationStatusUnlocked(coincidence::RegisterNodeResponse *response) const;
        void fillOrchestrationStatusUnlocked(coincidence::WaitForStartResponse *response) const;

        bool issueStartSignalLocked(uint64_t startTimeMs, const std::string &reason);
        void maybeAutoStartAfterDataplane();
        void markProducerComplete(uint32_t nodeId, uint64_t singlesSent);
        void drainAlignerIfAllComplete();
        void updateNodeStats(uint32_t nodeId, uint64_t singlesCount);
        void startRdmaIngest();
        void startShipRecv();
        void stopShipSender();
        std::string resolveNextCoinAddress() const;
        coincidence::DataPlaneKind observedDataPlaneKindUnlocked() const;
        coincidence::ProducerCommand pendingProducerCommand() const;
        void setPendingProducerCommand(coincidence::ProducerCommand command);
        bool ingestRdmaSlot(const openpni::distributed::dataplane::rdma::SlotChunkView &view,
                            std::string *errorMessage);
        bool pushTimestampedChunk(
            uint32_t nodeId,
            uint64_t chunkId,
            uint64_t computerClockMs,
            uint32_t durationMs,
            std::vector<Single> &&singles,
            std::string *errorMessage);

        StreamingTimeAligner &m_aligner;
        OrchestrationConfig m_orchestration;

        std::unordered_map<uint32_t, std::shared_ptr<NodeConnectionInfo>> m_nodeInfos;
        mutable std::shared_mutex m_nodeInfosMutex;

        mutable std::mutex m_orchestrationMutex;
        mutable std::condition_variable m_orchestrationCv;
        std::unordered_set<uint32_t> m_registeredNodes;
        std::unordered_set<uint32_t> m_dataplaneOpenNodes;
        std::unordered_set<uint32_t> m_completeNodes;

        std::atomic<bool> m_startSignalIssued{false};
        std::atomic<uint64_t> m_plannedStartTimeMs{0};
        std::atomic<bool> m_serverStopping{false};
        std::atomic<bool> m_allProducersComplete{false};
        std::atomic<uint32_t> m_observedDataPlaneKind{0};
        std::atomic<uint32_t> m_pendingProducerCommand{
            static_cast<uint32_t>(coincidence::CMD_NONE)};
        std::atomic<uint32_t> m_activeCoinId{0};

        TimeLease m_lease;
        mutable std::mutex m_leaseMutex;

        std::unique_ptr<openpni::distributed::dataplane::rdma::RdmaRecvServer> m_rdmaServer;
        std::unique_ptr<openpni::distributed::dataplane::rdma::RdmaRecvServer> m_shipServer;
        EpochShipAssembler m_shipAssembler;

        mutable std::mutex m_shipTxMutex;
        std::unique_ptr<openpni::distributed::dataplane::rdma::RdmaWriteSender> m_shipSender;
        std::shared_ptr<grpc::Channel> m_shipChannel;
        std::unique_ptr<coincidence::CoincidenceService::Stub> m_shipStub;
        uint32_t m_shipPeerCoinId = 0;
        EpochHandoff m_pendingShip;

        mutable std::mutex m_shipAckMutex;
        std::condition_variable m_shipAckCv;
        uint64_t m_shipAppliedEpoch = 0;
        bool m_shipApplyFailed = false;

        struct ComputeCoinInfo
        {
            uint32_t coinId = 0;
            std::string listenAddress;
            std::chrono::steady_clock::time_point lastHeartbeat{};
        };
        mutable std::mutex m_computeCoinsMutex;
        std::unordered_map<uint32_t, ComputeCoinInfo> m_computeCoins;

        struct PartialChunk
        {
            uint64_t chunkId = 0;
            uint64_t computerClockMs = 0;
            uint32_t durationMs = 0;
            std::vector<Single> singles;
            bool open = false;
        };
        std::mutex m_partialMutex;
        std::unordered_map<uint32_t, PartialChunk> m_partialByNode;
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
