#pragma once

#include <pni/io/IO.hpp>
#include <grpcpp/grpcpp.h>

#include "dataplane/rdma/RdmaWriteSender.hpp"
#include "protos/coincidence.grpc.pb.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace openpni::distributed::streaming
{

    using Single = openpni::Single;

    struct CoincidenceClientConfig
    {
        std::string serverAddress = "localhost:50051";
        uint32_t nodeId = 0;
        std::string nodeAddress = "";
        uint32_t channelCount = 0;
        std::string detectorType = "Unknown";
        bool remapLocalToGlobalChannels = false;
        uint32_t globalChannelOffset = 0;
        uint32_t crystalsPerChannel = 169 * 4;

        size_t maxPendingChunks = 100;
        size_t batchSize = 1000;

        uint32_t reconnectDelayMs = 1000;
        uint32_t maxReconnectAttempts = 10;

        uint32_t heartbeatIntervalMs = 1000;

        bool waitForStartSignal = true;
        uint32_t waitForStartTimeoutMs = 0;
        uint32_t waitForStartRpcTimeoutMs = 15000;
        uint32_t waitForStartRetryIntervalMs = 1000;

        bool requireRoce = false;
        bool forceInProcess = false;
        std::string rdmaDeviceName;
        int gidIndex = -1;
        uint32_t txSlotCount = 0;
        uint32_t requestedSlotCount = 0;
        uint32_t requestedSlotBytes = 0;
    };

    struct PendingChunk
    {
        uint64_t chunkId = 0;
        uint64_t computerClockMs = 0;
        uint32_t durationMs = 0;
        std::vector<Single> singles;
        const Single *borrowed = nullptr;
        uint32_t singlesCount = 0;
        bool hasTxLease = false;
        openpni::distributed::dataplane::rdma::TxSlotLease lease{};
        openpni::distributed::dataplane::rdma::SlotHeader hdr{};

        const Single *payload() const noexcept
        {
            return borrowed != nullptr ? borrowed : singles.data();
        }
    };

    struct WorkerTelemetry
    {
        uint64_t r2sSinglesOut = 0;
        uint64_t r2sLeaseUsed = 0;
        uint64_t r2sLeaseCap = 0;
        uint64_t acqPacketsTotal = 0;
        uint64_t acqBytesTotal = 0;
        bool acqRunning = false;
    };

    class CoincidenceClient
    {
    public:
        explicit CoincidenceClient(const CoincidenceClientConfig &config);
        ~CoincidenceClient();

        bool start();
        void stop();

        bool sendSingles(
            const std::vector<Single> &singles,
            uint64_t computerClock_ms,
            uint32_t duration_ms);
        bool sendSingles(
            std::span<const Single> singles,
            uint64_t computerClock_ms,
            uint32_t duration_ms);
        bool sendSingles(
            std::vector<Single> &&singles,
            uint64_t computerClock_ms,
            uint32_t duration_ms);
        /** InProcess: queue a view without copying (span must stay valid until sent).
         *  RoCE: copies into TX like sendSingles(span). Remap copies. */
        bool sendSinglesView(
            std::span<const Single> singles,
            uint64_t computerClock_ms,
            uint32_t duration_ms);

        bool getServerStatus(coincidence::StatusResponse *response);

        uint64_t getTotalSinglesSent() const;
        size_t getPendingMessageCount() const;

        bool isRunning() const;
        bool isConnected() const;
        bool isPaused() const;
        bool stopProduceRequested() const;
        uint64_t lastRttMs() const;
        coincidence::SourceState sourceState() const;
        uint32_t rdmaSlotCount() const;
        uint64_t rdmaSlotsInFlight() const;
        uint32_t rdmaCreditRemaining() const;

        bool waitForServerStartSignal(uint32_t timeoutMs = 0);
        bool waitUntilIdle();
        bool notifyProducerComplete();
        openpni::distributed::dataplane::rdma::DataPlaneKind dataPlaneKind() const;

        void setTelemetryHook(std::function<WorkerTelemetry()> hook);

    private:
        static uint64_t nowMs();

        bool registerNode();
        bool openRdmaDataPlane();
        void senderLoop();
        bool sendChunk(const PendingChunk &chunk);
        bool enqueuePendingChunk(std::unique_ptr<PendingChunk> chunk);
        bool fillRoceTxAndEnqueue(
            std::span<const Single> singles,
            uint64_t computerClock_ms,
            uint32_t duration_ms);
        bool remapChannels(std::vector<Single> *singles);
        bool useRoceTxFill() const;
        void flushPendingMessages();
        void heartbeatLoop();
        void applyProducerCommand(coincidence::ProducerCommand command);
        coincidence::SourceState currentSourceState() const;
        void fillHeartbeatTelemetry(coincidence::HeartbeatRequest *request);

        CoincidenceClientConfig m_config;

        std::shared_ptr<grpc::Channel> m_channel;
        std::unique_ptr<coincidence::CoincidenceService::Stub> m_stub;
        std::unique_ptr<openpni::distributed::dataplane::rdma::RdmaWriteSender> m_rdmaSender;

        mutable std::mutex m_mutex;
        std::condition_variable m_cv;
        std::queue<std::unique_ptr<PendingChunk>> m_pendingMessages;

        std::thread m_senderThread;
        std::thread m_heartbeatThread;

        std::atomic<bool> m_running{false};
        std::atomic<bool> m_connected{false};
        std::atomic<bool> m_producerComplete{false};
        std::atomic<bool> m_paused{false};
        std::atomic<bool> m_stopProduce{false};
        std::atomic<bool> m_remapSampleLogged{false};
        std::atomic<uint64_t> m_chunkIdCounter{0};
        std::atomic<uint64_t> m_totalSinglesSent{0};
        std::atomic<bool> m_sendInFlight{false};
        std::atomic<uint64_t> m_lastRttMs{0};
        std::mutex m_telemetryMutex;
        std::function<WorkerTelemetry()> m_telemetryHook;
    };

} // namespace openpni::distributed::streaming
