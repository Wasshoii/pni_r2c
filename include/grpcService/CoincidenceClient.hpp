#pragma once

#include <pni/io/IO.hpp>
#include <grpcpp/grpcpp.h>

#include "dataplane/rdma/RdmaWriteSender.hpp"
#include "protos/coincidence.grpc.pb.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
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

        uint32_t heartbeatIntervalMs = 5000;

        bool waitForStartSignal = true;
        uint32_t waitForStartTimeoutMs = 0;
        uint32_t waitForStartRpcTimeoutMs = 15000;
        uint32_t waitForStartRetryIntervalMs = 1000;
    };

    struct PendingChunk
    {
        uint64_t chunkId = 0;
        uint64_t computerClockMs = 0;
        uint32_t durationMs = 0;
        std::vector<uint8_t> packed;
        uint32_t singlesCount = 0;
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

        bool getServerStatus(coincidence::StatusResponse *response);

        uint64_t getTotalSinglesSent() const;
        size_t getPendingMessageCount() const;

        bool isRunning() const;
        bool isConnected() const;

        bool waitForServerStartSignal(uint32_t timeoutMs = 0);

    private:
        static uint64_t nowMs();
        void waitUntil(uint64_t plannedStartMs);

        bool registerNode();
        bool openRdmaDataPlane();
        void senderLoop();
        bool sendChunk(const PendingChunk &chunk);
        void flushPendingMessages();
        void heartbeatLoop();

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
        std::atomic<bool> m_remapSampleLogged{false};
        std::atomic<uint64_t> m_chunkIdCounter{0};
        std::atomic<uint64_t> m_totalSinglesSent{0};
    };

} // namespace openpni::distributed::streaming
