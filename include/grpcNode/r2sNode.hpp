#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include "core/r2s/R2S.hpp"

namespace openpni::distributed::grpcnode
{
    namespace r2s = openpni::distributed::r2s;

    struct NodeRunStats
    {
        bool success = false;
        uint64_t callbackCount = 0;
        uint64_t singlesSent = 0;
        uint64_t grpcMessagesSent = 0;

#ifdef DEBUG
        uint64_t enqueueCalls = 0;
        uint64_t enqueueTotalNs = 0;
        uint64_t enqueueWaitNs = 0;
        uint64_t enqueuePushNs = 0;
        uint64_t maxEnqueueWaitNs = 0;

        uint64_t serializeBuildNs = 0;
        uint64_t writeNs = 0;
        uint64_t maxWriteNs = 0;
        uint64_t estimatedWireBytes = 0;

        uint64_t peakQueueSegments = 0;
        uint64_t peakQueueSingles = 0;
        uint64_t peakQueueBytes = 0;
        uint64_t processRssBytes = 0;
        uint64_t runElapsedMs = 0;
#endif
    };

    class R2SGrpcNode final
    {
    public:
        struct InitOptions
        {
            r2s::R2SProcessConfig r2sConfig;
            std::string serverAddress;
            uint32_t nodeId = 0;
            std::string nodeAddress = "127.0.0.1";
            uint32_t channelCount = 0;
            std::string detectorType = "BDM2";
            size_t maxPendingSegments = 128;
            uint32_t batchSegmentsPerMessage = 1;
            uint32_t progressLogInterval = 50;

            bool waitForStartSignal = true;
            uint32_t waitForStartTimeoutMs = 0;
            uint32_t waitForStartRpcTimeoutMs = 15000;
            uint32_t waitForStartRetryIntervalMs = 1000;
            /**
             * Parallel StreamSingles writers per node.
             * Default 1 keeps per-node chunk arrival ordered for streaming coincidence.
             * Values > 1 are experimental/benchmark-only and may interleave arrival order.
             */
            uint32_t parallelStreams = 1;
        };

        explicit R2SGrpcNode(InitOptions init);

        R2SGrpcNode(
            const r2s::R2SProcessConfig &r2sConfig,
            std::string serverAddress,
            uint32_t nodeId,
            uint32_t channelCount,
            size_t maxPendingSegments = 128,
            std::string nodeAddress = "127.0.0.1",
            std::string detectorType = "BDM2",
            uint32_t progressLogInterval = 50,
            bool waitForStartSignal = true,
            uint32_t waitForStartTimeoutMs = 0,
            uint32_t waitForStartRpcTimeoutMs = 15000,
            uint32_t waitForStartRetryIntervalMs = 1000,
            uint32_t batchSegmentsPerMessage = 1);

        bool run();

        const NodeRunStats &stats() const;

    private:
        InitOptions m_init;
        NodeRunStats m_stats;
    };
} // namespace openpni::distributed::grpcnode
