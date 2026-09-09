#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "core/streaming/StreamingCoincidence.hpp"
#include "grpcService/CoincidenceServiceImpl.hpp"
#include "protos/coincidence.pb.h"

namespace openpni::distributed::grpcnode
{
    namespace streaming = openpni::distributed::streaming;

    class CoinGrpcNode final
    {
    public:
        struct InitOptions
        {
            streaming::TimeAlignerConfig alignerConfig;
            std::string listenAddress = "0.0.0.0:50051";
            uint32_t expectedNodeCount = 1;
            bool autoStartWhenAllRegistered = true;
            uint32_t startLeadTimeMs = 0;
            uint32_t waitForStartDefaultTimeoutMs = 30000;
            bool rejectStreamBeforeStart = true;
            bool requireRoce = false;
            bool forceInProcess = false;
            std::string rdmaDeviceName;
            int gidIndex = -1;
            uint32_t slotCount = 0;
            size_t slotBytes = 0;
            uint32_t heartbeatTimeoutMs = 3000;
            uint32_t coinId = 0;
            bool enableTimeShard = false;
            uint64_t plannedLeaseSpan_100fs = 0;
            uint64_t minLease_100fs = 0;
            uint32_t nextCoinId = 1;
        };

        explicit CoinGrpcNode(InitOptions init);

        ~CoinGrpcNode();

        CoinGrpcNode(
            const streaming::TimeAlignerConfig &alignerConfig,
            std::string listenAddress,
            uint32_t expectedNodeCount,
            bool autoStartWhenAllRegistered = true,
            uint32_t startLeadTimeMs = 1000,
            uint32_t waitForStartDefaultTimeoutMs = 30000,
            bool rejectStreamBeforeStart = true);

        bool start();
        void stop();
        void wait();

        bool waitForAllNodes(uint32_t timeoutMs = 0) const;
        bool waitForStartSignal(uint32_t timeoutMs = 0) const;

        uint32_t connectedNodeCount() const;
        uint32_t dataplaneOpenCount() const;
        uint32_t expectedNodeCount() const;
        bool startSignalIssued() const;
        uint64_t plannedStartTimeMs() const;
        bool allProducersComplete() const;

        bool copyStatus(openpni::distributed::coincidence::StatusResponse *out) const;

        const streaming::ProcessingStatistics &statistics() const;

        streaming::StreamingTimeAligner &aligner();
        const streaming::StreamingTimeAligner &aligner() const;
        streaming::CoincidenceServiceImpl &service();

        bool isServerRunning() const;
        bool isAlignerRunning() const;

    private:
        class Impl;
        std::unique_ptr<Impl> m_impl;
    };

} // namespace openpni::distributed::grpcnode
