#include "grpcNode/coinNode.hpp"

#include <iostream>
#include <utility>
#include <glog/logging.h>

#include "grpcService/CoincidenceServiceImpl.hpp"

namespace openpni::distributed::grpcnode
{
    namespace coincidence = openpni::distributed::coincidence;
    namespace
    {
        CoinGrpcNode::InitOptions normalizeInit(CoinGrpcNode::InitOptions init)
        {
            if (init.expectedNodeCount == 0)
            {
                LOG(WARNING) << "[CoinGrpcNode] expectedNodeCount is 0, force set to 1";
                init.expectedNodeCount = 1;
            }
            return init;
        }

        streaming::CoincidenceServiceImpl::OrchestrationConfig toOrchestrationConfig(
            const CoinGrpcNode::InitOptions &init)
        {
            streaming::CoincidenceServiceImpl::OrchestrationConfig cfg;
            cfg.expectedNodeCount = init.expectedNodeCount;
            cfg.autoStartWhenAllRegistered = init.autoStartWhenAllRegistered;
            cfg.startLeadTimeMs = init.startLeadTimeMs;
            cfg.waitForStartDefaultTimeoutMs = init.waitForStartDefaultTimeoutMs;
            cfg.rejectStreamBeforeStart = init.rejectStreamBeforeStart;
            cfg.requireRoce = init.requireRoce;
            cfg.forceInProcess = init.forceInProcess;
            cfg.deviceName = init.rdmaDeviceName;
            cfg.gidIndex = init.gidIndex;
            cfg.slotCount = init.slotCount;
            cfg.slotBytes = init.slotBytes;
            cfg.heartbeatTimeoutMs = init.heartbeatTimeoutMs;
            cfg.coinId = init.coinId;
            cfg.enableTimeShard = init.enableTimeShard;
            cfg.plannedLeaseSpan_100fs = init.plannedLeaseSpan_100fs;
            cfg.minLease_100fs = init.minLease_100fs;
            cfg.nextCoinId = init.nextCoinId;
            return cfg;
        }
    } // namespace

    class CoinGrpcNode::Impl
    {
    public:
        explicit Impl(InitOptions init)
            : init_(normalizeInit(std::move(init))),
              server_(
                  init_.alignerConfig,
                  init_.expectedNodeCount,
                  init_.listenAddress,
                  toOrchestrationConfig(init_))
        {
        }

        bool start()
        {
            server_.start();
            return server_.isRunning();
        }

        void stop()
        {
            server_.stop();
        }

        void wait()
        {
            server_.wait();
        }

        bool waitForAllNodes(uint32_t timeoutMs) const
        {
            return server_.waitForAllNodes(timeoutMs);
        }

        bool waitForStartSignal(uint32_t timeoutMs) const
        {
            return server_.waitForStartSignal(timeoutMs);
        }

        uint32_t connectedNodeCount() const
        {
            return server_.getService().connectedNodeCount();
        }

        uint32_t dataplaneOpenCount() const
        {
            return server_.getService().dataplaneOpenCount();
        }

        bool allProducersComplete() const
        {
            return server_.getService().allProducersComplete();
        }

        bool copyStatus(coincidence::StatusResponse *out) const
        {
            if (!out)
            {
                return false;
            }
            grpc::ServerContext ctx;
            coincidence::StatusRequest req;
            req.set_include_node_stats(true);
            return const_cast<streaming::CoincidenceServiceImpl &>(server_.getService())
                       .GetStatus(&ctx, &req, out)
                       .ok();
        }

        uint32_t expectedNodeCount() const
        {
            return server_.getService().expectedNodeCount();
        }

        bool startSignalIssued() const
        {
            return server_.getService().startSignalIssued();
        }

        uint64_t plannedStartTimeMs() const
        {
            return server_.getService().plannedStartTimeMs();
        }

        const streaming::ProcessingStatistics &statistics() const
        {
            return server_.getStatistics();
        }

        streaming::StreamingTimeAligner &aligner()
        {
            return server_.getAligner();
        }

        streaming::CoincidenceServiceImpl &service()
        {
            return server_.getService();
        }

        const streaming::StreamingTimeAligner &aligner() const
        {
            return server_.getAligner();
        }

        bool isServerRunning() const
        {
            return server_.isRunning();
        }

        bool isAlignerRunning() const
        {
            return server_.getAligner().isRunning();
        }

    private:
        InitOptions init_;
        streaming::CoincidenceServer server_;
    };

    CoinGrpcNode::CoinGrpcNode(InitOptions init)
        : m_impl(std::make_unique<Impl>(std::move(init)))
    {
    }

    CoinGrpcNode::~CoinGrpcNode() = default;

    CoinGrpcNode::CoinGrpcNode(
        const streaming::TimeAlignerConfig &alignerConfig,
        std::string listenAddress,
        uint32_t expectedNodeCount,
        bool autoStartWhenAllRegistered,
        uint32_t startLeadTimeMs,
        uint32_t waitForStartDefaultTimeoutMs,
        bool rejectStreamBeforeStart)
        : CoinGrpcNode(InitOptions{
              alignerConfig,
              std::move(listenAddress),
              expectedNodeCount,
              autoStartWhenAllRegistered,
              startLeadTimeMs,
              waitForStartDefaultTimeoutMs,
              rejectStreamBeforeStart})
    {
    }

    bool CoinGrpcNode::start()
    {
        return m_impl->start();
    }

    void CoinGrpcNode::stop()
    {
        m_impl->stop();
    }

    void CoinGrpcNode::wait()
    {
        m_impl->wait();
    }

    bool CoinGrpcNode::waitForAllNodes(uint32_t timeoutMs) const
    {
        return m_impl->waitForAllNodes(timeoutMs);
    }

    bool CoinGrpcNode::waitForStartSignal(uint32_t timeoutMs) const
    {
        return m_impl->waitForStartSignal(timeoutMs);
    }

    uint32_t CoinGrpcNode::connectedNodeCount() const
    {
        return m_impl->connectedNodeCount();
    }

    uint32_t CoinGrpcNode::dataplaneOpenCount() const
    {
        return m_impl->dataplaneOpenCount();
    }

    uint32_t CoinGrpcNode::expectedNodeCount() const
    {
        return m_impl->expectedNodeCount();
    }

    bool CoinGrpcNode::startSignalIssued() const
    {
        return m_impl->startSignalIssued();
    }

    uint64_t CoinGrpcNode::plannedStartTimeMs() const
    {
        return m_impl->plannedStartTimeMs();
    }

    bool CoinGrpcNode::allProducersComplete() const
    {
        return m_impl->allProducersComplete();
    }

    bool CoinGrpcNode::copyStatus(openpni::distributed::coincidence::StatusResponse *out) const
    {
        return m_impl->copyStatus(out);
    }

    const streaming::ProcessingStatistics &CoinGrpcNode::statistics() const
    {
        return m_impl->statistics();
    }

    streaming::StreamingTimeAligner &CoinGrpcNode::aligner()
    {
        return m_impl->aligner();
    }

    streaming::CoincidenceServiceImpl &CoinGrpcNode::service()
    {
        return m_impl->service();
    }

    const streaming::StreamingTimeAligner &CoinGrpcNode::aligner() const
    {
        return m_impl->aligner();
    }

    bool CoinGrpcNode::isServerRunning() const
    {
        return m_impl->isServerRunning();
    }

    bool CoinGrpcNode::isAlignerRunning() const
    {
        return m_impl->isAlignerRunning();
    }

} // namespace openpni::distributed::grpcnode
