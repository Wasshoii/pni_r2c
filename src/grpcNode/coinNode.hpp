#pragma once

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>

#include "core/streaming-coin/CoincidenceServiceImpl.hpp"

namespace openpni::distributed::grpcnode
{
    namespace streaming = openpni::distributed::streaming;

    /**
     * @brief 符合主机节点（gRPC Server + 流式符合计算）
     *
     * 负责：
     * 1. 接收各 R2S 节点流式单事件数据
     * 2. 全节点注册完成后发出统一开始信号
     * 3. 驱动 StreamingTimeAligner 做流式符合计算
     */
    class CoinGrpcNode final
    {
    public:
        struct InitOptions
        {
            streaming::TimeAlignerConfig alignerConfig;
            std::string listenAddress = "0.0.0.0:50051";

            // 期望连接的 R2S 节点数量
            uint32_t expectedNodeCount = 1;

            // 编排策略
            bool autoStartWhenAllRegistered = true;
            uint32_t startLeadTimeMs = 1000;
            uint32_t waitForStartDefaultTimeoutMs = 30000;
            bool rejectStreamBeforeStart = true;
        };

        explicit CoinGrpcNode(InitOptions init)
            : m_init(normalizeInit(std::move(init))),
              m_server(
                  m_init.alignerConfig,
                  m_init.expectedNodeCount,
                  m_init.listenAddress,
                  toOrchestrationConfig(m_init))
        {
        }

        CoinGrpcNode(
            const streaming::TimeAlignerConfig &alignerConfig,
            std::string listenAddress,
            uint32_t expectedNodeCount,
            bool autoStartWhenAllRegistered = true,
            uint32_t startLeadTimeMs = 1000,
            uint32_t waitForStartDefaultTimeoutMs = 30000,
            bool rejectStreamBeforeStart = true)
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

        bool start()
        {
            m_server.start();
            return m_server.isRunning();
        }

        void stop()
        {
            m_server.stop();
        }

        void wait()
        {
            m_server.wait();
        }

        bool waitForAllNodes(uint32_t timeoutMs = 0) const
        {
            return m_server.waitForAllNodes(timeoutMs);
        }

        bool waitForStartSignal(uint32_t timeoutMs = 0) const
        {
            return m_server.waitForStartSignal(timeoutMs);
        }

        uint32_t connectedNodeCount() const
        {
            return m_server.getService().connectedNodeCount();
        }

        uint32_t expectedNodeCount() const
        {
            return m_server.getService().expectedNodeCount();
        }

        bool startSignalIssued() const
        {
            return m_server.getService().startSignalIssued();
        }

        uint64_t plannedStartTimeMs() const
        {
            return m_server.getService().plannedStartTimeMs();
        }

        const streaming::ProcessingStatistics &statistics() const
        {
            return m_server.getStatistics();
        }

        streaming::StreamingTimeAligner &aligner() { return m_server.getAligner(); }
        const streaming::StreamingTimeAligner &aligner() const { return m_server.getAligner(); }

        bool isServerRunning() const { return m_server.isRunning(); }
        bool isAlignerRunning() const { return m_server.getAligner().isRunning(); }

    private:
        static InitOptions normalizeInit(InitOptions init)
        {
            if (init.expectedNodeCount == 0)
            {
                std::cerr << "[CoinGrpcNode] expectedNodeCount is 0, force set to 1" << std::endl;
                init.expectedNodeCount = 1;
            }
            return init;
        }

        static streaming::CoincidenceServiceImpl::OrchestrationConfig toOrchestrationConfig(
            const InitOptions &init)
        {
            streaming::CoincidenceServiceImpl::OrchestrationConfig cfg;
            cfg.expectedNodeCount = init.expectedNodeCount;
            cfg.autoStartWhenAllRegistered = init.autoStartWhenAllRegistered;
            cfg.startLeadTimeMs = init.startLeadTimeMs;
            cfg.waitForStartDefaultTimeoutMs = init.waitForStartDefaultTimeoutMs;
            cfg.rejectStreamBeforeStart = init.rejectStreamBeforeStart;
            return cfg;
        }

        InitOptions m_init;
        streaming::CoincidenceServer m_server;
    };

} // namespace openpni::distributed::grpcnode
