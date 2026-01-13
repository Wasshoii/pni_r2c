#pragma once

#include "StreamingCoincidence.hpp"

#include <grpcpp/grpcpp.h>
#include "protos/coincidence.grpc.pb.h"

#include <unordered_map>
#include <shared_mutex>
#include <chrono>

namespace openpni::distributed::streaming
{

    /**
     * @brief 节点连接信息
     */
    struct NodeConnectionInfo
    {
        uint32_t nodeId;
        std::string address;
        uint32_t channelCount;
        std::string detectorType;
        std::chrono::steady_clock::time_point lastHeartbeat;
        std::atomic<uint64_t> singlesReceived{0};
        std::atomic<uint64_t> chunksReceived{0};
        std::atomic<bool> connected{false};
    };

    /**
     * @brief gRPC 服务：接收分布式节点的单事件数据
     */
    class CoincidenceServiceImpl final : public coincidence::CoincidenceService::Service
    {
    public:
        explicit CoincidenceServiceImpl(StreamingTimeAligner &aligner)
            : m_aligner(aligner)
        {
            // 初始化节点连接信息
            for (size_t i = 0; i < aligner.getNodeCount(); ++i)
            {
                auto info = std::make_shared<NodeConnectionInfo>();
                info->nodeId = i;
                m_nodeInfos[i] = info;
            }
        }

        /**
         * @brief 流式接收单事件数据（核心接口）
         */
        grpc::Status StreamSingles(
            grpc::ServerContext *context,
            grpc::ServerReader<coincidence::SingleChunkMessage> *reader,
            coincidence::StreamResponse *response) override
        {
            coincidence::SingleChunkMessage msg;
            uint64_t totalReceived = 0;

            while (reader->Read(&msg))
            {
                uint32_t nodeId = msg.node_id();

                // 获取节点缓冲区
                auto *buffer = m_aligner.getNodeBuffer(nodeId);
                if (!buffer)
                {
                    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                        "Invalid node ID: " + std::to_string(nodeId));
                }

                // 转换消息为内部格式
                TimestampedSingleChunk chunk;
                chunk.nodeId = nodeId;
                chunk.chunkId = msg.chunk_id();
                chunk.computerClock_ms = msg.computer_clock_ms();
                chunk.duration_ms = msg.duration_ms();

                // 解析单事件数据
                chunk.singles.reserve(msg.singles_size());
                for (const auto &s : msg.singles())
                {
                    openpni::basic::GlobalSingle_t single;
                    single.globalCrystalIndex = s.crystal_index();
                    single.energy = s.energy();
                    single.timeValue_pico = s.time_pico();
                    chunk.singles.push_back(single);
                }

                // 推送到缓冲区
                if (!buffer->push(std::move(chunk)))
                {
                    return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                                        "Buffer full or closed for node " + std::to_string(nodeId));
                }

                totalReceived += msg.singles_size();

                // 更新节点统计
                updateNodeStats(nodeId, msg.singles_size());
            }

            response->set_success(true);
            response->set_singles_received(totalReceived);
            response->set_message("Successfully received " + std::to_string(totalReceived) + " singles");

            return grpc::Status::OK;
        }

        /**
         * @brief 获取处理状态
         */
        grpc::Status GetStatus(
            grpc::ServerContext *context,
            const coincidence::StatusRequest *request,
            coincidence::StatusResponse *response) override
        {
            const auto &stats = m_aligner.getStatistics();

            response->set_total_singles_received(stats.totalSinglesReceived.load());
            response->set_total_singles_processed(stats.totalSinglesProcessed.load());
            response->set_total_prompt_pairs(stats.totalPromptPairs.load());
            response->set_total_delay_pairs(stats.totalDelayPairs.load());
            response->set_alignment_windows_processed(stats.alignmentWindowsProcessed.load());
            response->set_avg_processing_time_ms(stats.avgProcessingTime_ms.load());
            response->set_current_time_boundary_pico(stats.currentTimeBoundary_pico.load());
            response->set_is_running(m_aligner.isRunning());

            // 添加节点统计
            if (request->include_node_stats())
            {
                std::shared_lock<std::shared_mutex> lock(m_nodeInfosMutex);
                for (const auto &[nodeId, info] : m_nodeInfos)
                {
                    auto *nodeStatus = response->add_node_stats();
                    nodeStatus->set_node_id(nodeId);
                    nodeStatus->set_chunks_received(info->chunksReceived.load());
                    nodeStatus->set_singles_received(info->singlesReceived.load());

                    auto *buffer = m_aligner.getNodeBuffer(nodeId);
                    nodeStatus->set_buffer_size(buffer ? buffer->size() : 0);
                    nodeStatus->set_connected(info->connected.load());
                }
            }

            return grpc::Status::OK;
        }

        /**
         * @brief 控制命令
         */
        grpc::Status Control(
            grpc::ServerContext *context,
            const coincidence::ControlRequest *request,
            coincidence::ControlResponse *response) override
        {
            switch (request->command())
            {
            case coincidence::ControlRequest::START:
                if (m_aligner.isRunning())
                {
                    response->set_success(false);
                    response->set_message("Aligner already running");
                }
                else
                {
                    m_aligner.start();
                    response->set_success(true);
                    response->set_message("Aligner started");
                }
                break;

            case coincidence::ControlRequest::STOP:
                if (!m_aligner.isRunning())
                {
                    response->set_success(false);
                    response->set_message("Aligner not running");
                }
                else
                {
                    m_aligner.stop(request->wait_for_completion());
                    response->set_success(true);
                    response->set_message("Aligner stopped");
                }
                break;

            case coincidence::ControlRequest::FLUSH:
                // Flush 通过 stop + start 实现
                if (m_aligner.isRunning())
                {
                    m_aligner.stop(true);
                    m_aligner.start();
                    response->set_success(true);
                    response->set_message("Aligner flushed and restarted");
                }
                else
                {
                    response->set_success(false);
                    response->set_message("Aligner not running");
                }
                break;

            default:
                response->set_success(false);
                response->set_message("Unknown command");
                break;
            }

            return grpc::Status::OK;
        }

        /**
         * @brief 更新配置（当前仅返回不支持动态更新的消息）
         */
        grpc::Status UpdateConfig(
            grpc::ServerContext *context,
            const coincidence::ConfigUpdateRequest *request,
            coincidence::ConfigUpdateResponse *response) override
        {
            // 当前版本不支持动态更新配置
            // 需要停止后重新配置
            response->set_success(false);
            response->set_message("Dynamic config update not supported. Please stop the service and reconfigure.");
            return grpc::Status::OK;
        }

        /**
         * @brief 节点注册
         */
        grpc::Status RegisterNode(
            grpc::ServerContext *context,
            const coincidence::RegisterNodeRequest *request,
            coincidence::RegisterNodeResponse *response) override
        {
            uint32_t nodeId = request->node_id();

            if (nodeId >= m_aligner.getNodeCount())
            {
                response->set_success(false);
                response->set_message("Node ID out of range. Expected 0-" +
                                      std::to_string(m_aligner.getNodeCount() - 1));
                return grpc::Status::OK;
            }

            // 更新节点信息
            {
                std::unique_lock<std::shared_mutex> lock(m_nodeInfosMutex);
                auto &info = m_nodeInfos[nodeId];
                if (!info)
                {
                    info = std::make_shared<NodeConnectionInfo>();
                }
                info->nodeId = nodeId;
                info->address = request->node_address();
                info->channelCount = request->channel_count();
                info->detectorType = request->detector_type();
                info->lastHeartbeat = std::chrono::steady_clock::now();
                info->connected = true;
            }

            response->set_success(true);
            response->set_message("Node " + std::to_string(nodeId) + " registered successfully");
            response->set_assigned_node_id(nodeId);

            std::cout << "[CoincidenceService] Node " << nodeId
                      << " registered from " << request->node_address()
                      << " (detector: " << request->detector_type() << ")" << std::endl;

            return grpc::Status::OK;
        }

        /**
         * @brief 心跳
         */
        grpc::Status Heartbeat(
            grpc::ServerContext *context,
            const coincidence::HeartbeatRequest *request,
            coincidence::HeartbeatResponse *response) override
        {
            uint32_t nodeId = request->node_id();

            {
                std::shared_lock<std::shared_mutex> lock(m_nodeInfosMutex);
                auto it = m_nodeInfos.find(nodeId);
                if (it != m_nodeInfos.end())
                {
                    it->second->lastHeartbeat = std::chrono::steady_clock::now();
                    it->second->connected = true;
                }
            }

            response->set_acknowledged(true);
            response->set_server_timestamp_ms(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());

            return grpc::Status::OK;
        }

    private:
        void updateNodeStats(uint32_t nodeId, uint64_t singlesCount)
        {
            std::shared_lock<std::shared_mutex> lock(m_nodeInfosMutex);
            auto it = m_nodeInfos.find(nodeId);
            if (it != m_nodeInfos.end())
            {
                it->second->singlesReceived += singlesCount;
                it->second->chunksReceived++;
                it->second->lastHeartbeat = std::chrono::steady_clock::now();
            }
        }

        StreamingTimeAligner &m_aligner;
        std::unordered_map<uint32_t, std::shared_ptr<NodeConnectionInfo>> m_nodeInfos;
        mutable std::shared_mutex m_nodeInfosMutex;
    };

    // ==================== 服务器启动辅助函数 ====================

    /**
     * @brief 创建并运行符合计算 gRPC 服务器
     *
     * @param aligner 时间对齐器引用
     * @param address 服务器地址（如 "0.0.0.0:50051"）
     * @return 服务器指针
     */
    inline std::unique_ptr<grpc::Server> createCoincidenceServer(
        StreamingTimeAligner &aligner,
        const std::string &address)
    {
        CoincidenceServiceImpl service(aligner);

        grpc::ServerBuilder builder;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        // 设置消息大小限制
        builder.SetMaxReceiveMessageSize(100 * 1024 * 1024); // 100MB
        builder.SetMaxSendMessageSize(10 * 1024 * 1024);     // 10MB

        std::cout << "[CoincidenceServer] Starting on " << address << std::endl;

        return builder.BuildAndStart();
    }

    /**
     * @brief 符合计算服务器包装类
     *
     * 封装服务器生命周期管理
     */
    class CoincidenceServer
    {
    public:
        CoincidenceServer(const TimeAlignerConfig &config, size_t nodeCount, const std::string &address)
            : m_aligner(config, nodeCount),
              m_service(m_aligner),
              m_address(address)
        {
        }

        /**
         * @brief 启动服务器
         */
        void start()
        {
            if (m_running.exchange(true))
            {
                std::cerr << "[CoincidenceServer] Already running" << std::endl;
                return;
            }

            // 启动时间对齐器
            m_aligner.start();

            // 构建 gRPC 服务器
            grpc::ServerBuilder builder;
            builder.AddListeningPort(m_address, grpc::InsecureServerCredentials());
            builder.RegisterService(&m_service);

            // 设置消息大小限制
            builder.SetMaxReceiveMessageSize(100 * 1024 * 1024);
            builder.SetMaxSendMessageSize(10 * 1024 * 1024);

            m_server = builder.BuildAndStart();

            std::cout << "[CoincidenceServer] Started on " << m_address << std::endl;
        }

        /**
         * @brief 停止服务器
         */
        void stop()
        {
            if (!m_running.exchange(false))
            {
                return;
            }

            // 停止 gRPC 服务器
            if (m_server)
            {
                m_server->Shutdown();
                m_server.reset();
            }

            // 停止时间对齐器
            m_aligner.stop(true);

            std::cout << "[CoincidenceServer] Stopped" << std::endl;
        }

        /**
         * @brief 等待服务器结束
         */
        void wait()
        {
            if (m_server)
            {
                m_server->Wait();
            }
        }

        /**
         * @brief 获取时间对齐器
         */
        StreamingTimeAligner &getAligner() { return m_aligner; }
        const StreamingTimeAligner &getAligner() const { return m_aligner; }

        /**
         * @brief 获取统计信息
         */
        const ProcessingStatistics &getStatistics() const
        {
            return m_aligner.getStatistics();
        }

        bool isRunning() const { return m_running.load(); }

    private:
        StreamingTimeAligner m_aligner;
        CoincidenceServiceImpl m_service;
        std::string m_address;
        std::unique_ptr<grpc::Server> m_server;
        std::atomic<bool> m_running{false};
    };

} // namespace openpni::distributed::streaming
