#pragma once
#include <pni/io/v1/PetDataType_v1.h>
#include <grpcpp/grpcpp.h>
#include "protos/coincidence.grpc.pb.h"

#include <pni/io/IO.hpp>
#include <vector>
#include <memory>
#include <atomic>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <iostream>

namespace openpni::distributed::streaming
{

    /**
     * @brief 符合计算客户端配置
     */
    struct CoincidenceClientConfig
    {
        std::string serverAddress = "localhost:50051";
        uint32_t nodeId = 0;
        std::string nodeAddress = "";
        uint32_t channelCount = 0;
        std::string detectorType = "Unknown";

        // 缓冲配置
        size_t maxPendingChunks = 100;
        size_t batchSize = 1000; // 每个消息的最大单事件数

        // 重连配置
        uint32_t reconnectDelayMs = 1000;
        uint32_t maxReconnectAttempts = 10;

        // 心跳配置
        uint32_t heartbeatIntervalMs = 5000;
    };

    /**
     * @brief 符合计算 gRPC 客户端
     *
     * 提供异步数据发送功能，自动处理连接管理和重试。
     */
    class CoincidenceClient
    {
    public:
        explicit CoincidenceClient(const CoincidenceClientConfig &config)
            : m_config(config)
        {
            // 创建 gRPC channel
            m_channel = grpc::CreateChannel(
                config.serverAddress,
                grpc::InsecureChannelCredentials());
            m_stub = coincidence::CoincidenceService::NewStub(m_channel);
        }

        ~CoincidenceClient()
        {
            stop();
        }

        /**
         * @brief 启动客户端（包括发送线程和心跳线程）
         */
        bool start()
        {
            if (m_running.exchange(true))
            {
                std::cerr << "[CoincidenceClient] Already running" << std::endl;
                return false;
            }

            // 注册节点
            if (!registerNode())
            {
                m_running = false;
                return false;
            }

            // 启动发送线程
            m_senderThread = std::thread([this]
                                         { senderLoop(); });

            // 启动心跳线程
            m_heartbeatThread = std::thread([this]
                                            { heartbeatLoop(); });

            std::cout << "[CoincidenceClient] Started for node " << m_config.nodeId << std::endl;
            return true;
        }

        /**
         * @brief 停止客户端
         */
        void stop()
        {
            if (!m_running.exchange(false))
            {
                return;
            }

            // 唤醒等待线程
            m_cv.notify_all();

            // 等待线程结束
            if (m_senderThread.joinable())
            {
                m_senderThread.join();
            }
            if (m_heartbeatThread.joinable())
            {
                m_heartbeatThread.join();
            }

            std::cout << "[CoincidenceClient] Stopped" << std::endl;
        }

        /**
         * @brief 发送单事件数据块
         *
         * @param singles 单事件数据
         * @param computerClock_ms 计算机时钟
         * @param duration_ms 持续时间
         * @return 成功返回true
         */
        bool sendSingles(
            const std::vector<openpni::v1::basic::GlobalSingle_t> &singles,
            uint64_t computerClock_ms,
            uint32_t duration_ms)
        {
            if (!m_running.load())
            {
                return false;
            }

            // 创建消息
            auto msg = std::make_unique<coincidence::SingleChunkMessage>();
            msg->set_node_id(m_config.nodeId);
            msg->set_chunk_id(m_chunkIdCounter++);
            msg->set_computer_clock_ms(computerClock_ms);
            msg->set_duration_ms(duration_ms);

            // 填充单事件数据
            for (const auto &s : singles)
            {
                auto *event = msg->add_singles();
                event->set_crystal_index(s.globalCrystalIndex);
                event->set_energy(s.energy);
                event->set_time_pico(s.timeValue_pico);
            }

            // 加入发送队列
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                // 等待队列有空间
                if (m_pendingMessages.size() >= m_config.maxPendingChunks)
                {
                    m_cv.wait(lock, [this]
                              { return m_pendingMessages.size() < m_config.maxPendingChunks || !m_running.load(); });
                }

                if (!m_running.load())
                {
                    return false;
                }

                m_pendingMessages.push(std::move(msg));
            }

            m_cv.notify_one();
            m_totalSinglesSent += singles.size();

            return true;
        }

        /**
         * @brief 获取服务器状态
         */
        bool getServerStatus(coincidence::StatusResponse *response)
        {
            grpc::ClientContext context;
            coincidence::StatusRequest request;
            request.set_include_node_stats(true);

            grpc::Status status = m_stub->GetStatus(&context, request, response);
            return status.ok();
        }

        /**
         * @brief 获取已发送的单事件总数
         */
        uint64_t getTotalSinglesSent() const
        {
            return m_totalSinglesSent.load();
        }

        /**
         * @brief 获取待发送的消息数
         */
        size_t getPendingMessageCount() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_pendingMessages.size();
        }

        bool isRunning() const { return m_running.load(); }
        bool isConnected() const { return m_connected.load(); }

    private:
        /**
         * @brief 注册节点到服务器
         */
        bool registerNode()
        {
            grpc::ClientContext context;
            coincidence::RegisterNodeRequest request;
            request.set_node_id(m_config.nodeId);
            request.set_node_address(m_config.nodeAddress);
            request.set_channel_count(m_config.channelCount);
            request.set_detector_type(m_config.detectorType);

            coincidence::RegisterNodeResponse response;
            grpc::Status status = m_stub->RegisterNode(&context, request, &response);

            if (status.ok() && response.success())
            {
                std::cout << "[CoincidenceClient] Node " << m_config.nodeId
                          << " registered successfully" << std::endl;
                m_connected = true;
                return true;
            }
            else
            {
                std::cerr << "[CoincidenceClient] Failed to register node: "
                          << (status.ok() ? response.message() : status.error_message())
                          << std::endl;
                return false;
            }
        }

        /**
         * @brief 发送线程主循环
         */
        void senderLoop()
        {
            while (m_running.load())
            {
                std::unique_ptr<coincidence::SingleChunkMessage> msg;

                // 获取消息
                {
                    std::unique_lock<std::mutex> lock(m_mutex);
                    m_cv.wait(lock, [this]
                              { return !m_pendingMessages.empty() || !m_running.load(); });

                    if (!m_running.load() && m_pendingMessages.empty())
                    {
                        break;
                    }

                    if (!m_pendingMessages.empty())
                    {
                        msg = std::move(m_pendingMessages.front());
                        m_pendingMessages.pop();
                    }
                }

                if (msg)
                {
                    sendMessage(*msg);
                }
            }

            // 发送剩余消息
            flushPendingMessages();
        }

        /**
         * @brief 发送单个消息（带重试）
         */
        bool sendMessage(const coincidence::SingleChunkMessage &msg)
        {
            uint32_t attempts = 0;

            while (attempts < m_config.maxReconnectAttempts && m_running.load())
            {
                grpc::ClientContext context;
                coincidence::StreamResponse response;

                // 创建流式写入器
                auto writer = m_stub->StreamSingles(&context, &response);

                if (writer->Write(msg) && writer->WritesDone())
                {
                    grpc::Status status = writer->Finish();
                    if (status.ok())
                    {
                        m_connected = true;
                        return true;
                    }
                }

                // 发送失败，重试
                m_connected = false;
                attempts++;
                std::cerr << "[CoincidenceClient] Send failed, attempt "
                          << attempts << "/" << m_config.maxReconnectAttempts << std::endl;

                std::this_thread::sleep_for(
                    std::chrono::milliseconds(m_config.reconnectDelayMs));
            }

            return false;
        }

        /**
         * @brief 刷新剩余消息
         */
        void flushPendingMessages()
        {
            std::lock_guard<std::mutex> lock(m_mutex);

            while (!m_pendingMessages.empty())
            {
                auto &msg = m_pendingMessages.front();
                sendMessage(*msg);
                m_pendingMessages.pop();
            }
        }

        /**
         * @brief 心跳线程
         */
        void heartbeatLoop()
        {
            while (m_running.load())
            {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(m_config.heartbeatIntervalMs));

                if (!m_running.load())
                {
                    break;
                }

                grpc::ClientContext context;
                coincidence::HeartbeatRequest request;
                request.set_node_id(m_config.nodeId);
                request.set_timestamp_ms(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count());
                request.set_singles_sent(m_totalSinglesSent.load());

                coincidence::HeartbeatResponse response;
                grpc::Status status = m_stub->Heartbeat(&context, request, &response);

                if (!status.ok())
                {
                    m_connected = false;
                    std::cerr << "[CoincidenceClient] Heartbeat failed: "
                              << status.error_message() << std::endl;
                }
                else
                {
                    m_connected = true;
                }
            }
        }

        // 配置
        CoincidenceClientConfig m_config;

        // gRPC
        std::shared_ptr<grpc::Channel> m_channel;
        std::unique_ptr<coincidence::CoincidenceService::Stub> m_stub;

        // 发送队列
        mutable std::mutex m_mutex;
        std::condition_variable m_cv;
        std::queue<std::unique_ptr<coincidence::SingleChunkMessage>> m_pendingMessages;

        // 线程
        std::thread m_senderThread;
        std::thread m_heartbeatThread;

        // 状态
        std::atomic<bool> m_running{false};
        std::atomic<bool> m_connected{false};
        std::atomic<uint64_t> m_chunkIdCounter{0};
        std::atomic<uint64_t> m_totalSinglesSent{0};
    };

} // namespace openpni::distributed::streaming
