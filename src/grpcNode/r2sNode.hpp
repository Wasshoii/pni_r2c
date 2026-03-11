#pragma once

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "coincidence.grpc.pb.h"
#include "core/aquisition-and-r2s/R2S.hpp"

namespace openpni::distributed::grpcnode
{
    namespace coincidence = openpni::distributed::coincidence;
    namespace r2s = openpni::distributed::r2s;

#ifdef DEBUG
    inline void updateAtomicMax(std::atomic<uint64_t> &target, uint64_t value)
    {
        uint64_t current = target.load(std::memory_order_relaxed);
        while (current < value &&
               !target.compare_exchange_weak(current, value,
                                             std::memory_order_relaxed,
                                             std::memory_order_relaxed))
        {
        }
    }

    inline uint64_t readProcessRssBytes()
    {
        std::ifstream in("/proc/self/status");
        if (!in.is_open())
        {
            return 0;
        }

        std::string key;
        uint64_t value = 0;
        std::string unit;

        while (in >> key)
        {
            if (key == "VmRSS:")
            {
                in >> value >> unit;
                return value * 1024ULL;
            }

            std::string line;
            std::getline(in, line);
        }

        return 0;
    }
#endif

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
            r2s::R2SProcessConfig r2sConfig;       // R2S处理配置
            std::string serverAddress;             // 接收机（符合节点）的 gRPC 服务器地址
            uint32_t nodeId = 0;                   // 节点ID，需唯一
            std::string nodeAddress = "127.0.0.1"; // 节点自身地址
            uint32_t channelCount = 0;             // 节点处理的通道数量（仅用于统计和日志）
            std::string detectorType = "BDM2";     // 探测器类型字符串（仅用于统计和日志）
            size_t maxPendingSegments = 128;       // 流式发送队列最大待处理段数量，超过则回调返回false停止处理
            uint32_t progressLogInterval = 50;     // 每处理多少次回调输出一次进度日志，0则不输出

            // 标准编排流程：注册后等待符合主机发出开始信号
            bool waitForStartSignal = true;
            uint32_t waitForStartTimeoutMs = 0;          // 0 = 无限等待
            uint32_t waitForStartRpcTimeoutMs = 15000;   // 单次 WaitForStart RPC 超时
            uint32_t waitForStartRetryIntervalMs = 1000; // 未就绪时重试间隔
        };

        explicit R2SGrpcNode(InitOptions init)
            : m_init(std::move(init))
        {
        }

        // Convenience init style similar to R2S.hpp usage:
        // provide R2SProcessConfig + gRPC essentials in one constructor.
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
            uint32_t waitForStartRetryIntervalMs = 1000)
        {
            m_init.r2sConfig = r2sConfig;
            m_init.serverAddress = std::move(serverAddress);
            m_init.nodeId = nodeId;
            m_init.nodeAddress = std::move(nodeAddress);
            m_init.channelCount = channelCount;
            m_init.detectorType = std::move(detectorType);
            m_init.maxPendingSegments = maxPendingSegments;
            m_init.progressLogInterval = progressLogInterval;
            m_init.waitForStartSignal = waitForStartSignal;
            m_init.waitForStartTimeoutMs = waitForStartTimeoutMs;
            m_init.waitForStartRpcTimeoutMs = waitForStartRpcTimeoutMs;
            m_init.waitForStartRetryIntervalMs = waitForStartRetryIntervalMs;
        }

        bool run()
        {
            m_stats = NodeRunStats{};

            PersistentNodeStreamSender::Config senderConfig;
            senderConfig.serverAddress = m_init.serverAddress;
            senderConfig.nodeId = m_init.nodeId;
            senderConfig.nodeAddress = m_init.nodeAddress;
            senderConfig.channelCount = m_init.channelCount;
            senderConfig.detectorType = m_init.detectorType;
            senderConfig.maxPendingSegments = m_init.maxPendingSegments;
            senderConfig.waitForStartSignal = m_init.waitForStartSignal;
            senderConfig.waitForStartTimeoutMs = m_init.waitForStartTimeoutMs;
            senderConfig.waitForStartRpcTimeoutMs = m_init.waitForStartRpcTimeoutMs;
            senderConfig.waitForStartRetryIntervalMs = m_init.waitForStartRetryIntervalMs;

            PersistentNodeStreamSender sender(std::move(senderConfig));
            if (!sender.start())
            {
                std::cerr << "[Node " << m_init.nodeId << "] failed to start persistent sender" << std::endl;
                return false;
            }

            r2s::R2SProcessConfig config = m_init.r2sConfig;
            config.onSinglesReady = [this, &sender](
                                        std::vector<r2s::GlobalSingle> &&singles,
                                        uint64_t clock_ms,
                                        uint32_t duration_ms) -> bool
            {
                m_stats.callbackCount += 1;

                const bool sent = sender.enqueue(std::move(singles), clock_ms, duration_ms);
                if (!sent)
                {
                    std::cerr << "[Node " << m_init.nodeId << "] enqueue failed at callback "
                              << m_stats.callbackCount << std::endl;
                    return false;
                }

                if (m_init.progressLogInterval > 0 &&
                    (m_stats.callbackCount == 1 || m_stats.callbackCount % m_init.progressLogInterval == 0))
                {
                    std::cout << "[Node " << m_init.nodeId << "] callbacks=" << m_stats.callbackCount
                              << " (streaming queue active)" << std::endl;
                }

                return true;
            };

#ifdef DEBUG
            const auto t0 = std::chrono::steady_clock::now();
#endif
            const bool r2sSuccess = r2s::processR2S(config);
            const bool streamSuccess = sender.stop();
#ifdef DEBUG
            const auto t1 = std::chrono::steady_clock::now();
#endif

            m_stats.singlesSent = sender.singlesSent();
            m_stats.grpcMessagesSent = sender.messagesSent();
            m_stats.success = r2sSuccess && streamSuccess;

#ifdef DEBUG
            m_stats.enqueueCalls = sender.enqueueCalls();
            m_stats.enqueueTotalNs = sender.enqueueTotalNs();
            m_stats.enqueueWaitNs = sender.enqueueWaitNs();
            m_stats.enqueuePushNs = sender.enqueuePushNs();
            m_stats.maxEnqueueWaitNs = sender.maxEnqueueWaitNs();
            m_stats.serializeBuildNs = sender.serializeBuildNs();
            m_stats.writeNs = sender.writeNs();
            m_stats.maxWriteNs = sender.maxWriteNs();
            m_stats.estimatedWireBytes = sender.estimatedWireBytes();
            m_stats.peakQueueSegments = sender.peakQueueSegments();
            m_stats.peakQueueSingles = sender.peakQueueSingles();
            m_stats.peakQueueBytes = sender.peakQueueBytes();
            m_stats.processRssBytes = readProcessRssBytes();
            m_stats.runElapsedMs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
#endif

            return m_stats.success;
        }

        const NodeRunStats &stats() const
        {
            return m_stats;
        }

    private:
        struct SegmentPayload
        {
            std::vector<r2s::GlobalSingle> singles;
            uint64_t clockMs = 0;
            uint32_t durationMs = 0;
        };

        class PersistentNodeStreamSender
        {
        public:
            struct Config
            {
                std::string serverAddress;
                uint32_t nodeId = 0;
                std::string nodeAddress;
                uint32_t channelCount = 0;
                std::string detectorType = "BDM2";
                size_t maxPendingSegments = 128;

                bool waitForStartSignal = true;
                uint32_t waitForStartTimeoutMs = 0;
                uint32_t waitForStartRpcTimeoutMs = 15000;
                uint32_t waitForStartRetryIntervalMs = 1000;
            };

            explicit PersistentNodeStreamSender(Config cfg)
                : m_cfg(std::move(cfg))
            {
            }

            ~PersistentNodeStreamSender()
            {
                stop();
            }

            bool start()
            {
                if (m_started.exchange(true))
                {
                    std::cerr << "[Node " << m_cfg.nodeId << "] sender already started" << std::endl;
                    return false;
                }

                m_channel = grpc::CreateChannel(m_cfg.serverAddress, grpc::InsecureChannelCredentials());
                m_stub = coincidence::CoincidenceService::NewStub(m_channel);
                if (!m_stub)
                {
                    std::cerr << "[Node " << m_cfg.nodeId << "] failed to create coincidence stub" << std::endl;
                    m_started = false;
                    return false;
                }

                if (!registerNode())
                {
                    m_started = false;
                    return false;
                }

                if (m_cfg.waitForStartSignal)
                {
                    uint64_t plannedStartMs = 0;
                    if (!waitForStartSignal(&plannedStartMs))
                    {
                        std::cerr << "[Node " << m_cfg.nodeId << "] wait-for-start failed" << std::endl;
                        m_started = false;
                        return false;
                    }

                    waitUntil(plannedStartMs);
                }

                m_streamContext = std::make_unique<grpc::ClientContext>();
                m_writer = m_stub->StreamSingles(m_streamContext.get(), &m_streamResponse);
                if (!m_writer)
                {
                    std::cerr << "[Node " << m_cfg.nodeId << "] failed to open StreamSingles writer" << std::endl;
                    m_started = false;
                    return false;
                }

                m_running = true;
                m_senderThread = std::thread([this]
                                             { senderLoop(); });
                return true;
            }

            bool enqueue(
                std::vector<r2s::GlobalSingle> &&singles,
                uint64_t clockMs,
                uint32_t durationMs)
            {
#ifdef DEBUG
                const auto t0 = std::chrono::steady_clock::now();
                m_enqueueCalls.fetch_add(1, std::memory_order_relaxed);
                const uint64_t singlesCount = static_cast<uint64_t>(singles.size());
#endif

                if (!m_running.load(std::memory_order_relaxed) || m_sendFailed.load(std::memory_order_relaxed))
                {
                    return false;
                }

                std::unique_lock<std::mutex> lock(m_mutex);
#ifdef DEBUG
                const auto tWaitStart = std::chrono::steady_clock::now();
#endif
                m_cvNotFull.wait(lock, [this]
                                 { return m_queue.size() < m_cfg.maxPendingSegments || !m_running.load(std::memory_order_relaxed) || m_sendFailed.load(std::memory_order_relaxed); });
#ifdef DEBUG
                const auto tWaitEnd = std::chrono::steady_clock::now();
#endif

                if (!m_running.load(std::memory_order_relaxed) || m_sendFailed.load(std::memory_order_relaxed))
                {
                    return false;
                }

                m_queue.push_back(SegmentPayload{std::move(singles), clockMs, durationMs});
#ifdef DEBUG
                const uint64_t currentSegments = m_queueSegmentsInFlight.fetch_add(1, std::memory_order_relaxed) + 1;
                const uint64_t currentSingles = m_queueSinglesInFlight.fetch_add(singlesCount, std::memory_order_relaxed) + singlesCount;
                updateAtomicMax(m_peakQueueSegments, currentSegments);
                updateAtomicMax(m_peakQueueSingles, currentSingles);
                const auto tPushEnd = std::chrono::steady_clock::now();
#endif
                lock.unlock();
                m_cvNotEmpty.notify_one();

#ifdef DEBUG
                const uint64_t waitNs = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(tWaitEnd - tWaitStart).count());
                const uint64_t pushNs = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(tPushEnd - tWaitEnd).count());
                const uint64_t totalNs = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(tPushEnd - t0).count());

                m_enqueueWaitNs.fetch_add(waitNs, std::memory_order_relaxed);
                m_enqueuePushNs.fetch_add(pushNs, std::memory_order_relaxed);
                m_enqueueTotalNs.fetch_add(totalNs, std::memory_order_relaxed);
                updateAtomicMax(m_maxEnqueueWaitNs, waitNs);
#endif
                return true;
            }

            bool stop()
            {
                if (!m_started.exchange(false))
                {
                    return !m_sendFailed.load(std::memory_order_relaxed);
                }

                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    m_running = false;
                }
                m_cvNotEmpty.notify_all();
                m_cvNotFull.notify_all();

                if (m_senderThread.joinable())
                {
                    m_senderThread.join();
                }

                bool ok = !m_sendFailed.load(std::memory_order_relaxed);

                if (m_writer)
                {
                    const bool writesDone = m_writer->WritesDone();
                    grpc::Status status = m_writer->Finish();
                    if (!writesDone || !status.ok() || !m_streamResponse.success())
                    {
                        ok = false;
                        std::cerr << "[Node " << m_cfg.nodeId << "] stream finish failed: "
                                  << (status.ok() ? m_streamResponse.message() : status.error_message())
                                  << std::endl;
                    }
                }

                m_writer.reset();
                m_streamContext.reset();
                m_stub.reset();
                return ok;
            }

            uint64_t singlesSent() const { return m_singlesSent.load(std::memory_order_relaxed); }
            uint64_t messagesSent() const { return m_messagesSent.load(std::memory_order_relaxed); }

#ifdef DEBUG
            uint64_t enqueueCalls() const { return m_enqueueCalls.load(std::memory_order_relaxed); }
            uint64_t enqueueTotalNs() const { return m_enqueueTotalNs.load(std::memory_order_relaxed); }
            uint64_t enqueueWaitNs() const { return m_enqueueWaitNs.load(std::memory_order_relaxed); }
            uint64_t enqueuePushNs() const { return m_enqueuePushNs.load(std::memory_order_relaxed); }
            uint64_t maxEnqueueWaitNs() const { return m_maxEnqueueWaitNs.load(std::memory_order_relaxed); }
            uint64_t serializeBuildNs() const { return m_serializeBuildNs.load(std::memory_order_relaxed); }
            uint64_t writeNs() const { return m_writeNs.load(std::memory_order_relaxed); }
            uint64_t maxWriteNs() const { return m_maxWriteNs.load(std::memory_order_relaxed); }
            uint64_t estimatedWireBytes() const { return m_estimatedWireBytes.load(std::memory_order_relaxed); }
            uint64_t peakQueueSegments() const { return m_peakQueueSegments.load(std::memory_order_relaxed); }
            uint64_t peakQueueSingles() const { return m_peakQueueSingles.load(std::memory_order_relaxed); }
            uint64_t peakQueueBytes() const
            {
                return peakQueueSingles() * static_cast<uint64_t>(sizeof(r2s::GlobalSingle));
            }
#endif

        private:
            bool registerNode()
            {
                grpc::ClientContext context;
                coincidence::RegisterNodeRequest request;
                request.set_node_id(m_cfg.nodeId);
                request.set_node_address(m_cfg.nodeAddress);
                request.set_channel_count(m_cfg.channelCount);
                request.set_detector_type(m_cfg.detectorType);

                coincidence::RegisterNodeResponse response;
                grpc::Status status = m_stub->RegisterNode(&context, request, &response);

                if (!status.ok() || !response.success())
                {
                    std::cerr << "[Node " << m_cfg.nodeId << "] register failed: "
                              << (status.ok() ? response.message() : status.error_message())
                              << std::endl;
                    return false;
                }

                return true;
            }

            static uint64_t nowMs()
            {
                return static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count());
            }

            void waitUntil(uint64_t plannedStartMs)
            {
                if (plannedStartMs == 0)
                {
                    return;
                }

                while (m_started.load(std::memory_order_relaxed))
                {
                    const uint64_t now = nowMs();
                    if (now >= plannedStartMs)
                    {
                        return;
                    }

                    const uint64_t remaining = plannedStartMs - now;
                    const uint64_t sleepMs = std::min<uint64_t>(remaining, 100);
                    std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
                }
            }

            bool waitForStartSignal(uint64_t *plannedStartMs)
            {
                const uint64_t beginMs = nowMs();

                while (m_started.load(std::memory_order_relaxed))
                {
                    grpc::ClientContext context;
                    coincidence::WaitForStartRequest request;
                    request.set_node_id(m_cfg.nodeId);
                    request.set_timeout_ms(m_cfg.waitForStartRpcTimeoutMs);

                    coincidence::WaitForStartResponse response;
                    grpc::Status status = m_stub->WaitForStart(&context, request, &response);
                    if (status.ok() && response.success() && response.start_signal_issued())
                    {
                        if (plannedStartMs)
                        {
                            *plannedStartMs = response.start_time_ms();
                        }

                        std::cout << "[Node " << m_cfg.nodeId
                                  << "] start signal received, planned_start_ms="
                                  << response.start_time_ms() << std::endl;
                        return true;
                    }

                    if (!status.ok())
                    {
                        std::cerr << "[Node " << m_cfg.nodeId << "] WaitForStart RPC failed: "
                                  << status.error_message() << std::endl;
                    }
                    else
                    {
                        std::cerr << "[Node " << m_cfg.nodeId << "] WaitForStart not ready: "
                                  << response.message() << std::endl;
                    }

                    if (m_cfg.waitForStartTimeoutMs > 0)
                    {
                        const uint64_t elapsed = nowMs() - beginMs;
                        if (elapsed >= m_cfg.waitForStartTimeoutMs)
                        {
                            std::cerr << "[Node " << m_cfg.nodeId
                                      << "] wait-for-start timeout after "
                                      << elapsed << " ms" << std::endl;
                            return false;
                        }
                    }

                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(m_cfg.waitForStartRetryIntervalMs));
                }

                return false;
            }

            void senderLoop()
            {
                while (true)
                {
                    SegmentPayload payload;

                    {
                        std::unique_lock<std::mutex> lock(m_mutex);
                        m_cvNotEmpty.wait(lock, [this]
                                          { return !m_queue.empty() || !m_running.load(std::memory_order_relaxed); });

                        if (m_queue.empty())
                        {
                            break;
                        }

                        payload = std::move(m_queue.front());
                        m_queue.pop_front();
                    }

#ifdef DEBUG
                    m_queueSegmentsInFlight.fetch_sub(1, std::memory_order_relaxed);
                    m_queueSinglesInFlight.fetch_sub(static_cast<uint64_t>(payload.singles.size()), std::memory_order_relaxed);
#endif
                    m_cvNotFull.notify_one();

                    if (!sendPayload(payload))
                    {
                        m_sendFailed = true;

                        {
                            std::lock_guard<std::mutex> lock(m_mutex);
                            m_running = false;
                        }

                        m_cvNotEmpty.notify_all();
                        m_cvNotFull.notify_all();
                        return;
                    }
                }
            }

            bool sendPayload(const SegmentPayload &payload)
            {
                if (payload.singles.empty())
                {
                    return true;
                }

#ifdef DEBUG
                const auto tBuildStart = std::chrono::steady_clock::now();
#endif
                coincidence::SingleChunkMessage msg;
                msg.set_node_id(m_cfg.nodeId);
                msg.set_chunk_id(m_chunkIdCounter.fetch_add(1, std::memory_order_relaxed));
                msg.set_computer_clock_ms(payload.clockMs);
                msg.set_duration_ms(payload.durationMs);
                msg.mutable_singles()->Reserve(static_cast<int>(payload.singles.size()));

                for (const auto &s : payload.singles)
                {
                    auto *event = msg.add_singles();
                    event->set_crystal_index(s.globalCrystalIndex);
                    event->set_energy(s.energy);
                    event->set_time_pico(s.timeValue_pico);
                }

#ifdef DEBUG
                const auto tBuildEnd = std::chrono::steady_clock::now();
                m_serializeBuildNs.fetch_add(
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(tBuildEnd - tBuildStart).count()),
                    std::memory_order_relaxed);
                m_estimatedWireBytes.fetch_add(static_cast<uint64_t>(msg.ByteSizeLong()), std::memory_order_relaxed);
                const auto tWriteStart = std::chrono::steady_clock::now();
#endif
                if (!m_writer->Write(msg))
                {
                    std::cerr << "[Node " << m_cfg.nodeId << "] stream write failed at chunk "
                              << msg.chunk_id() << std::endl;
                    return false;
                }

#ifdef DEBUG
                const auto tWriteEnd = std::chrono::steady_clock::now();
                const uint64_t writeNs = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(tWriteEnd - tWriteStart).count());
                m_writeNs.fetch_add(writeNs, std::memory_order_relaxed);
                updateAtomicMax(m_maxWriteNs, writeNs);
#endif
                m_messagesSent.fetch_add(1, std::memory_order_relaxed);
                m_singlesSent.fetch_add(payload.singles.size(), std::memory_order_relaxed);

                return true;
            }

            Config m_cfg;

            std::shared_ptr<grpc::Channel> m_channel;
            std::unique_ptr<coincidence::CoincidenceService::Stub> m_stub;
            std::unique_ptr<grpc::ClientContext> m_streamContext;
            coincidence::StreamResponse m_streamResponse;
            std::unique_ptr<grpc::ClientWriter<coincidence::SingleChunkMessage>> m_writer;

            std::thread m_senderThread;
            std::deque<SegmentPayload> m_queue;
            std::mutex m_mutex;
            std::condition_variable m_cvNotEmpty;
            std::condition_variable m_cvNotFull;

            std::atomic<bool> m_started{false};
            std::atomic<bool> m_running{false};
            std::atomic<bool> m_sendFailed{false};
            std::atomic<uint64_t> m_chunkIdCounter{0};
            std::atomic<uint64_t> m_singlesSent{0};
            std::atomic<uint64_t> m_messagesSent{0};

#ifdef DEBUG
            std::atomic<uint64_t> m_enqueueCalls{0};
            std::atomic<uint64_t> m_enqueueTotalNs{0};
            std::atomic<uint64_t> m_enqueueWaitNs{0};
            std::atomic<uint64_t> m_enqueuePushNs{0};
            std::atomic<uint64_t> m_maxEnqueueWaitNs{0};

            std::atomic<uint64_t> m_serializeBuildNs{0};
            std::atomic<uint64_t> m_writeNs{0};
            std::atomic<uint64_t> m_maxWriteNs{0};
            std::atomic<uint64_t> m_estimatedWireBytes{0};

            std::atomic<uint64_t> m_queueSegmentsInFlight{0};
            std::atomic<uint64_t> m_queueSinglesInFlight{0};
            std::atomic<uint64_t> m_peakQueueSegments{0};
            std::atomic<uint64_t> m_peakQueueSingles{0};
#endif
        };

        InitOptions m_init;
        NodeRunStats m_stats;
    };
} // namespace openpni::distributed::grpcnode
