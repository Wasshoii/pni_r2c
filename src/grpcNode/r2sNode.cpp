#include "grpcNode/r2sNode.hpp"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <exception>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <thread>
#include <utility>
#include <vector>
#include <glog/logging.h>

#include "coincidence.grpc.pb.h"
#include "core/streaming/PackedSingle.hpp"
#include "dataplane/rdma/ProtoConvert.hpp"
#include "dataplane/rdma/RdmaWriteSender.hpp"

#ifdef DEBUG
#include <fstream>
#endif

namespace openpni::distributed::grpcnode
{
    namespace coincidence = openpni::distributed::coincidence;
    namespace streaming = openpni::distributed::streaming;
    namespace rdma = openpni::distributed::dataplane::rdma;

#ifdef DEBUG
    namespace
    {
        void updateAtomicMax(std::atomic<uint64_t> &target, uint64_t value)
        {
            uint64_t current = target.load(std::memory_order_relaxed);
            while (current < value &&
                   !target.compare_exchange_weak(current, value,
                                                 std::memory_order_relaxed,
                                                 std::memory_order_relaxed))
            {
            }
        }

        uint64_t readProcessRssBytes()
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
    } // namespace
#endif

    namespace
    {
        struct SegmentPayload
        {
            std::vector<r2s::Single> singles;
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
                uint32_t crystalsPerChannel = 0;
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
                    LOG(WARNING) << "[Node " << m_cfg.nodeId << "] sender already started";
                    return false;
                }

                grpc::ChannelArguments channelArgs;
                // Control-plane only (Register / WaitForStart / OpenDataPlane).
                channelArgs.SetInt("grpc.max_receive_message_length", 4 * 1024 * 1024);
                channelArgs.SetInt("grpc.max_send_message_length", 4 * 1024 * 1024);
                channelArgs.SetInt("grpc.keepalive_time_ms", 20000);
                channelArgs.SetInt("grpc.keepalive_timeout_ms", 10000);

                m_channel = grpc::CreateCustomChannel(
                    m_cfg.serverAddress,
                    grpc::InsecureChannelCredentials(),
                    channelArgs);
                m_stub = coincidence::CoincidenceService::NewStub(m_channel);
                if (!m_stub)
                {
                    LOG(ERROR) << "[Node " << m_cfg.nodeId << "] failed to create coincidence stub";
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
                        LOG(ERROR) << "[Node " << m_cfg.nodeId << "] wait-for-start failed";
                        m_started = false;
                        return false;
                    }
                    waitUntil(plannedStartMs);
                }

                if (!openRdmaDataPlane())
                {
                    LOG(ERROR) << "[Node " << m_cfg.nodeId << "] OpenDataPlane / RDMA connect failed";
                    m_started = false;
                    return false;
                }

                m_running = true;
                m_senderThread = std::thread([this] { senderLoop(); });

                LOG(INFO) << "[Node " << m_cfg.nodeId
                          << "] RDMA singles sender started";
                return true;
            }

            bool enqueue(
                std::vector<r2s::Single> &&singles,
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
                                 { return m_queue.size() < m_cfg.maxPendingSegments ||
                                          !m_running.load(std::memory_order_relaxed) ||
                                          m_sendFailed.load(std::memory_order_relaxed); });
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

                m_running = false;
                m_cvNotEmpty.notify_all();
                m_cvNotFull.notify_all();

                if (m_senderThread.joinable())
                {
                    m_senderThread.join();
                }

                if (m_rdmaSender)
                {
                    m_rdmaSender->close();
                    m_rdmaSender.reset();
                }

                m_stub.reset();
                return !m_sendFailed.load(std::memory_order_relaxed);
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
                return peakQueueSingles() * static_cast<uint64_t>(sizeof(r2s::Single));
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
                    LOG(ERROR) << "[Node " << m_cfg.nodeId << "] register failed: "
                               << (status.ok() ? response.message() : status.error_message());
                    return false;
                }
                return true;
            }

            bool openRdmaDataPlane()
            {
                rdma::RdmaWriteSender::Config sc;
                sc.nodeId = m_cfg.nodeId;
                m_rdmaSender = std::make_unique<rdma::RdmaWriteSender>(std::move(sc));

                rdma::RdmaEndpointInfo localEp{};
                if (!m_rdmaSender->prepareLocalEndpoint(&localEp))
                {
                    LOG(ERROR) << "[Node " << m_cfg.nodeId << "] prepareLocalEndpoint failed";
                    return false;
                }

                grpc::ClientContext context;
                coincidence::OpenDataPlaneRequest req;
                req.set_node_id(m_cfg.nodeId);
                rdma::fillProtoEndpoint(localEp, req.mutable_node_endpoint());

                coincidence::OpenDataPlaneResponse resp;
                grpc::Status status = m_stub->OpenDataPlane(&context, req, &resp);
                if (!status.ok() || !resp.success())
                {
                    LOG(ERROR) << "[Node " << m_cfg.nodeId << "] OpenDataPlane failed: "
                               << (status.ok() ? resp.message() : status.error_message());
                    return false;
                }

                const auto coinEp = rdma::fromProtoEndpoint(resp.coin_endpoint());
                if (!m_rdmaSender->connect(coinEp))
                {
                    LOG(ERROR) << "[Node " << m_cfg.nodeId << "] RDMA connect failed";
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
                        LOG(INFO) << "[Node " << m_cfg.nodeId
                                  << "] start signal received, planned_start_ms="
                                  << response.start_time_ms();
                        return true;
                    }

                    if (!status.ok())
                    {
                        LOG(WARNING) << "[Node " << m_cfg.nodeId << "] WaitForStart RPC failed: "
                                     << status.error_message();
                    }

                    if (m_cfg.waitForStartTimeoutMs > 0)
                    {
                        const uint64_t elapsed = nowMs() - beginMs;
                        if (elapsed >= m_cfg.waitForStartTimeoutMs)
                        {
                            LOG(ERROR) << "[Node " << m_cfg.nodeId
                                       << "] wait-for-start timeout after "
                                       << elapsed << " ms";
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
                try
                {
                    while (true)
                    {
                        SegmentPayload payload;
                        {
                            std::unique_lock<std::mutex> lock(m_mutex);
                            m_cvNotEmpty.wait(lock, [this]
                                              { return !m_queue.empty() ||
                                                       !m_running.load(std::memory_order_relaxed); });
                            if (m_queue.empty())
                            {
                                break;
                            }
#ifdef DEBUG
                            m_queueSegmentsInFlight.fetch_sub(1, std::memory_order_relaxed);
                            m_queueSinglesInFlight.fetch_sub(
                                static_cast<uint64_t>(m_queue.front().singles.size()),
                                std::memory_order_relaxed);
#endif
                            payload = std::move(m_queue.front());
                            m_queue.pop_front();
                        }
                        m_cvNotFull.notify_all();

                        if (!sendSegment(payload))
                        {
                            m_sendFailed = true;
                            m_running = false;
                            m_cvNotEmpty.notify_all();
                            m_cvNotFull.notify_all();
                            return;
                        }
                    }
                    return;
                }
                catch (const std::exception &e)
                {
                    LOG(ERROR) << "[Node " << m_cfg.nodeId << "] senderLoop exception: " << e.what();
                }
                catch (...)
                {
                    LOG(ERROR) << "[Node " << m_cfg.nodeId << "] senderLoop unknown exception";
                }

                m_sendFailed = true;
                m_running = false;
                m_cvNotEmpty.notify_all();
                m_cvNotFull.notify_all();
            }

            bool sendSegment(const SegmentPayload &payload)
            {
                if (!m_rdmaSender || payload.singles.empty())
                {
                    return payload.singles.empty();
                }

                constexpr size_t kMaxSinglesPerChunk = 8000000;
                size_t offset = 0;
                const size_t total = payload.singles.size();
                while (offset < total)
                {
                    const size_t count = std::min(kMaxSinglesPerChunk, total - offset);
                    const uint64_t chunkId = m_chunkIdCounter.fetch_add(1, std::memory_order_relaxed);

#ifdef DEBUG
                    const auto t0 = std::chrono::steady_clock::now();
#endif
                    const bool ok = m_rdmaSender->sendPackedSingles(
                        chunkId,
                        payload.clockMs,
                        payload.durationMs,
                        payload.singles.data() + offset,
                        static_cast<uint32_t>(count));
#ifdef DEBUG
                    const auto t1 = std::chrono::steady_clock::now();
                    const uint64_t writeNs = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
                    m_writeNs.fetch_add(writeNs, std::memory_order_relaxed);
                    updateAtomicMax(m_maxWriteNs, writeNs);
                    m_estimatedWireBytes.fetch_add(
                        static_cast<uint64_t>(count) * streaming::kPackedSingleSize,
                        std::memory_order_relaxed);
#endif
                    if (!ok)
                    {
                        LOG(ERROR) << "[Node " << m_cfg.nodeId << "] RDMA send failed at chunk "
                                   << chunkId;
                        return false;
                    }

                    m_messagesSent.fetch_add(1, std::memory_order_relaxed);
                    m_singlesSent.fetch_add(static_cast<uint64_t>(count), std::memory_order_relaxed);
                    offset += count;
                }
                return true;
            }

            Config m_cfg;

            std::shared_ptr<grpc::Channel> m_channel;
            std::unique_ptr<coincidence::CoincidenceService::Stub> m_stub;
            std::unique_ptr<rdma::RdmaWriteSender> m_rdmaSender;

            std::deque<SegmentPayload> m_queue;
            std::mutex m_mutex;
            std::condition_variable m_cvNotEmpty;
            std::condition_variable m_cvNotFull;
            std::thread m_senderThread;

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
    } // namespace

    R2SGrpcNode::R2SGrpcNode(InitOptions init)
        : m_init(std::move(init))
    {
    }

    R2SGrpcNode::R2SGrpcNode(
        const r2s::R2SProcessConfig &r2sConfig,
        std::string serverAddress,
        uint32_t nodeId,
        uint32_t channelCount,
        size_t maxPendingSegments,
        std::string nodeAddress,
        std::string detectorType,
        uint32_t progressLogInterval,
        bool waitForStartSignal,
        uint32_t waitForStartTimeoutMs,
        uint32_t waitForStartRpcTimeoutMs,
        uint32_t waitForStartRetryIntervalMs)
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

    bool R2SGrpcNode::run()
    {
        m_stats = NodeRunStats{};

        if (m_init.r2sConfig.crystalsPerChannel == 0)
        {
            LOG(ERROR) << "[Node " << m_init.nodeId
                       << "] invalid crystalsPerChannel=0 in R2S config";
            return false;
        }

        PersistentNodeStreamSender::Config senderConfig;
        senderConfig.serverAddress = m_init.serverAddress;
        senderConfig.nodeId = m_init.nodeId;
        senderConfig.nodeAddress = m_init.nodeAddress;
        senderConfig.channelCount = m_init.channelCount;
        senderConfig.detectorType = m_init.detectorType;
        senderConfig.crystalsPerChannel = m_init.r2sConfig.crystalsPerChannel;
        senderConfig.maxPendingSegments = m_init.maxPendingSegments;
        senderConfig.waitForStartSignal = m_init.waitForStartSignal;
        senderConfig.waitForStartTimeoutMs = m_init.waitForStartTimeoutMs;
        senderConfig.waitForStartRpcTimeoutMs = m_init.waitForStartRpcTimeoutMs;
        senderConfig.waitForStartRetryIntervalMs = m_init.waitForStartRetryIntervalMs;

        PersistentNodeStreamSender sender(std::move(senderConfig));
        if (!sender.start())
        {
            LOG(ERROR) << "[Node " << m_init.nodeId << "] failed to start persistent sender";
            return false;
        }

        r2s::R2SProcessConfig config = m_init.r2sConfig;
        config.onSinglesReady = nullptr;
        config.onSinglesSpanReady = [this, &sender](
                                        std::span<r2s::Single const> singles,
                                        uint64_t clock_ms,
                                        uint32_t duration_ms) -> bool
        {
            m_stats.callbackCount += 1;

            std::vector<r2s::Single> hostSingles;
            try
            {
                hostSingles = r2s::materializeSinglesOnHost(singles);
            }
            catch (const std::exception &e)
            {
                LOG(ERROR) << "[Node " << m_init.nodeId
                           << "] failed to materialize singles on host: "
                           << e.what();
                return false;
            }

            const bool sent = sender.enqueue(std::move(hostSingles), clock_ms, duration_ms);
            if (!sent)
            {
                LOG(ERROR) << "[Node " << m_init.nodeId << "] enqueue failed at callback "
                           << m_stats.callbackCount;
                return false;
            }

            if (m_init.progressLogInterval > 0)
            {
                LOG_EVERY_N(INFO, static_cast<int>(m_init.progressLogInterval))
                    << "[Node " << m_init.nodeId << "] callbacks=" << m_stats.callbackCount
                    << " (streaming queue active)";
            }

            return true;
        };

#ifdef DEBUG
        const auto t0 = std::chrono::steady_clock::now();
#endif
        bool r2sSuccess = false;
        if (std::filesystem::is_directory(config.rawdataPath))
        {
            const std::string rawDir = config.rawdataPath;
            LOG(INFO) << "[Node " << m_init.nodeId
                      << "] rawdataPath is directory, using processR2SDirectory: " << rawDir;
            r2sSuccess = r2s::processR2SDirectory(config, rawDir, false);
        }
        else
        {
            r2sSuccess = r2s::processR2S(config);
        }
        const bool streamSuccess = sender.stop();
#ifdef DEBUG
        const auto t1 = std::chrono::steady_clock::now();
#endif

        m_stats.singlesSent = sender.singlesSent();
        m_stats.rdmaChunksSent = sender.messagesSent();
        m_stats.success = r2sSuccess && streamSuccess;

#ifdef DEBUG
        m_stats.enqueueCalls = sender.enqueueCalls();
        m_stats.enqueueTotalNs = sender.enqueueTotalNs();
        m_stats.enqueueWaitNs = sender.enqueueWaitNs();
        m_stats.enqueuePushNs = sender.enqueuePushNs();
        m_stats.maxEnqueueWaitNs = sender.maxEnqueueWaitNs();
        m_stats.writeNs = sender.writeNs();
        m_stats.maxWriteNs = sender.maxWriteNs();
        m_stats.peakQueueSegments = sender.peakQueueSegments();
        m_stats.peakQueueSingles = sender.peakQueueSingles();
        m_stats.peakQueueBytes = sender.peakQueueBytes();
        m_stats.processRssBytes = readProcessRssBytes();
        m_stats.runElapsedMs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
#endif

        return m_stats.success;
    }

    const NodeRunStats &R2SGrpcNode::stats() const
    {
        return m_stats;
    }

} // namespace openpni::distributed::grpcnode
