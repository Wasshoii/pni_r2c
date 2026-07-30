#include "grpcService/CoincidenceClient.hpp"
#include "core/streaming/PackedSingle.hpp"
#include "dataplane/rdma/ProtoConvert.hpp"

#include <chrono>
#include <cstring>
#include <limits>
#include <thread>
#include <utility>
#include <glog/logging.h>

namespace openpni::distributed::streaming
{
    namespace rdma = openpni::distributed::dataplane::rdma;

    CoincidenceClient::CoincidenceClient(const CoincidenceClientConfig &config)
        : m_config(config)
    {
        m_channel = grpc::CreateChannel(
            config.serverAddress,
            grpc::InsecureChannelCredentials());
        m_stub = coincidence::CoincidenceService::NewStub(m_channel);
    }

    CoincidenceClient::~CoincidenceClient()
    {
        stop();
    }

    bool CoincidenceClient::start()
    {
        if (m_running.exchange(true))
        {
            LOG(WARNING) << "Already running";
            return false;
        }

        if (!registerNode())
        {
            m_running = false;
            return false;
        }

        if (m_config.waitForStartSignal)
        {
            if (!waitForServerStartSignal(m_config.waitForStartTimeoutMs))
            {
                m_running = false;
                return false;
            }
        }

        if (!openRdmaDataPlane())
        {
            m_running = false;
            return false;
        }

        m_senderThread = std::thread([this]()
                                     { senderLoop(); });

        m_heartbeatThread = std::thread([this]()
                                        { heartbeatLoop(); });

        LOG(INFO) << "Started RDMA client for node " << m_config.nodeId;
        return true;
    }

    void CoincidenceClient::stop()
    {
        if (!m_running.exchange(false))
        {
            return;
        }

        m_cv.notify_all();

        if (m_senderThread.joinable())
        {
            m_senderThread.join();
        }
        if (m_heartbeatThread.joinable())
        {
            m_heartbeatThread.join();
        }

        if (m_rdmaSender)
        {
            m_rdmaSender->close();
            m_rdmaSender.reset();
        }

        LOG(INFO) << "Stopped";
    }

    bool CoincidenceClient::openRdmaDataPlane()
    {
        rdma::RdmaWriteSender::Config sc;
        sc.nodeId = m_config.nodeId;
        m_rdmaSender = std::make_unique<rdma::RdmaWriteSender>(std::move(sc));

        rdma::RdmaEndpointInfo localEp{};
        if (!m_rdmaSender->prepareLocalEndpoint(&localEp))
        {
            LOG(ERROR) << "prepareLocalEndpoint failed";
            return false;
        }

        grpc::ClientContext context;
        coincidence::OpenDataPlaneRequest req;
        req.set_node_id(m_config.nodeId);
        rdma::fillProtoEndpoint(localEp, req.mutable_node_endpoint());

        coincidence::OpenDataPlaneResponse resp;
        grpc::Status status = m_stub->OpenDataPlane(&context, req, &resp);
        if (!status.ok() || !resp.success())
        {
            LOG(ERROR) << "OpenDataPlane failed: "
                       << (status.ok() ? resp.message() : status.error_message());
            return false;
        }

        if (!m_rdmaSender->connect(rdma::fromProtoEndpoint(resp.coin_endpoint())))
        {
            LOG(ERROR) << "RDMA connect failed";
            return false;
        }
        m_connected = true;
        return true;
    }

    bool CoincidenceClient::sendSingles(
        const std::vector<Single> &singles,
        uint64_t computerClock_ms,
        uint32_t duration_ms)
    {
        if (!m_running.load())
        {
            return false;
        }

        auto chunk = std::make_unique<PendingChunk>();
        chunk->chunkId = m_chunkIdCounter++;
        chunk->computerClockMs = computerClock_ms;
        chunk->durationMs = duration_ms;
        chunk->singlesCount = static_cast<uint32_t>(singles.size());
        chunk->packed.resize(singles.size() * kPackedSingleSize);

        if (m_config.remapLocalToGlobalChannels)
        {
            auto *dst = reinterpret_cast<Single *>(chunk->packed.data());
            for (size_t i = 0; i < singles.size(); ++i)
            {
                const auto &s = singles[i];
                const uint64_t globalChannel = static_cast<uint64_t>(s.channelIndex) +
                                               static_cast<uint64_t>(m_config.globalChannelOffset);
                if (globalChannel > static_cast<uint64_t>(std::numeric_limits<uint16_t>::max()))
                {
                    LOG(ERROR) << "remapped channel index overflow: " << globalChannel;
                    return false;
                }

                dst[i].channelIndex = static_cast<uint16_t>(globalChannel);
                dst[i].crystalIndex = s.crystalIndex;
                dst[i].timevalue_100fs = s.timevalue_100fs;
                dst[i].energy = s.energy;

                if (i == 0 && !m_remapSampleLogged.exchange(true))
                {
                    LOG(INFO) << "remap sample node=" << m_config.nodeId
                              << " local_channel=" << s.channelIndex
                              << " global_channel=" << dst[i].channelIndex;
                }
            }
        }
        else
        {
            packSinglesToBinary(singles.data(), singles.size(), chunk->packed.data());
        }

        {
            std::unique_lock<std::mutex> lock(m_mutex);

            if (m_pendingMessages.size() >= m_config.maxPendingChunks)
            {
                m_cv.wait(lock, [this]()
                          { return m_pendingMessages.size() < m_config.maxPendingChunks || !m_running.load(); });
            }

            if (!m_running.load())
            {
                return false;
            }

            m_pendingMessages.push(std::move(chunk));
        }

        m_cv.notify_one();
        m_totalSinglesSent += singles.size();

        return true;
    }

    bool CoincidenceClient::getServerStatus(coincidence::StatusResponse *response)
    {
        grpc::ClientContext context;
        coincidence::StatusRequest request;
        request.set_include_node_stats(true);

        grpc::Status status = m_stub->GetStatus(&context, request, response);
        return status.ok();
    }

    uint64_t CoincidenceClient::getTotalSinglesSent() const
    {
        return m_totalSinglesSent.load();
    }

    size_t CoincidenceClient::getPendingMessageCount() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_pendingMessages.size();
    }

    bool CoincidenceClient::isRunning() const
    {
        return m_running.load();
    }

    bool CoincidenceClient::isConnected() const
    {
        return m_connected.load();
    }

    bool CoincidenceClient::waitForServerStartSignal(uint32_t timeoutMs)
    {
        const uint64_t startTs = nowMs();

        while (m_running.load())
        {
            grpc::ClientContext context;
            coincidence::WaitForStartRequest request;
            request.set_node_id(m_config.nodeId);
            request.set_timeout_ms(m_config.waitForStartRpcTimeoutMs);

            coincidence::WaitForStartResponse response;
            grpc::Status status = m_stub->WaitForStart(&context, request, &response);

            if (status.ok() && response.success() && response.start_signal_issued())
            {
                const uint64_t plannedStartMs = response.start_time_ms();
                waitUntil(plannedStartMs);
                LOG(INFO) << "Start signal received, planned_start_ms=" << plannedStartMs;
                return true;
            }

            if (!status.ok())
            {
                LOG(ERROR) << "WaitForStart RPC failed: " << status.error_message();
            }

            if (timeoutMs > 0)
            {
                const uint64_t elapsed = nowMs() - startTs;
                if (elapsed >= timeoutMs)
                {
                    LOG(ERROR) << "WaitForStart timed out after " << elapsed << " ms";
                    return false;
                }
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(m_config.waitForStartRetryIntervalMs));
        }

        return false;
    }

    uint64_t CoincidenceClient::nowMs()
    {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
    }

    void CoincidenceClient::waitUntil(uint64_t plannedStartMs)
    {
        if (plannedStartMs == 0)
        {
            return;
        }

        while (m_running.load())
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

    bool CoincidenceClient::registerNode()
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
            LOG(INFO) << "Node " << m_config.nodeId << " registered successfully";
            m_connected = true;
            return true;
        }

        LOG(ERROR) << "Failed to register node: "
                   << (status.ok() ? response.message() : status.error_message());
        return false;
    }

    void CoincidenceClient::senderLoop()
    {
        while (m_running.load())
        {
            std::unique_ptr<PendingChunk> msg;

            {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_cv.wait(lock, [this]()
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
                sendChunk(*msg);
            }
        }

        flushPendingMessages();
    }

    bool CoincidenceClient::sendChunk(const PendingChunk &chunk)
    {
        if (!m_rdmaSender)
        {
            return false;
        }
        const bool ok = m_rdmaSender->sendPackedSingles(
            chunk.chunkId,
            chunk.computerClockMs,
            chunk.durationMs,
            chunk.packed.data(),
            chunk.singlesCount);
        m_connected = ok;
        return ok;
    }

    void CoincidenceClient::flushPendingMessages()
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        while (!m_pendingMessages.empty())
        {
            auto &msg = m_pendingMessages.front();
            sendChunk(*msg);
            m_pendingMessages.pop();
        }
    }

    void CoincidenceClient::heartbeatLoop()
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
                LOG(WARNING) << "Heartbeat failed: " << status.error_message();
            }
            else
            {
                m_connected = true;
            }
        }
    }

} // namespace openpni::distributed::streaming
