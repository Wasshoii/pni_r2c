#include "grpcService/CoincidenceClient.hpp"

#include <chrono>
#include <iostream>
#include <limits>
#include <thread>
#include <utility>

namespace openpni::distributed::streaming
{

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
            std::cerr << "[CoincidenceClient] Already running" << std::endl;
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

        m_senderThread = std::thread([this]()
                                     { senderLoop(); });

        m_heartbeatThread = std::thread([this]()
                                        { heartbeatLoop(); });

        std::cout << "[CoincidenceClient] Started for node " << m_config.nodeId << std::endl;
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

        std::cout << "[CoincidenceClient] Stopped" << std::endl;
    }

    bool CoincidenceClient::sendSingles(
        const std::vector<GlobalSingle> &singles,
        uint64_t computerClock_ms,
        uint32_t duration_ms)
    {
        if (!m_running.load())
        {
            return false;
        }

        auto msg = std::make_unique<coincidence::SingleChunkMessage>();
        msg->set_node_id(m_config.nodeId);
        msg->set_chunk_id(m_chunkIdCounter++);
        msg->set_computer_clock_ms(computerClock_ms);
        msg->set_duration_ms(duration_ms);

        for (const auto &s : singles)
        {
            auto *event = msg->add_singles();
            uint32_t crystalIndexToSend = s.globalCrystalIndex;

            if (m_config.remapLocalToGlobalChannels)
            {
                if (m_config.crystalsPerChannel == 0)
                {
                    std::cerr << "[CoincidenceClient] invalid crystalsPerChannel=0" << std::endl;
                    return false;
                }

                const uint32_t localChannel = s.globalCrystalIndex / m_config.crystalsPerChannel;
                const uint32_t crystalInChannel = s.globalCrystalIndex % m_config.crystalsPerChannel;
                const uint64_t globalChannel = static_cast<uint64_t>(localChannel) +
                                              static_cast<uint64_t>(m_config.globalChannelOffset);
                const uint64_t remapped =
                    globalChannel * static_cast<uint64_t>(m_config.crystalsPerChannel) +
                    static_cast<uint64_t>(crystalInChannel);

                if (remapped > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()))
                {
                    std::cerr << "[CoincidenceClient] remapped crystal index overflow: " << remapped << std::endl;
                    return false;
                }

                crystalIndexToSend = static_cast<uint32_t>(remapped);

                if (!m_remapSampleLogged.exchange(true))
                {
                    std::cout << "[CoincidenceClient] remap sample node=" << m_config.nodeId
                              << " local_channel=" << localChannel
                              << " global_channel=" << globalChannel
                              << " local_index=" << s.globalCrystalIndex
                              << " remapped_index=" << crystalIndexToSend
                              << std::endl;
                }
            }

            event->set_crystal_index(crystalIndexToSend);
            event->set_energy(s.energy);
            event->set_time_pico(s.timeValue_pico);
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

            m_pendingMessages.push(std::move(msg));
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
                std::cout << "[CoincidenceClient] Start signal received, planned_start_ms="
                          << plannedStartMs << std::endl;
                return true;
            }

            if (!status.ok())
            {
                std::cerr << "[CoincidenceClient] WaitForStart RPC failed: "
                          << status.error_message() << std::endl;
            }
            else
            {
                std::cerr << "[CoincidenceClient] WaitForStart not ready: "
                          << response.message() << std::endl;
            }

            if (timeoutMs > 0)
            {
                const uint64_t elapsed = nowMs() - startTs;
                if (elapsed >= timeoutMs)
                {
                    std::cerr << "[CoincidenceClient] WaitForStart timed out after "
                              << elapsed << " ms" << std::endl;
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
            std::cout << "[CoincidenceClient] Node " << m_config.nodeId
                      << " registered successfully" << std::endl;
            m_connected = true;
            return true;
        }

        std::cerr << "[CoincidenceClient] Failed to register node: "
                  << (status.ok() ? response.message() : status.error_message())
                  << std::endl;
        return false;
    }

    void CoincidenceClient::senderLoop()
    {
        while (m_running.load())
        {
            std::unique_ptr<coincidence::SingleChunkMessage> msg;

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
                sendMessage(*msg);
            }
        }

        flushPendingMessages();
    }

    bool CoincidenceClient::sendMessage(const coincidence::SingleChunkMessage &msg)
    {
        uint32_t attempts = 0;

        while (attempts < m_config.maxReconnectAttempts && m_running.load())
        {
            grpc::ClientContext context;
            coincidence::StreamResponse response;

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

            m_connected = false;
            attempts++;
            std::cerr << "[CoincidenceClient] Send failed, attempt "
                      << attempts << "/" << m_config.maxReconnectAttempts << std::endl;

            std::this_thread::sleep_for(
                std::chrono::milliseconds(m_config.reconnectDelayMs));
        }

        return false;
    }

    void CoincidenceClient::flushPendingMessages()
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        while (!m_pendingMessages.empty())
        {
            auto &msg = m_pendingMessages.front();
            sendMessage(*msg);
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
                std::cerr << "[CoincidenceClient] Heartbeat failed: "
                          << status.error_message() << std::endl;
            }
            else
            {
                m_connected = true;
            }
        }
    }

} // namespace openpni::distributed::streaming
