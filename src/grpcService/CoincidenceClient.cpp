#include "grpcService/CoincidenceClient.hpp"
#include "core/streaming/PackedSingle.hpp"
#include "dataplane/rdma/ProtoConvert.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
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

        std::cout << "[CoincidenceClient] handshake=reg node=" << m_config.nodeId << std::endl;
        if (!registerNode())
        {
            m_running = false;
            return false;
        }

        std::cout << "[CoincidenceClient] handshake=dp node=" << m_config.nodeId << std::endl;
        if (!openRdmaDataPlane())
        {
            m_running = false;
            return false;
        }

        if (m_config.waitForStartSignal)
        {
            std::cout << "[CoincidenceClient] handshake=wait node=" << m_config.nodeId << std::endl;
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

        std::cout << "[CoincidenceClient] handshake=run node=" << m_config.nodeId << std::endl;
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
        sc.deviceName = m_config.rdmaDeviceName;
        sc.requireRoce = m_config.requireRoce;
        sc.forceInProcess = m_config.forceInProcess;
        sc.gidIndex = m_config.gidIndex;
        if (m_config.txSlotCount > 0)
        {
            sc.txSlotCount = m_config.txSlotCount;
        }
        m_rdmaSender = std::make_unique<rdma::RdmaWriteSender>(std::move(sc));

        rdma::RdmaEndpointInfo localEp{};
        if (!m_rdmaSender->prepareLocalEndpoint(&localEp))
        {
            LOG(ERROR) << "prepareLocalEndpoint failed";
            return false;
        }
        if (m_config.requireRoce && localEp.kind != rdma::DataPlaneKind::RdmaRoceV2)
        {
            LOG(ERROR) << "requireRoce but local endpoint is not RoCE";
            return false;
        }

        grpc::ClientContext context;
        coincidence::OpenDataPlaneRequest req;
        req.set_node_id(m_config.nodeId);
        if (m_config.requestedSlotCount > 0)
        {
            req.set_requested_slot_count(m_config.requestedSlotCount);
        }
        if (m_config.requestedSlotBytes > 0)
        {
            req.set_requested_slot_bytes(m_config.requestedSlotBytes);
        }
        rdma::fillProtoEndpoint(localEp, req.mutable_node_endpoint());

        coincidence::OpenDataPlaneResponse resp;
        grpc::Status status = m_stub->OpenDataPlane(&context, req, &resp);
        if (!status.ok() || !resp.success())
        {
            LOG(ERROR) << "OpenDataPlane failed: "
                       << (status.ok() ? resp.message() : status.error_message());
            return false;
        }

        const auto coinEp = rdma::fromProtoEndpoint(resp.coin_endpoint());
        if (m_config.requireRoce && coinEp.kind != rdma::DataPlaneKind::RdmaRoceV2)
        {
            LOG(ERROR) << "requireRoce but coin endpoint is InProcess";
            return false;
        }
        if (!m_rdmaSender->connect(coinEp))
        {
            LOG(ERROR) << "RDMA connect failed";
            return false;
        }
        LOG(INFO) << "RDMA dataplane connected node=" << m_config.nodeId
                  << " kind=" << static_cast<uint32_t>(m_rdmaSender->kind())
                  << " localDevice=" << localEp.deviceName
                  << " gidIndex=" << localEp.gidIndex
                  << " localQp=" << localEp.qpNum
                  << " remoteQp=" << coinEp.qpNum
                  << " slots=" << m_rdmaSender->slotCount()
                  << " stride=" << m_rdmaSender->slotStride();
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
        chunk->singles.resize(singles.size());

        if (m_config.remapLocalToGlobalChannels)
        {
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

                chunk->singles[i] = s;
                chunk->singles[i].channelIndex = static_cast<uint16_t>(globalChannel);

                if (i == 0 && !m_remapSampleLogged.exchange(true))
                {
                    LOG(INFO) << "remap sample node=" << m_config.nodeId
                              << " local_channel=" << s.channelIndex
                              << " global_channel=" << chunk->singles[i].channelIndex;
                }
            }
        }
        else
        {
            std::memcpy(chunk->singles.data(), singles.data(),
                        singles.size() * kPackedSingleSize);
        }

        {
            std::unique_lock<std::mutex> lock(m_mutex);

            if (m_paused.load(std::memory_order_acquire))
            {
                m_cv.wait(lock, [this]()
                          {
                              return !m_paused.load(std::memory_order_acquire) ||
                                     m_stopProduce.load(std::memory_order_acquire) ||
                                     !m_running.load();
                          });
            }

            if (!m_running.load() || m_stopProduce.load(std::memory_order_acquire))
            {
                return false;
            }

            if (m_pendingMessages.size() >= m_config.maxPendingChunks)
            {
                m_cv.wait(lock, [this]()
                          { return m_pendingMessages.size() < m_config.maxPendingChunks ||
                                   !m_running.load() ||
                                   m_stopProduce.load(std::memory_order_acquire); });
            }

            if (!m_running.load() || m_stopProduce.load(std::memory_order_acquire))
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

    bool CoincidenceClient::notifyProducerComplete()
    {
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait(lock, [this]()
                      { return (m_pendingMessages.empty() && !m_sendInFlight.load()) ||
                               !m_running.load(); });
        }
        m_producerComplete.store(true, std::memory_order_release);

        grpc::ClientContext context;
        coincidence::NotifyProducerCompleteRequest request;
        request.set_node_id(m_config.nodeId);
        request.set_singles_sent(m_totalSinglesSent.load());
        coincidence::NotifyProducerCompleteResponse response;
        grpc::Status status = m_stub->NotifyProducerComplete(&context, request, &response);
        if (!status.ok() || !response.success())
        {
            LOG(ERROR) << "NotifyProducerComplete failed: "
                       << (status.ok() ? response.message() : status.error_message());
            return false;
        }
        LOG(INFO) << "Producer complete node=" << m_config.nodeId
                  << " singles_sent=" << m_totalSinglesSent.load()
                  << " all_complete=" << (response.all_complete() ? "true" : "false");
        return true;
    }

    rdma::DataPlaneKind CoincidenceClient::dataPlaneKind() const
    {
        return m_rdmaSender ? m_rdmaSender->kind() : rdma::DataPlaneKind::InProcess;
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

    bool CoincidenceClient::isPaused() const
    {
        return m_paused.load(std::memory_order_acquire);
    }

    bool CoincidenceClient::stopProduceRequested() const
    {
        return m_stopProduce.load(std::memory_order_acquire);
    }

    uint64_t CoincidenceClient::lastRttMs() const
    {
        return m_lastRttMs.load(std::memory_order_acquire);
    }

    coincidence::SourceState CoincidenceClient::sourceState() const
    {
        return currentSourceState();
    }

    uint32_t CoincidenceClient::rdmaSlotCount() const
    {
        return m_rdmaSender ? m_rdmaSender->slotCount() : 0;
    }

    uint64_t CoincidenceClient::rdmaSlotsInFlight() const
    {
        return m_rdmaSender ? m_rdmaSender->slotsInFlight() : 0;
    }

    uint32_t CoincidenceClient::rdmaCreditRemaining() const
    {
        return m_rdmaSender ? m_rdmaSender->creditRemaining() : 0;
    }

    void CoincidenceClient::setTelemetryHook(std::function<WorkerTelemetry()> hook)
    {
        std::lock_guard<std::mutex> lock(m_telemetryMutex);
        m_telemetryHook = std::move(hook);
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
                LOG(INFO) << "Start signal received (wall-clock wait skipped; PET time is in singles)";
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
                    // Wake producers blocked on maxPendingChunks (same CV as empty-wait).
                    m_cv.notify_all();
                }
            }

            if (msg)
            {
                m_sendInFlight.store(true, std::memory_order_release);
                sendChunk(*msg);
                m_sendInFlight.store(false, std::memory_order_release);
                m_cv.notify_all();
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

        const size_t maxPerSlot = rdma::maxSinglesPerSlot(m_rdmaSender->slotStride());
        if (maxPerSlot == 0 || chunk.singles.empty())
        {
            return chunk.singles.empty();
        }

        uint32_t offset = 0;
        bool first = true;
        while (offset < chunk.singlesCount)
        {
            const uint32_t count = static_cast<uint32_t>(std::min<size_t>(
                maxPerSlot, static_cast<size_t>(chunk.singlesCount - offset)));

            rdma::TxSlotLease lease{};
            if (!m_rdmaSender->acquireTxSlot(&lease) || !lease.payload)
            {
                const bool ok = m_rdmaSender->sendPackedSingles(
                    chunk.chunkId,
                    chunk.computerClockMs,
                    chunk.durationMs,
                    chunk.singles.data() + offset,
                    chunk.singlesCount - offset);
                m_connected = ok;
                return ok;
            }

            std::memcpy(lease.payload, chunk.singles.data() + offset,
                        static_cast<size_t>(count) * kPackedSingleSize);

            rdma::SlotHeader hdr{};
            rdma::clearSlotHeader(&hdr);
            hdr.chunkId = chunk.chunkId;
            hdr.computerClockMs = chunk.computerClockMs;
            hdr.durationMs = chunk.durationMs;
            hdr.flags = 0;
            if (first)
            {
                hdr.flags = static_cast<uint16_t>(hdr.flags | rdma::kSlotFlagSof);
                first = false;
            }
            if (offset + count >= chunk.singlesCount)
            {
                hdr.flags = static_cast<uint16_t>(hdr.flags | rdma::kSlotFlagEof);
            }
            if (count < chunk.singlesCount)
            {
                hdr.flags = static_cast<uint16_t>(hdr.flags | rdma::kSlotFlagPartial);
            }

            if (!m_rdmaSender->commitTxSlot(lease, hdr, count))
            {
                m_connected = false;
                return false;
            }
            offset += count;
        }
        m_connected = true;
        return true;
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
                std::chrono::milliseconds(m_config.heartbeatIntervalMs == 0
                                              ? 1000u
                                              : m_config.heartbeatIntervalMs));

            if (!m_running.load())
            {
                break;
            }

            grpc::ClientContext context;
            coincidence::HeartbeatRequest request;
            request.set_node_id(m_config.nodeId);
            const uint64_t sendTs = nowMs();
            request.set_timestamp_ms(sendTs);
            request.set_singles_sent(m_totalSinglesSent.load());
            request.set_producer_complete(m_producerComplete.load(std::memory_order_acquire));
            fillHeartbeatTelemetry(&request);

            coincidence::HeartbeatResponse response;
            grpc::Status status = m_stub->Heartbeat(&context, request, &response);

            if (!status.ok())
            {
                m_connected = false;
                LOG(WARNING) << "Heartbeat failed: " << status.error_message();
                continue;
            }

            m_connected = true;
            if (response.echo_timestamp_ms() != 0)
            {
                const uint64_t now = nowMs();
                if (now >= response.echo_timestamp_ms())
                {
                    m_lastRttMs.store(now - response.echo_timestamp_ms(), std::memory_order_release);
                }
            }
            applyProducerCommand(response.command());
        }
    }

    coincidence::SourceState CoincidenceClient::currentSourceState() const
    {
        if (m_producerComplete.load(std::memory_order_acquire))
        {
            return coincidence::SOURCE_STATE_COMPLETE;
        }
        if (m_paused.load(std::memory_order_acquire))
        {
            return coincidence::SOURCE_STATE_PAUSED;
        }
        if (m_running.load(std::memory_order_acquire))
        {
            return coincidence::SOURCE_STATE_RUNNING;
        }
        return coincidence::SOURCE_STATE_IDLE;
    }

    void CoincidenceClient::fillHeartbeatTelemetry(coincidence::HeartbeatRequest *request)
    {
        request->set_source_state(currentSourceState());
        request->set_chunks_pending(getPendingMessageCount());
        request->set_chunks_pending_cap(m_config.maxPendingChunks);
        request->set_last_rtt_ms(m_lastRttMs.load(std::memory_order_acquire));
        if (m_rdmaSender)
        {
            request->set_rdma_slot_count(m_rdmaSender->slotCount());
            request->set_rdma_slots_in_flight(m_rdmaSender->slotsInFlight());
            request->set_rdma_credit_remaining(m_rdmaSender->creditRemaining());
        }
        WorkerTelemetry tel;
        {
            std::lock_guard<std::mutex> lock(m_telemetryMutex);
            if (m_telemetryHook)
            {
                tel = m_telemetryHook();
            }
        }
        request->set_r2s_singles_out(tel.r2sSinglesOut);
        request->set_r2s_lease_used(tel.r2sLeaseUsed);
        request->set_r2s_lease_cap(tel.r2sLeaseCap);
        request->set_acq_packets_total(tel.acqPacketsTotal);
        request->set_acq_bytes_total(tel.acqBytesTotal);
        request->set_acq_running(tel.acqRunning);
    }

    void CoincidenceClient::applyProducerCommand(coincidence::ProducerCommand command)
    {
        switch (command)
        {
        case coincidence::CMD_PAUSE_PRODUCE:
            if (!m_paused.exchange(true))
            {
                LOG(INFO) << "Producer paused node=" << m_config.nodeId;
            }
            m_cv.notify_all();
            break;
        case coincidence::CMD_START_PRODUCE:
            if (m_paused.exchange(false))
            {
                LOG(INFO) << "Producer resumed node=" << m_config.nodeId;
            }
            m_cv.notify_all();
            break;
        case coincidence::CMD_STOP_PRODUCE:
        case coincidence::CMD_DRAIN:
            if (m_producerComplete.load(std::memory_order_acquire))
            {
                break;
            }
            if (m_stopProduce.exchange(true))
            {
                break;
            }
            LOG(INFO) << "Producer stop requested node=" << m_config.nodeId;
            m_paused.store(false, std::memory_order_release);
            m_cv.notify_all();
            (void)notifyProducerComplete();
            break;
        case coincidence::CMD_NONE:
        default:
            break;
        }
    }

} // namespace openpni::distributed::streaming
