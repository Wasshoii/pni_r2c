#include "grpcService/CoincidenceServiceImpl.hpp"
#include "core/streaming/PackedSingle.hpp"
#include "dataplane/rdma/ProtoConvert.hpp"
#include "dataplane/rdma/RdmaContext.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <cstring>
#include <iostream>
#include <utility>
#include <vector>
#include <glog/logging.h>

namespace openpni::distributed::streaming
{
    namespace rdma = openpni::distributed::dataplane::rdma;

    CoincidenceServiceImpl::CoincidenceServiceImpl(StreamingTimeAligner &aligner)
        : CoincidenceServiceImpl(aligner, OrchestrationConfig{})
    {
    }

    CoincidenceServiceImpl::CoincidenceServiceImpl(
        StreamingTimeAligner &aligner,
        OrchestrationConfig orchestration)
        : m_aligner(aligner),
          m_orchestration(std::move(orchestration))
    {
        if (m_orchestration.expectedNodeCount == 0)
        {
            m_orchestration.expectedNodeCount = static_cast<uint32_t>(aligner.getNodeCount());
        }

        for (size_t i = 0; i < aligner.getNodeCount(); ++i)
        {
            auto info = std::make_shared<NodeConnectionInfo>();
            info->nodeId = static_cast<uint32_t>(i);
            m_nodeInfos[static_cast<uint32_t>(i)] = info;
        }

        startRdmaIngest();
    }

    void CoincidenceServiceImpl::startRdmaIngest()
    {
        rdma::RdmaRecvServer::Config cfg;
        cfg.requireRoce = m_orchestration.requireRoce;
        cfg.forceInProcess = m_orchestration.forceInProcess;
        cfg.deviceName = m_orchestration.deviceName;
        cfg.gidIndex = m_orchestration.gidIndex;
        if (m_orchestration.slotCount > 0)
        {
            cfg.slotCount = m_orchestration.slotCount;
        }
        if (m_orchestration.slotBytes > 0)
        {
            cfg.slotBytes = m_orchestration.slotBytes;
        }
        LOG(INFO) << "RDMA recv server requireRoce=" << (cfg.requireRoce ? "true" : "false")
                  << " forceInProcess=" << (cfg.forceInProcess ? "true" : "false")
                  << " device=" << (cfg.deviceName.empty() ? "(auto)" : cfg.deviceName)
                  << " gidIndex=" << cfg.gidIndex
                  << " slots=" << cfg.slotCount
                  << " slotBytes=" << cfg.slotBytes;
        m_rdmaServer = std::make_unique<rdma::RdmaRecvServer>(cfg);
        m_rdmaServer->setIngest([this](const rdma::SlotChunkView &view) -> bool
                                {
            std::string err;
            if (!ingestRdmaSlot(view, &err))
            {
                LOG(ERROR) << "RDMA ingest failed: " << err;
                return false;
            }
            return true; });
        m_rdmaServer->start();
    }

    bool CoincidenceServiceImpl::ingestPackedSinglesChunk(
        uint32_t nodeId,
        uint64_t chunkId,
        uint64_t computerClockMs,
        uint32_t durationMs,
        const void *singlesPacked,
        uint32_t singlesCount,
        std::string *errorMessage)
    {
        if (m_orchestration.rejectStreamBeforeStart &&
            !m_startSignalIssued.load(std::memory_order_acquire))
        {
            if (errorMessage)
            {
                *errorMessage = "Acquisition not started yet";
            }
            return false;
        }

        auto *buffer = m_aligner.getNodeBuffer(static_cast<uint16_t>(nodeId));
        if (!buffer)
        {
            if (errorMessage)
            {
                *errorMessage = "Invalid node ID: " + std::to_string(nodeId);
            }
            return false;
        }

        if (!singlesPacked || singlesCount == 0)
        {
            return true;
        }

        TimestampedSingleChunk chunk;
        chunk.nodeId = static_cast<uint16_t>(nodeId);
        chunk.chunkId = chunkId;
        chunk.computerClock_ms = computerClockMs;
        chunk.duration_ms = durationMs;
        chunk.singles.resize(singlesCount);
        unpackBinaryToSingles(singlesPacked, singlesCount, chunk.singles.data());

        if (!buffer->push(std::move(chunk)))
        {
            if (errorMessage)
            {
                *errorMessage = "Buffer full or closed for node " + std::to_string(nodeId);
            }
            return false;
        }

        updateNodeStats(nodeId, singlesCount);
        return true;
    }

    bool CoincidenceServiceImpl::ingestRdmaSlot(const rdma::SlotChunkView &view,
                                                std::string *errorMessage)
    {
        const bool sof = (view.flags & rdma::kSlotFlagSof) != 0;
        const bool eof = (view.flags & rdma::kSlotFlagEof) != 0;
        const bool partial = (view.flags & rdma::kSlotFlagPartial) != 0;

        if (!partial || (sof && eof))
        {
            std::lock_guard<std::mutex> lock(m_partialMutex);
            m_partialByNode.erase(view.nodeId);
            return ingestPackedSinglesChunk(
                view.nodeId, view.chunkId, view.computerClockMs, view.durationMs,
                view.singlesPacked, view.singlesCount, errorMessage);
        }

        std::vector<uint8_t> assembled;
        uint64_t chunkId = view.chunkId;
        uint64_t clockMs = view.computerClockMs;
        uint32_t durationMs = view.durationMs;
        uint32_t totalSingles = 0;
        bool complete = false;

        {
            std::lock_guard<std::mutex> lock(m_partialMutex);
            auto &part = m_partialByNode[view.nodeId];
            if (sof)
            {
                part = PartialChunk{};
                part.chunkId = view.chunkId;
                part.computerClockMs = view.computerClockMs;
                part.durationMs = view.durationMs;
                part.open = true;
            }
            else if (!part.open || part.chunkId != view.chunkId)
            {
                if (errorMessage)
                {
                    *errorMessage = "partial slot without SOF or chunkId mismatch";
                }
                return false;
            }

            const size_t nbytes = static_cast<size_t>(view.singlesCount) * kPackedSingleSize;
            const auto *src = static_cast<const uint8_t *>(view.singlesPacked);
            part.packed.insert(part.packed.end(), src, src + nbytes);

            if (eof)
            {
                assembled.swap(part.packed);
                chunkId = part.chunkId;
                clockMs = part.computerClockMs;
                durationMs = part.durationMs;
                totalSingles = static_cast<uint32_t>(assembled.size() / kPackedSingleSize);
                part.open = false;
                complete = true;
            }
        }

        if (!complete)
        {
            return true;
        }
        return ingestPackedSinglesChunk(
            view.nodeId, chunkId, clockMs, durationMs,
            assembled.data(), totalSingles, errorMessage);
    }

    grpc::Status CoincidenceServiceImpl::StreamSingles(
        grpc::ServerContext *context,
        grpc::ServerReader<coincidence::SingleChunkMessage> *reader,
        coincidence::StreamResponse *response)
    {
        (void)context;
        (void)reader;
        (void)response;
        return grpc::Status(
            grpc::StatusCode::UNIMPLEMENTED,
            "StreamSingles disabled; use OpenDataPlane + RDMA data plane");
    }

    grpc::Status CoincidenceServiceImpl::OpenDataPlane(
        grpc::ServerContext *context,
        const coincidence::OpenDataPlaneRequest *request,
        coincidence::OpenDataPlaneResponse *response)
    {
        (void)context;
        const uint32_t nodeId = request->node_id();
        if (nodeId >= m_aligner.getNodeCount())
        {
            response->set_success(false);
            response->set_message("Node ID out of range");
            return grpc::Status::OK;
        }

        {
            std::lock_guard<std::mutex> lock(m_orchestrationMutex);
            if (m_registeredNodes.find(nodeId) == m_registeredNodes.end())
            {
                response->set_success(false);
                response->set_message("Node not registered; call RegisterNode before OpenDataPlane");
                return grpc::Status::OK;
            }
        }

        if (!m_rdmaServer)
        {
            response->set_success(false);
            response->set_message("RDMA server not initialized");
            return grpc::Status::OK;
        }

        if (request->requested_slot_count() != 0 || request->requested_slot_bytes() != 0)
        {
            LOG(INFO) << "OpenDataPlane node=" << nodeId
                      << " requested_slot_count=" << request->requested_slot_count()
                      << " requested_slot_bytes=" << request->requested_slot_bytes()
                      << " (server decides actual sizes)";
        }

        auto session = m_rdmaServer->ensureSession(nodeId);
        if (!session)
        {
            response->set_success(false);
            response->set_message("Failed to prepare RDMA receive session");
            return grpc::Status::OK;
        }

        if (m_orchestration.requireRoce && session->kind() != rdma::DataPlaneKind::RdmaRoceV2)
        {
            response->set_success(false);
            response->set_message("requireRoce but receive session is not RoCE");
            return grpc::Status::OK;
        }

        if (request->has_node_endpoint())
        {
            const auto remote = rdma::fromProtoEndpoint(request->node_endpoint());
            if (m_orchestration.requireRoce && remote.kind != rdma::DataPlaneKind::RdmaRoceV2)
            {
                response->set_success(false);
                response->set_message("requireRoce but node endpoint is InProcess");
                return grpc::Status::OK;
            }
            if (session->kind() == rdma::DataPlaneKind::RdmaRoceV2)
            {
                if (!session->acceptRemote(remote))
                {
                    response->set_success(false);
                    response->set_message("Failed to accept remote RDMA endpoint");
                    return grpc::Status::OK;
                }
            }
        }

        const auto local = session->localEndpoint();
        const auto protoKind = rdma::toProto(local.kind);
        {
            std::unique_lock<std::shared_mutex> nlock(m_nodeInfosMutex);
            auto &info = m_nodeInfos[nodeId];
            if (!info)
            {
                info = std::make_shared<NodeConnectionInfo>();
                info->nodeId = nodeId;
            }
            info->dataplaneOpen = true;
            info->dataPlaneKind.store(static_cast<uint32_t>(protoKind));
        }
        {
            std::lock_guard<std::mutex> lock(m_orchestrationMutex);
            m_dataplaneOpenNodes.insert(nodeId);
            m_observedDataPlaneKind.store(static_cast<uint32_t>(protoKind));
        }
        m_orchestrationCv.notify_all();

        response->set_success(true);
        response->set_message("Data plane ready");
        response->set_data_plane_kind(protoKind);
        rdma::fillProtoEndpoint(local, response->mutable_coin_endpoint());
        LOG(INFO) << "OpenDataPlane node=" << nodeId
                  << " kind=" << static_cast<uint32_t>(local.kind)
                  << " device=" << local.deviceName
                  << " gidIndex=" << local.gidIndex
                  << " qp=" << local.qpNum
                  << " slots=" << local.slotCount
                  << " stride=" << local.slotStride;
        maybeAutoStartAfterDataplane();
        return grpc::Status::OK;
    }

    grpc::Status CoincidenceServiceImpl::WaitForStart(
        grpc::ServerContext *context,
        const coincidence::WaitForStartRequest *request,
        coincidence::WaitForStartResponse *response)
    {
        (void)context;

        const uint32_t nodeId = request->node_id();
        if (nodeId >= m_aligner.getNodeCount())
        {
            return grpc::Status(
                grpc::StatusCode::INVALID_ARGUMENT,
                "Node ID out of range. Expected 0-" +
                    std::to_string(m_aligner.getNodeCount() - 1));
        }

        uint32_t timeoutMs = request->timeout_ms();
        if (timeoutMs == 0)
        {
            timeoutMs = m_orchestration.waitForStartDefaultTimeoutMs;
        }

        std::unique_lock<std::mutex> lock(m_orchestrationMutex);

        if (m_registeredNodes.find(nodeId) == m_registeredNodes.end())
        {
            fillOrchestrationStatusUnlocked(response);
            response->set_success(false);
            response->set_start_signal_issued(false);
            response->set_start_time_ms(0);
            response->set_message("Node not registered yet");
            return grpc::Status::OK;
        }

        const auto readyPredicate = [this]()
        {
            return m_startSignalIssued.load(std::memory_order_acquire) ||
                   m_serverStopping.load(std::memory_order_acquire);
        };

        bool ready = readyPredicate();
        if (!ready)
        {
            if (timeoutMs == 0)
            {
                m_orchestrationCv.wait(lock, readyPredicate);
                ready = readyPredicate();
            }
            else
            {
                ready = m_orchestrationCv.wait_for(
                    lock,
                    std::chrono::milliseconds(timeoutMs),
                    readyPredicate);
            }
        }

        fillOrchestrationStatusUnlocked(response);
        if (m_serverStopping.load(std::memory_order_acquire))
        {
            response->set_success(false);
            response->set_start_signal_issued(false);
            response->set_start_time_ms(0);
            response->set_message("Server stopping");
            return grpc::Status::OK;
        }

        if (!ready || !m_startSignalIssued.load(std::memory_order_acquire))
        {
            response->set_success(false);
            response->set_start_signal_issued(false);
            response->set_start_time_ms(0);
            response->set_message("Timed out waiting for start signal");
            return grpc::Status::OK;
        }

        response->set_success(true);
        response->set_start_signal_issued(true);
        response->set_start_time_ms(m_plannedStartTimeMs.load(std::memory_order_acquire));
        response->set_message("Start signal issued");
        return grpc::Status::OK;
    }

    grpc::Status CoincidenceServiceImpl::GetStatus(
        grpc::ServerContext *context,
        const coincidence::StatusRequest *request,
        coincidence::StatusResponse *response)
    {
        (void)context;

        const auto &stats = m_aligner.getStatistics();

        response->set_total_singles_received(stats.totalSinglesReceived.load());
        response->set_total_singles_processed(stats.totalSinglesProcessed.load());
        response->set_total_prompt_pairs(stats.totalPromptPairs.load());
        response->set_total_delay_pairs(stats.totalDelayPairs.load());
        response->set_alignment_windows_processed(stats.chunksProcessed.load());
        response->set_avg_processing_time_ms(stats.avgProcessingTime_ms.load());
        response->set_current_time_boundary_pico(stats.currentTimeBoundary_pico.load());
        response->set_is_running(m_aligner.isRunning());

        {
            std::lock_guard<std::mutex> lock(m_orchestrationMutex);
            fillOrchestrationStatusUnlocked(response);
        }

        if (request->include_node_stats())
        {
            const auto now = std::chrono::steady_clock::now();
            const uint32_t timeoutMs = m_orchestration.heartbeatTimeoutMs == 0
                                           ? 3000u
                                           : m_orchestration.heartbeatTimeoutMs;
            std::shared_lock<std::shared_mutex> lock(m_nodeInfosMutex);
            for (const auto &[nodeId, info] : m_nodeInfos)
            {
                auto *nodeStatus = response->add_node_stats();
                nodeStatus->set_node_id(nodeId);
                nodeStatus->set_chunks_received(info->chunksReceived.load());
                nodeStatus->set_singles_received(info->singlesReceived.load());

                auto *buffer = m_aligner.getNodeBuffer(static_cast<uint16_t>(nodeId));
                nodeStatus->set_buffer_size(buffer ? buffer->size() : 0);
                nodeStatus->set_dataplane_open(info->dataplaneOpen.load());
                nodeStatus->set_data_plane_kind(
                    static_cast<coincidence::DataPlaneKind>(info->dataPlaneKind.load()));
                nodeStatus->set_singles_sent_heartbeat(info->singlesSentHeartbeat.load());
                nodeStatus->set_producer_complete(info->producerComplete.load());
                nodeStatus->set_source_state(info->sourceState);
                nodeStatus->set_chunks_pending(info->chunksPending);
                nodeStatus->set_chunks_pending_cap(info->chunksPendingCap);
                nodeStatus->set_rdma_slots_in_flight(info->rdmaSlotsInFlight);
                nodeStatus->set_rdma_slot_count(info->rdmaSlotCount);
                nodeStatus->set_rdma_credit_remaining(info->rdmaCreditRemaining);
                nodeStatus->set_r2s_singles_out(info->r2sSinglesOut);
                nodeStatus->set_r2s_lease_used(info->r2sLeaseUsed);
                nodeStatus->set_r2s_lease_cap(info->r2sLeaseCap);
                nodeStatus->set_acq_packets_total(info->acqPacketsTotal);
                nodeStatus->set_acq_bytes_total(info->acqBytesTotal);
                nodeStatus->set_acq_running(info->acqRunning);
                nodeStatus->set_last_rtt_ms(info->lastRttMs);

                uint64_t ageMs = 0;
                bool live = false;
                if (info->lastHeartbeat.time_since_epoch().count() != 0)
                {
                    ageMs = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            now - info->lastHeartbeat)
                            .count());
                    live = ageMs <= timeoutMs;
                }
                nodeStatus->set_last_heartbeat_age_ms(ageMs);
                nodeStatus->set_connected(live);
            }
        }

        return grpc::Status::OK;
    }

    grpc::Status CoincidenceServiceImpl::Control(
        grpc::ServerContext *context,
        const coincidence::ControlRequest *request,
        coincidence::ControlResponse *response)
    {
        (void)context;

        switch (request->command())
        {
        case coincidence::ControlRequest::START:
        {
            const uint64_t startTimeMs = nowMs() + m_orchestration.startLeadTimeMs;
            bool issued = false;

            {
                std::lock_guard<std::mutex> lock(m_orchestrationMutex);
                issued = issueStartSignalLocked(startTimeMs, "manual START command");
            }

            if (!issued)
            {
                response->set_success(false);
                response->set_message("Start signal already issued");
                return grpc::Status::OK;
            }

            if (!m_aligner.isRunning())
            {
                m_aligner.start();
            }
            setPendingProducerCommand(coincidence::CMD_START_PRODUCE);
            m_orchestrationCv.notify_all();

            response->set_success(true);
            response->set_message("Aligner started and start signal issued");
            break;
        }

        case coincidence::ControlRequest::STOP:
        {
            setPendingProducerCommand(coincidence::CMD_STOP_PRODUCE);
            m_orchestrationCv.notify_all();
            response->set_success(true);
            response->set_message("STOP_PRODUCE queued for workers; aligner drains when all complete");
            break;
        }

        case coincidence::ControlRequest::PAUSE:
            setPendingProducerCommand(coincidence::CMD_PAUSE_PRODUCE);
            response->set_success(true);
            response->set_message("PAUSE_PRODUCE queued for workers");
            break;

        case coincidence::ControlRequest::RESUME:
            setPendingProducerCommand(coincidence::CMD_START_PRODUCE);
            response->set_success(true);
            response->set_message("START_PRODUCE queued for workers");
            break;

        case coincidence::ControlRequest::FLUSH:
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

        case coincidence::ControlRequest::DRAIN:
            setPendingProducerCommand(coincidence::CMD_STOP_PRODUCE);
            if (m_aligner.isRunning())
            {
                m_aligner.stop(true);
            }
            m_allProducersComplete.store(true, std::memory_order_release);
            m_orchestrationCv.notify_all();
            response->set_success(true);
            response->set_message("STOP_PRODUCE queued and aligner drained");
            break;

        default:
            response->set_success(false);
            response->set_message("Unknown command");
            break;
        }

        return grpc::Status::OK;
    }

    grpc::Status CoincidenceServiceImpl::UpdateConfig(
        grpc::ServerContext *context,
        const coincidence::ConfigUpdateRequest *request,
        coincidence::ConfigUpdateResponse *response)
    {
        (void)context;
        (void)request;

        response->set_success(false);
        response->set_message("Dynamic config update not supported. Please stop the service and reconfigure.");
        return grpc::Status::OK;
    }

    grpc::Status CoincidenceServiceImpl::RegisterNode(
        grpc::ServerContext *context,
        const coincidence::RegisterNodeRequest *request,
        coincidence::RegisterNodeResponse *response)
    {
        (void)context;

        const uint32_t nodeId = request->node_id();
        if (nodeId >= m_aligner.getNodeCount())
        {
            response->set_success(false);
            response->set_message("Node ID out of range. Expected 0-" +
                                  std::to_string(m_aligner.getNodeCount() - 1));
            return grpc::Status::OK;
        }

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

        {
            std::lock_guard<std::mutex> lock(m_orchestrationMutex);
            m_registeredNodes.insert(nodeId);
        }
        m_orchestrationCv.notify_all();

        {
            std::lock_guard<std::mutex> lock(m_orchestrationMutex);
            fillOrchestrationStatusUnlocked(response);
        }

        response->set_success(true);
        response->set_assigned_node_id(nodeId);
        response->set_message("Node " + std::to_string(nodeId) + " registered successfully");
        response->set_data_plane_kind(
            rdma::RdmaDevice::hasVerbsDevice()
                ? coincidence::DATA_PLANE_RDMA_ROCE_V2
                : coincidence::DATA_PLANE_INPROCESS);

        LOG(INFO) << "Node " << nodeId
                  << " registered from " << request->node_address()
                  << " (detector: " << request->detector_type() << ")";

        return grpc::Status::OK;
    }

    grpc::Status CoincidenceServiceImpl::Heartbeat(
        grpc::ServerContext *context,
        const coincidence::HeartbeatRequest *request,
        coincidence::HeartbeatResponse *response)
    {
        (void)context;

        const uint32_t nodeId = request->node_id();

        {
            std::unique_lock<std::shared_mutex> lock(m_nodeInfosMutex);
            auto it = m_nodeInfos.find(nodeId);
            if (it != m_nodeInfos.end())
            {
                it->second->lastHeartbeat = std::chrono::steady_clock::now();
                it->second->connected = true;
                it->second->singlesSentHeartbeat.store(request->singles_sent());
                it->second->sourceState = request->source_state();
                it->second->chunksPending = request->chunks_pending();
                it->second->chunksPendingCap = request->chunks_pending_cap();
                it->second->rdmaSlotsInFlight = request->rdma_slots_in_flight();
                it->second->rdmaSlotCount = request->rdma_slot_count();
                it->second->rdmaCreditRemaining = request->rdma_credit_remaining();
                it->second->r2sSinglesOut = request->r2s_singles_out();
                it->second->r2sLeaseUsed = request->r2s_lease_used();
                it->second->r2sLeaseCap = request->r2s_lease_cap();
                it->second->acqPacketsTotal = request->acq_packets_total();
                it->second->acqBytesTotal = request->acq_bytes_total();
                it->second->acqRunning = request->acq_running();
                it->second->lastRttMs = request->last_rtt_ms();
            }
        }

        if (request->producer_complete())
        {
            markProducerComplete(nodeId, request->singles_sent());
        }

        response->set_acknowledged(true);
        response->set_server_timestamp_ms(nowMs());
        response->set_echo_timestamp_ms(request->timestamp_ms());
        response->set_command(pendingProducerCommand());
        response->set_start_signal_issued(m_startSignalIssued.load(std::memory_order_acquire));
        return grpc::Status::OK;
    }

    grpc::Status CoincidenceServiceImpl::NotifyProducerComplete(
        grpc::ServerContext *context,
        const coincidence::NotifyProducerCompleteRequest *request,
        coincidence::NotifyProducerCompleteResponse *response)
    {
        (void)context;
        const uint32_t nodeId = request->node_id();
        if (nodeId >= m_aligner.getNodeCount())
        {
            response->set_success(false);
            response->set_message("Node ID out of range");
            return grpc::Status::OK;
        }

        markProducerComplete(nodeId, request->singles_sent());

        std::lock_guard<std::mutex> lock(m_orchestrationMutex);
        response->set_success(true);
        response->set_producers_complete_count(static_cast<uint32_t>(m_completeNodes.size()));
        response->set_all_complete(m_allProducersComplete.load(std::memory_order_acquire));
        response->set_message(response->all_complete() ? "all producers complete" : "producer complete recorded");
        return grpc::Status::OK;
    }

    bool CoincidenceServiceImpl::waitForAllNodes(uint32_t timeoutMs) const
    {
        std::unique_lock<std::mutex> lock(m_orchestrationMutex);
        const auto readyPredicate = [this]()
        {
            return m_registeredNodes.size() >= m_orchestration.expectedNodeCount ||
                   m_serverStopping.load(std::memory_order_acquire);
        };

        if (readyPredicate())
        {
            return m_registeredNodes.size() >= m_orchestration.expectedNodeCount;
        }

        if (timeoutMs == 0)
        {
            m_orchestrationCv.wait(lock, readyPredicate);
            return m_registeredNodes.size() >= m_orchestration.expectedNodeCount;
        }

        const bool ready = m_orchestrationCv.wait_for(
            lock,
            std::chrono::milliseconds(timeoutMs),
            readyPredicate);

        return ready && (m_registeredNodes.size() >= m_orchestration.expectedNodeCount);
    }

    bool CoincidenceServiceImpl::waitForStartSignal(uint32_t timeoutMs) const
    {
        std::unique_lock<std::mutex> lock(m_orchestrationMutex);
        const auto readyPredicate = [this]()
        {
            return m_startSignalIssued.load(std::memory_order_acquire) ||
                   m_serverStopping.load(std::memory_order_acquire);
        };

        if (readyPredicate())
        {
            return m_startSignalIssued.load(std::memory_order_acquire);
        }

        if (timeoutMs == 0)
        {
            m_orchestrationCv.wait(lock, readyPredicate);
            return m_startSignalIssued.load(std::memory_order_acquire);
        }

        const bool ready = m_orchestrationCv.wait_for(
            lock,
            std::chrono::milliseconds(timeoutMs),
            readyPredicate);

        return ready && m_startSignalIssued.load(std::memory_order_acquire);
    }

    uint32_t CoincidenceServiceImpl::expectedNodeCount() const
    {
        std::lock_guard<std::mutex> lock(m_orchestrationMutex);
        return m_orchestration.expectedNodeCount;
    }

    uint32_t CoincidenceServiceImpl::connectedNodeCount() const
    {
        std::lock_guard<std::mutex> lock(m_orchestrationMutex);
        return static_cast<uint32_t>(m_registeredNodes.size());
    }

    uint32_t CoincidenceServiceImpl::dataplaneOpenCount() const
    {
        std::lock_guard<std::mutex> lock(m_orchestrationMutex);
        return static_cast<uint32_t>(m_dataplaneOpenNodes.size());
    }

    uint32_t CoincidenceServiceImpl::producersCompleteCount() const
    {
        std::lock_guard<std::mutex> lock(m_orchestrationMutex);
        return static_cast<uint32_t>(m_completeNodes.size());
    }

    bool CoincidenceServiceImpl::startSignalIssued() const
    {
        return m_startSignalIssued.load(std::memory_order_acquire);
    }

    uint64_t CoincidenceServiceImpl::plannedStartTimeMs() const
    {
        return m_plannedStartTimeMs.load(std::memory_order_acquire);
    }

    bool CoincidenceServiceImpl::allProducersComplete() const
    {
        return m_allProducersComplete.load(std::memory_order_acquire);
    }

    coincidence::DataPlaneKind CoincidenceServiceImpl::dataPlaneKind() const
    {
        return static_cast<coincidence::DataPlaneKind>(
            m_observedDataPlaneKind.load(std::memory_order_acquire));
    }

    void CoincidenceServiceImpl::notifyServerStopping()
    {
        m_serverStopping.store(true, std::memory_order_release);
        m_orchestrationCv.notify_all();
        if (m_rdmaServer)
        {
            m_rdmaServer->stop();
        }
    }

    void CoincidenceServiceImpl::clearServerStoppingState()
    {
        m_serverStopping.store(false, std::memory_order_release);
    }

    uint64_t CoincidenceServiceImpl::nowMs()
    {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
    }

    void CoincidenceServiceImpl::fillOrchestrationStatusUnlocked(coincidence::StatusResponse *response) const
    {
        response->set_expected_node_count(m_orchestration.expectedNodeCount);
        response->set_connected_node_count(static_cast<uint32_t>(m_registeredNodes.size()));
        response->set_start_signal_issued(m_startSignalIssued.load(std::memory_order_acquire));
        response->set_data_plane_kind(observedDataPlaneKindUnlocked());
        response->set_dataplane_open_count(static_cast<uint32_t>(m_dataplaneOpenNodes.size()));
        response->set_producers_complete_count(static_cast<uint32_t>(m_completeNodes.size()));
        response->set_all_producers_complete(m_allProducersComplete.load(std::memory_order_acquire));
    }

    void CoincidenceServiceImpl::fillOrchestrationStatusUnlocked(coincidence::RegisterNodeResponse *response) const
    {
        response->set_expected_node_count(m_orchestration.expectedNodeCount);
        response->set_connected_node_count(static_cast<uint32_t>(m_registeredNodes.size()));
        response->set_start_signal_issued(m_startSignalIssued.load(std::memory_order_acquire));
        response->set_planned_start_time_ms(m_plannedStartTimeMs.load(std::memory_order_acquire));
    }

    void CoincidenceServiceImpl::fillOrchestrationStatusUnlocked(coincidence::WaitForStartResponse *response) const
    {
        response->set_expected_node_count(m_orchestration.expectedNodeCount);
        response->set_connected_node_count(static_cast<uint32_t>(m_registeredNodes.size()));
    }

    bool CoincidenceServiceImpl::issueStartSignalLocked(uint64_t startTimeMs, const std::string &reason)
    {
        if (m_startSignalIssued.load(std::memory_order_acquire))
        {
            return false;
        }

        m_startSignalIssued.store(true, std::memory_order_release);
        m_plannedStartTimeMs.store(startTimeMs, std::memory_order_release);

        LOG(INFO) << "Start signal issued (reason=" << reason
                  << ", start_time_ms=" << startTimeMs
                  << ", registered=" << m_registeredNodes.size()
                  << " dataplane_open=" << m_dataplaneOpenNodes.size()
                  << "/" << m_orchestration.expectedNodeCount << ")";
        return true;
    }

    coincidence::DataPlaneKind CoincidenceServiceImpl::observedDataPlaneKindUnlocked() const
    {
        return static_cast<coincidence::DataPlaneKind>(
            m_observedDataPlaneKind.load(std::memory_order_acquire));
    }

    coincidence::ProducerCommand CoincidenceServiceImpl::pendingProducerCommand() const
    {
        return static_cast<coincidence::ProducerCommand>(
            m_pendingProducerCommand.load(std::memory_order_acquire));
    }

    void CoincidenceServiceImpl::setPendingProducerCommand(coincidence::ProducerCommand command)
    {
        m_pendingProducerCommand.store(static_cast<uint32_t>(command), std::memory_order_release);
        LOG(INFO) << "Pending producer command=" << static_cast<uint32_t>(command);
    }

    void CoincidenceServiceImpl::maybeAutoStartAfterDataplane()
    {
        bool shouldStart = false;
        uint64_t startTimeMs = 0;

        {
            std::lock_guard<std::mutex> lock(m_orchestrationMutex);
            if (m_orchestration.autoStartWhenAllRegistered &&
                m_registeredNodes.size() >= m_orchestration.expectedNodeCount &&
                m_dataplaneOpenNodes.size() >= m_orchestration.expectedNodeCount)
            {
                startTimeMs = 0;
                if (m_orchestration.startLeadTimeMs > 0)
                {
                    startTimeMs = nowMs() + m_orchestration.startLeadTimeMs;
                }
                shouldStart = issueStartSignalLocked(startTimeMs, "all dataplanes open");
            }
        }

        if (!shouldStart)
        {
            return;
        }

        if (!m_aligner.isRunning())
        {
            m_aligner.start();
        }
        m_orchestrationCv.notify_all();
    }

    void CoincidenceServiceImpl::markProducerComplete(uint32_t nodeId, uint64_t singlesSent)
    {
        {
            std::unique_lock<std::shared_mutex> nlock(m_nodeInfosMutex);
            auto it = m_nodeInfos.find(nodeId);
            if (it != m_nodeInfos.end())
            {
                it->second->producerComplete = true;
                it->second->singlesSentHeartbeat.store(singlesSent);
            }
        }
        {
            std::lock_guard<std::mutex> lock(m_orchestrationMutex);
            m_completeNodes.insert(nodeId);
        }
        m_orchestrationCv.notify_all();
        drainAlignerIfAllComplete();
    }

    void CoincidenceServiceImpl::drainAlignerIfAllComplete()
    {
        bool shouldDrain = false;
        {
            std::lock_guard<std::mutex> lock(m_orchestrationMutex);
            if (m_completeNodes.size() >= m_orchestration.expectedNodeCount &&
                !m_allProducersComplete.load(std::memory_order_acquire))
            {
                m_allProducersComplete.store(true, std::memory_order_release);
                shouldDrain = true;
            }
        }
        if (!shouldDrain)
        {
            return;
        }
        LOG(INFO) << "All producers complete; draining aligner";
        if (m_aligner.isRunning())
        {
            m_aligner.stop(true);
        }
        m_orchestrationCv.notify_all();
    }

    void CoincidenceServiceImpl::updateNodeStats(uint32_t nodeId, uint64_t singlesCount)
    {
        std::unique_lock<std::shared_mutex> lock(m_nodeInfosMutex);
        auto it = m_nodeInfos.find(nodeId);
        if (it != m_nodeInfos.end())
        {
            it->second->singlesReceived += singlesCount;
            it->second->chunksReceived++;
        }
    }

    std::unique_ptr<grpc::Server> createCoincidenceServer(
        CoincidenceServiceImpl &service,
        const std::string &address)
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
        builder.SetMaxSendMessageSize(16 * 1024 * 1024);
        builder.AddChannelArgument("grpc.http2.initial_window_size", 64 * 1024 * 1024);
        builder.AddChannelArgument("grpc.http2.max_frame_size", 16777215);

        LOG(INFO) << "Starting on " << address;
        return builder.BuildAndStart();
    }

    CoincidenceServer::CoincidenceServer(
        const TimeAlignerConfig &config,
        size_t nodeCount,
        const std::string &address)
        : CoincidenceServer(
              config,
              nodeCount,
              address,
              CoincidenceServiceImpl::OrchestrationConfig{})
    {
    }

    CoincidenceServer::CoincidenceServer(
        const TimeAlignerConfig &config,
        size_t nodeCount,
        const std::string &address,
        CoincidenceServiceImpl::OrchestrationConfig orchestration)
        : m_aligner(config, nodeCount),
          m_service(m_aligner, normalizeOrchestration(std::move(orchestration), nodeCount)),
          m_address(address)
    {
    }

    void CoincidenceServer::start()
    {
        if (m_running.exchange(true))
        {
            LOG(WARNING) << "Already running";
            return;
        }

        m_service.clearServerStoppingState();

        grpc::ServerBuilder builder;
        builder.AddListeningPort(m_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&m_service);

        builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
        builder.SetMaxSendMessageSize(16 * 1024 * 1024);
        builder.AddChannelArgument("grpc.http2.initial_window_size", 64 * 1024 * 1024);
        builder.AddChannelArgument("grpc.http2.max_frame_size", 16777215);

        m_server = builder.BuildAndStart();
        if (!m_server)
        {
            m_running.store(false);
            LOG(ERROR) << "Failed to start on " << m_address;
            return;
        }

        LOG(INFO) << "Started on " << m_address
                  << ", waiting for " << m_service.expectedNodeCount()
                  << " node(s)";
    }

    void CoincidenceServer::stop()
    {
        if (!m_running.exchange(false))
        {
            return;
        }

        m_service.notifyServerStopping();

        if (m_server)
        {
            m_server->Shutdown();
            m_server.reset();
        }

        if (m_aligner.isRunning())
        {
            m_aligner.stop(true);
        }

        LOG(INFO) << "Stopped";
    }

    void CoincidenceServer::wait()
    {
        if (m_server)
        {
            m_server->Wait();
        }
    }

    bool CoincidenceServer::waitForAllNodes(uint32_t timeoutMs) const
    {
        return m_service.waitForAllNodes(timeoutMs);
    }

    bool CoincidenceServer::waitForStartSignal(uint32_t timeoutMs) const
    {
        return m_service.waitForStartSignal(timeoutMs);
    }

    StreamingTimeAligner &CoincidenceServer::getAligner()
    {
        return m_aligner;
    }

    const StreamingTimeAligner &CoincidenceServer::getAligner() const
    {
        return m_aligner;
    }

    const ProcessingStatistics &CoincidenceServer::getStatistics() const
    {
        return m_aligner.getStatistics();
    }

    const CoincidenceServiceImpl &CoincidenceServer::getService() const
    {
        return m_service;
    }

    CoincidenceServiceImpl &CoincidenceServer::getService()
    {
        return m_service;
    }

    bool CoincidenceServer::isRunning() const
    {
        return m_running.load();
    }

    CoincidenceServiceImpl::OrchestrationConfig CoincidenceServer::normalizeOrchestration(
        CoincidenceServiceImpl::OrchestrationConfig orchestration,
        size_t nodeCount)
    {
        if (orchestration.expectedNodeCount == 0)
        {
            orchestration.expectedNodeCount = static_cast<uint32_t>(nodeCount);
        }
        return orchestration;
    }

} // namespace openpni::distributed::streaming
