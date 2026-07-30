#include "grpcService/CoincidenceServiceImpl.hpp"
#include "core/streaming/PackedSingle.hpp"
#include "dataplane/rdma/ProtoConvert.hpp"
#include "dataplane/rdma/RdmaContext.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <iostream>
#include <utility>
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
        m_rdmaServer = std::make_unique<rdma::RdmaRecvServer>(cfg);
        m_rdmaServer->setIngest([this](const rdma::SlotChunkView &view) -> bool
                                {
            std::string err;
            if (!ingestPackedSinglesChunk(
                    view.nodeId,
                    view.chunkId,
                    view.computerClockMs,
                    view.durationMs,
                    view.singlesPacked,
                    view.singlesCount,
                    &err))
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

        if (!m_rdmaServer)
        {
            response->set_success(false);
            response->set_message("RDMA server not initialized");
            return grpc::Status::OK;
        }

        auto session = m_rdmaServer->ensureSession(nodeId);
        if (!session)
        {
            response->set_success(false);
            response->set_message("Failed to prepare RDMA receive session");
            return grpc::Status::OK;
        }

        if (request->has_node_endpoint() &&
            session->kind() == rdma::DataPlaneKind::RdmaRoceV2)
        {
            const auto remote = rdma::fromProtoEndpoint(request->node_endpoint());
            if (!session->acceptRemote(remote))
            {
                response->set_success(false);
                response->set_message("Failed to accept remote RDMA endpoint");
                return grpc::Status::OK;
            }
        }

        const auto local = session->localEndpoint();
        response->set_success(true);
        response->set_message("Data plane ready");
        response->set_data_plane_kind(rdma::toProto(local.kind));
        rdma::fillProtoEndpoint(local, response->mutable_coin_endpoint());
        LOG(INFO) << "OpenDataPlane node=" << nodeId
                  << " kind=" << static_cast<uint32_t>(local.kind)
                  << " slots=" << local.slotCount
                  << " stride=" << local.slotStride;
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
            std::shared_lock<std::shared_mutex> lock(m_nodeInfosMutex);
            for (const auto &[nodeId, info] : m_nodeInfos)
            {
                auto *nodeStatus = response->add_node_stats();
                nodeStatus->set_node_id(nodeId);
                nodeStatus->set_chunks_received(info->chunksReceived.load());
                nodeStatus->set_singles_received(info->singlesReceived.load());

                auto *buffer = m_aligner.getNodeBuffer(static_cast<uint16_t>(nodeId));
                nodeStatus->set_buffer_size(buffer ? buffer->size() : 0);
                nodeStatus->set_connected(info->connected.load());
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
            m_orchestrationCv.notify_all();

            response->set_success(true);
            response->set_message("Aligner started and start signal issued");
            break;
        }

        case coincidence::ControlRequest::STOP:
        {
            {
                std::lock_guard<std::mutex> lock(m_orchestrationMutex);
                m_startSignalIssued.store(false, std::memory_order_release);
                m_plannedStartTimeMs.store(0, std::memory_order_release);
            }
            m_orchestrationCv.notify_all();

            if (m_aligner.isRunning())
            {
                m_aligner.stop(request->wait_for_completion());
            }

            response->set_success(true);
            response->set_message("Aligner stopped and start barrier reset");
            break;
        }

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

        maybeAutoStartAfterRegister();

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
            }
        }

        response->set_acknowledged(true);
        response->set_server_timestamp_ms(nowMs());
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

    bool CoincidenceServiceImpl::startSignalIssued() const
    {
        return m_startSignalIssued.load(std::memory_order_acquire);
    }

    uint64_t CoincidenceServiceImpl::plannedStartTimeMs() const
    {
        return m_plannedStartTimeMs.load(std::memory_order_acquire);
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
                  << ", connected=" << m_registeredNodes.size()
                  << "/" << m_orchestration.expectedNodeCount << ")";
        return true;
    }

    void CoincidenceServiceImpl::maybeAutoStartAfterRegister()
    {
        bool shouldStart = false;
        uint64_t startTimeMs = 0;

        {
            std::lock_guard<std::mutex> lock(m_orchestrationMutex);
            if (m_orchestration.autoStartWhenAllRegistered &&
                m_registeredNodes.size() >= m_orchestration.expectedNodeCount)
            {
                startTimeMs = nowMs() + m_orchestration.startLeadTimeMs;
                shouldStart = issueStartSignalLocked(startTimeMs, "all nodes registered");
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

    void CoincidenceServiceImpl::updateNodeStats(uint32_t nodeId, uint64_t singlesCount)
    {
        std::unique_lock<std::shared_mutex> lock(m_nodeInfosMutex);
        auto it = m_nodeInfos.find(nodeId);
        if (it != m_nodeInfos.end())
        {
            it->second->singlesReceived += singlesCount;
            it->second->chunksReceived++;
            it->second->lastHeartbeat = std::chrono::steady_clock::now();
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
