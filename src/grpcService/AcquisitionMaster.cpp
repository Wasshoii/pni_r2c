#include "grpcService/AcquisitionMaster.hpp"

#include <algorithm>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <utility>
#include <glog/logging.h>

namespace openpni::distributed::acquisition
{

    grpc::Status AcquisitionControlServiceImpl::Connect(
        grpc::ServerContext *context,
        grpc::ServerReaderWriter<MasterCommand, NodeStatus> *stream)
    {
        (void)context;

        NodeStatus firstStatus;
        if (!stream->Read(&firstStatus))
        {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Node closed stream before sending status");
        }

        const std::string nodeId = resolveNodeId(firstStatus);
        if (nodeId.empty())
        {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "NodeStatus.info.node_id is empty");
        }

        auto session = getOrCreateSession(nodeId);
        updateStatusUnlocked(session, firstStatus);

        std::atomic<bool> connectionAlive{true};
        std::thread writerThread([&]()
                                 { writerLoop(stream, session, connectionAlive); });

        NodeStatus status;
        while (stream->Read(&status))
        {
            updateStatusUnlocked(session, status);
        }

        connectionAlive.store(false, std::memory_order_release);
        {
            std::lock_guard<std::mutex> lock(session->mutex);
            session->connected = false;
            session->streamClosed = true;
        }
        session->cv.notify_all();
        m_nodesCv.notify_all();

        if (writerThread.joinable())
        {
            writerThread.join();
        }

        return grpc::Status::OK;
    }

    bool AcquisitionControlServiceImpl::EnqueueCommand(const std::string &nodeId, const MasterCommand &command)
    {
        auto session = findSession(nodeId);
        if (!session)
        {
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(session->mutex);
            if (!session->connected)
            {
                return false;
            }
            session->pendingCommands.push_back(command);
        }

        session->cv.notify_one();
        return true;
    }

    size_t AcquisitionControlServiceImpl::BroadcastCommand(const MasterCommand &command)
    {
        const auto nodeIds = ListConnectedNodeIds();
        size_t sent = 0;
        for (const auto &nodeId : nodeIds)
        {
            if (EnqueueCommand(nodeId, command))
            {
                ++sent;
            }
        }
        return sent;
    }

    std::vector<std::string> AcquisitionControlServiceImpl::ListConnectedNodeIds() const
    {
        std::vector<std::string> result;
        std::lock_guard<std::mutex> lock(m_nodesMutex);
        result.reserve(m_nodes.size());
        for (const auto &[nodeId, session] : m_nodes)
        {
            if (!session)
            {
                continue;
            }

            std::lock_guard<std::mutex> sessionLock(session->mutex);
            if (session->connected)
            {
                result.push_back(nodeId);
            }
        }
        return result;
    }

    size_t AcquisitionControlServiceImpl::ConnectedNodeCount() const
    {
        size_t count = 0;
        std::lock_guard<std::mutex> lock(m_nodesMutex);
        for (const auto &[_, session] : m_nodes)
        {
            if (!session)
            {
                continue;
            }

            std::lock_guard<std::mutex> sessionLock(session->mutex);
            if (session->connected)
            {
                ++count;
            }
        }
        return count;
    }

    bool AcquisitionControlServiceImpl::WaitForConnectedNodes(size_t targetCount, uint32_t timeoutMs)
    {
        std::unique_lock<std::mutex> lock(m_nodesMutex);

        const auto ready = [&]()
        {
            return countConnectedNodesLocked() >= targetCount;
        };

        if (ready())
        {
            return true;
        }

        if (timeoutMs == 0)
        {
            m_nodesCv.wait(lock, ready);
            return ready();
        }

        return m_nodesCv.wait_for(lock, std::chrono::milliseconds(timeoutMs), ready);
    }

    std::vector<AcquisitionControlServiceImpl::ConnectedNode> AcquisitionControlServiceImpl::SnapshotNodes() const
    {
        std::vector<ConnectedNode> snapshot;
        std::lock_guard<std::mutex> lock(m_nodesMutex);
        snapshot.reserve(m_nodes.size());

        for (const auto &[_, session] : m_nodes)
        {
            if (!session)
            {
                continue;
            }

            std::lock_guard<std::mutex> sessionLock(session->mutex);
            ConnectedNode node;
            node.info = session->info;
            node.lastStatus = session->lastStatus;
            node.connected = session->connected;
            node.lastUpdate = session->lastUpdate;
            snapshot.push_back(std::move(node));
        }

        return snapshot;
    }

    std::string AcquisitionControlServiceImpl::resolveNodeId(const NodeStatus &status)
    {
        if (status.has_info() && !status.info().node_id().empty())
        {
            return status.info().node_id();
        }

        return {};
    }

    std::shared_ptr<AcquisitionControlServiceImpl::NodeSession>
    AcquisitionControlServiceImpl::getOrCreateSession(const std::string &nodeId)
    {
        std::lock_guard<std::mutex> lock(m_nodesMutex);
        auto &slot = m_nodes[nodeId];
        if (!slot)
        {
            slot = std::make_shared<NodeSession>();
        }

        {
            std::lock_guard<std::mutex> sessionLock(slot->mutex);
            slot->connected = true;
            slot->streamClosed = false;
        }

        m_nodesCv.notify_all();
        return slot;
    }

    std::shared_ptr<AcquisitionControlServiceImpl::NodeSession>
    AcquisitionControlServiceImpl::findSession(const std::string &nodeId) const
    {
        std::lock_guard<std::mutex> lock(m_nodesMutex);
        auto it = m_nodes.find(nodeId);
        if (it == m_nodes.end())
        {
            return nullptr;
        }
        return it->second;
    }

    void AcquisitionControlServiceImpl::updateStatusUnlocked(
        const std::shared_ptr<NodeSession> &session,
        const NodeStatus &status)
    {
        if (!session)
        {
            return;
        }

        {
            std::lock_guard<std::mutex> lock(session->mutex);
            session->lastStatus = status;
            session->lastUpdate = std::chrono::steady_clock::now();
            session->connected = true;
            if (status.has_info())
            {
                session->info = status.info();
            }
        }

        m_nodesCv.notify_all();
    }

    void AcquisitionControlServiceImpl::writerLoop(
        grpc::ServerReaderWriter<MasterCommand, NodeStatus> *stream,
        const std::shared_ptr<NodeSession> &session,
        const std::atomic<bool> &connectionAlive)
    {
        while (connectionAlive.load(std::memory_order_acquire))
        {
            MasterCommand command;
            {
                std::unique_lock<std::mutex> lock(session->mutex);
                session->cv.wait(lock, [&]()
                                 { return !session->pendingCommands.empty() || !connectionAlive.load(std::memory_order_acquire) || session->streamClosed; });

                if (session->pendingCommands.empty())
                {
                    if (!connectionAlive.load(std::memory_order_acquire) || session->streamClosed)
                    {
                        return;
                    }
                    continue;
                }

                command = session->pendingCommands.front();
                session->pendingCommands.pop_front();
            }

            if (!stream->Write(command))
            {
                return;
            }
        }
    }

    size_t AcquisitionControlServiceImpl::countConnectedNodesLocked() const
    {
        size_t count = 0;
        for (const auto &[_, session] : m_nodes)
        {
            if (!session)
            {
                continue;
            }

            std::lock_guard<std::mutex> sessionLock(session->mutex);
            if (session->connected)
            {
                ++count;
            }
        }
        return count;
    }

    AcquisitionMaster::AcquisitionMaster()
        : service_(std::make_shared<AcquisitionControlServiceImpl>())
    {
    }

    AcquisitionMaster::~AcquisitionMaster()
    {
        StopServer();
    }

    void AcquisitionMaster::Initialize(const AcquisitionTask &global_task)
    {
        global_task_ = global_task;
    }

    void AcquisitionMaster::StartServer(const std::string &server_address)
    {
        if (server_)
        {
            LOG(WARNING) << "Server already running";
            return;
        }

        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(service_.get());

        builder.SetMaxReceiveMessageSize(32 * 1024 * 1024);
        builder.SetMaxSendMessageSize(32 * 1024 * 1024);

        server_ = builder.BuildAndStart();
        if (!server_)
        {
            throw std::runtime_error("Failed to start AcquisitionMaster server at " + server_address);
        }

        LOG(INFO) << "Listening on " << server_address;
    }

    void AcquisitionMaster::Wait()
    {
        if (server_)
        {
            server_->Wait();
        }
    }

    void AcquisitionMaster::StopServer()
    {
        if (server_)
        {
            server_->Shutdown();
            server_.reset();
            LOG(INFO) << "Stopped";
        }
    }

    bool AcquisitionMaster::WaitForConnectedNodes(size_t nodeCount, uint32_t timeoutMs)
    {
        return service_->WaitForConnectedNodes(nodeCount, timeoutMs);
    }

    size_t AcquisitionMaster::ConnectedNodeCount() const
    {
        return service_->ConnectedNodeCount();
    }

    bool AcquisitionMaster::SendConfigureToNode(const std::string &nodeId, const AcquisitionTask &task, const std::string &message)
    {
        try
        {
            const auto effectiveTask = BuildEffectiveGlobalTask(task);
            return service_->EnqueueCommand(nodeId, MakeConfigureCommand(effectiveTask, message));
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "SendConfigureToNode failed: " << e.what();
            return false;
        }
    }

    size_t AcquisitionMaster::BroadcastConfigure(const AcquisitionTask &task, const std::string &message)
    {
        try
        {
            const auto effectiveTask = BuildEffectiveGlobalTask(task);
            return service_->BroadcastCommand(MakeConfigureCommand(effectiveTask, message));
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "BroadcastConfigure failed: " << e.what();
            return 0;
        }
    }

    void AcquisitionMaster::DistributeTasks()
    {
        const auto nodeIds = SortNodeIds(service_->ListConnectedNodeIds());
        if (nodeIds.empty())
        {
            LOG(WARNING) << "No connected nodes, skip task distribution";
            return;
        }

        std::vector<AcquisitionTask> nodeTasks;
        try
        {
            nodeTasks = BuildNodeTasks(nodeIds);
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "Failed to build node tasks: " << e.what();
            return;
        }

        if (nodeTasks.empty())
        {
            LOG(WARNING) << "No task generated, skip CONFIGURE broadcast";
            return;
        }
        size_t configured = 0;

        for (size_t i = 0; i < nodeIds.size(); ++i)
        {
            AcquisitionTask task = nodeTasks[i];
            if (configureTaskOverrideFn_)
            {
                std::string overrideError;
                if (!configureTaskOverrideFn_(nodeIds[i], &task, &overrideError))
                {
                    LOG(ERROR) << "Skip CONFIGURE for node=" << nodeIds[i]
                               << ", override failed: " << overrideError;
                    continue;
                }
            }

            const std::string message = "assigned_channels=" + std::to_string(task.channels_size());
            if (service_->EnqueueCommand(nodeIds[i], MakeConfigureCommand(task, message)))
            {
                ++configured;
            }
        }

        LOG(INFO) << "Sent CONFIGURE to " << configured
                  << "/" << nodeIds.size() << " node(s)";
    }

    size_t AcquisitionMaster::SendStart(uint64_t plannedStartTimeMs, uint32_t durationMs)
    {
        return service_->BroadcastCommand(MakeStartCommand(plannedStartTimeMs, durationMs));
    }

    size_t AcquisitionMaster::SendStop(const std::string &reason)
    {
        return service_->BroadcastCommand(MakeStopCommand(reason));
    }

    size_t AcquisitionMaster::SendShutdown(const std::string &reason)
    {
        return service_->BroadcastCommand(MakeShutdownCommand(reason));
    }

    void AcquisitionMaster::SetConfigureTaskOverrideFn(ConfigureTaskOverrideFn fn)
    {
        configureTaskOverrideFn_ = std::move(fn);
    }

    std::vector<AcquisitionControlServiceImpl::ConnectedNode> AcquisitionMaster::SnapshotNodes() const
    {
        return service_->SnapshotNodes();
    }

    std::vector<std::string> AcquisitionMaster::SortNodeIds(std::vector<std::string> nodeIds)
    {
        std::sort(nodeIds.begin(), nodeIds.end());
        return nodeIds;
    }

    MasterCommand AcquisitionMaster::MakeConfigureCommand(const AcquisitionTask &task, const std::string &message)
    {
        MasterCommand cmd;
        cmd.set_type(CMD_CONFIGURE);
        *cmd.mutable_task() = task;
        cmd.set_message(message);
        return cmd;
    }

    MasterCommand AcquisitionMaster::MakeStartCommand(uint64_t plannedStartTimeMs, uint32_t durationMs)
    {
        MasterCommand cmd;
        cmd.set_type(CMD_START);
        auto *start = cmd.mutable_start_control();
        start->set_planned_start_time_ms(plannedStartTimeMs);
        start->set_duration_ms(durationMs);
        return cmd;
    }

    MasterCommand AcquisitionMaster::MakeStopCommand(const std::string &reason)
    {
        MasterCommand cmd;
        cmd.set_type(CMD_STOP);
        cmd.mutable_stop_control()->set_reason(reason);
        return cmd;
    }

    MasterCommand AcquisitionMaster::MakeShutdownCommand(const std::string &reason)
    {
        MasterCommand cmd;
        cmd.set_type(CMD_SHUTDOWN);
        cmd.mutable_shutdown_control()->set_reason(reason);
        return cmd;
    }

    uint32_t AcquisitionMaster::CheckedSteppedValue(uint32_t base, uint32_t stride, size_t index, const char *field)
    {
        const uint64_t value = static_cast<uint64_t>(base) + static_cast<uint64_t>(stride) * static_cast<uint64_t>(index);
        if (value > std::numeric_limits<uint32_t>::max())
        {
            throw std::invalid_argument(std::string(field) + " overflow while generating channel mapping");
        }
        return static_cast<uint32_t>(value);
    }

    AcquisitionTask AcquisitionMaster::BuildEffectiveGlobalTask(const AcquisitionTask &task)
    {
        AcquisitionTask effective = task;

        if (effective.channels_size() > 0)
        {
            effective.set_total_channel_num(static_cast<uint32_t>(effective.channels_size()));
            return effective;
        }

        if (effective.detector_sources_size() == 0)
        {
            return effective;
        }

        if (!effective.has_destination_rule())
        {
            throw std::invalid_argument("detector_sources is set but destination_rule is missing; destination is required to match acquisition.cpp source==destination semantics");
        }

        const uint32_t destIp = effective.destination_rule().ip_destination();
        const uint32_t destPortBase = effective.destination_rule().port_destination_base();
        const uint32_t destPortStride =
            effective.destination_rule().port_destination_stride() > 0 ? effective.destination_rule().port_destination_stride() : 1;

        const uint32_t channelBase = effective.has_channel_index_rule() ? effective.channel_index_rule().channel_index_base() : 0;
        const uint32_t channelStride =
            (effective.has_channel_index_rule() && effective.channel_index_rule().channel_index_stride() > 0)
                ? effective.channel_index_rule().channel_index_stride()
                : 1;

        effective.clear_channels();
        for (int i = 0; i < effective.detector_sources_size(); ++i)
        {
            const auto &src = effective.detector_sources(i);
            auto *channel = effective.add_channels();
            channel->set_ip_source(src.ip_source());
            channel->set_port_source(src.port_source());
            channel->set_ip_destination(destIp);
            channel->set_port_destination(CheckedSteppedValue(destPortBase, destPortStride, static_cast<size_t>(i), "port_destination"));
            channel->set_channel_index(CheckedSteppedValue(channelBase, channelStride, static_cast<size_t>(i), "channel_index"));
        }

        effective.set_total_channel_num(static_cast<uint32_t>(effective.channels_size()));
        return effective;
    }

    std::vector<AcquisitionTask> AcquisitionMaster::BuildNodeTasks(const std::vector<std::string> &nodeIds) const
    {
        std::vector<AcquisitionTask> tasks;
        if (nodeIds.empty())
        {
            return tasks;
        }

        const auto effectiveTask = BuildEffectiveGlobalTask(global_task_);
        const auto totalChannels = static_cast<size_t>(effectiveTask.channels_size());
        if (totalChannels == 0)
        {
            throw std::invalid_argument("global acquisition task has no channels; provide channels or detector_sources mapping input");
        }

        const size_t nodeCount = nodeIds.size();
        const size_t base = totalChannels / nodeCount;
        const size_t remain = totalChannels % nodeCount;

        tasks.resize(nodeCount);
        size_t cursor = 0;
        for (size_t i = 0; i < nodeCount; ++i)
        {
            AcquisitionTask task;
            task.set_storage_unit_size(effectiveTask.storage_unit_size());
            task.set_max_buffer_size(effectiveTask.max_buffer_size());
            task.set_time_switch_buffer_ms(effectiveTask.time_switch_buffer_ms());
            task.set_min_packet_size(effectiveTask.min_packet_size());
            task.set_algorithm_type(effectiveTask.algorithm_type());
            if (effectiveTask.has_dpdk_options())
            {
                *task.mutable_dpdk_options() = effectiveTask.dpdk_options();
            }
            task.set_session_name(effectiveTask.session_name());
            task.set_reserved_storage_gib(effectiveTask.reserved_storage_gib());
            task.set_max_file_size_mb(effectiveTask.max_file_size_mb());

            const size_t assigned = base + ((i < remain) ? 1 : 0);
            for (size_t j = 0; j < assigned; ++j)
            {
                if (cursor >= totalChannels)
                {
                    break;
                }

                auto *channel = task.add_channels();
                *channel = effectiveTask.channels(static_cast<int>(cursor));
                // Runtime acquisition uses node-local channel indexing; normalize each node task
                // to [0..assigned-1] while preserving global source/destination mapping.
                channel->set_channel_index(static_cast<uint32_t>(j));
                ++cursor;
            }

            task.set_total_channel_num(static_cast<uint32_t>(task.channels_size()));
            tasks[i] = std::move(task);
        }

        return tasks;
    }

} // namespace openpni::distributed::acquisition
