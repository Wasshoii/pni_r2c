#pragma once

#include "protos/acquisition.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace openpni::distributed::acquisition
{

    class AcquisitionControlServiceImpl final : public AcquisitionControlService::Service
    {
    public:
        struct ConnectedNode
        {
            NodeInfo info;
            NodeStatus lastStatus;
            bool connected = false;
            std::chrono::steady_clock::time_point lastUpdate;
        };

        grpc::Status Connect(
            grpc::ServerContext *context,
            grpc::ServerReaderWriter<MasterCommand, NodeStatus> *stream) override;

        bool EnqueueCommand(const std::string &nodeId, const MasterCommand &command);
        size_t BroadcastCommand(const MasterCommand &command);
        std::vector<std::string> ListConnectedNodeIds() const;
        size_t ConnectedNodeCount() const;
        bool WaitForConnectedNodes(size_t targetCount, uint32_t timeoutMs);
        std::vector<ConnectedNode> SnapshotNodes() const;

    private:
        struct NodeSession
        {
            std::mutex mutex;
            std::condition_variable cv;
            std::deque<MasterCommand> pendingCommands;

            NodeInfo info;
            NodeStatus lastStatus;
            std::chrono::steady_clock::time_point lastUpdate{};

            bool connected = false;
            bool streamClosed = false;
        };

        static std::string resolveNodeId(const NodeStatus &status);
        std::shared_ptr<NodeSession> getOrCreateSession(const std::string &nodeId);
        std::shared_ptr<NodeSession> findSession(const std::string &nodeId) const;
        void updateStatusUnlocked(const std::shared_ptr<NodeSession> &session, const NodeStatus &status);
        void writerLoop(
            grpc::ServerReaderWriter<MasterCommand, NodeStatus> *stream,
            const std::shared_ptr<NodeSession> &session,
            const std::atomic<bool> &connectionAlive);
        size_t countConnectedNodesLocked() const;

        mutable std::mutex m_nodesMutex;
        mutable std::condition_variable m_nodesCv;
        std::unordered_map<std::string, std::shared_ptr<NodeSession>> m_nodes;
    };

    /**
     * @brief Master Server for Distributed Acquisition
     */
    class AcquisitionMaster
    {
    public:
        using ConfigureTaskOverrideFn = std::function<bool(const std::string &, AcquisitionTask *, std::string *)>;

        AcquisitionMaster();
        ~AcquisitionMaster();

        void Initialize(const AcquisitionTask &global_task);
        void StartServer(const std::string &server_address);
        void Wait();
        void StopServer();
        void DistributeTasks();

        bool WaitForConnectedNodes(size_t nodeCount, uint32_t timeoutMs = 0);
        size_t ConnectedNodeCount() const;

        bool SendConfigureToNode(const std::string &nodeId, const AcquisitionTask &task, const std::string &message = "");
        size_t BroadcastConfigure(const AcquisitionTask &task, const std::string &message = "");
        size_t SendStart(uint64_t plannedStartTimeMs, uint32_t durationMs = 0);
        size_t SendStop(const std::string &reason = "");
        size_t SendShutdown(const std::string &reason = "");
        void SetConfigureTaskOverrideFn(ConfigureTaskOverrideFn fn);

        std::vector<AcquisitionControlServiceImpl::ConnectedNode> SnapshotNodes() const;

    private:
        static std::vector<std::string> SortNodeIds(std::vector<std::string> nodeIds);
        static MasterCommand MakeConfigureCommand(const AcquisitionTask &task, const std::string &message);
        static MasterCommand MakeStartCommand(uint64_t plannedStartTimeMs, uint32_t durationMs);
        static MasterCommand MakeStopCommand(const std::string &reason);
        static MasterCommand MakeShutdownCommand(const std::string &reason);
        static uint32_t CheckedSteppedValue(uint32_t base, uint32_t stride, size_t index, const char *field);
        static AcquisitionTask BuildEffectiveGlobalTask(const AcquisitionTask &task);

        std::vector<AcquisitionTask> BuildNodeTasks(const std::vector<std::string> &nodeIds) const;

        std::shared_ptr<AcquisitionControlServiceImpl> service_;
        std::unique_ptr<grpc::Server> server_;
        AcquisitionTask global_task_;
        ConfigureTaskOverrideFn configureTaskOverrideFn_;
    };

} // namespace openpni::distributed::acquisition
