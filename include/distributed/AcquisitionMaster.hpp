#pragma once

#include "protos/acquisition.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <vector>
#include <string>
#include <mutex>
#include <unordered_map>

namespace openpni::distributed::acquisition
{

    // Forward declaration
    class AcquisitionControlServiceImpl;

    /**
     * @brief Master Server for Distributed Acquisition
     *
     * Responsibilities:
     * 1. Manage connected nodes.
     * 2. Distribute acquisition tasks (channels) to nodes.
     * 3. Monitor node status.
     */
    class AcquisitionMaster
    {
    public:
        AcquisitionMaster();
        ~AcquisitionMaster();

        // Initialize with global acquisition configuration
        // This corresponds to the "Master server receives acquisition info" requirement
        void Initialize(const AcquisitionTask &global_task);

        // Start the gRPC server to listen for nodes
        void StartServer(const std::string &server_address);

        // Wait for server to shutdown
        void Wait();

        // Logic to group detectors and assign to nodes
        // "Server groups detectors based on the number of distributed nodes"
        void DistributeTasks();

    private:
        std::unique_ptr<grpc::Server> server_;
        AcquisitionTask global_task_;

        // Map of connected nodes
        struct NodeContext
        {
            NodeInfo info;
            NodeStatus last_status;
            // Queue for sending commands to this node
            // In a real implementation, this would interact with the gRPC stream
        };

        std::mutex mutex_;
        std::unordered_map<std::string, NodeContext> nodes_;
    };

} // namespace openpni::distributed::acquisition
