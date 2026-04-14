#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "protos/acquisition.pb.h"

namespace openpni
{
    struct RawDataView;
}

namespace openpni::distributed::grpcnode
{
    namespace acqproto = openpni::distributed::acquisition;

    class AcquisitionGrpcNode
    {
    public:
        using FileReadyCallback = std::function<void(const std::string &)>;
        using RawDataReadyCallback = std::function<bool(const openpni::RawDataView &)>;

        struct InitOptions
        {
            std::string masterAddress = "127.0.0.1:50071";
            std::string nodeId = "acq-node-0";
            std::string nodeAddress = "127.0.0.1";

            std::string outputRoot = "Data/raw_data";
            std::string sessionNamePrefix = "acq_session";
            size_t maxFileSizeMb = 1024;
            uint64_t reservedStorageGiB = 20;
            bool enableRawFileWrite = true;

            uint32_t statusIntervalMs = 1000;

            bool strictBindIpsOwnershipCheck = true;
            bool strictNumaTopologyCheck = false;
            bool requireBindIpsSingleNuma = true;
            bool requireCpuAffinityOnNuma = false;
            int32_t expectedNumaNode = -1;
            std::vector<uint32_t> cpuAffinityCores;
        };

        explicit AcquisitionGrpcNode(InitOptions options);

        ~AcquisitionGrpcNode();

        void setFileReadyCallback(FileReadyCallback callback);

        void setRawDataReadyCallback(RawDataReadyCallback callback);

        bool run();

        void stop();

        acqproto::NodeState state() const;

    private:
        class Impl;
        std::unique_ptr<Impl> m_impl;
    };

} // namespace openpni::distributed::grpcnode
