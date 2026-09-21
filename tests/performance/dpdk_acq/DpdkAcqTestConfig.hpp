#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace openpni::distributed::test::dpdk_acq
{
    struct DetectorSource
    {
        std::string detectorId;
        std::string sourceIp;
        uint16_t sourcePort = 0;
    };

    struct MasterSection
    {
        std::string listenAddress = "0.0.0.0:50093";
        uint32_t expectedNodeCount = 1;
        uint32_t statusIntervalMs = 1000;
        uint32_t startDurationMs = 0;
        uint32_t waitConnectTimeoutMs = 30000;
        uint32_t shutdownGraceMs = 1000;
    };

    struct NodeSection
    {
        std::string masterAddress = "127.0.0.1:50093";
        std::string nodeId = "dpdk-acq-node-0";
        std::string nodeAddress = "127.0.0.1";
        bool enableRawFileWrite = false;
        std::string outputRoot = "Data/raw_data";
        std::string sessionNamePrefix = "dpdk_acq";
        size_t maxFileSizeMb = 1024;
        bool overwriteExisting = true;
        uint64_t reservedStorageGiB = 20;
        bool fsyncEachSegment = true;
        bool enableCpuAffinity = false;
        std::vector<uint16_t> cpuAffinityCores;
        bool strictBindIpsOwnershipCheck = false;
        bool strictNumaTopologyCheck = false;
        bool requireBindIpsSingleNuma = true;
        bool requireCpuAffinityOnNuma = false;
        int32_t expectedNumaNode = -1;
    };

    struct AcquisitionSection
    {
        uint16_t channelCount = 144;
        std::string sourceIp = "10.10.1.10";
        uint16_t sourcePortBase = 17100;
        std::string destinationIp = "10.10.1.20";
        uint16_t destinationPortBase = 18100;
        std::vector<DetectorSource> detectorSources;
        std::string sessionName = "dpdk_acq_session";
        uint32_t storageUnitSize = 2048;
        uint32_t minPacketSize = 1;
        uint64_t maxBufferSize = 4ull * 1024ull * 1024ull * 1024ull;
        uint32_t timeSwitchBufferMs = 50;
        uint32_t dpdkRxRingsPerPort = 1;
        uint32_t dpdkMbufPoolSize = 65535;
        uint32_t dpdkMbufCacheSize = 250;
        std::vector<std::string> dpdkBindIps;
        std::vector<std::string> dpdkExtraEalArgs;
        std::string dpdkLocalLoopbackIface;
    };

    struct DpdkAcqTestConfig
    {
        MasterSection master;
        NodeSection node;
        AcquisitionSection acquisition;
    };

    bool loadDpdkAcqTestConfig(const std::string &path, DpdkAcqTestConfig *cfg, std::string *errorMessage);
}
