#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace openpni::distributed::app
{
    struct AcquisitionNodeSection
    {
        std::string masterAddress = "127.0.0.1:50093";
        std::string nodeId = "acq-r2s-node-0";
        std::string nodeAddress = "127.0.0.1";
        std::string outputRoot = "Data/raw_data";
        std::string sessionNamePrefix = "acq_r2s_node";
        uint32_t statusIntervalMs = 500;
        bool enableRawFileWrite = false;
    };

    struct R2SSection
    {
        std::string calibrationDir = "Data/bdm2/calibration";
        std::string resultDir = "Data/result/Bdm2/split";
        std::vector<uint16_t> channelIndices = {0, 1, 2, 3};
        bool sortDataByTime = true;
        bool saveData2SingleFile = false;
        bool asyncFileWrite = false;
    };

    struct BridgeSection
    {
        bool enabled = true;
        size_t queueCapacity = 256;
        size_t reservePacketsPerSlot = 4096;
        size_t reserveBytesPerSlot = 4 * 1024 * 1024;
        bool blockWhenQueueFull = true;
        uint64_t queueFullWarnEvery = 5000;
        uint16_t inputChannelCount = 4;
    };

    struct CoinClientSection
    {
        bool enabled = true;
        std::string serverAddress = "127.0.0.1:50061";
        uint32_t nodeId = 0;
        std::string nodeAddress = "127.0.0.1";
        uint32_t channelCount = 4;
        std::string detectorType = "BDM2";
        bool remapLocalToGlobalChannels = false;
        uint32_t globalChannelOffset = 0;
        uint32_t crystalsPerChannel = 169 * 4;
        size_t maxPendingChunks = 128;
        size_t batchSize = 1;
        uint32_t heartbeatIntervalMs = 5000;
        bool waitForStartSignal = true;
        uint32_t waitForStartTimeoutMs = 0;
        uint32_t waitForStartRpcTimeoutMs = 15000;
        uint32_t waitForStartRetryIntervalMs = 1000;
    };

    struct RuntimeSection
    {
        uint32_t shutdownGraceMs = 1000;
    };

    struct AcqR2SNodeConfig
    {
        AcquisitionNodeSection acqNode;
        R2SSection r2s;
        BridgeSection bridge;
        CoinClientSection coinClient;
        RuntimeSection runtime;
    };

    struct CoinMasterSection
    {
        std::string listenAddress = "0.0.0.0:50061";
        uint32_t expectedNodeCount = 1;
        bool autoStartWhenAllRegistered = true;
        uint32_t startLeadTimeMs = 1000;
        uint32_t waitForStartDefaultTimeoutMs = 30000;
        bool rejectStreamBeforeStart = true;
        uint32_t statusPrintIntervalMs = 2000;
        uint32_t runSeconds = 0;
    };

    struct CoinProtocolSection
    {
        uint32_t timeWindowPs = 2000;
        uint32_t delayTimePs = 2'000'000;
        float energyLowerEV = 350000.0F;
        float energyUpperEV = 650000.0F;
    };

    struct AlignerSection
    {
        std::string outputDir = "Data/result/Bdm2/pniCoin";
        uint16_t channelNum = 48;
        uint32_t crystalsPerChannel = 169 * 4;
        uint64_t networkLatencyMarginPico = 5'000'000'000ULL;
        uint32_t processingIntervalMs = 200;
        size_t maxChunksPerNode = 100;
        size_t maxTotalMemoryBytes = 2ULL * 1024ULL * 1024ULL * 1024ULL;
        bool useMemoryPool = true;
        bool savePrompt = true;
        bool saveDelay = true;
        CoinProtocolSection coinProtocol;
    };

    struct AcqControlSection
    {
        struct DetectorSource
        {
            std::string detectorId;
            std::string sourceIp;
            uint16_t sourcePort = 0;
        };

        bool enabled = false;
        std::string masterAddress = "127.0.0.1:50093";
        bool autoDistributeWhenAllConnected = true;
        bool autoStartOnCoinStartSignal = true;
        uint32_t startDurationMs = 0;

        uint16_t sourcePortBase = 17100;
        uint16_t destinationPortBase = 18100;
        uint16_t channelCount = 4;
        std::string sourceIp = "127.0.0.1";
        std::vector<DetectorSource> detectorSources;
        std::string destinationIp = "127.0.0.1";

        std::string sessionName = "app_coin_master_session";
        uint32_t storageUnitSize = 2048;
        uint32_t minPacketSize = 1;
        uint64_t maxBufferSize = 4ULL * 1024ULL * 1024ULL * 1024ULL;
        uint32_t timeSwitchBufferMs = 200;
        uint32_t reservedStorageGiB = 20;
        uint32_t maxFileSizeMb = 256;
        uint32_t dpdkCopyThreadNum = 8;
        uint32_t dpdkRxRingsPerPort = 1;
        uint32_t dpdkMbufDoublePointerSizeMultiply = 32;
        uint32_t dpdkMbufDoublePointerNumMultiply = 2;
    };

    struct CoinMasterConfig
    {
        CoinMasterSection coinMaster;
        AlignerSection aligner;
        AcqControlSection acquisitionControl;
    };

    bool loadAcqR2SNodeConfig(const std::string &path, AcqR2SNodeConfig *cfg, std::string *errorMessage);
    bool loadCoinMasterConfig(const std::string &path, CoinMasterConfig *cfg, std::string *errorMessage);

} // namespace openpni::distributed::app
