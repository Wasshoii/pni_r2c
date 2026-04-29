#include "app/common/AppConfig.hpp"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <cmath>
#include <cstdint>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_set>

namespace openpni::distributed::app
{
    namespace
    {
        using google::protobuf::Struct;
        using google::protobuf::Value;

        bool readFileText(const std::string &path, std::string *text, std::string *errorMessage)
        {
            std::ifstream ifs(path);
            if (!ifs)
            {
                if (errorMessage)
                {
                    *errorMessage = "failed to open file: " + path;
                }
                return false;
            }

            std::ostringstream oss;
            oss << ifs.rdbuf();
            *text = oss.str();
            return true;
        }

        bool parseJsonAsStruct(const std::string &jsonText, Struct *out, std::string *errorMessage)
        {
            google::protobuf::util::JsonParseOptions opts;
            opts.ignore_unknown_fields = false;
            const auto status = google::protobuf::util::JsonStringToMessage(jsonText, out, opts);
            if (!status.ok())
            {
                if (errorMessage)
                {
                    *errorMessage = "json parse error: " + std::string(status.message());
                }
                return false;
            }
            return true;
        }

        const Value *findField(const Struct &obj, const std::string &key)
        {
            const auto it = obj.fields().find(key);
            if (it == obj.fields().end())
            {
                return nullptr;
            }
            return &it->second;
        }

        const Struct *findObject(const Struct &root, const std::string &key)
        {
            const Value *v = findField(root, key);
            if (!v || v->kind_case() != Value::kStructValue)
            {
                return nullptr;
            }
            return &v->struct_value();
        }

        bool readString(const Struct &obj, const std::string &key, std::string *out)
        {
            const Value *v = findField(obj, key);
            if (!v)
            {
                return true;
            }
            if (v->kind_case() != Value::kStringValue)
            {
                return false;
            }
            *out = v->string_value();
            return true;
        }

        bool readBool(const Struct &obj, const std::string &key, bool *out)
        {
            const Value *v = findField(obj, key);
            if (!v)
            {
                return true;
            }
            if (v->kind_case() != Value::kBoolValue)
            {
                return false;
            }
            *out = v->bool_value();
            return true;
        }

        bool numberFromValue(const Value &v, double *out)
        {
            if (v.kind_case() != Value::kNumberValue)
            {
                return false;
            }
            *out = v.number_value();
            return true;
        }

        template <typename UInt>
        bool readUInt(const Struct &obj, const std::string &key, UInt *out)
        {
            const Value *v = findField(obj, key);
            if (!v)
            {
                return true;
            }

            double num = 0.0;
            if (!numberFromValue(*v, &num) || num < 0.0 || std::floor(num) != num)
            {
                return false;
            }

            if (num > static_cast<double>(std::numeric_limits<UInt>::max()))
            {
                return false;
            }

            *out = static_cast<UInt>(num);
            return true;
        }

        template <typename Int>
        bool readInt(const Struct &obj, const std::string &key, Int *out)
        {
            const Value *v = findField(obj, key);
            if (!v)
            {
                return true;
            }

            double num = 0.0;
            if (!numberFromValue(*v, &num) || std::floor(num) != num)
            {
                return false;
            }

            if (num < static_cast<double>(std::numeric_limits<Int>::min()) ||
                num > static_cast<double>(std::numeric_limits<Int>::max()))
            {
                return false;
            }

            *out = static_cast<Int>(num);
            return true;
        }

        bool readFloat(const Struct &obj, const std::string &key, float *out)
        {
            const Value *v = findField(obj, key);
            if (!v)
            {
                return true;
            }

            double num = 0.0;
            if (!numberFromValue(*v, &num))
            {
                return false;
            }
            *out = static_cast<float>(num);
            return true;
        }

        bool readUInt16Array(const Struct &obj, const std::string &key, std::vector<uint16_t> *out)
        {
            const Value *v = findField(obj, key);
            if (!v)
            {
                return true;
            }
            if (v->kind_case() != Value::kListValue)
            {
                return false;
            }

            std::vector<uint16_t> values;
            values.reserve(static_cast<size_t>(v->list_value().values_size()));
            for (const auto &item : v->list_value().values())
            {
                double num = 0.0;
                if (!numberFromValue(item, &num) || num < 0.0 || std::floor(num) != num || num > 65535.0)
                {
                    return false;
                }
                values.push_back(static_cast<uint16_t>(num));
            }

            *out = std::move(values);
            return true;
        }

        bool readStringArray(const Struct &obj, const std::string &key, std::vector<std::string> *out)
        {
            const Value *v = findField(obj, key);
            if (!v)
            {
                return true;
            }
            if (v->kind_case() != Value::kListValue)
            {
                return false;
            }

            std::vector<std::string> values;
            values.reserve(static_cast<size_t>(v->list_value().values_size()));
            for (const auto &item : v->list_value().values())
            {
                if (item.kind_case() != Value::kStringValue)
                {
                    return false;
                }
                const auto &s = item.string_value();
                if (s.empty())
                {
                    return false;
                }
                values.push_back(s);
            }

            *out = std::move(values);
            return true;
        }

        bool loadRoot(const std::string &path, Struct *root, std::string *errorMessage)
        {
            std::string text;
            if (!readFileText(path, &text, errorMessage))
            {
                return false;
            }
            if (!parseJsonAsStruct(text, root, errorMessage))
            {
                return false;
            }
            return true;
        }

        bool parseAcquisitionAlgorithm(
            const std::string &algo,
            AcqControlSection::AcquisitionAlgorithm *out)
        {
            if (algo == "socket" || algo == "SOCKET" || algo == "Socket")
            {
                *out = AcqControlSection::AcquisitionAlgorithm::Socket;
                return true;
            }
            if (algo == "dpdk" || algo == "DPDK" || algo == "Dpdk")
            {
                *out = AcqControlSection::AcquisitionAlgorithm::Dpdk;
                return true;
            }
            return false;
        }

        bool parseNodeAlgorithmOverride(
            const std::string &algo,
            AcqControlSection::NodeOverride::AlgorithmOverride *out)
        {
            if (algo.empty() || algo == "inherit" || algo == "INHERIT" || algo == "Inherit")
            {
                *out = AcqControlSection::NodeOverride::AlgorithmOverride::Inherit;
                return true;
            }
            if (algo == "socket" || algo == "SOCKET" || algo == "Socket")
            {
                *out = AcqControlSection::NodeOverride::AlgorithmOverride::Socket;
                return true;
            }
            if (algo == "dpdk" || algo == "DPDK" || algo == "Dpdk")
            {
                *out = AcqControlSection::NodeOverride::AlgorithmOverride::Dpdk;
                return true;
            }
            return false;
        }

        bool fail(std::string *errorMessage, const std::string &message)
        {
            if (errorMessage)
            {
                *errorMessage = message;
            }
            return false;
        }

        bool applyAcqNodeSection(const Struct &root, AcqR2SNodeConfig *cfg, std::string *err)
        {
            if (const Struct *sec = findObject(root, "acqNode"))
            {
                if (!readString(*sec, "masterAddress", &cfg->acqNode.masterAddress))
                {
                    return fail(err, "acqNode.masterAddress must be string");
                }
                if (!readString(*sec, "nodeId", &cfg->acqNode.nodeId))
                {
                    return fail(err, "acqNode.nodeId must be string");
                }
                if (!readString(*sec, "nodeAddress", &cfg->acqNode.nodeAddress))
                {
                    return fail(err, "acqNode.nodeAddress must be string");
                }
                if (!readString(*sec, "outputRoot", &cfg->acqNode.outputRoot))
                {
                    return fail(err, "acqNode.outputRoot must be string");
                }
                if (!readStringArray(*sec, "outputRoots", &cfg->acqNode.outputRoots))
                {
                    return fail(err, "acqNode.outputRoots must be string array");
                }
                if (!readString(*sec, "shardStrategy", &cfg->acqNode.shardStrategy))
                {
                    return fail(err, "acqNode.shardStrategy must be string");
                }
                if (!readString(*sec, "manifestFilename", &cfg->acqNode.manifestFilename))
                {
                    return fail(err, "acqNode.manifestFilename must be string");
                }
                if (!readString(*sec, "sessionNamePrefix", &cfg->acqNode.sessionNamePrefix))
                {
                    return fail(err, "acqNode.sessionNamePrefix must be string");
                }
                if (!readUInt(*sec, "maxFileSizeMb", &cfg->acqNode.maxFileSizeMb))
                {
                    return fail(err, "acqNode.maxFileSizeMb must be non-negative integer");
                }
                if (!readUInt(*sec, "reservedStorageGiB", &cfg->acqNode.reservedStorageGiB))
                {
                    return fail(err, "acqNode.reservedStorageGiB must be non-negative integer");
                }
                if (!readUInt(*sec, "statusIntervalMs", &cfg->acqNode.statusIntervalMs))
                {
                    return fail(err, "acqNode.statusIntervalMs must be non-negative integer");
                }
                if (!readBool(*sec, "enableRawFileWrite", &cfg->acqNode.enableRawFileWrite))
                {
                    return fail(err, "acqNode.enableRawFileWrite must be bool");
                }
                if (!readUInt(*sec, "asyncQueueDepth", &cfg->acqNode.asyncQueueDepth))
                {
                    return fail(err, "acqNode.asyncQueueDepth must be non-negative integer");
                }
                if (!readUInt(*sec, "writerThreadsPerShard", &cfg->acqNode.writerThreadsPerShard))
                {
                    return fail(err, "acqNode.writerThreadsPerShard must be non-negative integer");
                }
                if (!readBool(*sec, "useSpillToDisk", &cfg->acqNode.useSpillToDisk))
                {
                    return fail(err, "acqNode.useSpillToDisk must be bool");
                }
                if (!readBool(*sec, "failOnQueueFull", &cfg->acqNode.failOnQueueFull))
                {
                    return fail(err, "acqNode.failOnQueueFull must be bool");
                }
                if (!readBool(*sec, "fsyncEachSegment", &cfg->acqNode.fsyncEachSegment))
                {
                    return fail(err, "acqNode.fsyncEachSegment must be bool");
                }
            }
            return true;
        }

        bool applyR2SSection(const Struct &root, AcqR2SNodeConfig *cfg, std::string *err)
        {
            if (const Struct *sec = findObject(root, "r2s"))
            {
                if (!readString(*sec, "calibrationDir", &cfg->r2s.calibrationDir))
                {
                    return fail(err, "r2s.calibrationDir must be string");
                }
                if (!readString(*sec, "resultDir", &cfg->r2s.resultDir))
                {
                    return fail(err, "r2s.resultDir must be string");
                }
                if (!readUInt16Array(*sec, "channelIndices", &cfg->r2s.channelIndices))
                {
                    return fail(err, "r2s.channelIndices must be integer array in [0,65535]");
                }
                if (!readBool(*sec, "sortDataByTime", &cfg->r2s.sortDataByTime))
                {
                    return fail(err, "r2s.sortDataByTime must be bool");
                }
                if (!readBool(*sec, "saveData2SingleFile", &cfg->r2s.saveData2SingleFile))
                {
                    return fail(err, "r2s.saveData2SingleFile must be bool");
                }
                if (!readBool(*sec, "asyncFileWrite", &cfg->r2s.asyncFileWrite))
                {
                    return fail(err, "r2s.asyncFileWrite must be bool");
                }
            }
            return true;
        }

        bool applyBridgeSection(const Struct &root, AcqR2SNodeConfig *cfg, std::string *err)
        {
            if (const Struct *sec = findObject(root, "bridge"))
            {
                if (!readBool(*sec, "enabled", &cfg->bridge.enabled))
                {
                    return fail(err, "bridge.enabled must be bool");
                }
                if (!readUInt(*sec, "queueCapacity", &cfg->bridge.queueCapacity))
                {
                    return fail(err, "bridge.queueCapacity must be non-negative integer");
                }
                if (!readUInt(*sec, "reservePacketsPerSlot", &cfg->bridge.reservePacketsPerSlot))
                {
                    return fail(err, "bridge.reservePacketsPerSlot must be non-negative integer");
                }
                if (!readUInt(*sec, "reserveBytesPerSlot", &cfg->bridge.reserveBytesPerSlot))
                {
                    return fail(err, "bridge.reserveBytesPerSlot must be non-negative integer");
                }
                if (!readBool(*sec, "blockWhenQueueFull", &cfg->bridge.blockWhenQueueFull))
                {
                    return fail(err, "bridge.blockWhenQueueFull must be bool");
                }
                if (!readUInt(*sec, "queueFullWarnEvery", &cfg->bridge.queueFullWarnEvery))
                {
                    return fail(err, "bridge.queueFullWarnEvery must be non-negative integer");
                }
                if (!readUInt(*sec, "inputChannelCount", &cfg->bridge.inputChannelCount))
                {
                    return fail(err, "bridge.inputChannelCount must be non-negative integer");
                }
            }
            return true;
        }

        bool applyCoinClientSection(const Struct &root, AcqR2SNodeConfig *cfg, std::string *err)
        {
            if (const Struct *sec = findObject(root, "coinClient"))
            {
                if (!readBool(*sec, "enabled", &cfg->coinClient.enabled))
                {
                    return fail(err, "coinClient.enabled must be bool");
                }
                if (!readString(*sec, "serverAddress", &cfg->coinClient.serverAddress))
                {
                    return fail(err, "coinClient.serverAddress must be string");
                }
                if (!readUInt(*sec, "nodeId", &cfg->coinClient.nodeId))
                {
                    return fail(err, "coinClient.nodeId must be non-negative integer");
                }
                if (!readString(*sec, "nodeAddress", &cfg->coinClient.nodeAddress))
                {
                    return fail(err, "coinClient.nodeAddress must be string");
                }
                if (!readUInt(*sec, "channelCount", &cfg->coinClient.channelCount))
                {
                    return fail(err, "coinClient.channelCount must be non-negative integer");
                }
                if (!readString(*sec, "detectorType", &cfg->coinClient.detectorType))
                {
                    return fail(err, "coinClient.detectorType must be string");
                }
                if (!readBool(*sec, "remapLocalToGlobalChannels", &cfg->coinClient.remapLocalToGlobalChannels))
                {
                    return fail(err, "coinClient.remapLocalToGlobalChannels must be bool");
                }
                if (!readUInt(*sec, "globalChannelOffset", &cfg->coinClient.globalChannelOffset))
                {
                    return fail(err, "coinClient.globalChannelOffset must be non-negative integer");
                }
                if (!readUInt(*sec, "crystalsPerChannel", &cfg->coinClient.crystalsPerChannel))
                {
                    return fail(err, "coinClient.crystalsPerChannel must be non-negative integer");
                }
                if (!readUInt(*sec, "maxPendingChunks", &cfg->coinClient.maxPendingChunks))
                {
                    return fail(err, "coinClient.maxPendingChunks must be non-negative integer");
                }
                if (!readUInt(*sec, "batchSize", &cfg->coinClient.batchSize))
                {
                    return fail(err, "coinClient.batchSize must be non-negative integer");
                }
                if (!readUInt(*sec, "heartbeatIntervalMs", &cfg->coinClient.heartbeatIntervalMs))
                {
                    return fail(err, "coinClient.heartbeatIntervalMs must be non-negative integer");
                }
                if (!readBool(*sec, "waitForStartSignal", &cfg->coinClient.waitForStartSignal))
                {
                    return fail(err, "coinClient.waitForStartSignal must be bool");
                }
                if (!readUInt(*sec, "waitForStartTimeoutMs", &cfg->coinClient.waitForStartTimeoutMs))
                {
                    return fail(err, "coinClient.waitForStartTimeoutMs must be non-negative integer");
                }
                if (!readUInt(*sec, "waitForStartRpcTimeoutMs", &cfg->coinClient.waitForStartRpcTimeoutMs))
                {
                    return fail(err, "coinClient.waitForStartRpcTimeoutMs must be non-negative integer");
                }
                if (!readUInt(*sec, "waitForStartRetryIntervalMs", &cfg->coinClient.waitForStartRetryIntervalMs))
                {
                    return fail(err, "coinClient.waitForStartRetryIntervalMs must be non-negative integer");
                }
            }
            return true;
        }

        bool applyRuntimeSection(const Struct &root, AcqR2SNodeConfig *cfg, std::string *err)
        {
            if (const Struct *sec = findObject(root, "runtime"))
            {
                if (!readUInt(*sec, "shutdownGraceMs", &cfg->runtime.shutdownGraceMs))
                {
                    return fail(err, "runtime.shutdownGraceMs must be non-negative integer");
                }
                if (!readBool(*sec, "enableCpuAffinity", &cfg->runtime.enableCpuAffinity))
                {
                    return fail(err, "runtime.enableCpuAffinity must be bool");
                }
                if (!readUInt16Array(*sec, "cpuAffinityCores", &cfg->runtime.cpuAffinityCores))
                {
                    return fail(err, "runtime.cpuAffinityCores must be integer array in [0,65535]");
                }
                if (!readBool(*sec, "strictBindIpsOwnershipCheck", &cfg->runtime.strictBindIpsOwnershipCheck))
                {
                    return fail(err, "runtime.strictBindIpsOwnershipCheck must be bool");
                }
                if (!readBool(*sec, "strictNumaTopologyCheck", &cfg->runtime.strictNumaTopologyCheck))
                {
                    return fail(err, "runtime.strictNumaTopologyCheck must be bool");
                }
                if (!readBool(*sec, "requireBindIpsSingleNuma", &cfg->runtime.requireBindIpsSingleNuma))
                {
                    return fail(err, "runtime.requireBindIpsSingleNuma must be bool");
                }
                if (!readBool(*sec, "requireCpuAffinityOnNuma", &cfg->runtime.requireCpuAffinityOnNuma))
                {
                    return fail(err, "runtime.requireCpuAffinityOnNuma must be bool");
                }
                if (!readInt(*sec, "expectedNumaNode", &cfg->runtime.expectedNumaNode))
                {
                    return fail(err, "runtime.expectedNumaNode must be integer");
                }

                if (cfg->runtime.enableCpuAffinity && cfg->runtime.cpuAffinityCores.empty())
                {
                    return fail(err, "runtime.cpuAffinityCores must not be empty when runtime.enableCpuAffinity=true");
                }
                if (cfg->runtime.expectedNumaNode < -1)
                {
                    return fail(err, "runtime.expectedNumaNode must be -1 or >=0");
                }
            }
            return true;
        }

        bool applyCoinMasterSection(const Struct &root, CoinMasterConfig *cfg, std::string *err)
        {
            if (const Struct *sec = findObject(root, "coinMaster"))
            {
                if (!readString(*sec, "listenAddress", &cfg->coinMaster.listenAddress))
                {
                    return fail(err, "coinMaster.listenAddress must be string");
                }
                if (!readUInt(*sec, "expectedNodeCount", &cfg->coinMaster.expectedNodeCount))
                {
                    return fail(err, "coinMaster.expectedNodeCount must be non-negative integer");
                }
                if (!readBool(*sec, "autoStartWhenAllRegistered", &cfg->coinMaster.autoStartWhenAllRegistered))
                {
                    return fail(err, "coinMaster.autoStartWhenAllRegistered must be bool");
                }
                if (!readUInt(*sec, "startLeadTimeMs", &cfg->coinMaster.startLeadTimeMs))
                {
                    return fail(err, "coinMaster.startLeadTimeMs must be non-negative integer");
                }
                if (!readUInt(*sec, "waitForStartDefaultTimeoutMs", &cfg->coinMaster.waitForStartDefaultTimeoutMs))
                {
                    return fail(err, "coinMaster.waitForStartDefaultTimeoutMs must be non-negative integer");
                }
                if (!readBool(*sec, "rejectStreamBeforeStart", &cfg->coinMaster.rejectStreamBeforeStart))
                {
                    return fail(err, "coinMaster.rejectStreamBeforeStart must be bool");
                }
                if (!readUInt(*sec, "statusPrintIntervalMs", &cfg->coinMaster.statusPrintIntervalMs))
                {
                    return fail(err, "coinMaster.statusPrintIntervalMs must be non-negative integer");
                }
                if (!readUInt(*sec, "runSeconds", &cfg->coinMaster.runSeconds))
                {
                    return fail(err, "coinMaster.runSeconds must be non-negative integer");
                }
            }
            return true;
        }

        bool applyAlignerSection(const Struct &root, CoinMasterConfig *cfg, std::string *err)
        {
            if (const Struct *sec = findObject(root, "aligner"))
            {
                if (!readString(*sec, "outputDir", &cfg->aligner.outputDir))
                {
                    return fail(err, "aligner.outputDir must be string");
                }
                if (!readUInt(*sec, "channelNum", &cfg->aligner.channelNum))
                {
                    return fail(err, "aligner.channelNum must be non-negative integer");
                }
                if (!readUInt(*sec, "crystalsPerChannel", &cfg->aligner.crystalsPerChannel))
                {
                    return fail(err, "aligner.crystalsPerChannel must be non-negative integer");
                }
                if (!readUInt(*sec, "networkLatencyMarginPico", &cfg->aligner.networkLatencyMarginPico))
                {
                    return fail(err, "aligner.networkLatencyMarginPico must be non-negative integer");
                }
                if (!readUInt(*sec, "processingIntervalMs", &cfg->aligner.processingIntervalMs))
                {
                    return fail(err, "aligner.processingIntervalMs must be non-negative integer");
                }
                if (!readUInt(*sec, "maxChunksPerNode", &cfg->aligner.maxChunksPerNode))
                {
                    return fail(err, "aligner.maxChunksPerNode must be non-negative integer");
                }
                if (!readUInt(*sec, "maxTotalMemoryBytes", &cfg->aligner.maxTotalMemoryBytes))
                {
                    return fail(err, "aligner.maxTotalMemoryBytes must be non-negative integer");
                }
                if (!readBool(*sec, "useMemoryPool", &cfg->aligner.useMemoryPool))
                {
                    return fail(err, "aligner.useMemoryPool must be bool");
                }
                if (!readBool(*sec, "savePrompt", &cfg->aligner.savePrompt))
                {
                    return fail(err, "aligner.savePrompt must be bool");
                }
                if (!readBool(*sec, "saveDelay", &cfg->aligner.saveDelay))
                {
                    return fail(err, "aligner.saveDelay must be bool");
                }

                if (const Struct *protoSec = findObject(*sec, "coinProtocol"))
                {
                    if (!readUInt(*protoSec, "timeWindowPs", &cfg->aligner.coinProtocol.timeWindowPs))
                    {
                        return fail(err, "aligner.coinProtocol.timeWindowPs must be non-negative integer");
                    }
                    if (!readUInt(*protoSec, "delayTimePs", &cfg->aligner.coinProtocol.delayTimePs))
                    {
                        return fail(err, "aligner.coinProtocol.delayTimePs must be non-negative integer");
                    }
                    if (!readFloat(*protoSec, "energyLowerEV", &cfg->aligner.coinProtocol.energyLowerEV))
                    {
                        return fail(err, "aligner.coinProtocol.energyLowerEV must be number");
                    }
                    if (!readFloat(*protoSec, "energyUpperEV", &cfg->aligner.coinProtocol.energyUpperEV))
                    {
                        return fail(err, "aligner.coinProtocol.energyUpperEV must be number");
                    }
                }
            }
            return true;
        }

        bool applyAcqControlSection(const Struct &root, CoinMasterConfig *cfg, std::string *err)
        {
            if (const Struct *sec = findObject(root, "acquisitionControl"))
            {
                if (!readBool(*sec, "enabled", &cfg->acquisitionControl.enabled))
                {
                    return fail(err, "acquisitionControl.enabled must be bool");
                }
                if (!readString(*sec, "masterAddress", &cfg->acquisitionControl.masterAddress))
                {
                    return fail(err, "acquisitionControl.masterAddress must be string");
                }
                if (!readBool(*sec, "autoDistributeWhenAllConnected", &cfg->acquisitionControl.autoDistributeWhenAllConnected))
                {
                    return fail(err, "acquisitionControl.autoDistributeWhenAllConnected must be bool");
                }
                if (!readBool(*sec, "autoStartOnCoinStartSignal", &cfg->acquisitionControl.autoStartOnCoinStartSignal))
                {
                    return fail(err, "acquisitionControl.autoStartOnCoinStartSignal must be bool");
                }
                if (!readUInt(*sec, "startDurationMs", &cfg->acquisitionControl.startDurationMs))
                {
                    return fail(err, "acquisitionControl.startDurationMs must be non-negative integer");
                }

                std::string algo;
                if (!readString(*sec, "acquisitionAlgorithm", &algo))
                {
                    return fail(err, "acquisitionControl.acquisitionAlgorithm must be string");
                }
                if (!algo.empty())
                {
                    if (!parseAcquisitionAlgorithm(algo, &cfg->acquisitionControl.acquisitionAlgorithm))
                    {
                        return fail(err, "acquisitionControl.acquisitionAlgorithm must be one of [socket, dpdk]");
                    }
                }

                if (!readUInt(*sec, "sourcePortBase", &cfg->acquisitionControl.sourcePortBase))
                {
                    return fail(err, "acquisitionControl.sourcePortBase must be non-negative integer");
                }
                if (!readUInt(*sec, "destinationPortBase", &cfg->acquisitionControl.destinationPortBase))
                {
                    return fail(err, "acquisitionControl.destinationPortBase must be non-negative integer");
                }
                if (!readUInt(*sec, "channelCount", &cfg->acquisitionControl.channelCount))
                {
                    return fail(err, "acquisitionControl.channelCount must be non-negative integer");
                }
                if (!readString(*sec, "sourceIp", &cfg->acquisitionControl.sourceIp))
                {
                    return fail(err, "acquisitionControl.sourceIp must be string");
                }

                if (const Value *sourcesValue = findField(*sec, "detectorSources"))
                {
                    if (sourcesValue->kind_case() != Value::kListValue)
                    {
                        return fail(err, "acquisitionControl.detectorSources must be an array");
                    }

                    std::vector<AcqControlSection::DetectorSource> sources;
                    sources.reserve(static_cast<size_t>(sourcesValue->list_value().values_size()));

                    size_t idx = 0;
                    for (const auto &item : sourcesValue->list_value().values())
                    {
                        if (item.kind_case() != Value::kStructValue)
                        {
                            return fail(err, "acquisitionControl.detectorSources entries must be objects");
                        }

                        const Struct &itemObj = item.struct_value();
                        AcqControlSection::DetectorSource source;
                        if (!readString(itemObj, "detectorId", &source.detectorId))
                        {
                            return fail(err, "acquisitionControl.detectorSources[].detectorId must be string");
                        }
                        if (!readString(itemObj, "sourceIp", &source.sourceIp))
                        {
                            return fail(err, "acquisitionControl.detectorSources[].sourceIp must be string");
                        }
                        if (!readUInt(itemObj, "sourcePort", &source.sourcePort))
                        {
                            return fail(err, "acquisitionControl.detectorSources[].sourcePort must be non-negative integer");
                        }

                        if (source.sourceIp.empty())
                        {
                            return fail(err, "acquisitionControl.detectorSources[].sourceIp must not be empty");
                        }
                        if (source.sourcePort == 0)
                        {
                            return fail(err, "acquisitionControl.detectorSources[].sourcePort must be > 0");
                        }

                        if (source.detectorId.empty())
                        {
                            source.detectorId = "detector-" + std::to_string(idx);
                        }

                        sources.push_back(std::move(source));
                        ++idx;
                    }

                    cfg->acquisitionControl.detectorSources = std::move(sources);
                }

                if (!readString(*sec, "destinationIp", &cfg->acquisitionControl.destinationIp))
                {
                    return fail(err, "acquisitionControl.destinationIp must be string");
                }
                if (!readString(*sec, "sessionName", &cfg->acquisitionControl.sessionName))
                {
                    return fail(err, "acquisitionControl.sessionName must be string");
                }
                if (!readUInt(*sec, "storageUnitSize", &cfg->acquisitionControl.storageUnitSize))
                {
                    return fail(err, "acquisitionControl.storageUnitSize must be non-negative integer");
                }
                if (!readUInt(*sec, "minPacketSize", &cfg->acquisitionControl.minPacketSize))
                {
                    return fail(err, "acquisitionControl.minPacketSize must be non-negative integer");
                }
                if (!readUInt(*sec, "maxBufferSize", &cfg->acquisitionControl.maxBufferSize))
                {
                    return fail(err, "acquisitionControl.maxBufferSize must be non-negative integer");
                }
                if (!readUInt(*sec, "timeSwitchBufferMs", &cfg->acquisitionControl.timeSwitchBufferMs))
                {
                    return fail(err, "acquisitionControl.timeSwitchBufferMs must be non-negative integer");
                }
                if (!readUInt(*sec, "reservedStorageGiB", &cfg->acquisitionControl.reservedStorageGiB))
                {
                    return fail(err, "acquisitionControl.reservedStorageGiB must be non-negative integer");
                }
                if (!readUInt(*sec, "maxFileSizeMb", &cfg->acquisitionControl.maxFileSizeMb))
                {
                    return fail(err, "acquisitionControl.maxFileSizeMb must be non-negative integer");
                }
                if (!readUInt(*sec, "dpdkCopyThreadNum", &cfg->acquisitionControl.dpdkCopyThreadNum))
                {
                    return fail(err, "acquisitionControl.dpdkCopyThreadNum must be non-negative integer");
                }
                if (!readUInt(*sec, "dpdkRxRingsPerPort", &cfg->acquisitionControl.dpdkRxRingsPerPort))
                {
                    return fail(err, "acquisitionControl.dpdkRxRingsPerPort must be non-negative integer");
                }
                if (!readUInt(*sec, "dpdkMbufDoublePointerSizeMultiply", &cfg->acquisitionControl.dpdkMbufDoublePointerSizeMultiply))
                {
                    return fail(err, "acquisitionControl.dpdkMbufDoublePointerSizeMultiply must be non-negative integer");
                }
                if (!readUInt(*sec, "dpdkMbufDoublePointerNumMultiply", &cfg->acquisitionControl.dpdkMbufDoublePointerNumMultiply))
                {
                    return fail(err, "acquisitionControl.dpdkMbufDoublePointerNumMultiply must be non-negative integer");
                }
                if (!readStringArray(*sec, "dpdkBindIps", &cfg->acquisitionControl.dpdkBindIps))
                {
                    return fail(err, "acquisitionControl.dpdkBindIps must be non-empty string array");
                }

                if (const Value *overridesValue = findField(*sec, "nodeOverrides"))
                {
                    if (overridesValue->kind_case() != Value::kListValue)
                    {
                        return fail(err, "acquisitionControl.nodeOverrides must be an array");
                    }

                    std::vector<AcqControlSection::NodeOverride> overrides;
                    overrides.reserve(static_cast<size_t>(overridesValue->list_value().values_size()));
                    std::unordered_set<std::string> usedNodeIds;

                    for (const auto &item : overridesValue->list_value().values())
                    {
                        if (item.kind_case() != Value::kStructValue)
                        {
                            return fail(err, "acquisitionControl.nodeOverrides entries must be objects");
                        }

                        AcqControlSection::NodeOverride overrideCfg;
                        const Struct &itemObj = item.struct_value();

                        if (!readString(itemObj, "nodeId", &overrideCfg.nodeId) || overrideCfg.nodeId.empty())
                        {
                            return fail(err, "acquisitionControl.nodeOverrides[].nodeId must be non-empty string");
                        }
                        if (!usedNodeIds.insert(overrideCfg.nodeId).second)
                        {
                            return fail(err, "acquisitionControl.nodeOverrides contains duplicated nodeId: " + overrideCfg.nodeId);
                        }

                        std::string nodeAlgo;
                        if (!readString(itemObj, "acquisitionAlgorithm", &nodeAlgo))
                        {
                            return fail(err, "acquisitionControl.nodeOverrides[].acquisitionAlgorithm must be string");
                        }
                        if (!parseNodeAlgorithmOverride(nodeAlgo, &overrideCfg.acquisitionAlgorithm))
                        {
                            return fail(err, "acquisitionControl.nodeOverrides[].acquisitionAlgorithm must be one of [inherit, socket, dpdk]");
                        }

                        if (!readUInt(itemObj, "dpdkCopyThreadNum", &overrideCfg.dpdkCopyThreadNum))
                        {
                            return fail(err, "acquisitionControl.nodeOverrides[].dpdkCopyThreadNum must be non-negative integer");
                        }
                        if (!readUInt(itemObj, "dpdkRxRingsPerPort", &overrideCfg.dpdkRxRingsPerPort))
                        {
                            return fail(err, "acquisitionControl.nodeOverrides[].dpdkRxRingsPerPort must be non-negative integer");
                        }
                        if (!readUInt(itemObj, "dpdkMbufDoublePointerSizeMultiply", &overrideCfg.dpdkMbufDoublePointerSizeMultiply))
                        {
                            return fail(err, "acquisitionControl.nodeOverrides[].dpdkMbufDoublePointerSizeMultiply must be non-negative integer");
                        }
                        if (!readUInt(itemObj, "dpdkMbufDoublePointerNumMultiply", &overrideCfg.dpdkMbufDoublePointerNumMultiply))
                        {
                            return fail(err, "acquisitionControl.nodeOverrides[].dpdkMbufDoublePointerNumMultiply must be non-negative integer");
                        }
                        if (!readStringArray(itemObj, "dpdkBindIps", &overrideCfg.dpdkBindIps))
                        {
                            return fail(err, "acquisitionControl.nodeOverrides[].dpdkBindIps must be non-empty string array");
                        }

                        overrides.push_back(std::move(overrideCfg));
                    }

                    cfg->acquisitionControl.nodeOverrides = std::move(overrides);
                }
            }
            return true;
        }

    } // namespace

    bool loadAcqR2SNodeConfig(const std::string &path, AcqR2SNodeConfig *cfg, std::string *errorMessage)
    {
        if (!cfg)
        {
            return fail(errorMessage, "loadAcqR2SNodeConfig cfg is null");
        }

        Struct root;
        if (!loadRoot(path, &root, errorMessage))
        {
            return false;
        }

        return applyAcqNodeSection(root, cfg, errorMessage) &&
               applyR2SSection(root, cfg, errorMessage) &&
               applyBridgeSection(root, cfg, errorMessage) &&
               applyCoinClientSection(root, cfg, errorMessage) &&
               applyRuntimeSection(root, cfg, errorMessage);
    }

    bool loadCoinMasterConfig(const std::string &path, CoinMasterConfig *cfg, std::string *errorMessage)
    {
        if (!cfg)
        {
            return fail(errorMessage, "loadCoinMasterConfig cfg is null");
        }

        Struct root;
        if (!loadRoot(path, &root, errorMessage))
        {
            return false;
        }

        return applyCoinMasterSection(root, cfg, errorMessage) &&
               applyAlignerSection(root, cfg, errorMessage) &&
               applyAcqControlSection(root, cfg, errorMessage);
    }

} // namespace openpni::distributed::app
