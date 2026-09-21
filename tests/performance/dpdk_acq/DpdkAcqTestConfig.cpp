#include "tests/performance/dpdk_acq/DpdkAcqTestConfig.hpp"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <cmath>
#include <fstream>
#include <limits>
#include <sstream>

namespace openpni::distributed::test::dpdk_acq
{
    namespace
    {
        using google::protobuf::Struct;
        using google::protobuf::Value;

        bool fail(std::string *err, const std::string &message)
        {
            if (err)
            {
                *err = message;
            }
            return false;
        }

        const Struct *findObject(const Struct &root, const std::string &key)
        {
            const auto it = root.fields().find(key);
            if (it == root.fields().end() || it->second.kind_case() != Value::kStructValue)
            {
                return nullptr;
            }
            return &it->second.struct_value();
        }

        bool readString(const Struct &obj, const std::string &key, std::string *out)
        {
            const auto it = obj.fields().find(key);
            if (it == obj.fields().end())
            {
                return true;
            }
            if (it->second.kind_case() != Value::kStringValue)
            {
                return false;
            }
            *out = it->second.string_value();
            return true;
        }

        bool readBool(const Struct &obj, const std::string &key, bool *out)
        {
            const auto it = obj.fields().find(key);
            if (it == obj.fields().end())
            {
                return true;
            }
            if (it->second.kind_case() != Value::kBoolValue)
            {
                return false;
            }
            *out = it->second.bool_value();
            return true;
        }

        bool asUint64(const Value &v, uint64_t *out)
        {
            if (v.kind_case() == Value::kNumberValue)
            {
                const double n = v.number_value();
                if (!std::isfinite(n) || n < 0.0 || n > static_cast<double>(std::numeric_limits<uint64_t>::max()))
                {
                    return false;
                }
                *out = static_cast<uint64_t>(n);
                return true;
            }
            if (v.kind_case() == Value::kStringValue)
            {
                try
                {
                    *out = std::stoull(v.string_value());
                    return true;
                }
                catch (...)
                {
                    return false;
                }
            }
            return false;
        }

        bool readUInt(const Struct &obj, const std::string &key, uint32_t *out)
        {
            const auto it = obj.fields().find(key);
            if (it == obj.fields().end())
            {
                return true;
            }
            uint64_t v = 0;
            if (!asUint64(it->second, &v) || v > std::numeric_limits<uint32_t>::max())
            {
                return false;
            }
            *out = static_cast<uint32_t>(v);
            return true;
        }

        bool readUInt64(const Struct &obj, const std::string &key, uint64_t *out)
        {
            const auto it = obj.fields().find(key);
            if (it == obj.fields().end())
            {
                return true;
            }
            return asUint64(it->second, out);
        }

        bool readSize(const Struct &obj, const std::string &key, size_t *out)
        {
            uint64_t v = 0;
            const auto it = obj.fields().find(key);
            if (it == obj.fields().end())
            {
                return true;
            }
            if (!asUint64(it->second, &v))
            {
                return false;
            }
            *out = static_cast<size_t>(v);
            return true;
        }

        bool readUInt16(const Struct &obj, const std::string &key, uint16_t *out)
        {
            uint32_t v = 0;
            if (!readUInt(obj, key, &v))
            {
                return false;
            }
            if (obj.fields().find(key) == obj.fields().end())
            {
                return true;
            }
            if (v > 65535)
            {
                return false;
            }
            *out = static_cast<uint16_t>(v);
            return true;
        }

        bool readInt32(const Struct &obj, const std::string &key, int32_t *out)
        {
            const auto it = obj.fields().find(key);
            if (it == obj.fields().end())
            {
                return true;
            }
            if (it->second.kind_case() != Value::kNumberValue)
            {
                return false;
            }
            *out = static_cast<int32_t>(it->second.number_value());
            return true;
        }

        bool readStringArray(const Struct &obj, const std::string &key, std::vector<std::string> *out)
        {
            const auto it = obj.fields().find(key);
            if (it == obj.fields().end())
            {
                return true;
            }
            if (it->second.kind_case() != Value::kListValue)
            {
                return false;
            }
            std::vector<std::string> values;
            for (const auto &item : it->second.list_value().values())
            {
                if (item.kind_case() != Value::kStringValue)
                {
                    return false;
                }
                values.push_back(item.string_value());
            }
            *out = std::move(values);
            return true;
        }

        bool readUInt16Array(const Struct &obj, const std::string &key, std::vector<uint16_t> *out)
        {
            const auto it = obj.fields().find(key);
            if (it == obj.fields().end())
            {
                return true;
            }
            if (it->second.kind_case() != Value::kListValue)
            {
                return false;
            }
            std::vector<uint16_t> values;
            for (const auto &item : it->second.list_value().values())
            {
                uint64_t v = 0;
                if (!asUint64(item, &v) || v > 65535)
                {
                    return false;
                }
                values.push_back(static_cast<uint16_t>(v));
            }
            *out = std::move(values);
            return true;
        }
    } // namespace

    bool loadDpdkAcqTestConfig(const std::string &path, DpdkAcqTestConfig *cfg, std::string *errorMessage)
    {
        if (!cfg)
        {
            return fail(errorMessage, "config output is null");
        }

        std::ifstream ifs(path);
        if (!ifs)
        {
            return fail(errorMessage, "failed to open file: " + path);
        }
        std::ostringstream oss;
        oss << ifs.rdbuf();

        Struct root;
        google::protobuf::util::JsonParseOptions opts;
        opts.ignore_unknown_fields = false;
        const auto status = google::protobuf::util::JsonStringToMessage(oss.str(), &root, opts);
        if (!status.ok())
        {
            return fail(errorMessage, "json parse error: " + std::string(status.message()));
        }

        if (const Struct *sec = findObject(root, "master"))
        {
            if (!readString(*sec, "listenAddress", &cfg->master.listenAddress) ||
                !readUInt(*sec, "expectedNodeCount", &cfg->master.expectedNodeCount) ||
                !readUInt(*sec, "statusIntervalMs", &cfg->master.statusIntervalMs) ||
                !readUInt(*sec, "startDurationMs", &cfg->master.startDurationMs) ||
                !readUInt(*sec, "waitConnectTimeoutMs", &cfg->master.waitConnectTimeoutMs) ||
                !readUInt(*sec, "shutdownGraceMs", &cfg->master.shutdownGraceMs))
            {
                return fail(errorMessage, "invalid master section");
            }
        }

        if (const Struct *sec = findObject(root, "node"))
        {
            if (!readString(*sec, "masterAddress", &cfg->node.masterAddress) ||
                !readString(*sec, "nodeId", &cfg->node.nodeId) ||
                !readString(*sec, "nodeAddress", &cfg->node.nodeAddress) ||
                !readBool(*sec, "enableRawFileWrite", &cfg->node.enableRawFileWrite) ||
                !readString(*sec, "outputRoot", &cfg->node.outputRoot) ||
                !readString(*sec, "sessionNamePrefix", &cfg->node.sessionNamePrefix) ||
                !readSize(*sec, "maxFileSizeMb", &cfg->node.maxFileSizeMb) ||
                !readBool(*sec, "overwriteExisting", &cfg->node.overwriteExisting) ||
                !readUInt64(*sec, "reservedStorageGiB", &cfg->node.reservedStorageGiB) ||
                !readBool(*sec, "fsyncEachSegment", &cfg->node.fsyncEachSegment) ||
                !readBool(*sec, "enableCpuAffinity", &cfg->node.enableCpuAffinity) ||
                !readUInt16Array(*sec, "cpuAffinityCores", &cfg->node.cpuAffinityCores) ||
                !readBool(*sec, "strictBindIpsOwnershipCheck", &cfg->node.strictBindIpsOwnershipCheck) ||
                !readBool(*sec, "strictNumaTopologyCheck", &cfg->node.strictNumaTopologyCheck) ||
                !readBool(*sec, "requireBindIpsSingleNuma", &cfg->node.requireBindIpsSingleNuma) ||
                !readBool(*sec, "requireCpuAffinityOnNuma", &cfg->node.requireCpuAffinityOnNuma) ||
                !readInt32(*sec, "expectedNumaNode", &cfg->node.expectedNumaNode))
            {
                return fail(errorMessage, "invalid node section");
            }
        }

        if (const Struct *sec = findObject(root, "acquisition"))
        {
            if (!readUInt16(*sec, "channelCount", &cfg->acquisition.channelCount) ||
                !readString(*sec, "sourceIp", &cfg->acquisition.sourceIp) ||
                !readUInt16(*sec, "sourcePortBase", &cfg->acquisition.sourcePortBase) ||
                !readString(*sec, "destinationIp", &cfg->acquisition.destinationIp) ||
                !readUInt16(*sec, "destinationPortBase", &cfg->acquisition.destinationPortBase) ||
                !readString(*sec, "sessionName", &cfg->acquisition.sessionName) ||
                !readUInt(*sec, "storageUnitSize", &cfg->acquisition.storageUnitSize) ||
                !readUInt(*sec, "minPacketSize", &cfg->acquisition.minPacketSize) ||
                !readUInt64(*sec, "maxBufferSize", &cfg->acquisition.maxBufferSize) ||
                !readUInt(*sec, "timeSwitchBufferMs", &cfg->acquisition.timeSwitchBufferMs) ||
                !readUInt(*sec, "dpdkRxRingsPerPort", &cfg->acquisition.dpdkRxRingsPerPort) ||
                !readUInt(*sec, "dpdkMbufPoolSize", &cfg->acquisition.dpdkMbufPoolSize) ||
                !readUInt(*sec, "dpdkMbufCacheSize", &cfg->acquisition.dpdkMbufCacheSize) ||
                !readStringArray(*sec, "dpdkBindIps", &cfg->acquisition.dpdkBindIps) ||
                !readStringArray(*sec, "dpdkExtraEalArgs", &cfg->acquisition.dpdkExtraEalArgs) ||
                !readString(*sec, "dpdkLocalLoopbackIface", &cfg->acquisition.dpdkLocalLoopbackIface))
            {
                return fail(errorMessage, "invalid acquisition section");
            }

            const auto it = sec->fields().find("detectorSources");
            if (it != sec->fields().end())
            {
                if (it->second.kind_case() != Value::kListValue)
                {
                    return fail(errorMessage, "acquisition.detectorSources must be an array");
                }
                std::vector<DetectorSource> sources;
                for (const auto &item : it->second.list_value().values())
                {
                    if (item.kind_case() != Value::kStructValue)
                    {
                        return fail(errorMessage, "acquisition.detectorSources entries must be objects");
                    }
                    const Struct &entry = item.struct_value();
                    DetectorSource det;
                    if (!readString(entry, "detectorId", &det.detectorId) ||
                        !readString(entry, "sourceIp", &det.sourceIp) ||
                        !readUInt16(entry, "sourcePort", &det.sourcePort))
                    {
                        return fail(errorMessage, "invalid detectorSources entry");
                    }
                    sources.push_back(std::move(det));
                }
                cfg->acquisition.detectorSources = std::move(sources);
            }
        }

        if (cfg->node.enableCpuAffinity && cfg->node.cpuAffinityCores.empty())
        {
            return fail(errorMessage, "node.cpuAffinityCores must not be empty when enableCpuAffinity=true");
        }
        if (cfg->acquisition.dpdkBindIps.empty())
        {
            return fail(errorMessage, "acquisition.dpdkBindIps must not be empty");
        }
        return true;
    }
}
