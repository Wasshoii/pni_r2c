#include "grpcNode/acquisitionNode.hpp"

#include "core/acquisition/AcquisitionServer.hpp"

#include "protos/acquisition.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/sync_stream.h>

#include <glog/logging.h>

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <ifaddrs.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <sys/sysinfo.h>
#include <thread>
#include <unordered_map>

namespace openpni::distributed::grpcnode
{
    namespace acqproto = openpni::distributed::acquisition;

    namespace
    {
        std::vector<std::string> splitList(const std::string &value)
        {
            std::vector<std::string> items;
            std::string current;
            for (char ch : value)
            {
                if (ch == ',' || ch == ';')
                {
                    if (!current.empty())
                    {
                        items.push_back(current);
                        current.clear();
                    }
                    continue;
                }
                current.push_back(ch);
            }
            if (!current.empty())
            {
                items.push_back(current);
            }
            return items;
        }

        std::optional<acqproto::StorageConfig::ShardStrategy> parseShardStrategy(const std::string &value)
        {
            if (value.empty())
            {
                return std::nullopt;
            }
            if (value == "HashByChannel" || value == "hashbychannel" || value == "HASHBYCHANNEL")
            {
                return acqproto::StorageConfig::ShardStrategy::HashByChannel;
            }
            if (value == "FreeSpaceAware" || value == "freespaceaware" || value == "FREESPACEAWARE")
            {
                return acqproto::StorageConfig::ShardStrategy::FreeSpaceAware;
            }
            if (value == "RoundRobin" || value == "roundrobin" || value == "ROUNDROBIN")
            {
                return acqproto::StorageConfig::ShardStrategy::RoundRobin;
            }
            return std::nullopt;
        }

        template <typename T>
        void applyEnvIfEmpty(const char *name, T &target)
        {
            if (const char *value = std::getenv(name); value && value[0] != '\0')
            {
                target = value;
            }
        }
    }

    class AcquisitionGrpcNode::Impl
    {
    public:
        class INodeRuntime
        {
        public:
            virtual ~INodeRuntime() = default;
            virtual bool Start() = 0;
            virtual void Stop() = 0;
            virtual void SetFileCompleteCallback(FileReadyCallback callback) = 0;
            virtual void SetRawDataReadyCallback(RawDataReadyCallback callback) = 0;
            virtual void SetStatusReportCallback(std::function<void(const acqproto::NodeStatus &)> callback) = 0;
            virtual void SetDeferRawDataRelease(bool defer) = 0;
            virtual void ReleaseRawData(uint64_t packetCount) = 0;
        };

        template <typename AlgoType>
        class NodeRuntimeImpl final : public INodeRuntime
        {
        public:
            NodeRuntimeImpl(openpni::AcquisitionInfo acqInfo, acqproto::StorageConfig storageConfig)
                : m_node(std::make_unique<acqproto::DistributedAcquisitionNode<AlgoType>>(std::move(acqInfo), std::move(storageConfig)))
            {
            }

            bool Start() override
            {
                return m_node->Start();
            }

            void Stop() override
            {
                m_node->Stop();
            }

            void SetFileCompleteCallback(FileReadyCallback callback) override
            {
                m_node->SetFileCompleteCallback(std::move(callback));
            }

            void SetRawDataReadyCallback(RawDataReadyCallback callback) override
            {
                m_node->SetRawDataReadyCallback(std::move(callback));
            }

            void SetStatusReportCallback(std::function<void(const acqproto::NodeStatus &)> callback) override
            {
                m_node->SetStatusReportCallback(std::move(callback));
            }

            void SetDeferRawDataRelease(bool defer) override
            {
                m_node->SetDeferRawDataRelease(defer);
            }

            void ReleaseRawData(uint64_t packetCount) override
            {
                m_node->MakeRawDataReleaseFn()(packetCount);
            }

        private:
            std::unique_ptr<acqproto::DistributedAcquisitionNode<AlgoType>> m_node;
        };

        explicit Impl(InitOptions options)
            : init_(std::move(options))
        {
            lastStatus_.set_state(acqproto::STATE_IDLE);
        }

        ~Impl()
        {
            stop();
        }

        void setFileReadyCallback(FileReadyCallback callback)
        {
            std::lock_guard<std::mutex> lock(runtimeMutex_);
            fileReadyCallback_ = std::move(callback);
            if (runtimeNode_ && fileReadyCallback_)
            {
                runtimeNode_->SetFileCompleteCallback(fileReadyCallback_);
            }
        }

        void setRawDataReadyCallback(RawDataReadyCallback callback)
        {
            std::lock_guard<std::mutex> lock(runtimeMutex_);
            rawDataReadyCallback_ = std::move(callback);
            if (runtimeNode_ && rawDataReadyCallback_)
            {
                runtimeNode_->SetRawDataReadyCallback(rawDataReadyCallback_);
            }
        }

        void setDeferRawDataRelease(bool defer)
        {
            std::lock_guard<std::mutex> lock(runtimeMutex_);
            deferRawDataRelease_ = defer;
            if (runtimeNode_)
            {
                runtimeNode_->SetDeferRawDataRelease(defer);
            }
        }

        void releaseRawData(uint64_t packetCount)
        {
            INodeRuntime *node = nullptr;
            {
                std::lock_guard<std::mutex> lock(runtimeMutex_);
                node = runtimeNode_.get();
            }
            if (node)
            {
                node->ReleaseRawData(packetCount);
            }
        }

        bool run()
        {
            if (running_.exchange(true))
            {
                LOG(WARNING) << "[AcquisitionNode] already running";
                return false;
            }

            if (!openStream())
            {
                running_.store(false, std::memory_order_release);
                stream_.reset();
                context_.reset();
                stub_.reset();
                channel_.reset();
                return false;
            }

            statusThread_ = std::thread([this]()
                                        { statusLoop(); });

            acqproto::MasterCommand command;
            while (running_.load(std::memory_order_acquire) && stream_->Read(&command))
            {
                if (!handleCommand(command))
                {
                    setError("command handling failed");
                }
            }

            running_.store(false, std::memory_order_release);

            stopAcquisition(true);
            cleanupDurationThread();

            if (statusThread_.joinable())
            {
                statusThread_.join();
            }

            if (stream_)
            {
                stream_->WritesDone();
                const grpc::Status status = stream_->Finish();
                if (!status.ok())
                {
                    LOG(ERROR) << "[AcquisitionNode] Connect stream finished with error: "
                               << status.error_message();
                }
            }

            stream_.reset();
            context_.reset();
            stub_.reset();
            channel_.reset();

            return true;
        }

        void stop()
        {
            const bool wasRunning = running_.exchange(false, std::memory_order_acq_rel);
            if (!wasRunning)
            {
                cleanupDurationThread();
                return;
            }

            if (context_)
            {
                context_->TryCancel();
            }

            stopAcquisition(true);
            cleanupDurationThread();

            if (statusThread_.joinable())
            {
                statusThread_.join();
            }
        }

        acqproto::NodeState state() const
        {
            return state_.load(std::memory_order_acquire);
        }

    private:
        static std::unordered_map<std::string, std::string> parseCommandParams(const std::string &text)
        {
            std::unordered_map<std::string, std::string> params;
            std::stringstream ss(text);
            std::string item;

            while (std::getline(ss, item, ';'))
            {
                const auto pos = item.find('=');
                if (pos == std::string::npos)
                {
                    continue;
                }

                const auto key = item.substr(0, pos);
                const auto value = item.substr(pos + 1);
                if (!key.empty())
                {
                    params[key] = value;
                }
            }

            return params;
        }

        static uint64_t parseUint64(const std::unordered_map<std::string, std::string> &params, const std::string &key, uint64_t fallback)
        {
            const auto it = params.find(key);
            if (it == params.end())
            {
                return fallback;
            }

            try
            {
                return std::stoull(it->second);
            }
            catch (const std::exception &)
            {
                return fallback;
            }
        }

        static uint64_t nowMs()
        {
            return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
        }

        static std::string ipIntToString(uint32_t ip)
        {
            return std::to_string((ip >> 24) & 0xFF) + "." +
                   std::to_string((ip >> 16) & 0xFF) + "." +
                   std::to_string((ip >> 8) & 0xFF) + "." +
                   std::to_string(ip & 0xFF);
        }

        struct LocalIpBinding
        {
            std::string interfaceName;
            int numaNode = -1;
        };

        static bool readIntFromFile(const std::string &path, int *value)
        {
            std::ifstream ifs(path);
            if (!ifs)
            {
                return false;
            }
            int v = -1;
            if (!(ifs >> v))
            {
                return false;
            }
            *value = v;
            return true;
        }

        static int queryInterfaceNumaNode(const std::string &ifname)
        {
            int numaNode = -1;
            const std::string path = "/sys/class/net/" + ifname + "/device/numa_node";
            if (!readIntFromFile(path, &numaNode))
            {
                return -1;
            }
            return numaNode;
        }

        static int queryCpuNumaNode(uint32_t cpuCore)
        {
            const std::filesystem::path cpuPath =
                std::filesystem::path("/sys/devices/system/cpu") / ("cpu" + std::to_string(cpuCore));
            std::error_code ec;
            if (!std::filesystem::exists(cpuPath, ec))
            {
                return -1;
            }

            for (const auto &entry : std::filesystem::directory_iterator(cpuPath, ec))
            {
                if (ec)
                {
                    break;
                }
                const std::string name = entry.path().filename().string();
                if (!name.starts_with("node"))
                {
                    continue;
                }

                bool digitsOnly = true;
                for (size_t i = 4; i < name.size(); ++i)
                {
                    if (!std::isdigit(static_cast<unsigned char>(name[i])))
                    {
                        digitsOnly = false;
                        break;
                    }
                }

                if (digitsOnly && name.size() > 4)
                {
                    return std::stoi(name.substr(4));
                }
            }

            return -1;
        }

        static std::unordered_map<std::string, LocalIpBinding> collectLocalIpv4Bindings()
        {
            std::unordered_map<std::string, LocalIpBinding> result;

            struct ifaddrs *ifaddr = nullptr;
            if (::getifaddrs(&ifaddr) != 0 || !ifaddr)
            {
                return result;
            }

            for (struct ifaddrs *ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next)
            {
                if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET)
                {
                    continue;
                }
                if (!ifa->ifa_name)
                {
                    continue;
                }

                const auto *sin = reinterpret_cast<sockaddr_in *>(ifa->ifa_addr);
                char ipBuf[INET_ADDRSTRLEN] = {0};
                if (!::inet_ntop(AF_INET, &(sin->sin_addr), ipBuf, sizeof(ipBuf)))
                {
                    continue;
                }

                LocalIpBinding binding;
                binding.interfaceName = ifa->ifa_name;
                binding.numaNode = queryInterfaceNumaNode(binding.interfaceName);
                result[std::string(ipBuf)] = std::move(binding);
            }

            ::freeifaddrs(ifaddr);
            return result;
        }

        bool validateDpdkBindIpsAndNuma(const acqproto::NodeAcquisitionConfig &config)
        {
            if (config.dpdk.bind_ips.empty())
            {
                setError("DPDK bind_ips is empty");
                return false;
            }

            const auto localBindings = collectLocalIpv4Bindings();
            std::vector<std::string> unmatchedIps;
            std::set<int> bindIpNumaNodes;

            for (const auto &bindIp : config.dpdk.bind_ips)
            {
                const auto it = localBindings.find(bindIp);
                if (it == localBindings.end())
                {
                    unmatchedIps.push_back(bindIp);
                    continue;
                }
                if (it->second.numaNode >= 0)
                {
                    bindIpNumaNodes.insert(it->second.numaNode);
                }
            }

            if (!unmatchedIps.empty())
            {
                std::ostringstream oss;
                oss << "DPDK bind_ips not found on local NICs: ";
                for (size_t i = 0; i < unmatchedIps.size(); ++i)
                {
                    if (i > 0)
                    {
                        oss << ", ";
                    }
                    oss << unmatchedIps[i];
                }

                if (init_.strictBindIpsOwnershipCheck)
                {
                    setError(oss.str());
                    return false;
                }
                LOG(WARNING) << "[AcquisitionNode/DPDK] " << oss.str();
            }

            if (!init_.strictNumaTopologyCheck)
            {
                return true;
            }

            if (init_.requireBindIpsSingleNuma && bindIpNumaNodes.size() > 1)
            {
                std::ostringstream oss;
                oss << "DPDK bind_ips are spread across NUMA nodes: ";
                bool first = true;
                for (int node : bindIpNumaNodes)
                {
                    if (!first)
                    {
                        oss << ",";
                    }
                    first = false;
                    oss << node;
                }
                setError(oss.str());
                return false;
            }

            if (init_.expectedNumaNode >= 0)
            {
                for (int node : bindIpNumaNodes)
                {
                    if (node != init_.expectedNumaNode)
                    {
                        setError("DPDK bind_ips NUMA node does not match runtime.expectedNumaNode");
                        return false;
                    }
                }
            }

            if (init_.requireCpuAffinityOnNuma && !init_.cpuAffinityCores.empty())
            {
                const std::optional<int> bindNuma =
                    (bindIpNumaNodes.size() == 1) ? std::optional<int>(*bindIpNumaNodes.begin()) : std::nullopt;

                for (uint32_t core : init_.cpuAffinityCores)
                {
                    const int coreNuma = queryCpuNumaNode(core);
                    if (coreNuma < 0)
                    {
                        std::ostringstream oss;
                        oss << "Cannot resolve NUMA node for CPU core " << core;
                        setError(oss.str());
                        return false;
                    }

                    if (init_.expectedNumaNode >= 0 && coreNuma != init_.expectedNumaNode)
                    {
                        std::ostringstream oss;
                        oss << "CPU affinity core " << core << " is on NUMA " << coreNuma
                            << ", expected " << init_.expectedNumaNode;
                        setError(oss.str());
                        return false;
                    }

                    if (bindNuma.has_value() && coreNuma != bindNuma.value())
                    {
                        std::ostringstream oss;
                        oss << "CPU affinity core " << core << " NUMA=" << coreNuma
                            << " does not match DPDK bind_ips NUMA=" << bindNuma.value();
                        setError(oss.str());
                        return false;
                    }
                }
            }

            return true;
        }

        void logConfigureDetails(const acqproto::AcquisitionTask &task, const acqproto::StorageConfig &storageConfig)
        {
            std::ostringstream oss;

            const std::string algo = (task.algorithm_type() == acqproto::ALGORITHM_TYPE_DPDK) ? "DPDK" : "SOCKET";
            oss << "[AcquisitionNode/Config] node=" << init_.nodeId
                << " algorithm=" << algo
                << " storage_unit_size=" << task.storage_unit_size()
                << " min_packet_size=" << task.min_packet_size()
                << " max_buffer_size=" << task.max_buffer_size()
                << " time_switch_buffer_ms=" << task.time_switch_buffer_ms()
                << " session='" << storageConfig.session_name << "'"
                << " reserved_gib=" << storageConfig.total_reserved_gib
                << " max_file_size_mb=" << storageConfig.max_file_size_mb
                << "\n";

            if (task.has_dpdk_options())
            {
                const auto &dpdk = task.dpdk_options();
                oss << "[AcquisitionNode/Config] node=" << init_.nodeId
                    << " dpdk(copy_threads=" << dpdk.copy_thread_num()
                    << ",rx_rings_per_port=" << dpdk.rx_rings_per_port()
                    << ",ppsize_mul=" << dpdk.rte_mbuf_double_pointer_size_multiply()
                    << ",ppnum_mul=" << dpdk.rte_mbuf_double_pointer_num_multiply()
                    << ",bind_ips=" << dpdk.bind_ips_size() << ")"
                    << "\n";
            }

            oss << "[AcquisitionNode/Config] node=" << init_.nodeId
                << " assigned_channels=" << task.channels_size() << "\n";
            for (int i = 0; i < task.channels_size(); ++i)
            {
                const auto &ch = task.channels(i);
                oss << "[AcquisitionNode/Config] node=" << init_.nodeId
                    << " ch=" << i
                    << " src=" << ipIntToString(ch.ip_source()) << ":" << ch.port_source()
                    << " dst=" << ipIntToString(ch.ip_destination()) << ":" << ch.port_destination()
                    << " channel_index=" << ch.channel_index()
                    << "\n";
            }

            LOG(INFO) << oss.str();
        }

        bool openStream()
        {
            channel_ = grpc::CreateChannel(init_.masterAddress, grpc::InsecureChannelCredentials());
            stub_ = acqproto::AcquisitionControlService::NewStub(channel_);
            if (!stub_)
            {
                setError("failed to create AcquisitionControlService stub");
                return false;
            }

            context_ = std::make_unique<grpc::ClientContext>();
            stream_ = stub_->Connect(context_.get());
            if (!stream_)
            {
                setError("failed to create Connect stream");
                return false;
            }

            setState(acqproto::STATE_IDLE);
            return sendStatus(buildStatusSnapshot());
        }

        bool handleCommand(const acqproto::MasterCommand &command)
        {
            switch (command.type())
            {
            case acqproto::CMD_CONFIGURE:
                return handleConfigure(command);
            case acqproto::CMD_START:
                return handleStart(command);
            case acqproto::CMD_STOP:
                return handleStop(command);
            case acqproto::CMD_SHUTDOWN:
                if (command.has_shutdown_control() && !command.shutdown_control().reason().empty())
                {
                    LOG(INFO) << "[AcquisitionNode] shutdown reason: " << command.shutdown_control().reason();
                }
                running_.store(false, std::memory_order_release);
                stopAcquisition(true);
                cleanupDurationThread();
                setState(acqproto::STATE_IDLE);
                return true;
            default:
                setError("unknown command type=" + std::to_string(static_cast<int>(command.type())));
                return false;
            }
        }

        bool handleConfigure(const acqproto::MasterCommand &command)
        {
            const auto &task = command.task();
            acqproto::NodeAcquisitionConfig config;
            try
            {
                config = acqproto::MakeNodeAcquisitionConfig(task);
            }
            catch (const std::exception &e)
            {
                setError(std::string("configure command invalid: ") + e.what());
                return false;
            }

            if (!initializeDpdkIfNeeded(config))
            {
                return false;
            }

            openpni::AcquisitionInfo acqInfo;
            try
            {
                acqInfo = acqproto::MakeAcquisitionInfo(config);
            }
            catch (const std::exception &e)
            {
                setError(std::string("failed to build AcquisitionInfo: ") + e.what());
                return false;
            }

            acqproto::StorageConfig storageConfig;
            storageConfig.output_root = init_.outputRoot;
            storageConfig.session_name = makeSessionName(task.session_name(), command.message());
            storageConfig.max_file_size_mb =
                task.max_file_size_mb() > 0 ? static_cast<size_t>(task.max_file_size_mb()) : init_.maxFileSizeMb;
            storageConfig.overwrite_existing_file = init_.overwriteExisting;
            storageConfig.total_reserved_gib =
                task.reserved_storage_gib() > 0 ? task.reserved_storage_gib() : init_.reservedStorageGiB;
            storageConfig.enable_raw_file_write = init_.enableRawFileWrite;
            storageConfig.output_roots = init_.outputRoots;
            storageConfig.manifest_filename = init_.manifestFilename;
            storageConfig.async_queue_depth = init_.asyncQueueDepth;
            storageConfig.writer_threads_per_shard = init_.writerThreadsPerShard;
            storageConfig.use_spill_to_disk = init_.useSpillToDisk;
            storageConfig.fail_on_queue_full = init_.failOnQueueFull;
            storageConfig.fsync_each_segment = init_.fsyncEachSegment;

            if (const auto parsed = parseShardStrategy(init_.shardStrategy); parsed.has_value())
            {
                storageConfig.shard_strategy = parsed.value();
            }

            if (storageConfig.output_roots.empty())
            {
                if (const char *roots = std::getenv("PNI_R2C_RAW_OUTPUT_ROOTS"); roots && roots[0] != '\0')
                {
                    storageConfig.output_roots = splitList(roots);
                }
            }

            applyEnvIfEmpty("PNI_R2C_RAW_MANIFEST_FILENAME", storageConfig.manifest_filename);

            if (const char *strategy = std::getenv("PNI_R2C_RAW_SHARD_STRATEGY"); strategy && strategy[0] != '\0')
            {
                const auto parsed = parseShardStrategy(strategy);
                if (parsed.has_value())
                {
                    storageConfig.shard_strategy = parsed.value();
                }
            }

            if (const char *queueDepth = std::getenv("PNI_R2C_RAW_ASYNC_QUEUE_DEPTH"); queueDepth && queueDepth[0] != '\0')
            {
                storageConfig.async_queue_depth = static_cast<size_t>(std::strtoull(queueDepth, nullptr, 10));
            }

            logConfigureDetails(task, storageConfig);

            stopAcquisition(false);

            std::unique_ptr<INodeRuntime> runtimeNode;
            try
            {
                runtimeNode = createRuntime(config, std::move(acqInfo), std::move(storageConfig));
            }
            catch (const std::exception &e)
            {
                setError(std::string("failed to create acquisition runtime: ") + e.what());
                return false;
            }

            if (!runtimeNode)
            {
                setError("failed to create acquisition runtime");
                return false;
            }

            {
                std::lock_guard<std::mutex> lock(runtimeMutex_);
                runtimeNode_ = std::move(runtimeNode);
                if (fileReadyCallback_)
                {
                    runtimeNode_->SetFileCompleteCallback(fileReadyCallback_);
                }

                if (rawDataReadyCallback_)
                {
                    runtimeNode_->SetRawDataReadyCallback(rawDataReadyCallback_);
                }

                if (deferRawDataRelease_)
                {
                    runtimeNode_->SetDeferRawDataRelease(true);
                }

                runtimeNode_->SetStatusReportCallback(
                    [this](const acqproto::NodeStatus &status)
                    {
                        std::lock_guard<std::mutex> statusLock(statusMutex_);
                        lastStatus_ = status;
                    });
            }

            setState(acqproto::STATE_CONFIGURED);
            clearError();
            return true;
        }

        bool handleStart(const acqproto::MasterCommand &command)
        {
            {
                std::lock_guard<std::mutex> lock(runtimeMutex_);
                if (!runtimeNode_)
                {
                    setError("start command received before configure");
                    return false;
                }
            }

            uint64_t startTimeMs = 0;
            uint64_t durationMs = 0;
            if (command.has_start_control())
            {
                startTimeMs = command.start_control().planned_start_time_ms();
                durationMs = command.start_control().duration_ms();
            }
            else
            {
                const auto params = parseCommandParams(command.message());
                startTimeMs = parseUint64(params, "start_time_ms", 0);
                durationMs = parseUint64(params, "duration_ms", 0);
            }

            if (startTimeMs > 0)
            {
                waitUntil(startTimeMs);
            }

            bool started = false;
            {
                std::lock_guard<std::mutex> lock(runtimeMutex_);
                if (!runtimeNode_)
                {
                    setError("acquisition runtime missing before start");
                    return false;
                }
                started = runtimeNode_->Start();
            }

            if (!started)
            {
                setError("acquisition start failed");
                return false;
            }

            setState(acqproto::STATE_RUNNING);
            clearError();

            if (durationMs > 0)
            {
                scheduleDurationStop(durationMs);
            }

            return true;
        }

        bool handleStop(const acqproto::MasterCommand &command)
        {
            if (command.has_stop_control() && !command.stop_control().reason().empty())
            {
                LOG(INFO) << "[AcquisitionNode] stop reason: " << command.stop_control().reason();
            }
            stopAcquisition(true);
            cleanupDurationThread();
            clearError();
            return true;
        }

        std::unique_ptr<INodeRuntime> createRuntime(
            const acqproto::NodeAcquisitionConfig &config,
            openpni::AcquisitionInfo acqInfo,
            acqproto::StorageConfig storageConfig)
        {
            switch (config.runtime_type)
            {
            case acqproto::NodeAcquisitionConfig::RuntimeType::Socket:
                return std::make_unique<NodeRuntimeImpl<openpni::SocketAcquisition>>(std::move(acqInfo), std::move(storageConfig));

            case acqproto::NodeAcquisitionConfig::RuntimeType::Dpdk:
#if PNI_STANDARD_CONFIG_ENABLE_DPDK
                return std::make_unique<NodeRuntimeImpl<openpni::DPDKAcquisition>>(std::move(acqInfo), std::move(storageConfig));
#else
                throw std::runtime_error("DPDK runtime requested but this build does not enable DPDK");
#endif
            default:
                throw std::runtime_error("unknown acquisition runtime type");
            }
        }

        bool initializeDpdkIfNeeded(const acqproto::NodeAcquisitionConfig &config)
        {
            if (config.runtime_type != acqproto::NodeAcquisitionConfig::RuntimeType::Dpdk)
            {
                return true;
            }

            if (!validateDpdkBindIpsAndNuma(config))
            {
                return false;
            }

#if PNI_STANDARD_CONFIG_ENABLE_DPDK
            if (dpdkInitialized_)
            {
                return true;
            }

            try
            {
                openpni::dpdk::DPDKInitParam dpdkInfo;
                dpdkInfo.copyThreadNum = static_cast<int>(config.dpdk.copy_thread_num);
                dpdkInfo.etherIpBind = config.dpdk.bind_ips;
                dpdkInfo.rxThreadNumForEachPort = static_cast<int>(config.dpdk.rx_rings_per_port);
                dpdkInfo.rte_mbuf_double_pointer_size_multiply =
                    static_cast<uint16_t>(config.dpdk.rte_mbuf_double_pointer_size_multiply);
                dpdkInfo.rte_mbuf_double_pointer_num_pultiply =
                    static_cast<uint16_t>(config.dpdk.rte_mbuf_double_pointer_num_multiply);

                openpni::dpdk::InitDPDK(
                    dpdkInfo,
                    [](const std::string &message)
                    {
                        LOG(INFO) << "[AcquisitionNode/DPDK] " << message;
                    });
                dpdkInitialized_ = true;
                return true;
            }
            catch (const std::exception &e)
            {
                setError(std::string("DPDK initialization failed: ") + e.what());
                return false;
            }
#else
            setError("DPDK requested but current build does not enable PNI_STANDARD_CONFIG_ENABLE_DPDK");
            return false;
#endif
        }

        void waitUntil(uint64_t targetMs)
        {
            while (running_.load(std::memory_order_acquire))
            {
                const uint64_t now = nowMs();
                if (now >= targetMs)
                {
                    return;
                }

                const uint64_t remain = targetMs - now;
                std::this_thread::sleep_for(std::chrono::milliseconds(std::min<uint64_t>(remain, 100)));
            }
        }

        void scheduleDurationStop(uint64_t durationMs)
        {
            cleanupDurationThread();
            durationThreadRunning_.store(true, std::memory_order_release);

            durationThread_ = std::thread([this, durationMs]()
                                          {
                                              const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(durationMs);
                                              while (durationThreadRunning_.load(std::memory_order_acquire))
                                              {
                                                  const auto now = std::chrono::steady_clock::now();
                                                  if (now >= deadline)
                                                  {
                                                      break;
                                                  }

                                                  const auto remain = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
                                                  const uint64_t sleepMs = std::min<uint64_t>(100, static_cast<uint64_t>(std::max<int64_t>(remain, 1)));
                                                  std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
                                              }

                                              if (!durationThreadRunning_.load(std::memory_order_acquire))
                                              {
                                                  return;
                                              }

                                              stopAcquisition(true);
                                              setState(acqproto::STATE_CONFIGURED); });
        }

        void cleanupDurationThread()
        {
            durationThreadRunning_.store(false, std::memory_order_release);
            if (durationThread_.joinable())
            {
                if (durationThread_.get_id() == std::this_thread::get_id())
                {
                    durationThread_.detach();
                }
                else
                {
                    durationThread_.join();
                }
            }
        }

        void stopAcquisition(bool keepConfigured)
        {
            std::lock_guard<std::mutex> lock(runtimeMutex_);
            if (!runtimeNode_)
            {
                setState(acqproto::STATE_IDLE);
                return;
            }

            runtimeNode_->Stop();
            if (keepConfigured)
            {
                setState(acqproto::STATE_CONFIGURED);
            }
            else
            {
                runtimeNode_.reset();
                setState(acqproto::STATE_IDLE);
            }
        }

        std::string makeSessionName(const std::string &sessionName, const std::string &legacyMessage) const
        {
            if (!sessionName.empty())
            {
                return sessionName;
            }

            const auto params = parseCommandParams(legacyMessage);
            auto it = params.find("session");
            if (it != params.end() && !it->second.empty())
            {
                return it->second;
            }

            return init_.sessionNamePrefix + "_" + std::to_string(nowMs());
        }

        void statusLoop()
        {
            while (running_.load(std::memory_order_acquire))
            {
                const auto status = buildStatusSnapshot();
                if (!sendStatus(status))
                {
                    running_.store(false, std::memory_order_release);
                    break;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(init_.statusIntervalMs));
            }
        }

        bool sendStatus(const acqproto::NodeStatus &status)
        {
            std::lock_guard<std::mutex> lock(streamWriteMutex_);
            if (!stream_)
            {
                return false;
            }
            return stream_->Write(status);
        }

        acqproto::NodeStatus buildStatusSnapshot() const
        {
            acqproto::NodeStatus status;
            *status.mutable_info() = buildNodeInfo();
            status.set_state(state_.load(std::memory_order_acquire));

            {
                std::lock_guard<std::mutex> lock(statusMutex_);
                status.set_current_speed_mpps(lastStatus_.current_speed_mpps());
                status.set_current_bandwidth_mbps(lastStatus_.current_bandwidth_mbps());
                status.set_buffer_used(lastStatus_.buffer_used());
                status.set_buffer_volume(lastStatus_.buffer_volume());
                status.set_buffer_usage_percent(lastStatus_.buffer_usage_percent());
                status.set_total_rx_packets(lastStatus_.total_rx_packets());
                status.set_total_rx_bytes(lastStatus_.total_rx_bytes());
                status.set_error_packets(lastStatus_.error_packets());
            }

            {
                std::lock_guard<std::mutex> lock(errorMutex_);
                status.set_error_message(errorMessage_);
            }

            return status;
        }

        acqproto::NodeInfo buildNodeInfo() const
        {
            acqproto::NodeInfo info;
            info.set_node_id(init_.nodeId);
            info.set_ip_address(init_.nodeAddress);
            info.set_cpu_cores(std::max<uint32_t>(1, std::thread::hardware_concurrency()));

            struct sysinfo sysInfo;
            if (::sysinfo(&sysInfo) == 0)
            {
                const uint64_t total = static_cast<uint64_t>(sysInfo.totalram) * static_cast<uint64_t>(sysInfo.mem_unit);
                info.set_memory_size(total);
            }

            char hostName[256] = {0};
            if (::gethostname(hostName, sizeof(hostName) - 1) == 0)
            {
                info.set_hostname(hostName);
            }
            else
            {
                info.set_hostname("unknown-host");
            }

            return info;
        }

        void setState(acqproto::NodeState state)
        {
            state_.store(state, std::memory_order_release);
        }

        void clearError()
        {
            std::lock_guard<std::mutex> lock(errorMutex_);
            errorMessage_.clear();
            if (state_.load(std::memory_order_acquire) == acqproto::STATE_ERROR)
            {
                state_.store(acqproto::STATE_IDLE, std::memory_order_release);
            }
        }

        void setError(const std::string &message)
        {
            {
                std::lock_guard<std::mutex> lock(errorMutex_);
                errorMessage_ = message;
            }
            state_.store(acqproto::STATE_ERROR, std::memory_order_release);
            LOG(ERROR) << "[AcquisitionNode] " << message;
        }

        InitOptions init_;

        std::atomic<bool> running_{false};
        std::atomic<bool> durationThreadRunning_{false};
        std::atomic<acqproto::NodeState> state_{acqproto::STATE_IDLE};

        std::shared_ptr<grpc::Channel> channel_;
        std::unique_ptr<acqproto::AcquisitionControlService::Stub> stub_;
        std::unique_ptr<grpc::ClientContext> context_;
        std::unique_ptr<grpc::ClientReaderWriter<acqproto::NodeStatus, acqproto::MasterCommand>> stream_;

        mutable std::mutex streamWriteMutex_;

        std::thread statusThread_;
        std::thread durationThread_;

        mutable std::mutex runtimeMutex_;
        std::unique_ptr<INodeRuntime> runtimeNode_;
        FileReadyCallback fileReadyCallback_;
        RawDataReadyCallback rawDataReadyCallback_;
        bool deferRawDataRelease_{false};
        bool dpdkInitialized_{false};

        mutable std::mutex statusMutex_;
        acqproto::NodeStatus lastStatus_;

        mutable std::mutex errorMutex_;
        std::string errorMessage_;
    };

    AcquisitionGrpcNode::AcquisitionGrpcNode(InitOptions options)
        : m_impl(std::make_unique<Impl>(std::move(options)))
    {
    }

    AcquisitionGrpcNode::~AcquisitionGrpcNode() = default;

    void AcquisitionGrpcNode::setFileReadyCallback(FileReadyCallback callback)
    {
        m_impl->setFileReadyCallback(std::move(callback));
    }

    void AcquisitionGrpcNode::setRawDataReadyCallback(RawDataReadyCallback callback)
    {
        m_impl->setRawDataReadyCallback(std::move(callback));
    }

    void AcquisitionGrpcNode::setDeferRawDataRelease(bool defer)
    {
        m_impl->setDeferRawDataRelease(defer);
    }

    std::function<void(uint64_t)> AcquisitionGrpcNode::makeRawDataReleaseFn()
    {
        return [this](uint64_t packetCount)
        {
            m_impl->releaseRawData(packetCount);
        };
    }

    bool AcquisitionGrpcNode::run()
    {
        return m_impl->run();
    }

    void AcquisitionGrpcNode::stop()
    {
        m_impl->stop();
    }

    acqproto::NodeState AcquisitionGrpcNode::state() const
    {
        return m_impl->state();
    }

} // namespace openpni::distributed::grpcnode
