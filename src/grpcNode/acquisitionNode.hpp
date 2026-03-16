#pragma once

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>

#include <sys/sysinfo.h>
#include <unistd.h>

#include "core/aquisition-and-r2s/AquisitionSever.hpp"
#include "protos/acquisition.grpc.pb.h"

namespace openpni::distributed::grpcnode
{
    namespace acqproto = openpni::distributed::acquisition;

    class AcquisitionGrpcNode
    {
    public:
        using FileReadyCallback = acqproto::RollingRawFileOutput::FileReadyCallback;

        struct InitOptions
        {
            std::string masterAddress = "127.0.0.1:50071";
            std::string nodeId = "acq-node-0";
            std::string nodeAddress = "127.0.0.1";

            std::string outputRoot = "Data/raw_data";
            std::string sessionNamePrefix = "acq_session";
            size_t maxFileSizeMb = 1024;
            uint64_t reservedStorageGiB = 20;

            uint32_t statusIntervalMs = 1000;
        };

        explicit AcquisitionGrpcNode(InitOptions options)
            : m_init(std::move(options))
        {
            m_lastStatus.set_state(acqproto::STATE_IDLE);
        }

        ~AcquisitionGrpcNode()
        {
            stop();
        }

        void setFileReadyCallback(FileReadyCallback callback)
        {
            std::lock_guard<std::mutex> lock(m_runtimeMutex);
            m_fileReadyCallback = std::move(callback);
            if (m_runtimeNode && m_fileReadyCallback)
            {
                m_runtimeNode->SetFileCompleteCallback(m_fileReadyCallback);
            }
        }

        bool run()
        {
            if (m_running.exchange(true))
            {
                std::cerr << "[AcquisitionNode] already running" << std::endl;
                return false;
            }

            if (!openStream())
            {
                m_running.store(false, std::memory_order_release);
                m_stream.reset();
                m_context.reset();
                m_stub.reset();
                m_channel.reset();
                return false;
            }

            m_statusThread = std::thread([this]()
                                         { statusLoop(); });

            acqproto::MasterCommand command;
            while (m_running.load(std::memory_order_acquire) && m_stream->Read(&command))
            {
                if (!handleCommand(command))
                {
                    setError("command handling failed");
                }
            }

            m_running.store(false, std::memory_order_release);

            stopAcquisition(true);
            cleanupDurationThread();

            if (m_statusThread.joinable())
            {
                m_statusThread.join();
            }

            if (m_stream)
            {
                m_stream->WritesDone();
                const grpc::Status status = m_stream->Finish();
                if (!status.ok())
                {
                    std::cerr << "[AcquisitionNode] Connect stream finished with error: "
                              << status.error_message() << std::endl;
                }
            }

            m_stream.reset();
            m_context.reset();
            m_stub.reset();
            m_channel.reset();

            return true;
        }

        void stop()
        {
            const bool wasRunning = m_running.exchange(false, std::memory_order_acq_rel);
            if (!wasRunning)
            {
                cleanupDurationThread();
                return;
            }

            if (m_context)
            {
                m_context->TryCancel();
            }

            stopAcquisition(true);
            cleanupDurationThread();

            if (m_statusThread.joinable())
            {
                m_statusThread.join();
            }
        }

        acqproto::NodeState state() const
        {
            return m_state.load(std::memory_order_acquire);
        }

    private:
        class INodeRuntime
        {
        public:
            virtual ~INodeRuntime() = default;
            virtual bool Start() = 0;
            virtual void Stop() = 0;
            virtual void SetFileCompleteCallback(FileReadyCallback callback) = 0;
            virtual void SetStatusReportCallback(std::function<void(const acqproto::NodeStatus &)> callback) = 0;
        };

        template <typename AlgoType>
        class NodeRuntimeImpl final : public INodeRuntime
        {
        public:
            NodeRuntimeImpl(
                openpni::AcquisitionInfo acqInfo,
                acqproto::StorageConfig storageConfig)
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

            void SetStatusReportCallback(std::function<void(const acqproto::NodeStatus &)> callback) override
            {
                m_node->SetStatusReportCallback(std::move(callback));
            }

        private:
            std::unique_ptr<acqproto::DistributedAcquisitionNode<AlgoType>> m_node;
        };

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

        void logConfigureDetails(const acqproto::AcquisitionTask &task, const acqproto::StorageConfig &storageConfig)
        {
            static std::mutex s_logMutex;
            std::ostringstream oss;

            const std::string algo = (task.algorithm_type() == acqproto::ALGORITHM_TYPE_DPDK) ? "DPDK" : "SOCKET";
            oss << "[AcquisitionNode/Config] node=" << m_init.nodeId
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
                oss << "[AcquisitionNode/Config] node=" << m_init.nodeId
                    << " dpdk(copy_threads=" << dpdk.copy_thread_num()
                    << ",rx_rings_per_port=" << dpdk.rx_rings_per_port()
                    << ",ppsize_mul=" << dpdk.rte_mbuf_double_pointer_size_multiply()
                    << ",ppnum_mul=" << dpdk.rte_mbuf_double_pointer_num_multiply()
                    << ",bind_ips=" << dpdk.bind_ips_size() << ")"
                    << "\n";
            }

            oss << "[AcquisitionNode/Config] node=" << m_init.nodeId
                << " assigned_channels=" << task.channels_size() << "\n";
            for (int i = 0; i < task.channels_size(); ++i)
            {
                const auto &ch = task.channels(i);
                oss << "[AcquisitionNode/Config] node=" << m_init.nodeId
                    << " ch=" << i
                    << " src=" << ipIntToString(ch.ip_source()) << ":" << ch.port_source()
                    << " dst=" << ipIntToString(ch.ip_destination()) << ":" << ch.port_destination()
                    << " channel_index=" << ch.channel_index()
                    << "\n";
            }

            std::lock_guard<std::mutex> lock(s_logMutex);
            std::cout << oss.str();
        }

        bool openStream()
        {
            m_channel = grpc::CreateChannel(m_init.masterAddress, grpc::InsecureChannelCredentials());
            m_stub = acqproto::AcquisitionControlService::NewStub(m_channel);
            if (!m_stub)
            {
                setError("failed to create AcquisitionControlService stub");
                return false;
            }

            m_context = std::make_unique<grpc::ClientContext>();
            m_stream = m_stub->Connect(m_context.get());
            if (!m_stream)
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
                    std::cout << "[AcquisitionNode] shutdown reason: " << command.shutdown_control().reason() << std::endl;
                }
                m_running.store(false, std::memory_order_release);
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
            storageConfig.output_root = m_init.outputRoot;
            storageConfig.session_name = makeSessionName(task.session_name(), command.message());
            storageConfig.max_file_size_mb =
                task.max_file_size_mb() > 0 ? static_cast<size_t>(task.max_file_size_mb()) : m_init.maxFileSizeMb;
            storageConfig.total_reserved_gib =
                task.reserved_storage_gib() > 0 ? task.reserved_storage_gib() : m_init.reservedStorageGiB;

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
                std::lock_guard<std::mutex> lock(m_runtimeMutex);
                m_runtimeNode = std::move(runtimeNode);
                if (m_fileReadyCallback)
                {
                    m_runtimeNode->SetFileCompleteCallback(m_fileReadyCallback);
                }

                m_runtimeNode->SetStatusReportCallback(
                    [this](const acqproto::NodeStatus &status)
                    {
                        std::lock_guard<std::mutex> statusLock(m_statusMutex);
                        m_lastStatus = status;
                    });
            }

            setState(acqproto::STATE_CONFIGURED);
            clearError();
            return true;
        }

        bool handleStart(const acqproto::MasterCommand &command)
        {
            {
                std::lock_guard<std::mutex> lock(m_runtimeMutex);
                if (!m_runtimeNode)
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
                // Backward compatibility with previous string-based start parameters.
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
                std::lock_guard<std::mutex> lock(m_runtimeMutex);
                if (!m_runtimeNode)
                {
                    setError("acquisition runtime missing before start");
                    return false;
                }
                started = m_runtimeNode->Start();
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
                std::cout << "[AcquisitionNode] stop reason: " << command.stop_control().reason() << std::endl;
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

#if PNI_STANDARD_CONFIG_ENABLE_DPDK
            if (m_dpdkInitialized)
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
                        std::cout << "[AcquisitionNode/DPDK] " << message << std::endl;
                    });
                m_dpdkInitialized = true;
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
            while (m_running.load(std::memory_order_acquire))
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
            m_durationThreadRunning.store(true, std::memory_order_release);

            m_durationThread = std::thread([this, durationMs]()
                                           {
                                               const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(durationMs);
                                               while (m_durationThreadRunning.load(std::memory_order_acquire))
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

                                               if (!m_durationThreadRunning.load(std::memory_order_acquire))
                                               {
                                                   return;
                                               }

                                               stopAcquisition(true);
                                               setState(acqproto::STATE_CONFIGURED); });
        }

        void cleanupDurationThread()
        {
            m_durationThreadRunning.store(false, std::memory_order_release);
            if (m_durationThread.joinable())
            {
                if (m_durationThread.get_id() == std::this_thread::get_id())
                {
                    m_durationThread.detach();
                }
                else
                {
                    m_durationThread.join();
                }
            }
        }

        void stopAcquisition(bool keepConfigured)
        {
            std::lock_guard<std::mutex> lock(m_runtimeMutex);
            if (!m_runtimeNode)
            {
                setState(acqproto::STATE_IDLE);
                return;
            }

            m_runtimeNode->Stop();
            if (keepConfigured)
            {
                setState(acqproto::STATE_CONFIGURED);
            }
            else
            {
                m_runtimeNode.reset();
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

            return m_init.sessionNamePrefix + "_" + std::to_string(nowMs());
        }

        void statusLoop()
        {
            while (m_running.load(std::memory_order_acquire))
            {
                const auto status = buildStatusSnapshot();
                if (!sendStatus(status))
                {
                    m_running.store(false, std::memory_order_release);
                    break;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(m_init.statusIntervalMs));
            }
        }

        bool sendStatus(const acqproto::NodeStatus &status)
        {
            std::lock_guard<std::mutex> lock(m_streamWriteMutex);
            if (!m_stream)
            {
                return false;
            }
            return m_stream->Write(status);
        }

        acqproto::NodeStatus buildStatusSnapshot() const
        {
            acqproto::NodeStatus status;
            *status.mutable_info() = buildNodeInfo();
            status.set_state(m_state.load(std::memory_order_acquire));

            {
                std::lock_guard<std::mutex> lock(m_statusMutex);
                status.set_current_speed_mpps(m_lastStatus.current_speed_mpps());
                status.set_current_bandwidth_mbps(m_lastStatus.current_bandwidth_mbps());
                status.set_buffer_used(m_lastStatus.buffer_used());
                status.set_buffer_volume(m_lastStatus.buffer_volume());
                status.set_buffer_usage_percent(m_lastStatus.buffer_usage_percent());
                status.set_total_rx_packets(m_lastStatus.total_rx_packets());
                status.set_total_rx_bytes(m_lastStatus.total_rx_bytes());
                status.set_error_packets(m_lastStatus.error_packets());
            }

            {
                std::lock_guard<std::mutex> lock(m_errorMutex);
                status.set_error_message(m_errorMessage);
            }

            return status;
        }

        acqproto::NodeInfo buildNodeInfo() const
        {
            acqproto::NodeInfo info;
            info.set_node_id(m_init.nodeId);
            info.set_ip_address(m_init.nodeAddress);
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
            m_state.store(state, std::memory_order_release);
        }

        void clearError()
        {
            std::lock_guard<std::mutex> lock(m_errorMutex);
            m_errorMessage.clear();
            if (m_state.load(std::memory_order_acquire) == acqproto::STATE_ERROR)
            {
                m_state.store(acqproto::STATE_IDLE, std::memory_order_release);
            }
        }

        void setError(const std::string &message)
        {
            {
                std::lock_guard<std::mutex> lock(m_errorMutex);
                m_errorMessage = message;
            }
            m_state.store(acqproto::STATE_ERROR, std::memory_order_release);
            std::cerr << "[AcquisitionNode] " << message << std::endl;
        }

        InitOptions m_init;

        std::atomic<bool> m_running{false};
        std::atomic<bool> m_durationThreadRunning{false};
        std::atomic<acqproto::NodeState> m_state{acqproto::STATE_IDLE};

        std::shared_ptr<grpc::Channel> m_channel;
        std::unique_ptr<acqproto::AcquisitionControlService::Stub> m_stub;
        std::unique_ptr<grpc::ClientContext> m_context;
        std::unique_ptr<grpc::ClientReaderWriter<acqproto::NodeStatus, acqproto::MasterCommand>> m_stream;

        mutable std::mutex m_streamWriteMutex;

        std::thread m_statusThread;
        std::thread m_durationThread;

        mutable std::mutex m_runtimeMutex;
        std::unique_ptr<INodeRuntime> m_runtimeNode;
        FileReadyCallback m_fileReadyCallback;
        bool m_dpdkInitialized{false};

        mutable std::mutex m_statusMutex;
        acqproto::NodeStatus m_lastStatus;

        mutable std::mutex m_errorMutex;
        std::string m_errorMessage;
    };

} // namespace openpni::distributed::grpcnode
