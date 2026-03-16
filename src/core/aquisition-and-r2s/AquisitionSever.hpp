#pragma once

#include <string>
#include <vector>
#include <memory>
#include <filesystem>
#include <functional>
#include <iostream>
#include <format>
#include <algorithm>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>
#include <stdexcept>

#include <pni/node/Acquisition.hpp>
#include <pni/io/IO.hpp>
#include "protos/acquisition.pb.h" // 引用 proto 生成的头文件

namespace openpni::distributed::acquisition
{

    namespace fs = std::filesystem;

    // 存储配置
    struct StorageConfig
    {
        std::string output_root;          // 存储根目录
        std::string session_name;         // 本次采集的会话名（子目录）
        size_t max_file_size_mb = 512;    // 分卷大小阈值 (MB)
        uint64_t total_reserved_gib = 20; // 磁盘保留空间 (GiB)
        uint16_t channel_num = 0;         // 通道数（自动填充）
    };

    // 采集节点配置，用于生成 AcquisitionInfo
    struct NodeAcquisitionConfig
    {
        enum class RuntimeType
        {
            Socket,
            Dpdk
        };

        struct DpdkConfig
        {
            uint32_t copy_thread_num = 8;
            uint32_t rx_rings_per_port = 1;
            std::vector<std::string> bind_ips;
            uint32_t rte_mbuf_double_pointer_size_multiply = 32;
            uint32_t rte_mbuf_double_pointer_num_multiply = 2;
        };

        RuntimeType runtime_type = RuntimeType::Socket;
        uint32_t min_packet_size = 1024;
        uint32_t max_packet_size = 1024; // 对应 storageUnitSize
        uint64_t max_buffer_size = 4ull * 1024 * 1024 * 1024;
        uint32_t time_switch_buffer_ms = 1000;
        DpdkConfig dpdk;

        struct Channel
        {
            uint32_t ip_source;
            uint16_t port_source;
            uint32_t ip_dest;
            uint16_t port_dest;
            uint16_t channel_index;
        };
        std::vector<Channel> channels;
    };

    inline NodeAcquisitionConfig MakeNodeAcquisitionConfig(const AcquisitionTask &task)
    {
        NodeAcquisitionConfig config;

        switch (task.algorithm_type())
        {
        case ALGORITHM_TYPE_DPDK:
            config.runtime_type = NodeAcquisitionConfig::RuntimeType::Dpdk;
            break;
        case ALGORITHM_TYPE_UNSPECIFIED:
        case ALGORITHM_TYPE_SOCKET:
        default:
            config.runtime_type = NodeAcquisitionConfig::RuntimeType::Socket;
            break;
        }

        config.min_packet_size = task.min_packet_size() > 0 ? task.min_packet_size() : 1024;
        config.max_packet_size = task.storage_unit_size() > 0 ? task.storage_unit_size() : 1024;
        config.max_buffer_size = task.max_buffer_size() > 0 ? task.max_buffer_size() : (4ull * 1024ull * 1024ull * 1024ull);
        config.time_switch_buffer_ms = task.time_switch_buffer_ms() > 0 ? task.time_switch_buffer_ms() : 1000;

        if (task.has_dpdk_options())
        {
            const auto &dpdk = task.dpdk_options();
            config.dpdk.copy_thread_num = dpdk.copy_thread_num() > 0 ? dpdk.copy_thread_num() : 8;
            config.dpdk.rx_rings_per_port = dpdk.rx_rings_per_port() > 0 ? dpdk.rx_rings_per_port() : 1;
            config.dpdk.rte_mbuf_double_pointer_size_multiply =
                dpdk.rte_mbuf_double_pointer_size_multiply() > 0 ? dpdk.rte_mbuf_double_pointer_size_multiply() : 32;
            config.dpdk.rte_mbuf_double_pointer_num_multiply =
                dpdk.rte_mbuf_double_pointer_num_multiply() > 0 ? dpdk.rte_mbuf_double_pointer_num_multiply() : 2;
            config.dpdk.bind_ips.assign(dpdk.bind_ips().begin(), dpdk.bind_ips().end());
        }

        if (config.max_packet_size < config.min_packet_size)
        {
            throw std::invalid_argument("max_packet_size must be >= min_packet_size");
        }

        if (task.channels_size() == 0)
        {
            throw std::invalid_argument("AcquisitionTask.channels is empty");
        }

        config.channels.reserve(static_cast<size_t>(task.channels_size()));
        for (const auto &ch : task.channels())
        {
            NodeAcquisitionConfig::Channel item;
            item.ip_source = ch.ip_source();
            item.port_source = static_cast<uint16_t>(std::min<uint32_t>(ch.port_source(), 65535));
            item.ip_dest = ch.ip_destination();
            item.port_dest = static_cast<uint16_t>(std::min<uint32_t>(ch.port_destination(), 65535));
            item.channel_index = static_cast<uint16_t>(std::min<uint32_t>(ch.channel_index(), 65535));
            config.channels.push_back(item);
        }

        return config;
    }

    // 辅助函数：创建 AcquisitionInfo
    inline openpni::AcquisitionInfo MakeAcquisitionInfo(const NodeAcquisitionConfig &config)
    {
        if (config.channels.empty())
        {
            throw std::invalid_argument("NodeAcquisitionConfig.channels is empty");
        }
        if (config.max_packet_size < config.min_packet_size)
        {
            throw std::invalid_argument("max_packet_size must be >= min_packet_size");
        }

        openpni::AcquisitionInfo info;
        info.storageUnitSize = config.max_packet_size;
        info.maxBufferSize = config.max_buffer_size;
        info.timeSwitchBuffer_ms = std::max<uint32_t>(config.time_switch_buffer_ms, 10);
        info.totalChannelNum = config.channels.size();

        for (const auto &chan : config.channels)
        {
            openpni::AcquisitionInfo::ChannelSetting setting;
            setting.ipSource = chan.ip_source;
            setting.portSource = chan.port_source;
            setting.ipDestination = chan.ip_dest;
            setting.portDestination = chan.port_dest;
            setting.channelIndex = chan.channel_index;

            // 设置过滤器，参考 acquisition.cpp
            setting.quickFilter = [min = config.min_packet_size, max = config.max_packet_size](
                                      uint8_t * /*datagram*/, uint16_t length, uint32_t /*srcIp*/, uint16_t /*srcPort*/) noexcept -> bool
            {
                if (length < min || length > max)
                    return false;
                return true;
            };

            info.channelSettings.push_back(setting);
        }
        return info;
    }

    // 滚动文件写入器
    class RollingRawFileOutput
    {
    public:
        // 回调函数：当一个文件写满并关闭时调用，参数为文件绝对路径
        using FileReadyCallback = std::function<void(const std::string &)>;

        RollingRawFileOutput(const StorageConfig &config)
            : config_(config), current_size_(0), file_seq_(0)
        {

            session_dir_ = fs::path(config_.output_root) / config_.session_name;
            if (!fs::exists(session_dir_))
            {
                try
                {
                    fs::create_directories(session_dir_);
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Failed to create directory: " << e.what() << std::endl;
                }
            }
        }

        ~RollingRawFileOutput()
        {
            CloseCurrent();
        }

        void SetFileReadyCallback(FileReadyCallback cb)
        {
            callback_ = cb;
        }

        bool Write(const openpni::RawDataView &data)
        {
            // 估算本次写入数据的大小 (byte)
            size_t chunk_size = 0;
            // 注意：data.length 是每个包的有效负载长度。
            // 实际存储时，RawFileOutput 通常会添加包头（例如时间戳、长度等元信息）。
            // 根据 PnI 的实现习惯，RawData 存储格式通常包含一些头部开销。
            // 为了更准确地分卷，建议给每个包增加一些预估的头部大小，例如 32 字节。
            // 这样可以避免文件实际大小超过系统限制或预期过多。
            constexpr size_t ESTIMATED_PACKET_HEADER_SIZE = 32;

            if (data.length)
            {
                for (size_t i = 0; i < data.count; ++i)
                {
                    chunk_size += data.length[i] + ESTIMATED_PACKET_HEADER_SIZE;
                }
            }

            // 检查是否需要分卷
            // 如果当前有打开的文件，且加上新数据后超过最大限制
            if (writer_ && (current_size_ + chunk_size > config_.max_file_size_mb * 1024 * 1024))
            {
                Rotate();
            }

            // 如果没有打开的文件（刚开始或刚分卷），则新建
            if (!writer_)
            {
                OpenNew();
            }

            // 写入数据
            if (writer_)
            {
                // appendSegment 返回 bool 表示是否成功（如磁盘满则返回 false）
                if (writer_->appendSegment(data))
                {
                    current_size_ += chunk_size;
                    return true;
                }
                else
                {
                    std::cerr << "RawFileOutput::appendSegment failed. Disk full?" << std::endl;
                }
            }
            return false;
        }

        // 强制停止并关闭当前文件
        void Stop()
        {
            CloseCurrent();
        }

    private:
        void Rotate()
        {
            CloseCurrent(); // 关闭旧文件，触发回调
            OpenNew();      // 打开新文件
        }

        void OpenNew()
        {
            // 文件名格式：timestamp_sequence.raw
            // 这样可以保证时间顺序，方便后续处理
            auto now = std::chrono::system_clock::now();
            auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

            std::string filename = std::format("raw_{}_{:04d}.raw", timestamp, file_seq_++);
            current_path_ = (session_dir_ / filename).string();

            writer_ = std::make_unique<openpni::io::v1::RawFileOutput>();
            // 设置保留空间，避免撑爆磁盘
            writer_->setReservedBytes(config_.total_reserved_gib * 1024ull * 1024ull * 1024ull);
            writer_->setChannelNum(config_.channel_num);

            try
            {
                writer_->open(current_path_);
                current_size_ = 0;
                // std::cout << "Opened new volume: " << current_path_ << std::endl;
            }
            catch (const std::exception &e)
            {
                std::cerr << "Failed to open raw file " << current_path_ << ": " << e.what() << std::endl;
                writer_.reset();
            }
        }

        void CloseCurrent()
        {
            if (writer_)
            {
                writer_.reset(); // unique_ptr 析构会自动关闭文件
                // 触发回调，通知后续模块该文件已完成，可以处理
                if (callback_ && !current_path_.empty())
                {
                    callback_(current_path_);
                }
                current_path_.clear();
            }
        }

        StorageConfig config_;
        fs::path session_dir_;
        std::unique_ptr<openpni::io::v1::RawFileOutput> writer_;
        size_t current_size_;
        int file_seq_;
        std::string current_path_;
        FileReadyCallback callback_;
    };

    // 分布式采集节点工作类
    // 模板参数 AlgoType 可以是 SocketAcquisition 或 DPDKAcquisition
    template <typename AlgoType = openpni::SocketAcquisition>
    class DistributedAcquisitionNode
    {
    public:
        // 回调函数：用于向主服务器报告状态
        using StatusReportCallback = std::function<void(const openpni::distributed::acquisition::NodeStatus &)>;

        DistributedAcquisitionNode(AcquisitionInfo acq_info, StorageConfig storage_config)
            : acq_info_(acq_info), storage_config_(storage_config)
        {

            // 确保 config 中的 channel_num 正确
            storage_config_.channel_num = acq_info_.totalChannelNum;
        }

        ~DistributedAcquisitionNode()
        {
            Stop();
        }

        // 设置文件完成回调（连接到 R2S 转换模块）
        void SetFileCompleteCallback(RollingRawFileOutput::FileReadyCallback cb)
        {
            file_ready_callback_ = cb;
        }

        // 设置状态报告回调（用于 gRPC 推送状态）
        void SetStatusReportCallback(StatusReportCallback cb)
        {
            status_report_callback_ = cb;
        }

        // 启动采集
        bool Start()
        {
            if (running_.load(std::memory_order_acquire))
                return true;

            if (acq_info_.channelSettings.empty())
            {
                NotifyError("No acquisition channels configured");
                return false;
            }

            // 初始化 writer
            writer_ = std::make_unique<RollingRawFileOutput>(storage_config_);
            if (file_ready_callback_)
            {
                writer_->SetFileReadyCallback(file_ready_callback_);
            }

            running_.store(true, std::memory_order_release);

            // 启动采集线程
            worker_thread_ = std::thread([this]()
                                         { this->AcquisitionLoop(); });

            // 启动监控线程
            monitor_thread_ = std::thread([this]()
                                          { this->MonitorLoop(); });

            return true;
        }

        // 停止采集
        void Stop()
        {
            running_.store(false, std::memory_order_release);

            {
                std::lock_guard<std::mutex> lock(algo_mutex_);
                if (algo_)
                {
                    algo_->stop();
                }
            }

            if (worker_thread_.joinable())
            {
                worker_thread_.join();
            }
            if (monitor_thread_.joinable())
            {
                monitor_thread_.join();
            }
            if (writer_)
            {
                writer_->Stop();
                writer_.reset();
            }

            NotifyState(openpni::distributed::acquisition::STATE_IDLE);
        }

    private:
        void AcquisitionLoop()
        {
            // 日志 lambda
            auto logger = [](const std::string &message)
            {
                std::cout << "[Acquisition] " << message << std::endl;
            };

            // 初始化采集算法实例
            // 这里的构造函数签名参考 acquisition.cpp: Algo(param, logger)
            try
            {
                // 注意：AlgoType 的生命周期需要保持到 stop 被调用
                // 使用成员变量 algo_ 来持有它，以便 MonitorLoop 可以访问
                std::lock_guard<std::mutex> lock(algo_mutex_);
                algo_ = std::make_unique<AlgoType>(acq_info_, logger);
            }
            catch (const std::exception &e)
            {
                NotifyError(std::string("Failed to init algorithm: ") + e.what());
                running_.store(false, std::memory_order_release);
                return;
            }

            AlgoType *algo = nullptr;
            {
                std::lock_guard<std::mutex> lock(algo_mutex_);
                algo = algo_.get();
            }

            if (!algo)
            {
                NotifyError("Acquisition algorithm object is null");
                running_.store(false, std::memory_order_release);
                return;
            }

            if (!algo->start())
            {
                NotifyError("Failed to start acquisition");
                running_.store(false, std::memory_order_release);
                return;
            }

            auto readHandler = openpni::read_from_acquisition(std::ref(*algo));

            int missTime = 0;

            // 采集循环
            while (running_.load(std::memory_order_acquire) && !algo->isFinished())
            {
                // 尝试读取数据
                auto data_opt = algo->read();

                if (data_opt && data_opt->count > 0)
                {
                    // 写入文件（内部会自动处理分卷）
                    if (!writer_->Write(data_opt.value()))
                    {
                        NotifyError("Raw data write failed, stopping acquisition");
                        running_.store(false, std::memory_order_release);
                    }
                    missTime = 0;
                }
                else
                {
                    const auto sleepMs = std::max<int>(std::min<int>(missTime++, 100), 15);
                    std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
                }
            }

            {
                std::lock_guard<std::mutex> lock(algo_mutex_);
                if (algo_)
                {
                    algo_->stop();
                }
                // 停止后不要立即销毁 algo_，因为 monitor 可能会最后访问一次 status
            }

            running_.store(false, std::memory_order_release);
        }

        void MonitorLoop()
        {
            using namespace std::chrono;
            auto last_time = steady_clock::now();
            uint64_t last_bytes = 0;
            uint64_t last_packets = 0;

            while (running_.load(std::memory_order_acquire))
            {
                std::this_thread::sleep_for(milliseconds(1000));

                // 获取采集实例的状态
                typename AlgoType::Status status;
                bool algo_valid = false;
                {
                    std::lock_guard<std::mutex> lock(algo_mutex_);
                    if (algo_)
                    {
                        status = algo_->status();
                        algo_valid = true;
                    }
                }

                if (algo_valid && status_report_callback_)
                {
                    auto now = steady_clock::now();
                    auto duration_us = duration_cast<microseconds>(now - last_time).count();

                    if (duration_us > 0)
                    {
                        double duration_sec = duration_us / 1000000.0;
                        double speed_mbps = (status.totalRxBytes - last_bytes) / 1024.0 / 1024.0 / duration_sec;
                        double speed_mpps = (status.totalRxPackets - last_packets) / 1000000.0 / duration_sec;

                        openpni::distributed::acquisition::NodeStatus node_status;
                        node_status.set_state(openpni::distributed::acquisition::STATE_RUNNING);
                        node_status.set_current_bandwidth_mbps(speed_mbps);
                        node_status.set_current_speed_mpps(speed_mpps);

                        // Buffer status
                        node_status.set_buffer_used(status.used);
                        node_status.set_buffer_volume(status.volume);
                        if (status.volume > 0)
                        {
                            node_status.set_buffer_usage_percent((float)status.used / status.volume * 100.0f);
                        }

                        // Packet stats
                        node_status.set_total_rx_packets(status.totalRxPackets);
                        node_status.set_total_rx_bytes(status.totalRxBytes);

                        // Error handling (generic for socket, specific for dpdk if possible)
                        // acquisition.cpp shows socket::Status has 'unknown', dpdk::Status has 'ierrors', 'imissed' etc.
                        // We use the common 'unknown' field and try to adapt if needed.
                        // For generic template access, we rely on the common interface defined in acquisition.cpp usages.
                        // However, socket::Status and dpdk::Status differ slightly.
                        // We can use duck typing or simple access if fields overlap.
                        // Based on acquisition.cpp: both have 'unknown'.
                        uint64_t total_errors = status.unknown;

                        // If AlgoType is DPDK, we might want to sum up other errors, but requires specialization or SFINAE.
                        // For simplicity, we stick to common fields for now.
                        node_status.set_error_packets(total_errors);

                        status_report_callback_(node_status);

                        last_bytes = status.totalRxBytes;
                        last_packets = status.totalRxPackets;
                        last_time = now;
                    }
                }
            }
        }

        void NotifyState(openpni::distributed::acquisition::NodeState state)
        {
            if (!status_report_callback_)
            {
                return;
            }

            openpni::distributed::acquisition::NodeStatus status;
            status.set_state(state);
            status_report_callback_(status);
        }

        void NotifyError(const std::string &message)
        {
            std::cerr << message << std::endl;
            if (!status_report_callback_)
            {
                return;
            }

            openpni::distributed::acquisition::NodeStatus status;
            status.set_state(openpni::distributed::acquisition::STATE_ERROR);
            status.set_error_message(message);
            status_report_callback_(status);
        }

        AcquisitionInfo acq_info_;
        StorageConfig storage_config_;
        std::unique_ptr<RollingRawFileOutput> writer_;
        RollingRawFileOutput::FileReadyCallback file_ready_callback_;
        StatusReportCallback status_report_callback_;

        std::thread worker_thread_;
        std::thread monitor_thread_;
        std::atomic<bool> running_{false};

        std::mutex algo_mutex_;
        std::unique_ptr<AlgoType> algo_;
    };

} // namespace openpni::distributed::acquisition
