#include "core/acquisition/AcquisitionServer.hpp"

#include <glog/logging.h>

namespace openpni::distributed::acquisition
{

    NodeAcquisitionConfig MakeNodeAcquisitionConfig(const AcquisitionTask &task)
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

    openpni::AcquisitionInfo MakeAcquisitionInfo(const NodeAcquisitionConfig &config)
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

    RollingRawFileOutput::RollingRawFileOutput(const StorageConfig &config)
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
                LOG(ERROR) << "Failed to create directory: " << e.what();
            }
        }
    }

    RollingRawFileOutput::~RollingRawFileOutput()
    {
        CloseCurrent();
    }

    void RollingRawFileOutput::SetFileReadyCallback(FileReadyCallback cb)
    {
        callback_ = std::move(cb);
    }

    bool RollingRawFileOutput::Write(const openpni::RawDataView &data)
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

            LOG(ERROR) << "RawFileOutput::appendSegment failed. Disk full?";
        }
        return false;
    }

    void RollingRawFileOutput::Stop()
    {
        CloseCurrent();
    }

    void RollingRawFileOutput::Rotate()
    {
        CloseCurrent(); // 关闭旧文件，触发回调
        OpenNew();      // 打开新文件
    }

    void RollingRawFileOutput::OpenNew()
    {
        // 文件名格式：timestamp_sequence.raw
        // 这样可以保证时间顺序，方便后续处理
        const auto now = std::chrono::system_clock::now();
        const auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

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
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "Failed to open raw file " << current_path_ << ": " << e.what();
            writer_.reset();
        }
    }

    void RollingRawFileOutput::CloseCurrent()
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

} // namespace openpni::distributed::acquisition
