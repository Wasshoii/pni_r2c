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
        : config_(config)
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
        Stop();
    }

    void RollingRawFileOutput::SetFileReadyCallback(FileReadyCallback cb)
    {
        callback_ = std::move(cb);
        writer_.SetFileReadyCallback(callback_);
    }

    bool RollingRawFileOutput::Write(const openpni::RawDataView &data)
    {
        // 估算本次写入数据的大小 (byte)
        // 注意：data.length 是每个包的有效负载长度。
        // 实际存储时，RawFileOutput 通常会添加包头（例如时间戳、长度等元信息）。
        // 根据 PnI 的实现习惯，RawData 存储格式通常包含一些头部开销。
        // 为了更准确地分卷，建议给每个包增加一些预估的头部大小，例如 32 字节。
        // 这样可以避免文件实际大小超过系统限制或预期过多。
        constexpr size_t ESTIMATED_PACKET_HEADER_SIZE = 32;

        size_t chunk_size = 0;
        if (data.length)
        {
            for (size_t i = 0; i < data.count; ++i)
            {
                chunk_size += data.length[i] + ESTIMATED_PACKET_HEADER_SIZE;
            }
        }

        // 首次写入时打开文件；后续的分卷（滚动到下一个文件）由 writer_ 内部按
        // maxFileSizeBytes 自动处理。
        if (!opened_)
        {
            OpenNew();
        }

        if (!opened_)
        {
            return false;
        }

        if (writer_.AppendSegment(chunk_size, data))
        {
            return true;
        }

        LOG(ERROR) << "RawFileOutput::appendSegment failed. Disk full?";
        return false;
    }

    void RollingRawFileOutput::Stop()
    {
        if (opened_)
        {
            writer_.Stop();
            opened_ = false;
        }
    }

    void RollingRawFileOutput::OpenNew()
    {
        // 文件名前缀带时间戳，分卷序号由 RollingFileWriter 自动追加
        const auto now = std::chrono::system_clock::now();
        const auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        const std::string filePrefix = std::format("raw_{}", timestamp);

        openpni::distributed::coreio::RawDataWriterOptions options;
        options.io.reservedBytes = config_.total_reserved_gib * 1024ull * 1024ull * 1024ull;
        options.io.maxFileSizeBytes = config_.max_file_size_mb * 1024ull * 1024ull;
        options.io.enableOverrideExistingFile = config_.overwrite_existing_file;
        options.channelNum = config_.channel_num;

        opened_ = writer_.Open(
            session_dir_.string(), filePrefix, "raw", std::move(options),
            [](openpni::distributed::coreio::RawDataFileWriter &w, const std::string &path)
            {
                w.Open(path);
            });

        if (!opened_)
        {
            LOG(ERROR) << "Failed to open raw file with prefix " << filePrefix << " in " << session_dir_;
        }
    }

} // namespace openpni::distributed::acquisition
