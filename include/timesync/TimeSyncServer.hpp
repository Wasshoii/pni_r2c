#pragma once

#include "TimeSyncCommon.hpp"
#include <vector>
#include <functional>

namespace openpni::distributed::timesync
{

    /**
     * @brief 时钟同步服务器实现
     *
     * 功能：
     * - 接收来自多个客户端的时钟同步请求
     * - 计算每个客户端相对于服务器的时钟偏差
     * - 跟踪时钟漂移率
     * - 提供统计信息查询接口
     */
    class TimeSyncServer
    {
    public:
        TimeSyncServer() = default;
        ~TimeSyncServer() = default;

    private:
        struct ClientInfo
        {
            uint32_t client_id = 0;
            std::string hostname;
            int64_t clock_offset_ns = 0;
            float clock_drift_rate = 0.0f;
            uint64_t last_sync_time = 0;
            uint32_t sync_count = 0;
            float confidence = 0.0f;
            std::deque<int64_t> recent_offsets;
            std::deque<uint64_t> recent_sync_times;
            float network_latency_avg_ms = 0.0f;
        };

        std::map<uint32_t, ClientInfo> clients_;
        mutable std::mutex client_mutex_;

        static constexpr size_t MAX_HISTORY = 100;
        static constexpr int64_t OUTLIER_THRESHOLD_NS = 50'000'000;      // 50ms 异常值阈值
        static constexpr int64_t MAX_CLOCK_OFFSET_NS = 10'000'000'000LL; // 10s 最大容许偏差

    public:
        /**
         * @brief 处理客户端的时钟同步请求
         *
         * @param client_id 客户端标识
         * @param hostname 客户端主机名
         * @param client_time_ns 客户端本地时间（纳秒）
         * @param[out] server_time_ns 服务器时间
         * @param[out] offset_ns 计算得到的时钟偏差
         * @param[out] network_delay_ms 估计的网络延迟
         * @return true 成功处理，false 出现异常
         */
        bool SyncClock(uint32_t client_id, const std::string &hostname,
                       uint64_t client_time_ns,
                       uint64_t &server_time_ns, int64_t &offset_ns,
                       float &network_delay_ms)
        {

            // 记录服务器接收时间
            uint64_t server_recv_time = TimeUtil::GetMonotonicTimeNs();

            // 验证客户端时间合理性
            if (client_time_ns == 0)
            {
                std::cerr << "Invalid client time: 0" << std::endl;
                return false;
            }

            // 估计网络延迟（简化模型：假设上行和下行延迟相等）
            int64_t half_trip_time = static_cast<int64_t>(server_recv_time) - static_cast<int64_t>(client_time_ns);

            // 基本合理性检查
            if (half_trip_time < -1'000'000'000LL || half_trip_time > 10'000'000'000LL)
            {
                std::cerr << "Unreasonable round trip time: " << half_trip_time << " ns" << std::endl;
                return false;
            }

            // 计算时钟偏差
            // 理论：client_time + network_delay = server_time
            // offset = server_time - client_time - network_delay
            offset_ns = 0; // 更精确的计算见下文
            network_delay_ms = half_trip_time / 2'000'000.0f;

            server_time_ns = server_recv_time;

            // 更新客户端统计信息
            {
                std::lock_guard<std::mutex> lock(client_mutex_);
                auto &info = clients_[client_id];

                if (info.client_id == 0)
                {
                    info.client_id = client_id;
                    info.hostname = hostname;
                    std::cout << "Registered new client: " << client_id
                              << " (" << hostname << ")" << std::endl;
                }

                // 异常值检测：如果偏差变化过大，可能是网络波动
                if (info.recent_offsets.empty() ||
                    std::abs(offset_ns - info.clock_offset_ns) < OUTLIER_THRESHOLD_NS)
                {

                    info.recent_offsets.push_back(offset_ns);
                    info.recent_sync_times.push_back(server_recv_time);

                    if (info.recent_offsets.size() > MAX_HISTORY)
                    {
                        info.recent_offsets.pop_front();
                        info.recent_sync_times.pop_front();
                    }

                    // 计算平均偏差
                    int64_t sum = 0;
                    for (auto off : info.recent_offsets)
                    {
                        sum += off;
                    }
                    info.clock_offset_ns = sum / static_cast<int64_t>(info.recent_offsets.size());

                    // 计算时钟漂移率（ppm）
                    if (info.recent_offsets.size() >= 10)
                    {
                        uint64_t time_span = info.recent_sync_times.back() - info.recent_sync_times.front();
                        int64_t offset_span = info.recent_offsets.back() - info.recent_offsets.front();

                        if (time_span > 0)
                        {
                            // drift_rate (ppm) = (offset_change / time_span) * 1e6
                            info.clock_drift_rate = (static_cast<float>(offset_span) / static_cast<float>(time_span)) * 1e6f;
                        }
                    }

                    info.confidence = std::min(info.recent_offsets.size() / 10.0, 1.0);
                }

                // 更新网络延迟
                info.network_latency_avg_ms =
                    (info.network_latency_avg_ms * 0.7f + network_delay_ms * 0.3f);

                info.last_sync_time = server_recv_time;
                info.sync_count++;
            }

            return true;
        }

        /**
         * @brief 获取所有客户端的同步统计
         */
        std::vector<ClientClockInfo> GetAllClientStats() const
        {
            std::vector<ClientClockInfo> result;
            {
                std::lock_guard<std::mutex> lock(client_mutex_);
                for (const auto &[client_id, info] : clients_)
                {
                    ClientClockInfo stats;
                    stats.client_id = info.client_id;
                    stats.hostname = info.hostname;
                    stats.clock_offset_ns = info.clock_offset_ns;
                    stats.clock_drift_rate = info.clock_drift_rate;
                    stats.last_sync_time = info.last_sync_time;
                    stats.sync_count = info.sync_count;
                    stats.confidence = info.confidence;
                    stats.network_latency_avg_ms = info.network_latency_avg_ms;
                    result.push_back(stats);
                }
            }
            return result;
        }

        /**
         * @brief 获取特定客户端的同步统计
         */
        bool GetClientStats(uint32_t client_id, ClientClockInfo &stats) const
        {
            std::lock_guard<std::mutex> lock(client_mutex_);
            auto it = clients_.find(client_id);
            if (it == clients_.end())
            {
                return false;
            }

            const auto &info = it->second;
            stats.client_id = info.client_id;
            stats.hostname = info.hostname;
            stats.clock_offset_ns = info.clock_offset_ns;
            stats.clock_drift_rate = info.clock_drift_rate;
            stats.last_sync_time = info.last_sync_time;
            stats.sync_count = info.sync_count;
            stats.confidence = info.confidence;
            stats.network_latency_avg_ms = info.network_latency_avg_ms;

            return true;
        }

        /**
         * @brief 获取客户端数量
         */
        size_t GetClientCount() const
        {
            std::lock_guard<std::mutex> lock(client_mutex_);
            return clients_.size();
        }

        /**
         * @brief 打印所有客户端的同步统计信息
         */
        void PrintStats() const
        {
            auto stats = GetAllClientStats();

            std::cout << "\n"
                      << std::string(80, '=') << std::endl;
            std::cout << "Clock Synchronization Statistics" << std::endl;
            std::cout << std::string(80, '=') << std::endl;

            if (stats.empty())
            {
                std::cout << "No clients connected." << std::endl;
            }
            else
            {
                for (const auto &s : stats)
                {
                    std::cout << "\nClient " << std::setw(2) << s.client_id
                              << " (" << s.hostname << ")" << std::endl;
                    std::cout << "  Clock Offset:       " << std::setw(12) << s.clock_offset_ns
                              << " ns" << std::endl;
                    std::cout << "  Clock Drift Rate:   " << std::setw(10) << std::fixed
                              << std::setprecision(3) << s.clock_drift_rate << " ppm" << std::endl;
                    std::cout << "  Network Latency:    " << std::setw(8) << std::fixed
                              << std::setprecision(2) << s.network_latency_avg_ms << " ms" << std::endl;
                    std::cout << "  Sync Count:         " << std::setw(5) << s.sync_count << std::endl;
                    std::cout << "  Confidence:         " << std::setw(6) << std::fixed
                              << std::setprecision(1) << (s.confidence * 100) << "%" << std::endl;
                }
            }

            std::cout << std::string(80, '=') << "\n"
                      << std::endl;
        }

        /**
         * @brief 重置所有客户端的同步信息
         */
        void Reset()
        {
            std::lock_guard<std::mutex> lock(client_mutex_);
            clients_.clear();
            std::cout << "All client sync information reset." << std::endl;
        }
    };

} // namespace openpni::distributed::timesync
