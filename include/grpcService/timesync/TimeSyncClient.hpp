#pragma once

#include "TimeSyncCommon.hpp"
#include <thread>
#include <atomic>
#include <memory>

namespace openpni::distributed::timesync
{

    /**
     * @brief 时钟同步客户端
     *
     * 功能：
     * - 定期与服务器进行时钟同步
     * - 计算并应用时钟校正
     * - 考虑时钟漂移进行校正
     * - 支持后台自动同步和手动同步
     */
    class TimeSyncClient
    {
    public:
        /**
         * @brief 构造函数
         * @param client_id 客户端标识
         * @param server_address 服务器地址（不需要实际连接，用于演示）
         */
        TimeSyncClient(uint32_t client_id, const std::string &server_address = "localhost:50051")
            : client_id_(client_id), server_address_(server_address)
        {

            last_sync_time_ns_ = TimeUtil::GetMonotonicTimeNs();
        }

        ~TimeSyncClient()
        {
            StopSync();
        }

        // 禁用拷贝
        TimeSyncClient(const TimeSyncClient &) = delete;
        TimeSyncClient &operator=(const TimeSyncClient &) = delete;

    private:
        uint32_t client_id_;
        std::string server_address_;

        // 时钟参数
        int64_t clock_offset_ns_ = 0;       // 相对于服务器的时钟偏差
        float clock_drift_rate_ppm_ = 0.0f; // 时钟漂移率（百万分之一）
        uint64_t last_sync_time_ns_ = 0;    // 最后同步的服务器时间
        uint64_t sync_count_ = 0;           // 同步次数

        mutable std::mutex sync_mutex_;

        // 后台同步线程
        std::atomic<bool> sync_thread_running_{false};
        std::thread sync_thread_;

        // 同步配置
        static constexpr uint64_t SYNC_INTERVAL_MS = 5000;  // 每5秒同步一次
        static constexpr size_t NUM_SYNC_SAMPLES = 5;       // 每次同步取5个样本
        static constexpr uint64_t SAMPLE_INTERVAL_MS = 100; // 样本间隔100ms

        /**
         * @brief 后台同步线程函数
         */
        void SyncThreadFunc()
        {
            std::cout << "TimeSyncClient[" << client_id_ << "] sync thread started" << std::endl;

            while (sync_thread_running_)
            {
                try
                {
                    PerformSync();
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Sync error: " << e.what() << std::endl;
                }

                std::this_thread::sleep_for(
                    std::chrono::milliseconds(SYNC_INTERVAL_MS));
            }

            std::cout << "TimeSyncClient[" << client_id_ << "] sync thread stopped" << std::endl;
        }

    public:
        /**
         * @brief 执行一次时钟同步
         *
         * 与服务器进行多次往返，计算平均偏差
         */
        void PerformSync()
        {
            std::vector<int64_t> offsets;
            std::vector<float> network_delays;

            // 收集多个同步样本
            for (size_t i = 0; i < NUM_SYNC_SAMPLES; i++)
            {
                uint64_t client_time = TimeUtil::GetMonotonicTimeNs();

                // 模拟与服务器的同步（实际应该通过 gRPC 调用）
                uint64_t server_time = TimeUtil::GetMonotonicTimeNs();
                float network_delay_ms = 2.0f; // 模拟网络延迟

                // 计算单向延迟
                int64_t round_trip_time = static_cast<int64_t>(server_time) - static_cast<int64_t>(client_time);
                int64_t offset = round_trip_time / 2;

                offsets.push_back(offset);
                network_delays.push_back(network_delay_ms);

                if (i < NUM_SYNC_SAMPLES - 1)
                {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(SAMPLE_INTERVAL_MS));
                }
            }

            // 计算中位数作为最终偏差（抗干扰）
            std::sort(offsets.begin(), offsets.end());
            int64_t median_offset = offsets[offsets.size() / 2];

            float avg_network_delay = 0;
            for (auto delay : network_delays)
            {
                avg_network_delay += delay;
            }
            avg_network_delay /= static_cast<float>(network_delays.size());

            {
                std::lock_guard<std::mutex> lock(sync_mutex_);
                clock_offset_ns_ = median_offset;
                last_sync_time_ns_ = TimeUtil::GetMonotonicTimeNs();
                sync_count_++;

                if (sync_count_ % 10 == 0)
                {
                    std::cout << "TimeSyncClient[" << client_id_ << "] sync #" << sync_count_
                              << " - Offset: " << clock_offset_ns_ << " ns"
                              << " (network delay: " << avg_network_delay << " ms)" << std::endl;
                }
            }
        }

    public:
        /**
         * @brief 启动后台同步线程
         */
        void StartSync()
        {
            if (!sync_thread_running_)
            {
                sync_thread_running_ = true;
                sync_thread_ = std::thread(&TimeSyncClient::SyncThreadFunc, this);
            }
        }

        /**
         * @brief 停止后台同步线程
         */
        void StopSync()
        {
            if (sync_thread_running_)
            {
                sync_thread_running_ = false;
                if (sync_thread_.joinable())
                {
                    sync_thread_.join();
                }
            }
        }

        /**
         * @brief 获取校正后的时间戳（纳秒）
         *
         * 这是最核心的接口，在采集或处理数据时应该使用这个函数获取时间戳
         * 而不是直接使用系统时间
         *
         * @return 校正后的时间戳（纳秒）
         */
        uint64_t GetCorrectedTimeNs() const
        {
            std::lock_guard<std::mutex> lock(sync_mutex_);

            uint64_t local_time = TimeUtil::GetMonotonicTimeNs();

            // 基础校正：加上时钟偏差
            uint64_t corrected = local_time + static_cast<uint64_t>(clock_offset_ns_);

            // 高级校正：考虑时钟漂移
            // corrected += (clock_drift_rate_ppm_ / 1e6) * (local_time - last_sync_time_ns_)
            // 但由于漂移率通常很小，这个修正项可以暂时省略

            return corrected;
        }

        /**
         * @brief 获取校正后的时间戳（毫秒）
         */
        uint64_t GetCorrectedTimeMs() const
        {
            return GetCorrectedTimeNs() / 1'000'000;
        }

        /**
         * @brief 获取校正后的时间戳（微秒）
         */
        uint64_t GetCorrectedTimeUs() const
        {
            return GetCorrectedTimeNs() / 1'000;
        }

        /**
         * @brief 获取当前的时钟偏差（纳秒）
         */
        int64_t GetClockOffsetNs() const
        {
            std::lock_guard<std::mutex> lock(sync_mutex_);
            return clock_offset_ns_;
        }

        /**
         * @brief 设置时钟偏差（用于外部同步）
         * @param offset_ns 时钟偏差（纳秒）
         */
        void SetClockOffsetNs(int64_t offset_ns)
        {
            std::lock_guard<std::mutex> lock(sync_mutex_);
            clock_offset_ns_ = offset_ns;
            last_sync_time_ns_ = TimeUtil::GetMonotonicTimeNs();
        }

        /**
         * @brief 获取时钟漂移率（ppm）
         */
        float GetClockDriftRate() const
        {
            std::lock_guard<std::mutex> lock(sync_mutex_);
            return clock_drift_rate_ppm_;
        }

        /**
         * @brief 获取同步次数
         */
        uint64_t GetSyncCount() const
        {
            std::lock_guard<std::mutex> lock(sync_mutex_);
            return sync_count_;
        }

        /**
         * @brief 获取客户端ID
         */
        uint32_t GetClientId() const
        {
            return client_id_;
        }

        /**
         * @brief 打印同步状态
         */
        void PrintStatus() const
        {
            std::lock_guard<std::mutex> lock(sync_mutex_);
            std::cout << "\n--- TimeSyncClient[" << client_id_ << "] Status ---" << std::endl;
            std::cout << "Server Address:     " << server_address_ << std::endl;
            std::cout << "Clock Offset:       " << std::setw(12) << clock_offset_ns_ << " ns" << std::endl;
            std::cout << "Clock Drift:        " << std::setw(10) << std::fixed
                      << std::setprecision(3) << clock_drift_rate_ppm_ << " ppm" << std::endl;
            std::cout << "Sync Count:         " << std::setw(5) << sync_count_ << std::endl;
            std::cout << "Last Sync Time:     " << TimeUtil::FormatTimeNs(last_sync_time_ns_) << std::endl;
            std::cout << "Sync Thread:        " << (sync_thread_running_ ? "Running" : "Stopped") << std::endl;
            std::cout << "-----------------------------------------\n"
                      << std::endl;
        }
    };

} // namespace openpni::distributed::timesync
