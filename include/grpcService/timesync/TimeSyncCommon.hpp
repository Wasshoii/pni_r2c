#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <chrono>
#include <map>
#include <mutex>
#include <deque>
#include <cmath>
#include <iostream>
#include <iomanip>
#include <algorithm>

// 前向声明，避免包含生成的代码
namespace openpni::distributed::timesync
{

    /**
     * @brief 高精度时间工具类
     */
    class TimeUtil
    {
    public:
        /**
         * @brief 获取高精度单调时间（纳秒）
         * 推荐用于时间戳和延迟测量，不受系统时间调整影响
         */
        static uint64_t GetMonotonicTimeNs()
        {
            return std::chrono::nanoseconds(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
        }

        /**
         * @brief 获取高精度系统时间（纳秒）
         * 返回从 Unix Epoch (1970-01-01) 开始的纳秒数
         */
        static uint64_t GetSystemTimeNs()
        {
            return std::chrono::nanoseconds(
                       std::chrono::system_clock::now().time_since_epoch())
                .count();
        }

        /**
         * @brief 时间戳转为人类可读格式
         */
        static std::string FormatTimeNs(uint64_t timeNs)
        {
            auto ms = timeNs / 1'000'000;
            auto sec = ms / 1000;
            auto min = sec / 60;
            return std::to_string(min) + "m " + std::to_string(sec % 60) + "s " +
                   std::to_string(ms % 1000) + "ms";
        }
    };

    /**
     * @brief 客户端时钟信息记录
     */
    struct ClientClockInfo
    {
        uint32_t client_id = 0;
        std::string hostname;

        // 时钟参数
        int64_t clock_offset_ns = 0;   // 估计的时钟偏差
        float clock_drift_rate = 0.0f; // 时钟漂移率 (ppm)
        uint64_t last_sync_time = 0;   // 最后同步时间
        uint32_t sync_count = 0;       // 同步次数

        // 历史记录（用于计算漂移率）
        std::deque<int64_t> recent_offsets;
        std::deque<uint64_t> recent_sync_times;

        // 质量指标
        double confidence = 0.0;             // 同步置信度 (0-1)
        float network_latency_avg_ms = 0.0f; // 平均网络延迟
    };

} // namespace openpni::distributed::timesync
