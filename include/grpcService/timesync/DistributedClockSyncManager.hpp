#pragma once

#include "TimeSyncClient.hpp"
#include "TimeSyncServer.hpp"
#include <vector>
#include <memory>
#include <map>

namespace openpni::distributed::timesync
{

    /**
     * @brief 分布式时钟集成接口
     *
     * 用于将时钟同步功能集成到分布式数据采集和符合计算系统中
     *
     * 使用流程：
     * 1. 采集节点: 创建 TimeSyncClient，定期调用 PerformSync() 获取校正时间
     * 2. 合并节点: 使用 SegmentClockCalibration 进行段间的时钟校正
     * 3. 计算节点: 处理校正后的数据
     */
    class DistributedClockSyncManager
    {
    public:
        /**
         * @brief 创建客户端实例
         * @param client_id 客户端标识
         * @param server_address 服务器地址
         * @return 新创建的客户端指针
         */
        static std::shared_ptr<TimeSyncClient> CreateClient(
            uint32_t client_id, const std::string &server_address = "localhost:50051")
        {
            return std::make_shared<TimeSyncClient>(client_id, server_address);
        }

        /**
         * @brief 创建服务器实例
         * @return 新创建的服务器指针
         */
        static std::shared_ptr<TimeSyncServer> CreateServer()
        {
            return std::make_shared<TimeSyncServer>();
        }
    };

    /**
     * @brief 段的时钟校正信息
     *
     * 用于在合并和符合计算时进行时钟偏差的考虑
     */
    struct SegmentClockCalibration
    {
        size_t segment_index = 0;          // 段索引
        uint32_t source_client_id = 0;     // 来源客户端ID
        int64_t clock_offset_ns = 0;       // 该段的时钟偏差（纳秒）
        float clock_drift_rate_ppm = 0.0f; // 时钟漂移率（ppm）
        double confidence = 1.0;           // 校正置信度（0-1）
        uint64_t calibration_time_ns = 0;  // 校正时间
    };

    /**
     * @brief 带时钟校正的段时间信息
     */
    struct SegmentTimeWithCalibration
    {
        uint64_t segment_start_time_ns;   // 段开始时间（原始）
        uint64_t segment_end_time_ns;     // 段结束时间（原始）
        uint64_t corrected_start_time_ns; // 校正后的开始时间
        uint64_t corrected_end_time_ns;   // 校正后的结束时间
        uint32_t source_client_id;        // 来源客户端
        int64_t clock_offset_ns;          // 应用的时钟偏差
    };

    /**
     * @brief 时钟校正工具类
     */
    class ClockCalibrationUtil
    {
    public:
        /**
         * @brief 计算段的校正时间范围
         *
         * @param original_start_ns 原始开始时间（纳秒）
         * @param original_end_ns 原始结束时间（纳秒）
         * @param offset_ns 时钟偏差（纳秒）
         * @param drift_rate_ppm 时钟漂移率（ppm，可选）
         * @return 校正后的时间范围
         */
        static SegmentTimeWithCalibration CalibrateSegmentTime(
            uint64_t original_start_ns, uint64_t original_end_ns,
            uint32_t client_id, int64_t offset_ns, float drift_rate_ppm = 0.0f)
        {

            SegmentTimeWithCalibration result;
            result.segment_start_time_ns = original_start_ns;
            result.segment_end_time_ns = original_end_ns;
            result.source_client_id = client_id;
            result.clock_offset_ns = offset_ns;

            // 应用基础时钟偏差
            result.corrected_start_time_ns = original_start_ns + static_cast<uint64_t>(offset_ns);
            result.corrected_end_time_ns = original_end_ns + static_cast<uint64_t>(offset_ns);

            // 如果需要考虑漂移率，可以应用线性校正
            // corrected_time = original_time + offset + (drift_rate * elapsed_time)
            // 但通常漂移率很小，可以在后续精细处理时应用

            return result;
        }

        /**
         * @brief 比较两个段的时间重叠情况（考虑时钟偏差）
         *
         * @param seg1 第一个段的校正时间信息
         * @param seg2 第二个段的校正时间信息
         * @return 重叠时长（纳秒），负数表示无重叠
         */
        static int64_t GetTimeOverlap(
            const SegmentTimeWithCalibration &seg1,
            const SegmentTimeWithCalibration &seg2)
        {

            uint64_t overlap_start = std::max(seg1.corrected_start_time_ns,
                                              seg2.corrected_start_time_ns);
            uint64_t overlap_end = std::min(seg1.corrected_end_time_ns,
                                            seg2.corrected_end_time_ns);

            if (overlap_end <= overlap_start)
            {
                return -1; // 无重叠
            }

            return static_cast<int64_t>(overlap_end - overlap_start);
        }

        /**
         * @brief 判断两个段是否应该合并（考虑时间容限）
         *
         * @param seg1 第一个段
         * @param seg2 第二个段
         * @param time_tolerance_ns 时间容限（纳秒）
         * @return true 表示应该合并，false 表示不合并
         */
        static bool ShouldMerge(
            const SegmentTimeWithCalibration &seg1,
            const SegmentTimeWithCalibration &seg2,
            int64_t time_tolerance_ns = 1'000'000'000LL)
        {

            // 情况1：有时间重叠
            if (seg1.corrected_end_time_ns > seg2.corrected_start_time_ns &&
                seg1.corrected_start_time_ns < seg2.corrected_end_time_ns)
            {
                return true;
            }

            // 情况2：时间间隙小于容限
            int64_t gap = std::min(
                static_cast<int64_t>(seg2.corrected_start_time_ns) -
                    static_cast<int64_t>(seg1.corrected_end_time_ns),
                static_cast<int64_t>(seg1.corrected_start_time_ns) -
                    static_cast<int64_t>(seg2.corrected_end_time_ns));

            if (gap > 0 && gap <= time_tolerance_ns)
            {
                return true;
            }

            return false;
        }

        /**
         * @brief 校正单个事件的时间戳
         *
         * @param original_time_ns 原始时间戳（纳秒）
         * @param offset_ns 时钟偏差
         * @param drift_rate_ppm 时钟漂移率
         * @param base_time_ns 基准时间（用于计算漂移）
         * @return 校正后的时间戳
         */
        static uint64_t CalibrateEventTime(
            uint64_t original_time_ns,
            int64_t offset_ns,
            float drift_rate_ppm = 0.0f,
            uint64_t base_time_ns = 0)
        {

            // 基础校正
            uint64_t corrected = original_time_ns + static_cast<uint64_t>(offset_ns);

            // 漂移校正（可选）
            if (drift_rate_ppm != 0.0f && base_time_ns > 0)
            {
                int64_t time_diff = static_cast<int64_t>(original_time_ns) -
                                    static_cast<int64_t>(base_time_ns);
                float drift_correction = (drift_rate_ppm / 1e6f) * static_cast<float>(time_diff);
                corrected += static_cast<uint64_t>(drift_correction);
            }

            return corrected;
        }
    };

} // namespace openpni::distributed::timesync
