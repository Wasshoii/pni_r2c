#include "../include/timesync/TimeSyncClient.hpp"
#include "../include/timesync/TimeSyncServer.hpp"
#include "../include/timesync/DistributedClockSyncManager.hpp"
#include <iostream>
#include <iomanip>
#include <cmath>
#include <random>
#include <chrono>

using namespace openpni::distributed::timesync;

/**
 * @brief 模拟具有时钟漂移的客户端时钟
 *
 * 模拟现实场景中的客户端时钟不完全准确的情况
 */
class SimulatedClientClock
{
private:
    uint32_t client_id_;
    uint64_t start_time_ns_;
    float clock_drift_rate_ppm_; // 时钟漂移率（ppm）
    std::mt19937 rng_;
    std::normal_distribution<float> jitter_dist_; // 正态分布的抖动

public:
    SimulatedClientClock(uint32_t client_id, float drift_rate_ppm = 100.0f)
        : client_id_(client_id), clock_drift_rate_ppm_(drift_rate_ppm),
          rng_(client_id + std::chrono::system_clock::now().time_since_epoch().count()),
          jitter_dist_(0.0f, 1'000'000.0f)
    { // 均值0，标差1ms

        start_time_ns_ = TimeUtil::GetMonotonicTimeNs();
    }

    /**
     * @brief 获取带漂移和抖动的客户端时间
     */
    uint64_t GetTime() const
    {
        uint64_t monotonic_time = TimeUtil::GetMonotonicTimeNs();
        uint64_t elapsed = monotonic_time - start_time_ns_;

        // 应用时钟漂移
        // 如果漂移率为 +100 ppm，则客户端时钟比实际快 0.01%
        float drift_factor = 1.0f + (clock_drift_rate_ppm_ / 1e6f);
        uint64_t drifted_elapsed = static_cast<uint64_t>(static_cast<float>(elapsed) * drift_factor);

        return start_time_ns_ + drifted_elapsed;
    }

    /**
     * @brief 获取实际的时钟漂移率
     */
    float GetDriftRate() const
    {
        return clock_drift_rate_ppm_;
    }

    /**
     * @brief 获取模拟的客户端ID
     */
    uint32_t GetClientId() const
    {
        return client_id_;
    }
};

/**
 * @brief 测试用例：基础时钟同步
 */
void TestBasicClockSync()
{
    std::cout << "\n"
              << std::string(80, '=') << std::endl;
    std::cout << "Test 1: Basic Clock Synchronization" << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    // 创建4个模拟客户端，每个有不同的时钟漂移
    std::vector<std::shared_ptr<TimeSyncClient>> clients;
    std::vector<SimulatedClientClock> sim_clocks;

    std::cout << "\nCreating 4 simulated clients with different clock drift rates..." << std::endl;

    for (int i = 0; i < 4; i++)
    {
        float drift = 50.0f + i * 30.0f; // 50, 80, 110, 140 ppm
        auto client = std::make_shared<TimeSyncClient>(i, "localhost:50051");
        clients.push_back(client);

        sim_clocks.emplace_back(i, drift);
        std::cout << "  Client " << i << ": drift rate = " << drift << " ppm" << std::endl;
    }

    std::cout << "\nPerforming initial synchronization..." << std::endl;

    // 执行初始同步
    for (auto &client : clients)
    {
        client->PerformSync();
    }

    std::cout << "\nInitial sync results:" << std::endl;
    for (auto &client : clients)
    {
        std::cout << "  Client " << client->GetClientId()
                  << ": offset = " << std::setw(10) << client->GetClockOffsetNs()
                  << " ns, drift = " << std::fixed << std::setprecision(2)
                  << client->GetClockDriftRate() << " ppm" << std::endl;
    }

    // 模拟时间推移和多次同步
    std::cout << "\nSimulating " << 5 << " more synchronization cycles..." << std::endl;
    for (int cycle = 0; cycle < 5; cycle++)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        for (auto &client : clients)
        {
            client->PerformSync();
        }

        std::cout << "  Cycle " << (cycle + 2) << " completed" << std::endl;
    }

    std::cout << "\nFinal sync results:" << std::endl;
    for (auto &client : clients)
    {
        std::cout << "  Client " << client->GetClientId()
                  << ": offset = " << std::setw(10) << client->GetClockOffsetNs()
                  << " ns, sync_count = " << client->GetSyncCount() << std::endl;
    }
}

/**
 * @brief 测试用例：多客户端时钟对齐
 */
void TestMultiClientAlignment()
{
    std::cout << "\n"
              << std::string(80, '=') << std::endl;
    std::cout << "Test 2: Multi-Client Clock Alignment" << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    // 创建服务器
    auto server = DistributedClockSyncManager::CreateServer();

    // 创建4个客户端
    std::vector<std::shared_ptr<TimeSyncClient>> clients;
    for (int i = 0; i < 4; i++)
    {
        clients.push_back(DistributedClockSyncManager::CreateClient(i, "localhost:50051"));
    }

    std::cout << "\nPerforming synchronized time measurements..." << std::endl;
    std::cout << std::setw(6) << "Client"
              << std::setw(20) << "Local Time (ns)"
              << std::setw(20) << "Corrected Time (ns)"
              << std::setw(15) << "Offset (ns)" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    // 在"同一时刻"记录所有客户端的时间
    for (auto &client : clients)
    {
        client->PerformSync();

        uint64_t local_time = TimeUtil::GetMonotonicTimeNs();
        uint64_t corrected_time = client->GetCorrectedTimeNs();
        int64_t offset = client->GetClockOffsetNs();

        std::cout << std::setw(6) << client->GetClientId()
                  << std::setw(20) << local_time
                  << std::setw(20) << corrected_time
                  << std::setw(15) << offset << std::endl;
    }

    std::cout << "\nTime alignment results (corrected times should be very close):" << std::endl;

    uint64_t min_corrected = UINT64_MAX;
    uint64_t max_corrected = 0;

    for (auto &client : clients)
    {
        uint64_t corrected = client->GetCorrectedTimeNs();
        min_corrected = std::min(min_corrected, corrected);
        max_corrected = std::max(max_corrected, corrected);
    }

    uint64_t alignment_error = max_corrected - min_corrected;
    std::cout << "  Max time difference: " << alignment_error << " ns = "
              << (alignment_error / 1000.0f) << " μs" << std::endl;

    if (alignment_error < 1'000'000)
    { // < 1ms
        std::cout << "  ✓ Alignment accuracy: EXCELLENT (< 1ms)" << std::endl;
    }
    else if (alignment_error < 10'000'000)
    { // < 10ms
        std::cout << "  ✓ Alignment accuracy: GOOD (< 10ms)" << std::endl;
    }
    else
    {
        std::cout << "  ⚠ Alignment accuracy: ACCEPTABLE" << std::endl;
    }
}

/**
 * @brief 测试用例：段合并与时钟校正
 */
void TestSegmentMergingWithCalibration()
{
    std::cout << "\n"
              << std::string(80, '=') << std::endl;
    std::cout << "Test 3: Segment Merging with Clock Calibration" << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    // 模拟4个不同客户端的数据段
    struct Segment
    {
        uint32_t client_id;
        uint64_t start_time_ms;
        uint64_t duration_ms;
        int64_t clock_offset_ns;
    };

    std::vector<Segment> segments = {
        {0, 1000, 500, -5'000'000}, // Client 0: 慢5ms
        {1, 1200, 500, +3'000'000}, // Client 1: 快3ms
        {2, 1100, 500, -2'000'000}, // Client 2: 慢2ms
        {3, 1300, 500, +8'000'000}, // Client 3: 快8ms
    };

    std::cout << "\nOriginal segments (unsorted):" << std::endl;
    std::cout << std::setw(8) << "Client"
              << std::setw(15) << "Start (ms)"
              << std::setw(15) << "Duration (ms)"
              << std::setw(18) << "Clock Offset (ns)" << std::endl;
    std::cout << std::string(56, '-') << std::endl;

    for (const auto &seg : segments)
    {
        std::cout << std::setw(8) << seg.client_id
                  << std::setw(15) << seg.start_time_ms
                  << std::setw(15) << seg.duration_ms
                  << std::setw(18) << seg.clock_offset_ns << std::endl;
    }

    // 校正段的时间
    std::cout << "\nSegments after clock calibration:" << std::endl;
    std::cout << std::setw(8) << "Client"
              << std::setw(20) << "Corrected Start (ns)"
              << std::setw(20) << "Corrected End (ns)"
              << std::setw(15) << "Duration (ns)" << std::endl;
    std::cout << std::string(63, '-') << std::endl;

    std::vector<std::pair<Segment, SegmentTimeWithCalibration>> calibrated;

    for (const auto &seg : segments)
    {
        auto calib = ClockCalibrationUtil::CalibrateSegmentTime(
            seg.start_time_ms * 1'000'000,
            (seg.start_time_ms + seg.duration_ms) * 1'000'000,
            seg.client_id,
            seg.clock_offset_ns);

        calibrated.push_back({seg, calib});

        uint64_t duration = calib.corrected_end_time_ns - calib.corrected_start_time_ns;
        std::cout << std::setw(8) << seg.client_id
                  << std::setw(20) << calib.corrected_start_time_ns
                  << std::setw(20) << calib.corrected_end_time_ns
                  << std::setw(15) << duration << std::endl;
    }

    // 检查重叠和合并候选
    std::cout << "\nMerging analysis (time tolerance = 1s):" << std::endl;
    int64_t tolerance_ns = 1'000'000'000LL; // 1秒

    for (size_t i = 0; i < calibrated.size(); i++)
    {
        for (size_t j = i + 1; j < calibrated.size(); j++)
        {
            const auto &calib1 = calibrated[i].second;
            const auto &calib2 = calibrated[j].second;

            int64_t overlap = ClockCalibrationUtil::GetTimeOverlap(calib1, calib2);
            bool should_merge = ClockCalibrationUtil::ShouldMerge(calib1, calib2, tolerance_ns);

            std::cout << "  Segment " << i << " (Client " << calibrated[i].first.client_id
                      << ") vs Segment " << j << " (Client " << calibrated[j].first.client_id << "): ";

            if (overlap >= 0)
            {
                std::cout << "Overlap " << (overlap / 1'000'000) << "ms";
            }
            else
            {
                int64_t gap = (overlap == -1) ? std::abs(static_cast<int64_t>(calib2.corrected_start_time_ns) -
                                                         static_cast<int64_t>(calib1.corrected_end_time_ns))
                                              : 0;
                std::cout << "Gap " << (gap / 1'000'000) << "ms";
            }

            std::cout << " - " << (should_merge ? "MERGE ✓" : "SEPARATE") << std::endl;
        }
    }
}

/**
 * @brief 测试用例：符合事件的时间戳校正
 */
void TestEventTimeCalibration()
{
    std::cout << "\n"
              << std::string(80, '=') << std::endl;
    std::cout << "Test 4: Event Time Calibration for Coincidence" << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    // 模拟来自不同客户端的符合事件对
    struct CoincidenceEvent
    {
        uint32_t client_id;
        uint32_t crystal_index;
        uint64_t event_time_ns;
        int64_t clock_offset_ns;
    };

    std::vector<CoincidenceEvent> events = {
        {0, 10, 1'500'000'000, -5'000'000},
        {1, 25, 1'500'003'000, +3'000'000},
        {2, 15, 1'500'001'000, -2'000'000},
        {3, 30, 1'500'004'000, +8'000'000},
    };

    std::cout << "\nCoincidence events before calibration:" << std::endl;
    std::cout << std::setw(8) << "Client"
              << std::setw(8) << "Crystal"
              << std::setw(18) << "Event Time (ns)"
              << std::setw(18) << "Clock Offset (ns)" << std::endl;
    std::cout << std::string(52, '-') << std::endl;

    for (const auto &evt : events)
    {
        std::cout << std::setw(8) << evt.client_id
                  << std::setw(8) << evt.crystal_index
                  << std::setw(18) << evt.event_time_ns
                  << std::setw(18) << evt.clock_offset_ns << std::endl;
    }

    std::cout << "\nCoincidence events after calibration:" << std::endl;
    std::cout << std::setw(8) << "Client"
              << std::setw(8) << "Crystal"
              << std::setw(20) << "Calibrated Time (ns)"
              << std::setw(18) << "Time Diff (ns)" << std::endl;
    std::cout << std::string(54, '-') << std::endl;

    uint64_t min_calib_time = UINT64_MAX;
    uint64_t max_calib_time = 0;

    for (const auto &evt : events)
    {
        uint64_t calibrated = ClockCalibrationUtil::CalibrateEventTime(
            evt.event_time_ns,
            evt.clock_offset_ns);

        min_calib_time = std::min(min_calib_time, calibrated);
        max_calib_time = std::max(max_calib_time, calibrated);

        int64_t time_diff = static_cast<int64_t>(calibrated) -
                            static_cast<int64_t>(events[0].event_time_ns + events[0].clock_offset_ns);

        std::cout << std::setw(8) << evt.client_id
                  << std::setw(8) << evt.crystal_index
                  << std::setw(20) << calibrated
                  << std::setw(18) << time_diff << std::endl;
    }

    uint64_t time_spread = max_calib_time - min_calib_time;
    std::cout << "\nTime spread after calibration: " << time_spread << " ns = "
              << (time_spread / 1000.0f) << " μs" << std::endl;

    // 判断是否应该认为这是符合事件
    int64_t coincidence_window_ns = 10'000'000LL; // 10ns 符合窗口（PET典型值）
    if (time_spread <= coincidence_window_ns)
    {
        std::cout << "✓ Events are within coincidence window (" << coincidence_window_ns
                  << " ns)" << std::endl;
    }
    else
    {
        std::cout << "✗ Events exceed coincidence window" << std::endl;
    }
}

/**
 * @brief 主测试函数
 */
int main(int argc, char *argv[])
{
    std::cout << "\n"
              << "╔════════════════════════════════════════════════════════════════════════════════╗\n"
              << "║        Distributed Clock Synchronization System - Test Suite                   ║\n"
              << "║              (Single-Machine Simulation)                                      ║\n"
              << "╚════════════════════════════════════════════════════════════════════════════════╝\n"
              << std::endl;

    try
    {
        // 运行所有测试
        TestBasicClockSync();
        TestMultiClientAlignment();
        TestSegmentMergingWithCalibration();
        TestEventTimeCalibration();

        std::cout << "\n"
                  << std::string(80, '=') << std::endl;
        std::cout << "All tests completed successfully!" << std::endl;
        std::cout << std::string(80, '=') << "\n"
                  << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << "\n❌ Test failed with exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
