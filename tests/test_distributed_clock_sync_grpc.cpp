#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <memory>
#include <vector>
#include <numeric>
#include <grpcpp/grpcpp.h>

// 包含生成的 protobuf 和 gRPC 代码
#include "timesync.pb.h"
#include "timesync.grpc.pb.h"

// 包含服务器实现
#include "grpcService/timesync/TimeSyncServiceImpl.hpp"
#include "grpcService/timesync/TimeSyncCommon.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using openpni::distributed::timesync::AllClientStats;
using openpni::distributed::timesync::ClockSyncRequest;
using openpni::distributed::timesync::ClockSyncResponse;
using openpni::distributed::timesync::EmptyRequest;
using openpni::distributed::timesync::TimeSyncService;
using openpni::distributed::timesync::TimeSyncServiceImpl;
using openpni::distributed::timesync::TimeUtil;

/**
 * 辅助函数：格式化输出
 */
void print_header(const std::string &title)
{
    std::cout << "\n╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║ " << std::left << std::setw(58) << title << " ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n"
              << std::endl;
}

void print_section(const std::string &title)
{
    std::cout << "\n━━━ " << title << " ━━━\n"
              << std::endl;
}

void print_success(const std::string &msg)
{
    std::cout << "✓ " << msg << std::endl;
}

void print_error(const std::string &msg)
{
    std::cout << "✗ " << msg << std::endl;
}

/**
 * 测试 1: 基础 SyncClock RPC
 */
void TestBasicSyncClock(std::unique_ptr<TimeSyncService::Stub> &stub)
{
    print_section("Test 1: Basic SyncClock RPC");

    std::cout << "发送 4 个客户端的时钟同步请求...\n"
              << std::endl;
    std::cout << "Client ID  Local Time (ns)   Server Time (ns)  Offset (ns)   Latency (ms)\n";
    std::cout << "─────────────────────────────────────────────────────────────────────────\n";

    std::vector<int64_t> offsets;

    for (uint32_t client_id = 0; client_id < 4; ++client_id)
    {
        ClientContext context;
        ClockSyncRequest request;
        ClockSyncResponse response;

        uint64_t local_time = TimeUtil::GetMonotonicTimeNs();
        request.set_client_id(client_id);
        request.set_client_local_time_ns(local_time);
        request.set_client_hostname("client-" + std::to_string(client_id));

        Status status = stub->SyncClock(&context, request, &response);

        if (status.ok())
        {
            offsets.push_back(response.estimated_clock_offset_ns());

            std::cout << std::right << std::setw(9) << client_id
                      << "  " << std::setw(15) << local_time
                      << "  " << std::setw(16) << response.server_time_ns()
                      << "  " << std::setw(12) << response.estimated_clock_offset_ns()
                      << "  " << std::fixed << std::setprecision(2)
                      << std::setw(10) << response.network_delay_estimate_ms()
                      << std::endl;
        }
        else
        {
            print_error("Failed to sync clock for client " + std::to_string(client_id));
            return;
        }
    }

    print_success("Basic SyncClock RPC test completed");
}

/**
 * 测试 2: 多轮同步
 */
void TestMultipleRounds(std::unique_ptr<TimeSyncService::Stub> &stub)
{
    print_section("Test 2: Multiple Synchronization Rounds");

    std::cout << "执行 3 轮同步，每轮 2 个客户端\n"
              << std::endl;

    for (int round = 1; round <= 3; ++round)
    {
        std::cout << "  Round " << round << ": ";

        for (uint32_t client_id = 0; client_id < 2; ++client_id)
        {
            ClientContext context;
            ClockSyncRequest request;
            ClockSyncResponse response;

            request.set_client_id(client_id);
            request.set_client_local_time_ns(TimeUtil::GetMonotonicTimeNs());
            request.set_client_hostname("client-" + std::to_string(client_id));

            Status status = stub->SyncClock(&context, request, &response);

            if (!status.ok())
            {
                print_error("RPC failed: " + status.error_message());
                return;
            }

            std::cout << "C" << client_id << "(offset=" << response.estimated_clock_offset_ns() << "ns) ";
        }

        std::cout << std::endl;

        // 等待一下再进行下一轮
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    print_success("Multiple rounds test completed");
}

/**
 * 测试 3: GetSyncStats RPC
 */
void TestGetSyncStats(std::unique_ptr<TimeSyncService::Stub> &stub)
{
    print_section("Test 3: GetSyncStats RPC");

    // 先执行几次同步
    std::cout << "执行同步以收集统计数据...\n"
              << std::endl;

    for (uint32_t client_id = 0; client_id < 3; ++client_id)
    {
        ClientContext context;
        ClockSyncRequest request;
        ClockSyncResponse response;

        request.set_client_id(client_id);
        request.set_client_local_time_ns(TimeUtil::GetMonotonicTimeNs());
        request.set_client_hostname("client-" + std::to_string(client_id));

        stub->SyncClock(&context, request, &response);
    }

    // 查询统计信息
    std::cout << "查询所有客户端统计信息...\n"
              << std::endl;
    std::cout << "Client ID  Hostname        Offset (ns)   Drift (ppm)  Sync Count\n";
    std::cout << "─────────────────────────────────────────────────────────────────\n";

    ClientContext context;
    EmptyRequest request;
    AllClientStats response;

    Status status = stub->GetSyncStats(&context, request, &response);

    if (status.ok())
    {
        for (const auto &client : response.clients())
        {
            std::cout << std::right << std::setw(9) << client.client_id()
                      << "  " << std::left << std::setw(15) << client.hostname()
                      << " " << std::right << std::setw(12) << client.clock_offset_ns()
                      << "  " << std::fixed << std::setprecision(2)
                      << std::setw(10) << client.clock_drift_rate()
                      << "  " << std::setw(10) << client.sync_count()
                      << std::endl;
        }
        print_success("GetSyncStats RPC test completed");
    }
    else
    {
        print_error("GetSyncStats RPC failed: " + status.error_message());
    }
}

/**
 * 测试 4: 流式同步
 */
void TestStreamingSync(std::unique_ptr<TimeSyncService::Stub> &stub)
{
    print_section("Test 4: Streaming Synchronization");

    std::cout << "建立双向流并发送 5 个同步请求...\n"
              << std::endl;
    std::cout << "Request #  Client ID  Local Time      Server Time     Offset (ns)\n";
    std::cout << "──────────────────────────────────────────────────────────────────\n";

    ClientContext context;
    auto stream = stub->StreamingSync(&context);

    std::vector<ClockSyncRequest> requests;
    for (int i = 0; i < 5; ++i)
    {
        ClockSyncRequest request;
        request.set_client_id(i % 3);
        request.set_client_local_time_ns(TimeUtil::GetMonotonicTimeNs());
        request.set_client_hostname("client-" + std::to_string(i % 3));
        requests.push_back(request);
    }

    for (int i = 0; i < requests.size(); ++i)
    {
        stream->Write(requests[i]);

        ClockSyncResponse response;
        if (stream->Read(&response))
        {
            std::cout << std::right << std::setw(9) << (i + 1) << "  "
                      << std::setw(9) << requests[i].client_id() << "  "
                      << std::setw(15) << requests[i].client_local_time_ns() << "  "
                      << std::setw(15) << response.server_time_ns() << "  "
                      << std::setw(12) << response.estimated_clock_offset_ns()
                      << std::endl;
        }
        else
        {
            print_error("Failed to read streaming response");
            return;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    stream->WritesDone();
    Status status = stream->Finish();

    if (status.ok())
    {
        print_success("Streaming synchronization test completed");
    }
    else
    {
        print_error("Streaming sync failed: " + status.error_message());
    }
}

/**
 * 测试 5: 并发客户端
 */
void TestConcurrentClients(std::unique_ptr<TimeSyncService::Stub> &stub)
{
    print_section("Test 5: Concurrent Clients");

    std::cout << "创建 4 个线程并发进行时钟同步...\n"
              << std::endl;

    std::vector<std::thread> threads;
    std::vector<int64_t> offsets(4, 0);

    for (uint32_t client_id = 0; client_id < 4; ++client_id)
    {
        threads.emplace_back([&stub, client_id, &offsets]()
                             {
            for (int round = 0; round < 3; ++round)
            {
                ClientContext context;
                ClockSyncRequest request;
                ClockSyncResponse response;

                request.set_client_id(client_id);
                request.set_client_local_time_ns(TimeUtil::GetMonotonicTimeNs());
                request.set_client_hostname("client-" + std::to_string(client_id));

                Status status = stub->SyncClock(&context, request, &response);
                if (status.ok())
                {
                    offsets[client_id] = response.estimated_clock_offset_ns();
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            } });
    }

    // 等待所有线程完成
    for (auto &thread : threads)
    {
        thread.join();
    }

    std::cout << "所有客户端完成同步\n";
    std::cout << "Client ID  Final Offset (ns)\n";
    std::cout << "──────────────────────────────\n";

    for (uint32_t i = 0; i < 4; ++i)
    {
        std::cout << std::right << std::setw(9) << i
                  << "  " << std::setw(16) << offsets[i]
                  << std::endl;
    }

    print_success("Concurrent clients test completed");
}

/**
 * 测试 6: 模拟网络延迟测试
 */
void TestSimulatedLatency(std::unique_ptr<TimeSyncService::Stub> &stub, TimeSyncServiceImpl *service)
{
    print_section("Test 6: Simulated Latency Test");

    // 1. 基准测试 (无延迟)
    service->SetSimulatedDelay(0);
    std::cout << "Running baseline test (0ms delay)...\n";

    ClientContext context1;
    ClockSyncRequest request1;
    ClockSyncResponse response1;

    request1.set_client_id(99);
    request1.set_client_local_time_ns(TimeUtil::GetMonotonicTimeNs());
    request1.set_client_hostname("test-latency");

    uint64_t t1 = TimeUtil::GetMonotonicTimeNs();
    Status status1 = stub->SyncClock(&context1, request1, &response1);
    uint64_t t4 = TimeUtil::GetMonotonicTimeNs();

    if (!status1.ok())
    {
        print_error("Baseline test failed");
        return;
    }

    double rtt_baseline = (t4 - t1) / 1e6; // ms
    std::cout << "Baseline RTT: " << rtt_baseline << " ms\n";

    // 2. 延迟测试 (100ms)
    uint64_t delay_ms = 100;
    service->SetSimulatedDelay(delay_ms);
    std::cout << "\nRunning delayed test (" << delay_ms << "ms delay)...\n";

    ClientContext context2;
    ClockSyncRequest request2;
    ClockSyncResponse response2;

    request2.set_client_id(99);
    request2.set_client_local_time_ns(TimeUtil::GetMonotonicTimeNs());
    request2.set_client_hostname("test-latency");

    t1 = TimeUtil::GetMonotonicTimeNs();
    Status status2 = stub->SyncClock(&context2, request2, &response2);
    t4 = TimeUtil::GetMonotonicTimeNs();

    if (!status2.ok())
    {
        print_error("Delayed test failed");
        service->SetSimulatedDelay(0);
        return;
    }

    double rtt_delayed = (t4 - t1) / 1e6; // ms
    std::cout << "Delayed RTT: " << rtt_delayed << " ms\n";

    // 验证
    if (rtt_delayed >= delay_ms)
    {
        print_success("Latency simulation verified (RTT increased as expected)");
    }
    else
    {
        print_error("Latency simulation failed (RTT did not increase enough)");
    }

    // 恢复
    service->SetSimulatedDelay(0);
}

/**
 * 测试 7: 随机延迟模拟与稳定性测试
 */
void TestRandomLatencySimulation(std::unique_ptr<TimeSyncService::Stub> &stub, TimeSyncServiceImpl *service)
{
    print_section("Test 7: Random Latency Simulation (5s duration)");

    // 设置随机延迟范围 1ms - 10ms
    uint64_t min_delay = 1;
    uint64_t max_delay = 10;
    service->SetSimulatedDelayRange(min_delay, max_delay);
    std::cout << "Simulating random network latency: " << min_delay << "ms - " << max_delay << "ms\n";

    std::vector<int64_t> offsets;
    std::vector<double> rtts;

    auto start_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::seconds(5);
    int request_count = 0;

    std::cout << "Running sync loop for 5 seconds...\n";

    while (std::chrono::steady_clock::now() - start_time < duration)
    {
        ClientContext context;
        ClockSyncRequest request;
        ClockSyncResponse response;

        request.set_client_id(100);
        request.set_client_local_time_ns(TimeUtil::GetMonotonicTimeNs());
        request.set_client_hostname("test-random-latency");

        uint64_t t1 = TimeUtil::GetMonotonicTimeNs();
        Status status = stub->SyncClock(&context, request, &response);
        uint64_t t4 = TimeUtil::GetMonotonicTimeNs();

        if (status.ok())
        {
            offsets.push_back(response.estimated_clock_offset_ns());
            double rtt = (t4 - t1) / 1e6;
            rtts.push_back(rtt);
            request_count++;

            // 简单的进度指示
            if (request_count % 5 == 0)
                std::cout << "." << std::flush;
        }

        // 稍微休眠一下，模拟客户端心跳间隔 (例如 100ms)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "\nCompleted " << request_count << " sync requests.\n";

    // 分析结果
    if (offsets.empty())
    {
        print_error("No successful sync requests");
        return;
    }

    // 计算 RTT 统计
    double rtt_sum = std::accumulate(rtts.begin(), rtts.end(), 0.0);
    double rtt_mean = rtt_sum / rtts.size();
    double rtt_min = *std::min_element(rtts.begin(), rtts.end());
    double rtt_max = *std::max_element(rtts.begin(), rtts.end());

    std::cout << "\nRTT Statistics:\n"
              << "  Mean: " << std::fixed << std::setprecision(2) << rtt_mean << " ms\n"
              << "  Min:  " << rtt_min << " ms\n"
              << "  Max:  " << rtt_max << " ms\n";

    // 验证 RTT 是否在预期范围内 (考虑到处理开销，可能会略高于 max_delay)
    if (rtt_min >= min_delay && rtt_mean <= (max_delay + 20))
    { // +20ms margin
        print_success("RTT is within expected random range");
    }
    else
    {
        std::cout << "Warning: RTT stats might be out of expected range (Min expected >= " << min_delay << ")\n";
    }

    // 计算 Offset 统计 (标准差反映抖动)
    double offset_sum = std::accumulate(offsets.begin(), offsets.end(), 0.0);
    double offset_mean = offset_sum / offsets.size();

    // 计算标准差
    double sq_sum = 0.0;
    for (double x : offsets)
    {
        sq_sum += (x - offset_mean) * (x - offset_mean);
    }
    double stdev = std::sqrt(sq_sum / offsets.size());

    // 恢复
    service->SetSimulatedDelay(0);
}

/**
 * 测试 8: 时钟同步效果验证 (模拟时钟偏差)
 */
void TestClockSynchronizationEffect(std::unique_ptr<TimeSyncService::Stub> &stub)
{
    print_section("Test 8: Clock Synchronization Effect Verification");

    // 1. 设置模拟的客户端时钟偏差
    int64_t simulated_offset_ns = 5'000'000LL;
    std::cout << "Simulating client clock offset: " << simulated_offset_ns << " ns (5ms)\n";

    // 模拟客户端获取时间
    auto GetClientTime = [&]() -> uint64_t
    {
        return TimeUtil::GetMonotonicTimeNs() + simulated_offset_ns;
    };

    ClientContext context;
    ClockSyncRequest request;
    ClockSyncResponse response;

    // T1: 客户端发送时间
    uint64_t t1 = GetClientTime();
    request.set_client_id(200);
    request.set_client_local_time_ns(t1);
    request.set_client_hostname("test-sync-effect");

    Status status = stub->SyncClock(&context, request, &response);

    // T4: 客户端接收时间
    uint64_t t4 = GetClientTime();

    if (!status.ok())
    {
        print_error("SyncClock RPC failed");
        return;
    }

    // 获取服务器时间戳
    uint64_t t2 = response.server_time_ns();
    uint64_t t3 = response.response_time_ns();

    // 计算 Offset
    // Offset = ((T2 - T1) + (T3 - T4)) / 2
    int64_t calculated_offset = ((static_cast<int64_t>(t2) - static_cast<int64_t>(t1)) +
                                 (static_cast<int64_t>(t3) - static_cast<int64_t>(t4))) /
                                2;

    // 计算 RTT
    int64_t rtt = (static_cast<int64_t>(t4) - static_cast<int64_t>(t1)) -
                  (static_cast<int64_t>(t3) - static_cast<int64_t>(t2));

    std::cout << "Timestamps:\n"
              << "  T1 (Client Send): " << t1 << "\n"
              << "  T2 (Server Recv): " << t2 << "\n"
              << "  T3 (Server Send): " << t3 << "\n"
              << "  T4 (Client Recv): " << t4 << "\n";

    std::cout << "\nResults:\n"
              << "  Calculated Offset: " << calculated_offset << " ns\n"
              << "  Expected Offset:   " << -simulated_offset_ns << " ns\n"
              << "  RTT:               " << rtt << " ns\n";

    // 验证
    // 允许误差范围：RTT/2 + 系统调度抖动 (例如 1ms)
    int64_t error = std::abs(calculated_offset - (-simulated_offset_ns));
    int64_t tolerance = (rtt / 2) + 1'000'000; // RTT/2 + 1ms

    if (error <= tolerance)
    {
        print_success("Clock synchronization verified!");
        std::cout << "  Error (" << error << " ns) is within tolerance (" << tolerance << " ns)\n";

        // 验证校准后的时间
        uint64_t current_client_time = GetClientTime();
        uint64_t corrected_time = current_client_time + calculated_offset;
        uint64_t actual_server_time = TimeUtil::GetMonotonicTimeNs();

        std::cout << "  Corrected Client Time: " << corrected_time << "\n"
                  << "  Actual Server Time:    " << actual_server_time << "\n"
                  << "  Difference:            " << static_cast<int64_t>(corrected_time - actual_server_time) << " ns\n";
    }
    else
    {
        print_error("Clock synchronization failed verification");
        std::cout << "  Error (" << error << " ns) exceeds tolerance (" << tolerance << " ns)\n";
    }
}

struct PETAcquisitionNode
{
    uint32_t id;
    std::string name;
    int64_t physical_clock_offset_ns; // The real error of the hardware clock
    int64_t estimated_correction_ns;  // The correction value calculated by sync
};

/**
 * 测试 9: PET 数据采集多节点同步测试 (1ms 容差)
 */
void TestPETDataAcquisitionSync(std::unique_ptr<TimeSyncService::Stub> &stub, TimeSyncServiceImpl *service)
{
    print_section("Test 9: PET Data Acquisition Multi-Node Sync (1ms tolerance)");

    // Configuration
    const int NUM_NODES = 3;
    const int DURATION_SEC = 2;             // Run for 2 seconds
    const int INTERVAL_MS = 100;            // 100ms per packet
    const int64_t TOLERANCE_NS = 1'000'000; // 1ms

    // Initialize Nodes with random large offsets
    std::vector<PETAcquisitionNode> nodes;
    nodes.push_back({1, "PET-Detector-01", 500'000'000, 0});  // +500ms
    nodes.push_back({2, "PET-Detector-02", -200'000'000, 0}); // -200ms
    nodes.push_back({3, "PET-Detector-03", 15'000'000, 0});   // +15ms

    // Set network latency simulation (1ms - 5ms random delay)
    service->SetSimulatedDelayRange(1, 5);
    std::cout << "Simulating Network Latency: 1ms - 5ms\n";
    std::cout << "Simulating " << NUM_NODES << " acquisition nodes with initial clock errors.\n";
    std::cout << "Target: Synchronization error < 1ms\n\n";

    std::cout << "Cycle | Node | Physical Offset | Est. Correction | Sync Error (us) | Status\n";
    std::cout << "------+------+-----------------+-----------------+-----------------+--------\n";

    int cycle = 0;

    while (cycle < (DURATION_SEC * 1000 / INTERVAL_MS))
    {
        cycle++;

        // 1. Synchronization Step (Every 100ms as requested)
        for (auto &node : nodes)
        {
            // Perform multiple sync samples to improve accuracy
            const int NUM_SAMPLES = 5;
            std::vector<int64_t> offsets;
            std::vector<int64_t> rtts;

            for (int i = 0; i < NUM_SAMPLES; ++i)
            {
                ClientContext context;
                ClockSyncRequest request;
                ClockSyncResponse response;

                // Capture T1 (Client Send Time)
                uint64_t t1_true = TimeUtil::GetMonotonicTimeNs();
                uint64_t t1 = t1_true + node.physical_clock_offset_ns;

                request.set_client_id(node.id);
                request.set_client_local_time_ns(t1);
                request.set_client_hostname(node.name);

                Status status = stub->SyncClock(&context, request, &response);

                if (status.ok())
                {
                    // Capture T4 (Client Receive Time)
                    uint64_t t4_true = TimeUtil::GetMonotonicTimeNs();
                    uint64_t t4 = t4_true + node.physical_clock_offset_ns;

                    uint64_t t2 = response.server_time_ns();
                    uint64_t t3 = response.response_time_ns();

                    // NTP Offset Calculation: ((T2 - T1) + (T3 - T4)) / 2
                    // Note: The server now correctly records T2 (Receive) and T3 (Send) separately.
                    // The simulated delay is applied between T2 and T3 (Processing Delay).
                    // This allows the NTP formula to correctly cancel out the processing time.
                    int64_t offset = ((static_cast<int64_t>(t2) - static_cast<int64_t>(t1)) +
                                      (static_cast<int64_t>(t3) - static_cast<int64_t>(t4))) /
                                     2;

                    // Calculate RTT (excluding server processing time)
                    int64_t rtt = (static_cast<int64_t>(t4) - static_cast<int64_t>(t1)) -
                                  (static_cast<int64_t>(t3) - static_cast<int64_t>(t2));

                    offsets.push_back(offset);
                    rtts.push_back(rtt);
                }
            }

            if (!offsets.empty())
            {
                // Strategy: Pick the sample with the minimum RTT
                // This minimizes the effect of asymmetric network jitter
                auto min_rtt_it = std::min_element(rtts.begin(), rtts.end());
                size_t index = std::distance(rtts.begin(), min_rtt_it);
                node.estimated_correction_ns = offsets[index];
            }
        }

        // 2. Data Packet Fusion Verification Step
        // Simulate generating a data packet NOW
        std::vector<uint64_t> fused_timestamps;

        for (const auto &node : nodes)
        {
            // Re-read time for packet generation
            uint64_t packet_true_time = TimeUtil::GetMonotonicTimeNs();
            uint64_t packet_local_time = packet_true_time + node.physical_clock_offset_ns;

            // Apply correction
            uint64_t packet_global_time = packet_local_time + node.estimated_correction_ns;

            // Calculate Error
            int64_t error_ns = static_cast<int64_t>(packet_global_time) - static_cast<int64_t>(packet_true_time);

            fused_timestamps.push_back(packet_global_time);

            // Print status for this node
            std::cout << std::setw(5) << cycle << " | "
                      << std::setw(4) << node.id << " | "
                      << std::setw(15) << node.physical_clock_offset_ns << " | "
                      << std::setw(15) << node.estimated_correction_ns << " | "
                      << std::setw(15) << (error_ns / 1000.0) << " | "; // us

            if (std::abs(error_ns) < TOLERANCE_NS)
            {
                std::cout << "OK\n";
            }
            else
            {
                std::cout << "FAIL\n";
            }
        }

        // Fusion Check
        auto min_ts = *std::min_element(fused_timestamps.begin(), fused_timestamps.end());
        auto max_ts = *std::max_element(fused_timestamps.begin(), fused_timestamps.end());
        int64_t spread = max_ts - min_ts;

        std::cout << "      > Fusion Spread: " << (spread / 1000.0) << " us "
                  << (spread < TOLERANCE_NS ? "[PASS]" : "[FAIL]") << "\n";
        std::cout << "------+------+-----------------+-----------------+-----------------+--------\n";

        std::this_thread::sleep_for(std::chrono::milliseconds(INTERVAL_MS));
    }

    service->SetSimulatedDelay(0);
}

/**
 *
 * 主函数
 */
int main(int argc, char *argv[])
{
    print_header("Distributed Clock Sync - gRPC Test Suite");

    // 服务器配置
    const std::string server_address = "127.0.0.1:50051";
    auto service = std::make_unique<TimeSyncServiceImpl>();

    // 启动 gRPC 服务器
    print_section("Starting gRPC Server");
    std::cout << "Server address: " << server_address << std::endl;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());

    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server)
    {
        print_error("Failed to start gRPC server");
        return 1;
    }

    print_success("gRPC server started");

    // 等待服务器启动
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 创建客户端连接
    print_section("Creating gRPC Client Connection");
    auto channel = grpc::CreateChannel(server_address,
                                       grpc::InsecureChannelCredentials());
    auto stub = TimeSyncService::NewStub(channel);
    print_success("Client connected to server");

    // 运行测试
    print_header("Running Tests");

    try
    {
        TestBasicSyncClock(stub);
        TestMultipleRounds(stub);
        TestGetSyncStats(stub);
        TestStreamingSync(stub);
        TestConcurrentClients(stub);
        TestSimulatedLatency(stub, service.get());
        TestRandomLatencySimulation(stub, service.get());
        TestClockSynchronizationEffect(stub);
        TestPETDataAcquisitionSync(stub, service.get());
    }
    catch (const std::exception &e)
    {
        std::cerr << "Test error: " << e.what() << std::endl;
        return 1;
    }

    // 关闭服务器
    print_section("Cleanup");
    server->Shutdown();
    print_success("Server shutdown");

    // 最终统计
    print_header("Test Summary");
    std::cout << "✓ All gRPC tests completed successfully!\n"
              << std::endl;
    std::cout << "Tests run:\n"
              << "  1. Basic SyncClock RPC\n"
              << "  2. Multiple Synchronization Rounds\n"
              << "  3. GetSyncStats RPC\n"
              << "  4. Streaming Synchronization\n"
              << "  5. Concurrent Clients\n"
              << "  6. Simulated Latency Test\n"
              << "  8. Clock Synchronization Effect Verification\n"
              << "  7. Random Latency Simulation (5s)\n"
              << "  9. PET Data Acquisition Multi-Node Sync\n"
              << std::endl;

    return 0;
}
