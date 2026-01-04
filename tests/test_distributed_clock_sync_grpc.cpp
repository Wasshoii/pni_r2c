#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <memory>
#include <vector>
#include <grpcpp/grpcpp.h>

// 包含生成的 protobuf 和 gRPC 代码
#include "timesync.pb.h"
#include "timesync.grpc.pb.h"

// 包含服务器实现
#include "include/timesync/TimeSyncServiceImpl.hpp"
#include "include/timesync/TimeSyncCommon.hpp"

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
              << std::endl;

    return 0;
}
