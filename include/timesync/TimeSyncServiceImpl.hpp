#pragma once

#include "TimeSyncServer.hpp"
#include "timesync.pb.h"
#include "timesync.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <random>
#include <mutex>
#include <atomic>
#include <thread>

namespace openpni::distributed::timesync
{

    /**
     * @brief gRPC TimeSyncService 实现
     *
     * 将 TimeSyncServer 逻辑包装为 gRPC 服务
     */
    class TimeSyncServiceImpl : public TimeSyncService::Service
    {
    public:
        TimeSyncServiceImpl()
            : server_(std::make_unique<TimeSyncServer>())
        {
        }

        virtual ~TimeSyncServiceImpl() = default;

        /**
         * @brief 设置模拟延迟（用于测试）
         */
        void SetSimulatedDelay(uint64_t delay_ms)
        {
            SetSimulatedDelayRange(delay_ms, delay_ms);
        }

        /**
         * @brief 设置模拟随机延迟范围（用于测试）
         */
        void SetSimulatedDelayRange(uint64_t min_ms, uint64_t max_ms)
        {
            simulated_delay_min_ms_ = min_ms;
            simulated_delay_max_ms_ = max_ms;
        }

        /**
         * @brief 处理时钟同步请求 (RPC: SyncClock)
         */
        ::grpc::Status SyncClock(
            ::grpc::ServerContext *context,
            const ClockSyncRequest *request,
            ClockSyncResponse *response) override
        {
            try
            {
                // 1. 精确记录 T2 (Server Receive Time)
                // 务必在任何模拟延迟之前记录
                uint64_t t2_server_recv_ns = TimeUtil::GetMonotonicTimeNs();

                // 模拟网络/处理延迟
                uint64_t min_delay = simulated_delay_min_ms_.load();
                uint64_t max_delay = simulated_delay_max_ms_.load();

                if (max_delay > 0)
                {
                    uint64_t delay = 0;
                    if (min_delay >= max_delay)
                    {
                        delay = min_delay;
                    }
                    else
                    {
                        std::lock_guard<std::mutex> lock(rng_mutex_);
                        std::uniform_int_distribution<uint64_t> dist(min_delay, max_delay);
                        delay = dist(rng_);
                    }

                    if (delay > 0)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
                    }
                }

                // 3. 精确记录 T3 (Server Send Time)
                uint64_t t3_server_send_ns = TimeUtil::GetMonotonicTimeNs();

                uint64_t server_time_ns = 0;
                int64_t offset_ns = 0;
                float network_delay_ms = 0.0f;

                // 调用服务器逻辑，传入 T2
                bool success = server_->SyncClock(
                    request->client_id(),
                    request->client_hostname(),
                    request->client_local_time_ns(),
                    server_time_ns,
                    offset_ns,
                    network_delay_ms,
                    t2_server_recv_ns);

                if (!success)
                {
                    return ::grpc::Status(::grpc::StatusCode::INTERNAL,
                                          "Failed to synchronize clock");
                }

                // 填充响应
                response->set_server_time_ns(t2_server_recv_ns);
                response->set_client_request_time_ns(request->client_local_time_ns());
                response->set_response_time_ns(t3_server_send_ns);
                response->set_estimated_clock_offset_ns(offset_ns);
                response->set_network_delay_estimate_ms(network_delay_ms);

                return ::grpc::Status::OK;
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error in SyncClock: " << e.what() << std::endl;
                return ::grpc::Status(::grpc::StatusCode::INTERNAL,
                                      std::string("Exception: ") + e.what());
            }
        }

        /**
         * @brief 获取所有客户端的同步统计 (RPC: GetSyncStats)
         */
        ::grpc::Status GetSyncStats(
            ::grpc::ServerContext *context,
            const EmptyRequest *request,
            AllClientStats *response) override
        {
            try
            {
                auto stats = server_->GetAllClientStats();

                for (const auto &stat : stats)
                {
                    auto *client_stats = response->add_clients();
                    client_stats->set_client_id(stat.client_id);
                    client_stats->set_hostname(stat.hostname);
                    client_stats->set_last_sync_time(stat.last_sync_time);
                    client_stats->set_clock_offset_ns(stat.clock_offset_ns);
                    client_stats->set_clock_drift_rate(stat.clock_drift_rate);
                    client_stats->set_sync_count(stat.sync_count);
                }

                return ::grpc::Status::OK;
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error in GetSyncStats: " << e.what() << std::endl;
                return ::grpc::Status(::grpc::StatusCode::INTERNAL,
                                      std::string("Exception: ") + e.what());
            }
        }

        /**
         * @brief 流式时钟同步 (RPC: StreamingSync)
         */
        ::grpc::Status StreamingSync(
            ::grpc::ServerContext *context,
            ::grpc::ServerReaderWriter<ClockSyncResponse, ClockSyncRequest> *stream) override
        {
            try
            {
                ClockSyncRequest request;
                ClockSyncResponse response;

                while (stream->Read(&request))
                {
                    uint64_t server_time_ns = 0;
                    int64_t offset_ns = 0;
                    float network_delay_ms = 0.0f;

                    bool success = server_->SyncClock(
                        request.client_id(),
                        request.client_hostname(),
                        request.client_local_time_ns(),
                        server_time_ns,
                        offset_ns,
                        network_delay_ms);

                    if (!success)
                    {
                        return ::grpc::Status(::grpc::StatusCode::INTERNAL,
                                              "Failed to synchronize clock");
                    }

                    response.set_server_time_ns(server_time_ns);
                    response.set_client_request_time_ns(request.client_local_time_ns());
                    response.set_response_time_ns(TimeUtil::GetMonotonicTimeNs());
                    response.set_estimated_clock_offset_ns(offset_ns);
                    response.set_network_delay_estimate_ms(network_delay_ms);

                    if (!stream->Write(response))
                    {
                        return ::grpc::Status(::grpc::StatusCode::INTERNAL,
                                              "Failed to write response");
                    }
                }

                return ::grpc::Status::OK;
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error in StreamingSync: " << e.what() << std::endl;
                return ::grpc::Status(::grpc::StatusCode::INTERNAL,
                                      std::string("Exception: ") + e.what());
            }
        }

        /**
         * @brief 获取内部服务器实例（用于测试）
         */
        TimeSyncServer *GetServer()
        {
            return server_.get();
        }

    private:
        std::unique_ptr<TimeSyncServer> server_;
        std::atomic<uint64_t> simulated_delay_min_ms_{0};
        std::atomic<uint64_t> simulated_delay_max_ms_{0};
        std::mt19937 rng_{std::random_device{}()};
        std::mutex rng_mutex_;
    };

} // namespace openpni::distributed::timesync
