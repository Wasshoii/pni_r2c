#pragma once

#include "protos/coincidence.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

namespace openpni::distributed::streaming
{

    struct CoinPeerClientConfig
    {
        std::string masterAddress = "127.0.0.1:50061";
        uint32_t coinId = 1;
        std::string listenAddress = "0.0.0.0:50062";
        uint32_t heartbeatIntervalMs = 1000;
    };

    /** Compute-coin control-plane client: RegisterCoin + Heartbeat to Master. */
    class CoinPeerClient
    {
    public:
        explicit CoinPeerClient(CoinPeerClientConfig config);
        ~CoinPeerClient();

        bool start();
        void stop();
        bool isRunning() const;

    private:
        bool registerCoin();
        void heartbeatLoop();

        CoinPeerClientConfig m_config;
        std::shared_ptr<grpc::Channel> m_channel;
        std::unique_ptr<coincidence::CoincidenceService::Stub> m_stub;
        std::thread m_heartbeatThread;
        std::atomic<bool> m_running{false};
    };

} // namespace openpni::distributed::streaming
