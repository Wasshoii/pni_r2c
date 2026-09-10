#include "grpcService/CoinPeerClient.hpp"

#include <algorithm>
#include <chrono>
#include <glog/logging.h>

namespace openpni::distributed::streaming
{
    CoinPeerClient::CoinPeerClient(CoinPeerClientConfig config)
        : m_config(std::move(config))
    {
    }

    CoinPeerClient::~CoinPeerClient()
    {
        stop();
    }

    bool CoinPeerClient::start()
    {
        if (m_running.load(std::memory_order_acquire))
        {
            return true;
        }
        m_channel = grpc::CreateChannel(m_config.masterAddress, grpc::InsecureChannelCredentials());
        m_stub = coincidence::CoincidenceService::NewStub(m_channel);
        if (!registerCoin())
        {
            m_stub.reset();
            m_channel.reset();
            return false;
        }
        m_running.store(true, std::memory_order_release);
        m_heartbeatThread = std::thread([this]()
                                        { heartbeatLoop(); });
        return true;
    }

    void CoinPeerClient::stop()
    {
        const bool wasRunning = m_running.exchange(false, std::memory_order_acq_rel);
        if (m_heartbeatThread.joinable())
        {
            m_heartbeatThread.join();
        }
        m_stub.reset();
        m_channel.reset();
        (void)wasRunning;
    }

    bool CoinPeerClient::isRunning() const
    {
        return m_running.load(std::memory_order_acquire);
    }

    bool CoinPeerClient::registerCoin()
    {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        coincidence::RegisterCoinRequest req;
        req.set_coin_id(m_config.coinId);
        req.set_listen_address(m_config.listenAddress);
        coincidence::RegisterCoinResponse resp;
        const grpc::Status status = m_stub->RegisterCoin(&context, req, &resp);
        if (!status.ok() || !resp.success())
        {
            LOG(ERROR) << "RegisterCoin failed: "
                       << (status.ok() ? resp.message() : status.error_message());
            return false;
        }
        LOG(INFO) << "RegisterCoin ok coin=" << m_config.coinId
                  << " master=" << m_config.masterAddress;
        return true;
    }

    void CoinPeerClient::heartbeatLoop()
    {
        const uint32_t intervalMs = m_config.heartbeatIntervalMs == 0 ? 1000 : m_config.heartbeatIntervalMs;
        while (m_running.load(std::memory_order_acquire))
        {
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            coincidence::HeartbeatRequest req;
            req.set_coin_id(m_config.coinId);
            req.set_timestamp_ms(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count()));
            coincidence::HeartbeatResponse resp;
            const grpc::Status status = m_stub->Heartbeat(&context, req, &resp);
            if (!status.ok() || !resp.acknowledged())
            {
                LOG_EVERY_N(WARNING, 20) << "compute-coin heartbeat failed coin=" << m_config.coinId
                                         << " " << (status.ok() ? "nack" : status.error_message());
            }
            for (uint32_t slept = 0; slept < intervalMs && m_running.load(std::memory_order_acquire);)
            {
                const uint32_t step = std::min<uint32_t>(50, intervalMs - slept);
                std::this_thread::sleep_for(std::chrono::milliseconds(step));
                slept += step;
            }
        }
    }

} // namespace openpni::distributed::streaming
