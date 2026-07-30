#pragma once

#include "dataplane/rdma/RdmaContext.hpp"
#include "dataplane/rdma/RdmaTypes.hpp"
#include "dataplane/rdma/SlotRing.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

struct ibv_mr;

namespace openpni::distributed::dataplane::rdma
{

using IngestSlotFn = std::function<bool(const SlotChunkView &view)>;

/**
 * Per-node receive ring on the coincidence host.
 * Supports real RoCE RC (when verbs device exists) and in-process memcpy.
 */
class RdmaNodeRecvSession
{
public:
    struct Config
    {
        uint32_t nodeId = 0;
        uint32_t slotCount = kDefaultSlotCount;
        size_t slotBytes = kDefaultSlotBytes;
        bool preferHugePages = true;
        std::string deviceName;
    };

    explicit RdmaNodeRecvSession(Config cfg);
    ~RdmaNodeRecvSession();

    bool prepare();
    RdmaEndpointInfo localEndpoint() const;
    bool acceptRemote(const RdmaEndpointInfo &remote);
    void setIngest(IngestSlotFn fn);

    /** Poll notify ring; returns number of slots ingested. */
    int pollOnce(int maxSlots = 8);

    uint32_t nodeId() const noexcept { return m_cfg.nodeId; }
    SlotRing &ring() noexcept { return m_ring; }
    uint64_t inprocessHandle() const noexcept { return m_inprocessHandle; }
    DataPlaneKind kind() const noexcept { return m_kind; }

    void close();

private:
    bool prepareVerbs();
    bool prepareInProcess();
    bool ingestSlot(uint32_t slotIndex, const NotifyEntry &note);

    Config m_cfg;
    DataPlaneKind m_kind = DataPlaneKind::InProcess;
    SlotRing m_ring;
    IngestSlotFn m_ingest;

    std::unique_ptr<RdmaDevice> m_device;
    std::unique_ptr<RdmaConnection> m_conn;
    ibv_mr *m_mr = nullptr;
    RdmaEndpointInfo m_localEp{};
    uint64_t m_inprocessHandle = 0;
    uint64_t m_nextExpectedNotifySeq = 1;
    std::atomic<bool> m_ready{false};

    struct PendingChunk
    {
        bool active = false;
        uint64_t chunkId = 0;
        uint64_t computerClockMs = 0;
        uint32_t durationMs = 0;
        std::vector<uint8_t> packed;
    };
    PendingChunk m_pending;
};

/**
 * Owns per-node receive sessions and an optional poller thread.
 */
class RdmaRecvServer
{
public:
    struct Config
    {
        uint32_t slotCount = kDefaultSlotCount;
        size_t slotBytes = kDefaultSlotBytes;
        bool preferHugePages = true;
        std::string deviceName;
        bool startPoller = true;
        int pollSleepUs = 10;
    };

    explicit RdmaRecvServer(Config cfg);
    ~RdmaRecvServer();

    void setIngest(IngestSlotFn fn);

    std::shared_ptr<RdmaNodeRecvSession> ensureSession(uint32_t nodeId);
    std::shared_ptr<RdmaNodeRecvSession> getSession(uint32_t nodeId) const;

    void start();
    void stop();

    static std::shared_ptr<RdmaNodeRecvSession> findInProcessSession(uint64_t handle);

private:
    void pollLoop();

    Config m_cfg;
    IngestSlotFn m_ingest;
    mutable std::mutex m_mutex;
    std::unordered_map<uint32_t, std::shared_ptr<RdmaNodeRecvSession>> m_sessions;
    std::thread m_poller;
    std::atomic<bool> m_running{false};
};

} // namespace openpni::distributed::dataplane::rdma
