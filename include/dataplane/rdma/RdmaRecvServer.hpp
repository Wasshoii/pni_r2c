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

struct ibv_mr;

namespace openpni::distributed::dataplane::rdma
{

using IngestSlotFn = std::function<bool(const SlotChunkView &view)>;
/** Diagnostic: count singles and return credit without reading payload. */
using CreditOnlyFn = std::function<void(uint32_t nodeId, uint32_t singlesCount)>;

enum class IngestMode : uint8_t
{
    /** Zero-copy per-slot views into the receive ring (production default). */
    Full = 0,
    /** Advance credit only; optional count callback. Test/benchmark diagnostic. */
    CreditOnly = 1,
};

/**
 * Per-node receive ring on the coincidence host.
 * Supports real RoCE RC (when verbs device exists) and in-process memcpy.
 *
 * Full mode delivers one SlotChunkView per slot (no host-side reassembly copy).
 * Multi-slot logical chunks share chunkId; SOF/EOF/PARTIAL are in view.flags.
 * Payload pointers remain valid only for the duration of the ingest callback.
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
        bool forceInProcess = false;
        bool requireRoce = false;
        int gidIndex = -1;
        int recvWr = 128;
    };

    explicit RdmaNodeRecvSession(Config cfg);
    ~RdmaNodeRecvSession();

    bool prepare();
    RdmaEndpointInfo localEndpoint() const;
    bool acceptRemote(const RdmaEndpointInfo &remote);
    void setIngest(IngestSlotFn fn);
    void setIngestMode(IngestMode mode);
    void setCreditOnly(CreditOnlyFn fn);

    /** Poll notify ring; returns number of slots ingested. */
    int pollOnce(int maxSlots = 8);

    uint32_t nodeId() const noexcept { return m_cfg.nodeId; }
    SlotRing &ring() noexcept { return m_ring; }
    uint64_t inprocessHandle() const noexcept { return m_inprocessHandle; }
    DataPlaneKind kind() const noexcept { return m_kind; }
    IngestMode ingestMode() const noexcept { return m_mode; }

    void close();

private:
    bool prepareVerbs();
    bool prepareInProcess();
    bool ingestSlot(uint32_t slotIndex, const NotifyEntry *note);
    bool releaseSlot(uint32_t slotIndex);
    int pollNotifyRing(int maxSlots);
    int pollVerbsCompletions(int maxSlots);
    int retryPendingReady();
    bool postCreditWrite();

    Config m_cfg;
    DataPlaneKind m_kind = DataPlaneKind::InProcess;
    SlotRing m_ring;
    IngestSlotFn m_ingest;
    CreditOnlyFn m_creditOnly;
    IngestMode m_mode = IngestMode::Full;

    std::unique_ptr<RdmaDevice> m_device;
    std::unique_ptr<RdmaConnection> m_conn;
    ibv_mr *m_mr = nullptr;
    RdmaEndpointInfo m_localEp{};
    RdmaEndpointInfo m_remoteEp{};
    uint64_t m_inprocessHandle = 0;
    uint64_t m_nextExpectedNotifySeq = 1;
    bool m_pendingReady = false;
    int m_creditWritesSinceSignal = 0;
    std::atomic<bool> m_ready{false};
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
        bool forceInProcess = false;
        bool requireRoce = false;
        int gidIndex = -1;
        int recvWr = 128;
    };

    explicit RdmaRecvServer(Config cfg);
    ~RdmaRecvServer();

    void setIngest(IngestSlotFn fn);
    void setIngestMode(IngestMode mode);
    void setCreditOnly(CreditOnlyFn fn);

    std::shared_ptr<RdmaNodeRecvSession> ensureSession(uint32_t nodeId);
    std::shared_ptr<RdmaNodeRecvSession> getSession(uint32_t nodeId) const;

    void start();
    void stop();

    static std::shared_ptr<RdmaNodeRecvSession> findInProcessSession(uint64_t handle);

private:
    void pollLoop();
    void applyCallbacksLocked();

    Config m_cfg;
    IngestSlotFn m_ingest;
    CreditOnlyFn m_creditOnly;
    IngestMode m_mode = IngestMode::Full;
    mutable std::mutex m_mutex;
    std::unordered_map<uint32_t, std::shared_ptr<RdmaNodeRecvSession>> m_sessions;
    std::thread m_poller;
    std::atomic<bool> m_running{false};
    bool m_pollerSessionsDirty = true;
};

} // namespace openpni::distributed::dataplane::rdma
