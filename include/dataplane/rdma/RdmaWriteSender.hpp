#pragma once

#include "dataplane/rdma/HugepageArena.hpp"
#include "dataplane/rdma/RdmaContext.hpp"
#include "dataplane/rdma/RdmaTypes.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

struct ibv_mr;

namespace openpni::distributed::dataplane::rdma
{

class RdmaNodeRecvSession;

struct TxSlotLease
{
    uint32_t localIndex = UINT32_MAX;
    SlotHeader *header = nullptr;
    uint8_t *payload = nullptr;
    size_t payloadCapacity = 0;
};

/**
 * Node-side RDMA WRITE_WITH_IMM (or in-process memcpy) sender into coin receive ring.
 */
class RdmaWriteSender
{
public:
    struct Config
    {
        uint32_t nodeId = 0;
        std::string deviceName;
        size_t stagingSlotBytes = kDefaultSlotBytes;
        bool preferHugePages = true;
        bool forceInProcess = false;
        bool requireRoce = false;
        int gidIndex = -1;
        uint32_t txSlotCount = 8;
    };

    explicit RdmaWriteSender(Config cfg);
    ~RdmaWriteSender();

    /** Create local QP (INIT) and fill endpoint for OpenDataPlane handshake. */
    bool prepareLocalEndpoint(RdmaEndpointInfo *outLocal);

    bool connect(const RdmaEndpointInfo &coinEndpoint);
    void close();

    /**
     * Pack singles into one or more slots and push ordered by chunkId.
     * singles must be contiguous openpni::Single / 16-byte packed layout.
     * RoCE copies into a registered TX slot. InProcess writes the source
     * buffer directly into the receive ring (one memcpy).
     */
    bool sendPackedSingles(
        uint64_t chunkId,
        uint64_t computerClockMs,
        uint32_t durationMs,
        const void *singlesPacked,
        uint32_t singlesCount);

    /** Acquire a registered TX slot (verbs). InProcess synthesizes a heap slot. */
    bool acquireTxSlot(TxSlotLease *out);
    /** Write a filled TX slot to the next remote ring slot (waits credit). */
    bool commitTxSlot(const TxSlotLease &lease, const SlotHeader &hdr, uint32_t singlesCount);
    /** Drop a lease without posting (enqueue/send aborted). */
    void abortTxSlot(const TxSlotLease &lease);

    bool txCudaRegistered() const noexcept { return m_txCudaRegistered; }
    void setTxCudaRegistered(bool registered) noexcept { m_txCudaRegistered = registered; }
    void *txStagingBase() const noexcept;
    size_t txStagingBytes() const noexcept;
    uint64_t singlesSent() const noexcept { return m_singlesSent.load(); }
    uint64_t slotsSent() const noexcept { return m_slotsSent.load(); }
    bool ok() const noexcept { return m_connected; }
    DataPlaneKind kind() const noexcept { return m_kind; }
    uint32_t slotCount() const noexcept { return m_slotCount; }
    size_t slotStride() const noexcept { return m_slotStride; }
    uint64_t slotsInFlight() const noexcept;
    uint32_t creditRemaining() const noexcept;
    std::atomic<uint64_t> *creditMirror() noexcept { return m_creditMirror; }

private:
    bool connectVerbs(const RdmaEndpointInfo &coin);
    bool connectInProcess(const RdmaEndpointInfo &coin);
    bool waitForCredit(uint64_t needProducerSeq);
    bool setupTxArena();
    bool tryAcquireTxSlotLocked(TxSlotLease *out);
    bool acquireTxSlotLocked(TxSlotLease *out);
    void clearTxBusyLocked(uint32_t localIndex);
    bool recycleTxCompletionsLocked();
    uint8_t *txSlotBase(uint32_t localIndex) noexcept;
    bool postRemoteSlotLocked(uint32_t localIndex, uint32_t remoteSlot, uint32_t length, uint64_t seq);
    bool writeSlotInProcess(uint32_t slotIndex, const SlotHeader &hdr, const void *payload, size_t payloadBytes);

    Config m_cfg;
    DataPlaneKind m_kind = DataPlaneKind::InProcess;
    RdmaEndpointInfo m_remote{};

    std::unique_ptr<RdmaDevice> m_device;
    std::unique_ptr<RdmaConnection> m_conn;

    HugepageArena m_creditArena;
    std::atomic<uint64_t> *m_creditMirror = nullptr;
    ibv_mr *m_creditMr = nullptr;

    HugepageArena m_txArena;
    ibv_mr *m_txMr = nullptr;
    uint32_t m_txSlotCount = 0;
    std::vector<uint8_t> m_txBusy;

    // InProcess acquireTxSlot fallback (not registered).
    HugepageArena m_inprocessTxArena;
    std::vector<uint8_t> m_inprocessTxBusy;

    std::shared_ptr<RdmaNodeRecvSession> m_localSession;
    std::atomic<uint64_t> *m_remoteConsumer = nullptr;

    uint64_t m_producerSeq = 0;
    uint32_t m_slotCount = 0;
    size_t m_slotStride = 0;

    std::mutex m_sendMutex;
    bool m_connected = false;
    std::atomic<uint64_t> m_singlesSent{0};
    std::atomic<uint64_t> m_slotsSent{0};
    bool m_txCudaRegistered = false;
};

} // namespace openpni::distributed::dataplane::rdma
