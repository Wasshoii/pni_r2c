#pragma once

#include "dataplane/rdma/RdmaContext.hpp"
#include "dataplane/rdma/RdmaTypes.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

struct ibv_mr;

namespace openpni::distributed::dataplane::rdma
{

class RdmaNodeRecvSession;

/**
 * Node-side RDMA WRITE (or in-process memcpy) sender into coin receive ring.
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
     */
    bool sendPackedSingles(
        uint64_t chunkId,
        uint64_t computerClockMs,
        uint32_t durationMs,
        const void *singlesPacked,
        uint32_t singlesCount);

    uint64_t singlesSent() const noexcept { return m_singlesSent.load(); }
    uint64_t slotsSent() const noexcept { return m_slotsSent.load(); }
    bool ok() const noexcept { return m_connected; }

private:
    bool connectVerbs(const RdmaEndpointInfo &coin);
    bool connectInProcess(const RdmaEndpointInfo &coin);
    bool waitForCredit(uint64_t needProducerSeq);
    bool writeSlot(uint32_t slotIndex, const SlotHeader &hdr, const void *payload, size_t payloadBytes);
    bool writeNotify(uint32_t slotIndex, const NotifyEntry &note);

    Config m_cfg;
    DataPlaneKind m_kind = DataPlaneKind::InProcess;
    RdmaEndpointInfo m_remote{};

    std::unique_ptr<RdmaDevice> m_device;
    std::unique_ptr<RdmaConnection> m_conn;
    // Local staging buffer for one slot (header+payload) registered for verbs.
    std::vector<uint8_t> m_staging;
    ibv_mr *m_stagingMr = nullptr;

    // In-process: direct access to remote session ring.
    std::shared_ptr<RdmaNodeRecvSession> m_localSession;

    // Remote consumer progress (mirrored). For verbs, periodically READ/WRITE;
    // for in-process, read atomic from ring.
    std::atomic<uint64_t> *m_remoteConsumer = nullptr;
    uint64_t m_producerSeq = 0;
    uint32_t m_slotCount = 0;
    size_t m_slotStride = 0;

    std::mutex m_sendMutex;
    bool m_connected = false;
    std::atomic<uint64_t> m_singlesSent{0};
    std::atomic<uint64_t> m_slotsSent{0};
};

} // namespace openpni::distributed::dataplane::rdma
