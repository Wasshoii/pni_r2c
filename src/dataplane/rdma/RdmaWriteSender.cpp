#include "dataplane/rdma/RdmaWriteSender.hpp"
#include "dataplane/rdma/RdmaRecvServer.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <mutex>
#include <new>
#include <thread>

#include <infiniband/verbs.h>
#include <iostream>
#define LOG(severity) ::std::cerr
#define VLOG(level) if(true) ; else ::std::cerr
#define LOG_EVERY_N(severity, n) ::std::cerr

namespace
{
inline void cpuRelax()
{
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#else
    std::this_thread::yield();
#endif
}

inline void backoffSpin(uint32_t *spins)
{
    const uint32_t n = ++(*spins);
    if (n < 64u)
    {
        cpuRelax();
        return;
    }
    if (n < 256u)
    {
        std::this_thread::yield();
        return;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(10));
}
} // namespace

namespace openpni::distributed::dataplane::rdma
{

namespace
{
constexpr int kAccessLocal = IBV_ACCESS_LOCAL_WRITE;
constexpr int kAccessCredit = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
} // namespace

RdmaWriteSender::RdmaWriteSender(Config cfg)
    : m_cfg(std::move(cfg))
{
}

RdmaWriteSender::~RdmaWriteSender()
{
    close();
}

uint8_t *RdmaWriteSender::txSlotBase(uint32_t localIndex) noexcept
{
    auto *base = static_cast<uint8_t *>(m_txArena.data());
    if (!base)
    {
        base = static_cast<uint8_t *>(m_inprocessTxArena.data());
    }
    if (!base)
    {
        return nullptr;
    }
    return base + static_cast<size_t>(localIndex) * m_slotStride;
}

void *RdmaWriteSender::txStagingBase() const noexcept
{
    if (m_txArena.data() != nullptr)
    {
        return const_cast<void *>(m_txArena.data());
    }
    return const_cast<void *>(m_inprocessTxArena.data());
}

size_t RdmaWriteSender::txStagingBytes() const noexcept
{
    if (m_txArena.size() != 0)
    {
        return m_txArena.size();
    }
    return m_inprocessTxArena.size();
}

bool RdmaWriteSender::prepareLocalEndpoint(RdmaEndpointInfo *outLocal)
{
    if (!outLocal)
    {
        return false;
    }
    *outLocal = {};
    if (m_cfg.forceInProcess)
    {
        outLocal->kind = DataPlaneKind::InProcess;
        return true;
    }
    if (!RdmaDevice::hasVerbsDevice())
    {
        if (m_cfg.requireRoce)
        {
            LOG(ERROR) << "RdmaWriteSender: requireRoce but no verbs device";
            return false;
        }
        outLocal->kind = DataPlaneKind::InProcess;
        return true;
    }

    m_device = std::make_unique<RdmaDevice>();
    if (!m_device->open(m_cfg.deviceName, m_cfg.gidIndex))
    {
        m_device.reset();
        if (m_cfg.requireRoce)
        {
            return false;
        }
        outLocal->kind = DataPlaneKind::InProcess;
        return true;
    }
    m_conn = std::make_unique<RdmaConnection>();
    if (!m_conn->create(*m_device) || !m_conn->transitionToInit(m_device->portNum()))
    {
        m_conn.reset();
        m_device.reset();
        return false;
    }

    if (!m_creditArena.allocate(4096, false))
    {
        return false;
    }
    m_creditMirror = new (m_creditArena.data()) std::atomic<uint64_t>(0);
    m_creditMr = m_conn->registerMemory(m_creditArena.data(), m_creditArena.size(), kAccessCredit);
    if (!m_creditMr)
    {
        return false;
    }

    std::array<uint8_t, 16> gid{};
    uint16_t lid = 0;
    if (!m_device->queryGid(&gid, &lid))
    {
        return false;
    }
    outLocal->kind = DataPlaneKind::RdmaRoceV2;
    outLocal->gid = gid;
    outLocal->lid = lid;
    outLocal->qpNum = m_conn->qpNum();
    outLocal->psn = m_conn->localPsn();
    outLocal->deviceName = m_device->name();
    outLocal->portNum = m_device->portNum();
    outLocal->gidIndex = static_cast<uint32_t>(m_device->gidIndex());
    outLocal->creditMirrorRkey = m_creditMr->rkey;
    outLocal->creditMirrorAddr = reinterpret_cast<uint64_t>(m_creditMirror);
    return true;
}

bool RdmaWriteSender::connect(const RdmaEndpointInfo &coinEndpoint)
{
    m_remote = coinEndpoint;
    m_slotCount = coinEndpoint.slotCount;
    m_slotStride = coinEndpoint.slotStride;
    m_producerSeq = 0;

    if (m_cfg.forceInProcess || coinEndpoint.kind == DataPlaneKind::InProcess ||
        coinEndpoint.inprocessHandle != 0)
    {
        if (m_cfg.requireRoce)
        {
            LOG(ERROR) << "RdmaWriteSender: requireRoce but remote is InProcess";
            return false;
        }
        if (m_conn && m_creditMr)
        {
            m_conn->deregister(m_creditMr);
            m_creditMr = nullptr;
        }
        m_conn.reset();
        m_device.reset();
        m_creditArena.release();
        m_creditMirror = nullptr;
        return connectInProcess(coinEndpoint);
    }
    if (!RdmaDevice::hasVerbsDevice() && !m_conn)
    {
        LOG(ERROR) << "RdmaWriteSender: remote expects verbs but no local device";
        return false;
    }
    return connectVerbs(coinEndpoint);
}

bool RdmaWriteSender::connectInProcess(const RdmaEndpointInfo &coin)
{
    m_localSession = RdmaRecvServer::findInProcessSession(coin.inprocessHandle);
    if (!m_localSession)
    {
        LOG(ERROR) << "InProcess session handle not found: " << coin.inprocessHandle;
        return false;
    }
    m_kind = DataPlaneKind::InProcess;
    m_remoteConsumer = m_localSession->ring().consumerSeq();
    m_slotCount = m_localSession->ring().slotCount();
    m_slotStride = m_localSession->ring().slotStride();
    if (!setupTxArena())
    {
        return false;
    }
    m_connected = true;
    LOG(INFO) << "RdmaWriteSender node=" << m_cfg.nodeId << " connected InProcess slots="
              << m_slotCount << " stride=" << m_slotStride;
    return true;
}

bool RdmaWriteSender::setupTxArena()
{
    m_txSlotCount = std::max<uint32_t>(2, m_cfg.txSlotCount);
    if (m_slotStride == 0)
    {
        return false;
    }
    const size_t bytes = static_cast<size_t>(m_txSlotCount) * m_slotStride;
    if (m_kind == DataPlaneKind::RdmaRoceV2)
    {
        if (!m_txArena.allocate(bytes, m_cfg.preferHugePages))
        {
            return false;
        }
        m_txMr = m_conn->registerMemory(m_txArena.data(), m_txArena.size(), kAccessLocal);
        if (!m_txMr)
        {
            return false;
        }
        m_txBusy.assign(m_txSlotCount, 0);
    }
    else
    {
        if (!m_inprocessTxArena.allocate(bytes, false))
        {
            return false;
        }
        m_inprocessTxBusy.assign(m_txSlotCount, 0);
    }
    return true;
}

bool RdmaWriteSender::connectVerbs(const RdmaEndpointInfo &coin)
{
    if (!m_conn || !m_device)
    {
        RdmaEndpointInfo local{};
        if (!prepareLocalEndpoint(&local) || local.kind != DataPlaneKind::RdmaRoceV2)
        {
            LOG(ERROR) << "prepareLocalEndpoint failed for RoCE connect";
            return false;
        }
    }

    std::array<uint8_t, 16> localGid{};
    if (!m_device->queryGid(&localGid, nullptr))
    {
        return false;
    }

    if (!m_conn->transitionToRtr(coin, m_device->portNum(), localGid) ||
        !m_conn->transitionToRts(m_conn->localPsn()))
    {
        return false;
    }

    m_kind = DataPlaneKind::RdmaRoceV2;
    if (!setupTxArena())
    {
        return false;
    }
    m_connected = true;
    LOG(INFO) << "RdmaWriteSender node=" << m_cfg.nodeId << " connected RoCE qp="
              << m_conn->qpNum() << " remote_qp=" << coin.qpNum;
    return true;
}

void RdmaWriteSender::close()
{
    m_connected = false;
    if (m_conn)
    {
        m_conn->drainSendCompletions();
        if (m_txMr)
        {
            m_conn->deregister(m_txMr);
            m_txMr = nullptr;
        }
        if (m_creditMr)
        {
            m_conn->deregister(m_creditMr);
            m_creditMr = nullptr;
        }
    }
    m_conn.reset();
    m_device.reset();
    m_localSession.reset();
    m_remoteConsumer = nullptr;
    m_creditMirror = nullptr;
    m_creditArena.release();
    m_txArena.release();
    m_inprocessTxArena.release();
    m_txBusy.clear();
    m_inprocessTxBusy.clear();
    m_txBusyCount.store(0, std::memory_order_relaxed);
    m_txCudaRegistered = false;
}

bool RdmaWriteSender::waitForCredit(uint64_t needProducerSeq)
{
    const uint64_t limit = static_cast<uint64_t>(m_slotCount);
    uint32_t spins = 0;
    while (m_connected)
    {
        uint64_t consumer = 0;
        if (m_kind == DataPlaneKind::InProcess && m_remoteConsumer)
        {
            consumer = m_remoteConsumer->load(std::memory_order_acquire);
        }
        else if (m_kind == DataPlaneKind::RdmaRoceV2 && m_creditMirror)
        {
            consumer = m_creditMirror->load(std::memory_order_acquire);
        }
        else
        {
            return false;
        }

        if (needProducerSeq - consumer < limit)
        {
            return true;
        }
        if (m_kind == DataPlaneKind::RdmaRoceV2)
        {
            recycleTxCompletionsLocked();
        }
        backoffSpin(&spins);
    }
    return false;
}

uint64_t RdmaWriteSender::slotsInFlight() const noexcept
{
    uint64_t consumer = 0;
    if (m_kind == DataPlaneKind::InProcess && m_remoteConsumer)
    {
        consumer = m_remoteConsumer->load(std::memory_order_relaxed);
    }
    else if (m_creditMirror)
    {
        consumer = m_creditMirror->load(std::memory_order_relaxed);
    }
    const uint64_t prod = m_producerSeq;
    return prod > consumer ? prod - consumer : 0;
}

uint32_t RdmaWriteSender::creditRemaining() const noexcept
{
    if (m_slotCount == 0)
    {
        return 0;
    }
    const uint64_t inFlight = slotsInFlight();
    if (inFlight >= static_cast<uint64_t>(m_slotCount))
    {
        return 0;
    }
    return static_cast<uint32_t>(static_cast<uint64_t>(m_slotCount) - inFlight);
}

bool RdmaWriteSender::recycleTxCompletionsLocked()
{
    if (!m_conn)
    {
        return true;
    }
    RdmaWorkCompletion wcs[16];
    const int n = m_conn->pollCq(wcs, 16);
    if (n < 0)
    {
        return false;
    }
    for (int i = 0; i < n; ++i)
    {
        if (wcs[i].status != 0)
        {
            return false;
        }
        if (wcs[i].isRecv)
        {
            continue;
        }
        const uint64_t wr = wcs[i].wrId;
        if (wr >= 1 && wr <= m_txSlotCount)
        {
            clearTxBusyLocked(static_cast<uint32_t>(wr - 1));
        }
    }
    return true;
}

bool RdmaWriteSender::tryAcquireTxSlotLocked(TxSlotLease *out)
{
    if (!out)
    {
        return false;
    }
    auto &busy = (m_kind == DataPlaneKind::RdmaRoceV2) ? m_txBusy : m_inprocessTxBusy;
    if (busy.empty())
    {
        return false;
    }
    if (m_kind == DataPlaneKind::RdmaRoceV2 && !recycleTxCompletionsLocked())
    {
        return false;
    }
    for (uint32_t i = 0; i < m_txSlotCount; ++i)
    {
        if (busy[i] == 0)
        {
            busy[i] = 1;
            m_txBusyCount.fetch_add(1, std::memory_order_relaxed);
            uint8_t *base = txSlotBase(i);
            if (!base)
            {
                clearTxBusyLocked(i);
                return false;
            }
            out->localIndex = i;
            out->header = reinterpret_cast<SlotHeader *>(base);
            out->payload = base + kSlotHeaderBytes;
            out->payloadCapacity = m_slotStride - kSlotHeaderBytes;
            return true;
        }
    }
    return false;
}

bool RdmaWriteSender::acquireTxSlotLocked(TxSlotLease *out)
{
    uint32_t spins = 0;
    for (;;)
    {
        if (tryAcquireTxSlotLocked(out))
        {
            return true;
        }
        if (!m_connected)
        {
            return false;
        }
        backoffSpin(&spins);
    }
}

bool RdmaWriteSender::acquireTxSlot(TxSlotLease *out)
{
    uint32_t spins = 0;
    for (;;)
    {
        std::unique_lock<std::mutex> lock(m_sendMutex);
        if (!m_connected)
        {
            return false;
        }
        if (tryAcquireTxSlotLocked(out))
        {
            return true;
        }
        lock.unlock();
        backoffSpin(&spins);
    }
}

void RdmaWriteSender::clearTxBusyLocked(uint32_t localIndex)
{
    auto &busy = (m_kind == DataPlaneKind::RdmaRoceV2) ? m_txBusy : m_inprocessTxBusy;
    if (localIndex < busy.size() && busy[localIndex] != 0)
    {
        busy[localIndex] = 0;
        m_txBusyCount.fetch_sub(1, std::memory_order_relaxed);
    }
}

void RdmaWriteSender::abortTxSlot(const TxSlotLease &lease)
{
    std::lock_guard<std::mutex> lock(m_sendMutex);
    clearTxBusyLocked(lease.localIndex);
}

bool RdmaWriteSender::postRemoteSlotLocked(uint32_t localIndex, uint32_t remoteSlot,
                                           uint32_t length, uint64_t seq)
{
    uint8_t *base = txSlotBase(localIndex);
    const uint64_t remote = m_remote.baseAddr + static_cast<uint64_t>(remoteSlot) * m_slotStride;
    if (!m_conn->postWriteImm(base, length, m_txMr->lkey, remote, m_remote.rkey,
                              seqToImm(seq), /*wrId=*/localIndex + 1, /*signaled=*/true))
    {
        clearTxBusyLocked(localIndex);
        return false;
    }
    return true;
}

bool RdmaWriteSender::writeSlotInProcess(uint32_t slotIndex, const SlotHeader &hdr,
                                         const void *payload, size_t payloadBytes)
{
    if (!m_localSession)
    {
        return false;
    }
    SlotHeader *dst = m_localSession->ring().slotHeader(slotIndex);
    uint8_t *pl = m_localSession->ring().slotPayload(slotIndex);
    std::memcpy(pl, payload, payloadBytes);
    std::atomic_thread_fence(std::memory_order_release);
    *dst = hdr;

    NotifyEntry *note = &m_localSession->ring().notifyBase()[slotIndex];
    note->slotIndex = slotIndex;
    note->singlesCount = hdr.singlesCount;
    note->chunkId = hdr.chunkId;
    std::atomic_thread_fence(std::memory_order_release);
    note->seq = hdr.seq == 0 ? 1 : hdr.seq;
    return true;
}

bool RdmaWriteSender::commitTxSlot(const TxSlotLease &lease, const SlotHeader &hdr, uint32_t singlesCount)
{
    uint32_t spins = 0;
    for (;;)
    {
        std::unique_lock<std::mutex> lock(m_sendMutex);
        if (!m_connected || lease.header == nullptr)
        {
            clearTxBusyLocked(lease.localIndex);
            return false;
        }

        uint64_t consumer = 0;
        if (m_kind == DataPlaneKind::InProcess && m_remoteConsumer)
        {
            consumer = m_remoteConsumer->load(std::memory_order_acquire);
        }
        else if (m_kind == DataPlaneKind::RdmaRoceV2 && m_creditMirror)
        {
            consumer = m_creditMirror->load(std::memory_order_acquire);
        }
        else
        {
            clearTxBusyLocked(lease.localIndex);
            return false;
        }

        const uint64_t need = m_producerSeq + 1;
        if (need - consumer >= static_cast<uint64_t>(m_slotCount))
        {
            if (m_kind == DataPlaneKind::RdmaRoceV2 && !recycleTxCompletionsLocked())
            {
                clearTxBusyLocked(lease.localIndex);
                return false;
            }
            lock.unlock();
            backoffSpin(&spins);
            continue;
        }

        const uint32_t slotIndex = static_cast<uint32_t>(m_producerSeq % m_slotCount);
        SlotHeader local = hdr;
        local.magic = kSlotMagic;
        local.version = kSlotVersion;
        local.nodeId = m_cfg.nodeId;
        local.singlesCount = singlesCount;
        local.seq = static_cast<uint32_t>(m_producerSeq + 1);
        *lease.header = local;

        const size_t payloadBytes = static_cast<size_t>(singlesCount) * kPackedSingleBytes;
        if (m_kind == DataPlaneKind::InProcess)
        {
            if (!writeSlotInProcess(slotIndex, local, lease.payload, payloadBytes))
            {
                clearTxBusyLocked(lease.localIndex);
                return false;
            }
            clearTxBusyLocked(lease.localIndex);
        }
        else
        {
            const uint32_t length = static_cast<uint32_t>(kSlotHeaderBytes + payloadBytes);
            if (!postRemoteSlotLocked(lease.localIndex, slotIndex, length, m_producerSeq + 1))
            {
                return false;
            }
        }

        ++m_producerSeq;
        m_slotsSent.fetch_add(1, std::memory_order_relaxed);
        m_singlesSent.fetch_add(singlesCount, std::memory_order_relaxed);
        return true;
    }
}

bool RdmaWriteSender::sendPackedSingles(
    uint64_t chunkId,
    uint64_t computerClockMs,
    uint32_t durationMs,
    const void *singlesPacked,
    uint32_t singlesCount)
{
    std::lock_guard<std::mutex> lock(m_sendMutex);
    if (!m_connected || !singlesPacked || singlesCount == 0)
    {
        return singlesCount == 0;
    }

    const size_t maxPerSlot = maxSinglesPerSlot(m_slotStride);
    if (maxPerSlot == 0)
    {
        return false;
    }

    uint32_t offset = 0;
    bool first = true;
    while (offset < singlesCount)
    {
        const uint32_t count = static_cast<uint32_t>(
            std::min<size_t>(maxPerSlot, static_cast<size_t>(singlesCount - offset)));

        if (!waitForCredit(m_producerSeq + 1))
        {
            LOG(ERROR) << "waitForCredit failed node=" << m_cfg.nodeId;
            return false;
        }

        const uint32_t slotIndex = static_cast<uint32_t>(m_producerSeq % m_slotCount);
        SlotHeader hdr{};
        clearSlotHeader(&hdr);
        hdr.nodeId = m_cfg.nodeId;
        hdr.chunkId = chunkId;
        hdr.computerClockMs = computerClockMs;
        hdr.durationMs = durationMs;
        hdr.singlesCount = count;
        hdr.seq = static_cast<uint32_t>(m_producerSeq + 1);
        hdr.flags = 0;
        if (first)
        {
            hdr.flags = static_cast<uint16_t>(hdr.flags | kSlotFlagSof);
            first = false;
        }
        if (offset + count >= singlesCount)
        {
            hdr.flags = static_cast<uint16_t>(hdr.flags | kSlotFlagEof);
        }
        if (count < singlesCount)
        {
            hdr.flags = static_cast<uint16_t>(hdr.flags | kSlotFlagPartial);
        }

        const auto *src = static_cast<const uint8_t *>(singlesPacked) +
                          static_cast<size_t>(offset) * kPackedSingleBytes;
        const size_t payloadBytes = static_cast<size_t>(count) * kPackedSingleBytes;

        if (m_kind == DataPlaneKind::InProcess)
        {
            if (!writeSlotInProcess(slotIndex, hdr, src, payloadBytes))
            {
                return false;
            }
        }
        else
        {
            TxSlotLease lease{};
            if (!acquireTxSlotLocked(&lease))
            {
                LOG(ERROR) << "acquireTxSlot failed node=" << m_cfg.nodeId;
                return false;
            }
            *lease.header = hdr;
            std::memcpy(lease.payload, src, payloadBytes);
            const uint32_t length = static_cast<uint32_t>(kSlotHeaderBytes + payloadBytes);
            if (!postRemoteSlotLocked(lease.localIndex, slotIndex, length, m_producerSeq + 1))
            {
                return false;
            }
        }

        ++m_producerSeq;
        m_slotsSent.fetch_add(1, std::memory_order_relaxed);
        m_singlesSent.fetch_add(count, std::memory_order_relaxed);
        offset += count;
    }
    return true;
}

} // namespace openpni::distributed::dataplane::rdma
