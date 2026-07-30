#include "dataplane/rdma/RdmaWriteSender.hpp"
#include "dataplane/rdma/RdmaRecvServer.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>

#include <infiniband/verbs.h>
#include <iostream>
#define LOG(severity) ::std::cerr
#define VLOG(level) if(true) ; else ::std::cerr
#define LOG_EVERY_N(severity, n) ::std::cerr

namespace openpni::distributed::dataplane::rdma
{

RdmaWriteSender::RdmaWriteSender(Config cfg)
    : m_cfg(std::move(cfg))
{
}

RdmaWriteSender::~RdmaWriteSender()
{
    close();
}

bool RdmaWriteSender::connect(const RdmaEndpointInfo &coinEndpoint)
{
    m_remote = coinEndpoint;
    m_slotCount = coinEndpoint.slotCount;
    m_slotStride = coinEndpoint.slotStride;
    m_producerSeq = 0;

    if (coinEndpoint.kind == DataPlaneKind::InProcess || coinEndpoint.inprocessHandle != 0)
    {
        // Drop any half-prepared verbs resources.
        if (m_conn && m_stagingMr)
        {
            m_conn->deregister(m_stagingMr);
            m_stagingMr = nullptr;
        }
        m_conn.reset();
        m_device.reset();
        return connectInProcess(coinEndpoint);
    }
    if (!RdmaDevice::hasVerbsDevice() && !m_conn)
    {
        LOG(ERROR) << "RdmaWriteSender: remote expects verbs but no local device";
        return false;
    }
    return connectVerbs(coinEndpoint);
}

bool RdmaWriteSender::prepareLocalEndpoint(RdmaEndpointInfo *outLocal)
{
    if (!outLocal)
    {
        return false;
    }
    *outLocal = {};
    if (!RdmaDevice::hasVerbsDevice())
    {
        outLocal->kind = DataPlaneKind::InProcess;
        return true;
    }

    m_device = std::make_unique<RdmaDevice>();
    if (!m_device->open(m_cfg.deviceName))
    {
        m_device.reset();
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

    m_staging.resize(std::max(m_cfg.stagingSlotBytes, kDefaultSlotBytes));
    m_stagingMr = m_conn->registerMemory(
        m_staging.data(), m_staging.size(), IBV_ACCESS_LOCAL_WRITE);
    if (!m_stagingMr)
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
    return true;
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
    m_connected = true;
    LOG(INFO) << "RdmaWriteSender node=" << m_cfg.nodeId << " connected InProcess slots="
              << m_slotCount << " stride=" << m_slotStride;
    return true;
}

bool RdmaWriteSender::connectVerbs(const RdmaEndpointInfo &coin)
{
    // Local QP should already be in INIT via prepareLocalEndpoint(); if not, create now.
    if (!m_conn || !m_device)
    {
        RdmaEndpointInfo local{};
        if (!prepareLocalEndpoint(&local) || local.kind != DataPlaneKind::RdmaRoceV2)
        {
            LOG(ERROR) << "prepareLocalEndpoint failed for RoCE connect";
            return false;
        }
    }

    if (m_staging.size() < coin.slotStride)
    {
        // Cannot safely grow registered MR; require staging >= remote stride.
        if (m_staging.size() < static_cast<size_t>(coin.slotStride))
        {
            LOG(WARNING) << "staging size " << m_staging.size()
                         << " < remote slot stride " << coin.slotStride;
        }
    }

    std::array<uint8_t, 16> localGid{};
    if (!m_device->queryGid(&localGid, nullptr))
    {
        return false;
    }

    // Already INIT: complete RTR + RTS toward coin.
    if (!m_conn->transitionToRtr(coin, m_device->portNum(), localGid) ||
        !m_conn->transitionToRts(m_conn->localPsn()))
    {
        return false;
    }

    m_kind = DataPlaneKind::RdmaRoceV2;
    m_connected = true;
    LOG(INFO) << "RdmaWriteSender node=" << m_cfg.nodeId << " connected RoCE qp="
              << m_conn->qpNum() << " remote_qp=" << coin.qpNum;
    return true;
}

void RdmaWriteSender::close()
{
    m_connected = false;
    if (m_conn && m_stagingMr)
    {
        m_conn->deregister(m_stagingMr);
        m_stagingMr = nullptr;
    }
    m_conn.reset();
    m_device.reset();
    m_localSession.reset();
    m_remoteConsumer = nullptr;
    m_staging.clear();
}

bool RdmaWriteSender::waitForCredit(uint64_t needProducerSeq)
{
    const uint64_t limit = static_cast<uint64_t>(m_slotCount);
    while (m_connected)
    {
        uint64_t consumer = 0;
        if (m_kind == DataPlaneKind::InProcess && m_remoteConsumer)
        {
            consumer = m_remoteConsumer->load(std::memory_order_acquire);
        }
        else if (m_kind == DataPlaneKind::RdmaRoceV2 && m_conn && m_stagingMr)
        {
            // RDMA READ remote consumer into first 8 bytes of staging.
            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(m_staging.data());
            sge.length = sizeof(uint64_t);
            sge.lkey = m_stagingMr->lkey;

            ibv_send_wr wr{};
            wr.wr_id = 1;
            wr.opcode = IBV_WR_RDMA_READ;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = m_remote.consumerAddr;
            wr.wr.rdma.rkey = m_remote.consumerRkey;
            ibv_send_wr *bad = nullptr;
            if (ibv_post_send(m_conn->qp(), &wr, &bad) != 0)
            {
                LOG(ERROR) << "credit RDMA_READ post failed";
                return false;
            }
            // Busy poll completion.
            for (;;)
            {
                ibv_wc wc{};
                const int n = ibv_poll_cq(m_conn->cq(), 1, &wc);
                if (n > 0)
                {
                    if (wc.status != IBV_WC_SUCCESS)
                    {
                        LOG(ERROR) << "credit RDMA_READ wc failed";
                        return false;
                    }
                    break;
                }
                if (n < 0)
                {
                    return false;
                }
            }
            std::memcpy(&consumer, m_staging.data(), sizeof(consumer));
        }
        else
        {
            return false;
        }

        if (needProducerSeq - consumer < limit)
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    return false;
}

bool RdmaWriteSender::writeSlot(uint32_t slotIndex, const SlotHeader &hdr,
                                const void *payload, size_t payloadBytes)
{
    if (m_kind == DataPlaneKind::InProcess)
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
        return true;
    }

    // Pack into staging then RDMA WRITE whole slot region (header+payload).
    if (kSlotHeaderBytes + payloadBytes > m_staging.size())
    {
        LOG(ERROR) << "staging too small";
        return false;
    }
    std::memcpy(m_staging.data(), &hdr, sizeof(hdr));
    std::memcpy(m_staging.data() + kSlotHeaderBytes, payload, payloadBytes);
    const uint32_t length = static_cast<uint32_t>(kSlotHeaderBytes + payloadBytes);
    const uint64_t remote = m_remote.baseAddr + static_cast<uint64_t>(slotIndex) * m_slotStride;
    if (!m_conn->postWrite(m_staging.data(), length, m_stagingMr->lkey,
                           remote, m_remote.rkey, /*wrId=*/2, /*signaled=*/true))
    {
        return false;
    }
    // Wait completion.
    for (;;)
    {
        bool timedOut = false;
        if (!m_conn->pollOne(&timedOut))
        {
            return false;
        }
        if (!timedOut)
        {
            break;
        }
    }
    return true;
}

bool RdmaWriteSender::writeNotify(uint32_t slotIndex, const NotifyEntry &note)
{
    if (m_kind == DataPlaneKind::InProcess)
    {
        if (!m_localSession)
        {
            return false;
        }
        NotifyEntry *dst = &m_localSession->ring().notifyBase()[slotIndex];
        dst->slotIndex = note.slotIndex;
        dst->singlesCount = note.singlesCount;
        dst->chunkId = note.chunkId;
        std::atomic_thread_fence(std::memory_order_release);
        dst->seq = note.seq;
        return true;
    }

    std::memcpy(m_staging.data(), &note, sizeof(note));
    const uint64_t remote = m_remote.notifyAddr + static_cast<uint64_t>(slotIndex) * sizeof(NotifyEntry);
    if (!m_conn->postWrite(m_staging.data(), sizeof(note), m_stagingMr->lkey,
                           remote, m_remote.notifyRkey, /*wrId=*/3, /*signaled=*/true))
    {
        return false;
    }
    for (;;)
    {
        bool timedOut = false;
        if (!m_conn->pollOne(&timedOut))
        {
            return false;
        }
        if (!timedOut)
        {
            break;
        }
    }
    return true;
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
        if (!writeSlot(slotIndex, hdr, src, payloadBytes))
        {
            return false;
        }

        NotifyEntry note{};
        note.seq = m_producerSeq + 1;
        note.slotIndex = slotIndex;
        note.singlesCount = count;
        note.chunkId = chunkId;
        if (!writeNotify(slotIndex, note))
        {
            return false;
        }

        ++m_producerSeq;
        m_slotsSent.fetch_add(1, std::memory_order_relaxed);
        m_singlesSent.fetch_add(count, std::memory_order_relaxed);
        offset += count;
    }
    return true;
}

} // namespace openpni::distributed::dataplane::rdma
