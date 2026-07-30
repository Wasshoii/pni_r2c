#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <chrono>
#include <cstring>

#include <infiniband/verbs.h>
#include <iostream>
#define LOG(severity) ::std::cerr
#define VLOG(level) if(true) ; else ::std::cerr
#define LOG_EVERY_N(severity, n) ::std::cerr

namespace openpni::distributed::dataplane::rdma
{
namespace
{
std::mutex g_inprocessMutex;
std::unordered_map<uint64_t, std::weak_ptr<RdmaNodeRecvSession>> g_inprocessSessions;
std::atomic<uint64_t> g_inprocessHandleGen{1};
} // namespace

RdmaNodeRecvSession::RdmaNodeRecvSession(Config cfg)
    : m_cfg(std::move(cfg))
{
}

RdmaNodeRecvSession::~RdmaNodeRecvSession()
{
    close();
}

void RdmaNodeRecvSession::setIngest(IngestSlotFn fn)
{
    m_ingest = std::move(fn);
}

bool RdmaNodeRecvSession::prepare()
{
    close();
    m_nextExpectedNotifySeq = 1;
    SlotRingConfig ringCfg;
    ringCfg.slotCount = m_cfg.slotCount;
    ringCfg.slotBytes = m_cfg.slotBytes;
    ringCfg.preferHugePages = m_cfg.preferHugePages;
    if (!m_ring.init(ringCfg))
    {
        return false;
    }

    if (RdmaDevice::hasVerbsDevice())
    {
        if (prepareVerbs())
        {
            m_kind = DataPlaneKind::RdmaRoceV2;
            m_ready = true;
            return true;
        }
        LOG(WARNING) << "RdmaNodeRecvSession verbs prepare failed; falling back to InProcess";
    }
    else
    {
        LOG(WARNING) << "No IB verbs device; using InProcess data plane for node "
                     << m_cfg.nodeId;
    }
    return prepareInProcess();
}

bool RdmaNodeRecvSession::prepareVerbs()
{
    m_device = std::make_unique<RdmaDevice>();
    if (!m_device->open(m_cfg.deviceName))
    {
        m_device.reset();
        return false;
    }
    m_conn = std::make_unique<RdmaConnection>();
    if (!m_conn->create(*m_device))
    {
        m_conn.reset();
        m_device.reset();
        return false;
    }
    if (!m_conn->transitionToInit(m_device->portNum()))
    {
        m_conn.reset();
        m_device.reset();
        return false;
    }

    const int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    m_mr = m_conn->registerMemory(m_ring.base(), m_ring.totalBytes(), access);
    if (!m_mr)
    {
        m_conn.reset();
        m_device.reset();
        return false;
    }

    std::array<uint8_t, 16> gid{};
    uint16_t lid = 0;
    if (!m_device->queryGid(&gid, &lid))
    {
        return false;
    }

    m_localEp.kind = DataPlaneKind::RdmaRoceV2;
    m_localEp.gid = gid;
    m_localEp.lid = lid;
    m_localEp.qpNum = m_conn->qpNum();
    m_localEp.psn = m_conn->localPsn();
    m_localEp.rkey = m_mr->rkey;
    m_localEp.baseAddr = m_ring.baseAddr();
    m_localEp.slotCount = m_ring.slotCount();
    m_localEp.slotStride = static_cast<uint32_t>(m_ring.slotStride());
    m_localEp.notifyRkey = m_mr->rkey;
    m_localEp.notifyAddr = m_ring.notifyAddr();
    m_localEp.notifyCapacity = m_ring.slotCount();
    m_localEp.consumerRkey = m_mr->rkey;
    m_localEp.consumerAddr = m_ring.consumerSeqAddr();
    m_localEp.deviceName = m_device->name();
    m_localEp.portNum = m_device->portNum();
    m_localEp.gidIndex = static_cast<uint32_t>(m_device->gidIndex());
    m_localEp.inprocessHandle = 0;
    return true;
}

bool RdmaNodeRecvSession::prepareInProcess()
{
    m_kind = DataPlaneKind::InProcess;
    m_inprocessHandle = g_inprocessHandleGen.fetch_add(1);
    m_localEp = {};
    m_localEp.kind = DataPlaneKind::InProcess;
    m_localEp.baseAddr = m_ring.baseAddr();
    m_localEp.slotCount = m_ring.slotCount();
    m_localEp.slotStride = static_cast<uint32_t>(m_ring.slotStride());
    m_localEp.notifyAddr = m_ring.notifyAddr();
    m_localEp.notifyCapacity = m_ring.slotCount();
    m_localEp.consumerAddr = m_ring.consumerSeqAddr();
    m_localEp.inprocessHandle = m_inprocessHandle;
    m_ready = true;
    return true;
}

RdmaEndpointInfo RdmaNodeRecvSession::localEndpoint() const
{
    return m_localEp;
}

bool RdmaNodeRecvSession::acceptRemote(const RdmaEndpointInfo &remote)
{
    if (m_kind == DataPlaneKind::InProcess)
    {
        return true;
    }
    if (!m_conn || !m_device)
    {
        return false;
    }
    std::array<uint8_t, 16> localGid{};
    if (!m_device->queryGid(&localGid, nullptr))
    {
        return false;
    }
    // Already in INIT from prepareVerbs; complete RTR/RTS.
    if (!m_conn->transitionToRtr(remote, m_device->portNum(), localGid))
    {
        return false;
    }
    if (!m_conn->transitionToRts(m_conn->localPsn()))
    {
        return false;
    }
    return true;
}

bool RdmaNodeRecvSession::ingestSlot(uint32_t slotIndex, const NotifyEntry &note)
{
    if (!m_ingest)
    {
        return false;
    }
    if (slotIndex >= m_ring.slotCount())
    {
        return false;
    }
    const SlotHeader *hdr = m_ring.slotHeader(slotIndex);
    if (!hdr || hdr->magic != kSlotMagic || hdr->singlesCount == 0)
    {
        return false;
    }
    if (hdr->singlesCount != note.singlesCount && note.singlesCount != 0)
    {
        LOG(WARNING) << "Notify/header singlesCount mismatch node=" << m_cfg.nodeId;
    }

    const uint16_t flags = hdr->flags;
    const bool isSof = (flags & kSlotFlagSof) != 0;
    const bool isEof = (flags & kSlotFlagEof) != 0;
    const size_t payloadBytes = static_cast<size_t>(hdr->singlesCount) * kPackedSingleBytes;
    const void *payload = m_ring.slotPayload(slotIndex);

    if (isSof || !m_pending.active)
    {
        m_pending = {};
        m_pending.active = true;
        m_pending.chunkId = hdr->chunkId;
        m_pending.computerClockMs = hdr->computerClockMs;
        m_pending.durationMs = hdr->durationMs;
    }
    else if (m_pending.chunkId != hdr->chunkId)
    {
        LOG(WARNING) << "chunkId mismatch node=" << m_cfg.nodeId
                     << " pending=" << m_pending.chunkId << " slot=" << hdr->chunkId;
        m_pending = {};
        m_pending.active = true;
        m_pending.chunkId = hdr->chunkId;
        m_pending.computerClockMs = hdr->computerClockMs;
        m_pending.durationMs = hdr->durationMs;
    }

    const size_t off = m_pending.packed.size();
    m_pending.packed.resize(off + payloadBytes);
    std::memcpy(m_pending.packed.data() + off, payload, payloadBytes);

    if (isEof)
    {
        SlotChunkView view;
        view.nodeId = hdr->nodeId;
        view.chunkId = m_pending.chunkId;
        view.computerClockMs = m_pending.computerClockMs;
        view.durationMs = m_pending.durationMs;
        view.flags = flags;
        view.singlesCount = static_cast<uint32_t>(m_pending.packed.size() / kPackedSingleBytes);
        view.singlesPacked = m_pending.packed.data();

        if (!m_ingest(view))
        {
            return false;
        }
        m_pending = {};
    }

    // Clear notify and advance consumer credit.
    NotifyEntry *n = &m_ring.notifyBase()[slotIndex];
    std::atomic_thread_fence(std::memory_order_release);
    n->seq = 0;
    m_ring.consumerSeq()->fetch_add(1, std::memory_order_release);
    return true;
}

int RdmaNodeRecvSession::pollOnce(int maxSlots)
{
    if (!m_ready.load(std::memory_order_acquire) || !m_ingest)
    {
        return 0;
    }

    int done = 0;
    for (int i = 0; i < maxSlots; ++i)
    {
        bool progressed = false;
        for (uint32_t s = 0; s < m_ring.slotCount(); ++s)
        {
            NotifyEntry &note = m_ring.notifyBase()[s];
            const uint64_t seq = note.seq;
            if (seq == 0 || seq != m_nextExpectedNotifySeq)
            {
                continue;
            }
            // Acquire payload after observing notify seq.
            std::atomic_thread_fence(std::memory_order_acquire);
            if (!ingestSlot(s, note))
            {
                LOG(ERROR) << "ingest failed node=" << m_cfg.nodeId << " slot=" << s;
                return done;
            }
            ++m_nextExpectedNotifySeq;
            ++done;
            progressed = true;
            break;
        }
        if (!progressed)
        {
            break;
        }
    }
    return done;
}

void RdmaNodeRecvSession::close()
{
    m_ready = false;
    m_pending = {};
    m_nextExpectedNotifySeq = 1;
    if (m_inprocessHandle != 0)
    {
        std::lock_guard<std::mutex> lock(g_inprocessMutex);
        g_inprocessSessions.erase(m_inprocessHandle);
        m_inprocessHandle = 0;
    }
    if (m_conn && m_mr)
    {
        m_conn->deregister(m_mr);
        m_mr = nullptr;
    }
    m_conn.reset();
    m_device.reset();
    m_ring.reset();
    m_localEp = {};
}

RdmaRecvServer::RdmaRecvServer(Config cfg)
    : m_cfg(std::move(cfg))
{
}

RdmaRecvServer::~RdmaRecvServer()
{
    stop();
}

void RdmaRecvServer::setIngest(IngestSlotFn fn)
{
    m_ingest = std::move(fn);
    std::lock_guard<std::mutex> lock(m_mutex);
    for (auto &[id, session] : m_sessions)
    {
        (void)id;
        session->setIngest(m_ingest);
    }
}

std::shared_ptr<RdmaNodeRecvSession> RdmaRecvServer::ensureSession(uint32_t nodeId)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_sessions.find(nodeId);
    if (it != m_sessions.end())
    {
        return it->second;
    }

    RdmaNodeRecvSession::Config sc;
    sc.nodeId = nodeId;
    sc.slotCount = m_cfg.slotCount;
    sc.slotBytes = m_cfg.slotBytes;
    sc.preferHugePages = m_cfg.preferHugePages;
    sc.deviceName = m_cfg.deviceName;

    auto session = std::make_shared<RdmaNodeRecvSession>(std::move(sc));
    session->setIngest(m_ingest);
    if (!session->prepare())
    {
        return nullptr;
    }

    if (session->kind() == DataPlaneKind::InProcess)
    {
        std::lock_guard<std::mutex> g(g_inprocessMutex);
        g_inprocessSessions[session->inprocessHandle()] = session;
    }

    m_sessions.emplace(nodeId, session);
    return session;
}

std::shared_ptr<RdmaNodeRecvSession> RdmaRecvServer::getSession(uint32_t nodeId) const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_sessions.find(nodeId);
    return it == m_sessions.end() ? nullptr : it->second;
}

std::shared_ptr<RdmaNodeRecvSession> RdmaRecvServer::findInProcessSession(uint64_t handle)
{
    std::lock_guard<std::mutex> lock(g_inprocessMutex);
    auto it = g_inprocessSessions.find(handle);
    if (it == g_inprocessSessions.end())
    {
        return nullptr;
    }
    return it->second.lock();
}

void RdmaRecvServer::start()
{
    if (!m_cfg.startPoller)
    {
        return;
    }
    if (m_running.exchange(true))
    {
        return;
    }
    m_poller = std::thread([this]()
                           { pollLoop(); });
}

void RdmaRecvServer::stop()
{
    if (!m_running.exchange(false))
    {
        // Still close sessions.
    }
    if (m_poller.joinable())
    {
        m_poller.join();
    }
    std::lock_guard<std::mutex> lock(m_mutex);
    for (auto &[id, session] : m_sessions)
    {
        (void)id;
        session->close();
    }
    m_sessions.clear();
}

void RdmaRecvServer::pollLoop()
{
    while (m_running.load(std::memory_order_acquire))
    {
        int total = 0;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            for (auto &[id, session] : m_sessions)
            {
                (void)id;
                total += session->pollOnce(16);
            }
        }
        if (total == 0)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(m_cfg.pollSleepUs));
        }
    }
}

} // namespace openpni::distributed::dataplane::rdma
