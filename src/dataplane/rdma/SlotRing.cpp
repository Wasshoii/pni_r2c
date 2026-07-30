#include "dataplane/rdma/SlotRing.hpp"

#include <new>

#include <iostream>
#define LOG(severity) ::std::cerr
#define VLOG(level) if(true) ; else ::std::cerr
#define LOG_EVERY_N(severity, n) ::std::cerr

namespace openpni::distributed::dataplane::rdma
{

bool SlotRing::init(const SlotRingConfig &cfg)
{
    reset();
    if (cfg.slotCount == 0 || cfg.slotBytes < kSlotHeaderBytes + kPackedSingleBytes)
    {
        LOG(ERROR) << "SlotRing invalid config slotCount=" << cfg.slotCount
                   << " slotBytes=" << cfg.slotBytes;
        return false;
    }

    m_slotCount = cfg.slotCount;
    m_slotStride = cfg.slotBytes;

    const size_t slotsBytes = static_cast<size_t>(m_slotCount) * m_slotStride;
    const size_t notifyBytes = sizeof(NotifyEntry) * m_slotCount;
    const size_t creditBytes = sizeof(std::atomic<uint64_t>);
    const size_t total = slotsBytes + notifyBytes + creditBytes;

    if (!m_arena.allocate(total, cfg.preferHugePages))
    {
        return false;
    }

    auto *base = static_cast<uint8_t *>(m_arena.data());
    m_notify = reinterpret_cast<NotifyEntry *>(base + slotsBytes);
    m_consumerSeq = new (base + slotsBytes + notifyBytes) std::atomic<uint64_t>(0);
    return true;
}

void SlotRing::reset()
{
    m_arena.release();
    m_slotCount = 0;
    m_slotStride = 0;
    m_notify = nullptr;
    m_consumerSeq = nullptr;
}

SlotHeader *SlotRing::slotHeader(uint32_t index) noexcept
{
    return reinterpret_cast<SlotHeader *>(
        static_cast<uint8_t *>(m_arena.data()) + static_cast<size_t>(index) * m_slotStride);
}

const SlotHeader *SlotRing::slotHeader(uint32_t index) const noexcept
{
    return reinterpret_cast<const SlotHeader *>(
        static_cast<const uint8_t *>(m_arena.data()) + static_cast<size_t>(index) * m_slotStride);
}

uint8_t *SlotRing::slotPayload(uint32_t index) noexcept
{
    return reinterpret_cast<uint8_t *>(slotHeader(index)) + kSlotHeaderBytes;
}

const uint8_t *SlotRing::slotPayload(uint32_t index) const noexcept
{
    return reinterpret_cast<const uint8_t *>(slotHeader(index)) + kSlotHeaderBytes;
}

} // namespace openpni::distributed::dataplane::rdma
