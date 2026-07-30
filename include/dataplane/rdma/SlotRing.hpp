#pragma once

#include "dataplane/rdma/HugepageArena.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

namespace openpni::distributed::dataplane::rdma
{

struct SlotRingConfig
{
    uint32_t slotCount = kDefaultSlotCount;
    size_t slotBytes = kDefaultSlotBytes; // includes header
    bool preferHugePages = true;
};

/**
 * Contiguous slot array + trailing notify ring + credit word.
 *
 * Layout:
 *   [ slot0 | slot1 | ... | slotN-1 | NotifyEntry[N] | uint64_t consumerSeq ]
 */
class SlotRing
{
public:
    bool init(const SlotRingConfig &cfg);
    void reset();

    uint32_t slotCount() const noexcept { return m_slotCount; }
    size_t slotStride() const noexcept { return m_slotStride; }
    size_t totalBytes() const noexcept { return m_arena.size(); }
    void *base() noexcept { return m_arena.data(); }
    const void *base() const noexcept { return m_arena.data(); }
    HugepageArena &arena() noexcept { return m_arena; }

    SlotHeader *slotHeader(uint32_t index) noexcept;
    const SlotHeader *slotHeader(uint32_t index) const noexcept;
    uint8_t *slotPayload(uint32_t index) noexcept;
    const uint8_t *slotPayload(uint32_t index) const noexcept;

    NotifyEntry *notifyBase() noexcept { return m_notify; }
    const NotifyEntry *notifyBase() const noexcept { return m_notify; }
    size_t notifyBytes() const noexcept { return sizeof(NotifyEntry) * m_slotCount; }

    std::atomic<uint64_t> *consumerSeq() noexcept { return m_consumerSeq; }
    const std::atomic<uint64_t> *consumerSeq() const noexcept { return m_consumerSeq; }

    uintptr_t baseAddr() const noexcept { return reinterpret_cast<uintptr_t>(m_arena.data()); }
    uintptr_t notifyAddr() const noexcept { return reinterpret_cast<uintptr_t>(m_notify); }
    uintptr_t consumerSeqAddr() const noexcept { return reinterpret_cast<uintptr_t>(m_consumerSeq); }

    size_t maxSingles() const noexcept { return maxSinglesPerSlot(m_slotStride); }

private:
    HugepageArena m_arena;
    uint32_t m_slotCount = 0;
    size_t m_slotStride = 0;
    NotifyEntry *m_notify = nullptr;
    std::atomic<uint64_t> *m_consumerSeq = nullptr;
};

} // namespace openpni::distributed::dataplane::rdma
