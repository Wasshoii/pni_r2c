#pragma once

/**
 * @file SlotProtocol.hpp
 * @brief Fixed-layout slot header for RoCE v2 singles data plane.
 *
 * Payload immediately follows the 64-byte header and is a contiguous array of
 * 16-byte openpni::Single records (little-endian), matching PackedSingle.
 */

#include <cstdint>
#include <cstring>

namespace openpni::distributed::dataplane::rdma
{

inline constexpr uint32_t kSlotMagic = 0x52324441u; // 'R2DA'
inline constexpr uint16_t kSlotVersion = 1;
inline constexpr size_t kSlotHeaderBytes = 64;
inline constexpr size_t kPackedSingleBytes = 16;

inline constexpr uint32_t kSlotFlagSof = 1u << 0;
inline constexpr uint32_t kSlotFlagEof = 1u << 1;
inline constexpr uint32_t kSlotFlagPartial = 1u << 2;

#pragma pack(push, 1)
struct SlotHeader
{
    uint32_t magic = kSlotMagic;
    uint16_t version = kSlotVersion;
    uint16_t flags = 0;
    uint32_t nodeId = 0;
    uint32_t singlesCount = 0;
    uint64_t chunkId = 0;
    uint64_t computerClockMs = 0;
    uint32_t durationMs = 0;
    uint32_t seq = 0;
    uint32_t crc32 = 0; // optional; 0 = unused
    uint8_t reserved[20] = {};
};
#pragma pack(pop)

static_assert(sizeof(SlotHeader) == kSlotHeaderBytes, "SlotHeader must be 64 bytes");

#pragma pack(push, 1)
struct NotifyEntry
{
    uint64_t seq = 0;       // 0 = free/empty; non-zero = ready
    uint32_t slotIndex = 0;
    uint32_t singlesCount = 0;
    uint64_t chunkId = 0;
};
#pragma pack(pop)

inline constexpr size_t kDefaultSlotBytes = 4u * 1024u * 1024u; // 4 MiB
inline constexpr uint32_t kDefaultSlotCount = 64;

inline size_t maxSinglesPerSlot(size_t slotStrideBytes)
{
    if (slotStrideBytes <= kSlotHeaderBytes)
    {
        return 0;
    }
    return (slotStrideBytes - kSlotHeaderBytes) / kPackedSingleBytes;
}

inline void clearSlotHeader(SlotHeader *hdr)
{
    *hdr = SlotHeader{};
    hdr->magic = kSlotMagic;
    hdr->version = kSlotVersion;
}

/** Immediate data carries the 32-bit wrap of producer seq (1-based). */
inline uint32_t seqToImm(uint64_t seq) noexcept
{
    return static_cast<uint32_t>(seq);
}

inline uint32_t slotIndexFromSeq(uint64_t seq, uint32_t slotCount) noexcept
{
    if (slotCount == 0)
    {
        return 0;
    }
    return static_cast<uint32_t>((seq - 1u) % static_cast<uint64_t>(slotCount));
}

} // namespace openpni::distributed::dataplane::rdma
