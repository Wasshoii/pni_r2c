#pragma once

#include <array>
#include <cstdint>
#include <string>

namespace openpni::distributed::dataplane::rdma
{

enum class DataPlaneKind : uint32_t
{
    Unspecified = 0,
    RdmaRoceV2 = 1,
    InProcess = 2, // same-process memcpy path when no IB device (CI / localhost)
};

struct RdmaEndpointInfo
{
    DataPlaneKind kind = DataPlaneKind::RdmaRoceV2;
    std::array<uint8_t, 16> gid{};
    uint32_t lid = 0;
    uint32_t qpNum = 0;
    uint32_t psn = 0;
    uint32_t rkey = 0;
    uint64_t baseAddr = 0;
    uint32_t slotCount = 0;
    uint32_t slotStride = 0;
    uint32_t notifyRkey = 0;
    uint64_t notifyAddr = 0;
    uint32_t notifyCapacity = 0;
    uint32_t consumerRkey = 0;
    uint64_t consumerAddr = 0;
    std::string deviceName;
    uint32_t portNum = 1;
    uint32_t gidIndex = 0;
    /** Opaque local pointer token for InProcess mode (not valid across processes). */
    uint64_t inprocessHandle = 0;
};

struct SlotChunkView
{
    uint32_t nodeId = 0;
    uint64_t chunkId = 0;
    uint64_t computerClockMs = 0;
    uint32_t durationMs = 0;
    uint16_t flags = 0;
    uint32_t singlesCount = 0;
    const void *singlesPacked = nullptr;
};

} // namespace openpni::distributed::dataplane::rdma
