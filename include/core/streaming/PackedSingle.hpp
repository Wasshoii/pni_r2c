#pragma once

/**
 * @file PackedSingle.hpp
 * @brief Zero-copy pack/unpack helpers for openpni::Single over gRPC binary transport.
 *
 * openpni::Single is already #pragma pack(1) at 16 bytes, so pack/unpack
 * reduce to plain memcpy. These helpers are provided for clarity and to
 * static_assert the layout assumption.
 */

#include <cstdint>
#include <cstring>

#include <pni/PnI-Config.hpp>
#include <pni/core/CommonDataType.hpp>

namespace openpni::distributed::streaming
{

static_assert(sizeof(openpni::Single) == 16,
              "openpni::Single must be 16 bytes (packed) for zero-copy gRPC transport");

constexpr size_t kPackedSingleSize = 16;

inline void packSinglesToBinary(const openpni::Single *src, size_t count, void *dst)
{
    std::memcpy(dst, src, count * kPackedSingleSize);
}

inline void unpackBinaryToSingles(const void *src, size_t count, openpni::Single *dst)
{
    std::memcpy(dst, src, count * kPackedSingleSize);
}

} // namespace openpni::distributed::streaming
