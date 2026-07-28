#pragma once

#include <cstdint>
#include <span>

#include <pni/interface/SingleGenerator.hpp>
#include <pni/tools/CudaPtr.hpp>

namespace openpni::distributed::r2s::multi_gpu
{

struct DPacketsAsync
{
    ::openpni::detail::CudaUniquePointer<uint8_t> raw{"DPacketsAsync_raw"};
    ::openpni::detail::CudaUniquePointer<uint64_t> offset{"DPacketsAsync_offset"};
    ::openpni::detail::CudaUniquePointer<uint16_t> length{"DPacketsAsync_length"};
    ::openpni::detail::CudaUniquePointer<uint16_t> channel{"DPacketsAsync_channel"};
    uint64_t count{0};

    static DPacketsAsync FromHost(uint8_t const *h_raw,
                                  uint64_t const *h_offset,
                                  uint16_t const *h_length,
                                  uint16_t const *h_channel,
                                  uint64_t h_count);

    static DPacketsAsync FromHost(openpni::interface::ISingleGenerator::PacketsInfo h_packets);

    void ReserveFromHost(uint8_t const *h_raw,
                         uint64_t const *h_offset,
                         uint16_t const *h_length,
                         uint16_t const *h_channel,
                         uint64_t h_count);

    void ReserveFromHost(openpni::interface::ISingleGenerator::PacketsInfo h_packets);
};

} // namespace openpni::distributed::r2s::multi_gpu
