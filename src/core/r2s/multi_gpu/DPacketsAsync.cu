#include <pni/PnI-Config.hpp>
#include "core/r2s/multi_gpu/DPacketsAsync.cuh"

#include <span>

#include <pni/example/EasyParallel.hpp>

namespace openpni::distributed::r2s::multi_gpu
{

DPacketsAsync DPacketsAsync::FromHost(
    uint8_t const *h_raw,
    uint64_t const *h_offset,
    uint16_t const *h_length,
    uint16_t const *h_channel,
    uint64_t h_count)
{
    DPacketsAsync d_packets;
    d_packets.ReserveFromHost(h_raw, h_offset, h_length, h_channel, h_count);
    return d_packets;
}

DPacketsAsync DPacketsAsync::FromHost(
    openpni::interface::ISingleGenerator::PacketsInfo h_packets)
{
    return FromHost(h_packets.raw, h_packets.offset, h_packets.length, h_packets.channel, h_packets.count);
}

void DPacketsAsync::ReserveFromHost(
    openpni::interface::ISingleGenerator::PacketsInfo h_packets)
{
    ReserveFromHost(h_packets.raw, h_packets.offset, h_packets.length, h_packets.channel, h_packets.count);
}

void DPacketsAsync::ReserveFromHost(
    uint8_t const *__h_raw,
    uint64_t const *__h_offset,
    uint16_t const *__h_length,
    uint16_t const *__h_channel,
    uint64_t __h_count)
{
    if (__h_count == 0)
    {
        count = 0;
        return;
    }

    count = __h_count;
    raw.Reserve(static_cast<size_t>(__h_offset[__h_count - 1] + __h_length[__h_count - 1] - __h_offset[0]));
    raw.Allocator().copy_from_host_to_device(
        raw.Get(),
        ::std::span<const uint8_t>(
            __h_raw + __h_offset[0],
            static_cast<size_t>(__h_offset[__h_count - 1] + __h_length[__h_count - 1] - __h_offset[0])));
    offset.Reserve(__h_count);
    offset.Allocator().copy_from_host_to_device(
        offset.Get(),
        ::std::span<const uint64_t>(__h_offset, static_cast<size_t>(__h_count)));
    if (__h_offset[0] != 0)
    {
        openpni::d_parallel_sub(offset.Get(), static_cast<size_t>(__h_offset[0]), offset.Get(), offset.Elements());
    }
    length.Reserve(__h_count);
    length.Allocator().copy_from_host_to_device(
        length.Get(),
        ::std::span<const uint16_t>(__h_length, static_cast<size_t>(__h_count)));
    channel.Reserve(__h_count);
    channel.Allocator().copy_from_host_to_device(
        channel.Get(),
        ::std::span<const uint16_t>(__h_channel, static_cast<size_t>(__h_count)));
}

} // namespace openpni::distributed::r2s::multi_gpu
