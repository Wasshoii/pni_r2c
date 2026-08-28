#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>

#include <pni/PnI-Config.hpp>
#include <pni/core/CommonDataType.hpp>
#include <pni/tools/CudaPtr.hpp>
#include <pni/tools/HostUniquePtr.hpp>

#include "core/r2s/multi_gpu/HostCudaRegister.hpp"

namespace openpni::distributed::r2s::multi_gpu
{

struct PinnedRawSlot
{
    openpni::tools::HostUniquePtr<uint8_t> data{
        std::make_unique<openpni::detail::VAllocatorCUDAHost>()};
    openpni::tools::HostUniquePtr<uint16_t> length{
        std::make_unique<openpni::detail::VAllocatorCUDAHost>()};
    openpni::tools::HostUniquePtr<uint64_t> offset{
        std::make_unique<openpni::detail::VAllocatorCUDAHost>()};
    openpni::tools::HostUniquePtr<uint16_t> channel{
        std::make_unique<openpni::detail::VAllocatorCUDAHost>()};
    openpni::RawDataView view{};

    bool capture(const openpni::RawDataView &src)
    {
        view = {};
        view.clock_ms = src.clock_ms;
        view.duration_ms = src.duration_ms;
        view.channelNum = src.channelNum;

        const uint64_t count = src.count;
        if (count == 0)
        {
            return true;
        }
        if (src.data == nullptr || src.length == nullptr || src.offset == nullptr || src.channel == nullptr)
        {
            return false;
        }

        const uint64_t bytes = rawViewDataBytes(src);
        data.Reserve(static_cast<size_t>(bytes));
        length.Reserve(static_cast<size_t>(count));
        offset.Reserve(static_cast<size_t>(count));
        channel.Reserve(static_cast<size_t>(count));

        if (bytes != 0)
        {
            std::memcpy(data.Data(), src.data, static_cast<size_t>(bytes));
        }
        std::memcpy(length.Data(), src.length, static_cast<size_t>(count) * sizeof(uint16_t));
        std::memcpy(offset.Data(), src.offset, static_cast<size_t>(count) * sizeof(uint64_t));
        std::memcpy(channel.Data(), src.channel, static_cast<size_t>(count) * sizeof(uint16_t));

        view.data = data.Data();
        view.length = length.Data();
        view.offset = offset.Data();
        view.channel = channel.Data();
        view.count = count;
        return true;
    }
};

} // namespace openpni::distributed::r2s::multi_gpu
