#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include <cuda_runtime.h>
#include <pni/core/CommonDataType.hpp>

namespace openpni::distributed::r2s::multi_gpu
{

inline bool tryCudaHostRegister(void *ptr, size_t bytes)
{
    if (ptr == nullptr || bytes == 0)
    {
        return false;
    }

    const cudaError_t err = cudaHostRegister(ptr, bytes, cudaHostRegisterPortable);
    if (err == cudaSuccess)
    {
        return true;
    }

    if (err == cudaErrorHostMemoryAlreadyRegistered)
    {
        static_cast<void>(cudaGetLastError());
        return true;
    }

    static_cast<void>(cudaGetLastError());
    return false;
}

inline uint64_t rawViewDataBytes(const openpni::RawDataView &view)
{
    if (view.count == 0 || view.offset == nullptr || view.length == nullptr)
    {
        return 0;
    }
    uint64_t span = 0;
    for (uint64_t i = 0; i < view.count; ++i)
    {
        span = std::max(span, view.offset[i] + static_cast<uint64_t>(view.length[i]));
    }
    return span;
}

inline bool tryRegisterRawViewForH2D(const openpni::RawDataView &view)
{
    const uint64_t bytes = rawViewDataBytes(view);
    if (bytes == 0 || view.data == nullptr)
    {
        return false;
    }
    return tryCudaHostRegister(view.data, static_cast<size_t>(bytes));
}

} // namespace openpni::distributed::r2s::multi_gpu
