#pragma once

#include <iostream>
#include <memory>
#include <span>

#include <cuda_runtime.h>

#include <pni/tools/CudaPtr.hpp>
#include <pni/tools/HostUniquePtr.hpp>

namespace openpni::distributed::r2s::multi_gpu
{

template <typename T>
inline ::openpni::tools::HostUniquePtr<T> make_cuda_host_ptr_from_dcopy(std::span<const T> d_src,
                                                                        cudaStream_t stream)
{
    ::openpni::tools::HostUniquePtr<T> result{
        ::std::make_unique<::openpni::detail::VAllocatorCUDAHost>()};

    result.ResetPointer(d_src.size());
    if (d_src.empty())
    {
        return result;
    }

    ::openpni::detail::cuda_throw(cudaMemcpyAsync(result.Data(),
                                                  d_src.data(),
                                                  d_src.size_bytes(),
                                                  cudaMemcpyDeviceToHost,
                                                  stream),
                                  "Failed to copy device memory to pinned host memory");

    ::openpni::detail::cuda_throw(
        cudaStreamSynchronize(stream),
        "Failed to synchronize pinned host copy");

    return result;
}

template <typename T>
inline void copy_from_device_to_pinned_host_async(::openpni::tools::HostUniquePtr<T> &h_dst,
                                                  ::std::span<const T> d_src,
                                                  cudaStream_t stream)
{
    if (d_src.empty())
    {
        h_dst.ResetPointer(0);
        return;
    }
    h_dst.Reserve(d_src.size());
    ::openpni::detail::cuda_throw(cudaMemcpyAsync(h_dst.Data(),
                                                  d_src.data(),
                                                  d_src.size_bytes(),
                                                  cudaMemcpyDeviceToHost,
                                                  stream),
                                  "Failed to copy device memory to pinned host memory");

    ::openpni::detail::cuda_throw(cudaStreamSynchronize(stream),
                                  "Failed to synchronize pinned host copy");
}

} // namespace openpni::distributed::r2s::multi_gpu
