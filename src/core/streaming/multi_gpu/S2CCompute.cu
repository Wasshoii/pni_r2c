#include "core/streaming/multi_gpu/S2CCompute.cuh"

#include "core/r2s/multi_gpu/PinnedHostCopy.cuh"

#include <algorithm>
#include <stdexcept>
#include <string>

#include <pni/CudaSupport.hpp>

namespace openpni::distributed::streaming::multi_gpu
{

S2CCompute::S2CCompute(const S2CComputeConfig &config)
    : gpu_id_(config.gpuId),
      protocol_(config.protocol)
{
    if (config.crystal_nums_per_channel.empty())
    {
        throw std::invalid_argument("S2CCompute: crystal_nums_per_channel must not be empty");
    }
    if (std::any_of(config.crystal_nums_per_channel.begin(),
                    config.crystal_nums_per_channel.end(),
                    [](uint32_t crystal_num) { return crystal_num == 0; }))
    {
        throw std::invalid_argument("S2CCompute: crystal_nums_per_channel values must be > 0");
    }

    const cudaError_t err = cudaSetDevice(gpu_id_);
    if (err != cudaSuccess)
    {
        throw std::runtime_error(
            std::string("S2CCompute: cudaSetDevice(") + std::to_string(gpu_id_) +
            ") failed: " + cudaGetErrorString(err));
    }

    coincidence_.setTotalCrystalNumOfEachChannel(config.crystal_nums_per_channel);
}

cudaStream_t S2CCompute::stream() const noexcept
{
    return openpni::default_stream();
}

void S2CCompute::compute(const Data *data, Result *out)
{
    using openpni::distributed::r2s::multi_gpu::copy_from_device_to_pinned_host_async;

    if (!out)
    {
        return;
    }

    if (!data || data->empty())
    {
        out->actualPromptCount = 0;
        out->actualDelayCount = 0;
        out->prompt.ResetPointer(0);
        out->delay.ResetPointer(0);
        return;
    }

    openpni::detail::cuda_throw(
        cudaSetDevice(gpu_id_),
        "Failed to set CUDA device in S2CCompute::compute() for GPU " + std::to_string(gpu_id_));

    d_singles_.Reserve(data->size());
    d_singles_.CopyFromHost(std::span<const Single>(*data));

    const auto result = coincidence_.getDListmode(d_singles_.CSpan(), protocol_);

    out->actualPromptCount = result.prompt.size();
    out->actualDelayCount = result.delay.size();
    copy_from_device_to_pinned_host_async(out->prompt, result.prompt, stream());
    copy_from_device_to_pinned_host_async(out->delay, result.delay, stream());
}

} // namespace openpni::distributed::streaming::multi_gpu
