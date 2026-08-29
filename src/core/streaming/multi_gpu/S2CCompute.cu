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
      protocol_(config.protocol),
      max_singles_capacity_(config.maxSinglesCapacity)
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

    if (max_singles_capacity_ > 0)
    {
        ensureDeviceCapacity(max_singles_capacity_);
    }
}

cudaStream_t S2CCompute::stream() const noexcept
{
    return openpni::default_stream();
}

void S2CCompute::ensureDeviceCapacity(size_t elements)
{
    if (d_singles_.Elements() >= elements)
    {
        return;
    }
    d_singles_.Clear();
    d_singles_.Reserve(elements);
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

    // 设备缓冲只增不减，按 maxSinglesCapacity 一次性分配。批大小逐批变化时若跟着
    // 重新分配，每批都会引入一次 cudaFree + cudaMalloc 同步点。
    ensureDeviceCapacity(data->size());
    // 必须同步拷贝：getDListmode 并不跑在 stream() 上，异步版本无法保证上传先于内核，
    // 既有正确性风险，实测也会因跨流同步反而拖慢整体。输入是 pinned 内存，
    // 同步 cudaMemcpy 也不需要走 bounce buffer。
    openpni::detail::cuda_throw(
        cudaMemcpy(d_singles_.Data(), data->singles.data(),
                   data->size() * sizeof(Single), cudaMemcpyHostToDevice),
        "Failed to upload singles in S2CCompute::compute()");

    const auto result = coincidence_.getDListmode(
        d_singles_.CSpan(data->size()), protocol_, data->carryCutoffTime_100fs);

    // 配对数量取自设备端 span 的长度，无需依赖回拷。
    out->actualPromptCount = result.prompt.size();
    out->actualDelayCount = result.delay.size();

    if (data->copyPromptToHost)
    {
        copy_from_device_to_pinned_host_async(out->prompt, result.prompt, stream());
    }
    if (data->copyDelayToHost)
    {
        copy_from_device_to_pinned_host_async(out->delay, result.delay, stream());
    }
    if (!data->copyPromptToHost && !data->copyDelayToHost)
    {
        // 两路都不回拷时也要等内核完成，否则计数与后续批次会乱序。
        openpni::detail::cuda_throw(cudaStreamSynchronize(stream()),
                                    "Failed to synchronize S2CCompute stream");
    }
}

} // namespace openpni::distributed::streaming::multi_gpu
