#include <pni/PnI-Config.hpp>
#include "core/r2s/multi_gpu/PinnedHostCopy.cuh"
#include "core/r2s/multi_gpu/R2S50100Compute.cuh"

#include <stdexcept>

#include <pni/CudaSupport.hpp>

namespace openpni::distributed::r2s::multi_gpu
{

R2S50100Compute::R2S50100Compute(const R2S50100ComputeConfig &config)
    : gpu_id_(config.gpuId)
{
    if (config.local_calib_files.empty())
    {
        throw std::invalid_argument("R2S50100Compute: local_calib_files must not be empty");
    }

    cudaError_t err = cudaSetDevice(gpu_id_);
    if (err != cudaSuccess)
    {
        throw std::runtime_error(
            std::string("R2S50100Compute: cudaSetDevice(") + std::to_string(gpu_id_) +
            ") failed: " + cudaGetErrorString(err));
    }

    const int num_channels = static_cast<int>(config.local_calib_files.size());

    for (int c = 0; c < num_channels; ++c)
    {
        auto gen = std::make_unique<openpni::device::bdm50100_v2::BDM50100R2S>(
            openpni::device::bdm50100_v2::BDM50100Type::U4V2);

        if (!gen->setParams(config.r2s_params))
        {
            throw std::runtime_error(
                "R2S50100Compute: setParams failed for GPU " + std::to_string(gpu_id_) +
                " channel " + std::to_string(c));
        }

        gen->SetChannelIndex(static_cast<uint16_t>(c));

        if (!config.local_calib_files[static_cast<size_t>(c)].empty())
        {
            gen->LoadCalibration(config.local_calib_files[static_cast<size_t>(c)]);
        }

        if (!array_.AddSingleGenerator(gen.get()))
        {
            throw std::runtime_error(
                "R2S50100Compute: AddSingleGenerator failed for GPU " + std::to_string(gpu_id_) +
                " channel " + std::to_string(c));
        }

        channels_.push_back(std::move(gen));
    }
}

cudaStream_t R2S50100Compute::stream() const noexcept
{
    return openpni::default_stream();
}

void R2S50100Compute::compute(const Data *data, Result *out)
{
    if (!data || !out || data->count == 0)
    {
        out->actualSinglesCount = 0;
        out->singles.ResetPointer(0);
        return;
    }

    openpni::detail::cuda_throw(cudaSetDevice(gpu_id_),
               "Failed to set CUDA device in compute() for GPU " + std::to_string(gpu_id_));

    d_packets_async_.ReserveFromHost(
        data->data,
        data->offset,
        data->length,
        data->channel,
        data->count);

    openpni::interface::ISingleGenerator::PacketsInfo d_info{};
    d_info.raw = d_packets_async_.raw.Get();
    d_info.offset = d_packets_async_.offset.Get();
    d_info.length = d_packets_async_.length.Get();
    d_info.channel = d_packets_async_.channel.Get();
    d_info.count = d_packets_async_.count;

    const auto temp = array_.DRaw2Singles(d_info);

    out->actualSinglesCount = temp.size();
    copy_from_device_to_pinned_host_async(
        out->singles,
        std::span<const Single>(temp.data(), temp.size()),
        openpni::default_stream());
}

} // namespace openpni::distributed::r2s::multi_gpu
