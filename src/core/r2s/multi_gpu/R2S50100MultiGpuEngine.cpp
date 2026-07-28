#include "core/r2s/multi_gpu/R2S50100MultiGpuEngine.hpp"

#include "core/r2s/R2S.hpp"

#include <algorithm>
#include <stdexcept>

#include <cuda_runtime.h>
#include <glog/logging.h>

namespace openpni::distributed::r2s::multi_gpu
{

namespace
{
    std::vector<uint32_t> resolveGpuIds(const R2SProcessConfig &config)
    {
        if (!config.gpuIds.empty())
        {
            return config.gpuIds;
        }

        int device_count = 0;
        const cudaError_t err = cudaGetDeviceCount(&device_count);
        if (err != cudaSuccess || device_count < 1)
        {
            throw std::runtime_error(
                std::string("resolveGpuIds: cudaGetDeviceCount failed: ") + cudaGetErrorString(err));
        }

        std::vector<uint32_t> gpu_ids(static_cast<size_t>(device_count));
        for (int i = 0; i < device_count; ++i)
        {
            gpu_ids[static_cast<size_t>(i)] = static_cast<uint32_t>(i);
        }
        return gpu_ids;
    }
}

bool shouldUseMultiGpu50100(const R2SProcessConfig &config)
{
    // BDM50100 默认走多 GPU 引擎；仅当显式 enableMultiGpu=false 时回退 legacy 单 GPU。
    return config.detectorType == DetectorType::BDM50100 && config.enableMultiGpu;
}

R2S50100MultiGpuEngineConfig makeMultiGpuEngineConfig(
    const R2SProcessConfig &config,
    const std::vector<uint16_t> &channels_to_process)
{
    R2S50100MultiGpuEngineConfig engine_config;
    engine_config.r2s_params.matchXTalkEnabled = config.matchXTalkEnabled;
    engine_config.r2s_params.timeWindow = config.timeWindow;
    engine_config.r2s_params.timeShift = config.timeShift;
    engine_config.r2s_params.crossTalkEnabled = config.crossTalkEnabled;
    engine_config.r2s_params.crossTalkTimeWindow = config.crossTalkTimeWindow;
    engine_config.r2s_params.energyThresholds = config.energyThresholds;
    engine_config.gpu_ids = resolveGpuIds(config);
    engine_config.instance_per_gpu = std::max<uint32_t>(1u, config.instancePerGpu);
    engine_config.max_input_gibits = config.maxInputGibits;
    engine_config.input_burst_tolerance_coef = config.inputBurstToleranceCoef;

    engine_config.local_calib_files.reserve(channels_to_process.size());
    for (const auto global_channel : channels_to_process)
    {
        if (static_cast<size_t>(global_channel) >= config.calibrationFiles.size() ||
            config.calibrationFiles[global_channel].empty())
        {
            throw std::runtime_error(
                "makeMultiGpuEngineConfig: missing calibration for channel " +
                std::to_string(global_channel));
        }
        engine_config.local_calib_files.push_back(config.calibrationFiles[global_channel]);
    }

    return engine_config;
}

R2S50100MultiGpuEngine::~R2S50100MultiGpuEngine()
{
    finalize();
}

bool R2S50100MultiGpuEngine::initialize(const R2S50100MultiGpuEngineConfig &config)
{
    if (initialized_)
    {
        return true;
    }

    if (config.local_calib_files.empty())
    {
        LOG(ERROR) << "R2S50100MultiGpuEngine: local_calib_files is empty";
        return false;
    }

    if (config.gpu_ids.empty())
    {
        LOG(ERROR) << "R2S50100MultiGpuEngine: no GPU ids configured";
        return false;
    }

    gpu_ids_ = config.gpu_ids;

    std::vector<R2S50100SPSCProcessor::ComputePtr> computes;
    computes.reserve(static_cast<size_t>(config.gpu_ids.size()) *
                     static_cast<size_t>(config.instance_per_gpu));

    for (const uint32_t gpu_id : config.gpu_ids)
    {
        auto r2s_params = config.r2s_params;
        r2s_params.__deviceId = gpu_id;

        for (uint32_t instance_id = 0; instance_id < config.instance_per_gpu; ++instance_id)
        {
            (void)instance_id;
            R2S50100ComputeConfig compute_config{};
            compute_config.local_calib_files = config.local_calib_files;
            compute_config.r2s_params = r2s_params;
            compute_config.gpuId = static_cast<int>(gpu_id);
            computes.push_back(std::make_unique<R2S50100Compute>(compute_config));
        }
    }

    const long double actual_process_gibits =
        config.max_input_gibits * static_cast<long double>(config.input_burst_tolerance_coef);

    const size_t compute_instance_count = computes.size();

    processor_ = std::make_unique<R2S50100SPSCProcessor>(
        std::move(computes),
        config.ring_size,
        config.queue_cap,
        R2S50100SinglesResultPolicy(actual_process_gibits));

    initialized_ = true;
    LOG(INFO) << "R2S50100MultiGpuEngine initialized with " << gpu_ids_.size()
              << " GPU(s), " << config.local_calib_files.size() << " local channels, "
              << compute_instance_count << " compute instance(s)";
    return true;
}

SegmentSinglesResult R2S50100MultiGpuEngine::processSegmentSync(const openpni::RawDataView &view)
{
    if (!initialized_ || !processor_)
    {
        throw std::runtime_error("R2S50100MultiGpuEngine: not initialized");
    }

    if (!view.count || !view.data)
    {
        return {};
    }

    processor_->submit(&view);
    auto lease = processor_->next();

    if (lease.failed())
    {
        throw std::runtime_error("R2S50100MultiGpuEngine: compute failed");
    }

    if (!lease)
    {
        throw std::runtime_error("R2S50100MultiGpuEngine: empty result");
    }

    const auto &result = *lease;
    const uint64_t count = result.actualSinglesCount;
    if (count == 0)
    {
        return {};
    }

    return SegmentSinglesResult{
        std::span<const Single>(result.singles.Data(), static_cast<size_t>(count)),
        count};
}

void R2S50100MultiGpuEngine::finalize()
{
    processor_.reset();
    gpu_ids_.clear();
    initialized_ = false;
}

} // namespace openpni::distributed::r2s::multi_gpu
