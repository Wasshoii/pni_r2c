#include "core/streaming/multi_gpu/CoincidenceMultiGpuEngine.hpp"

#include "core/streaming/StreamingCoincidence.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>

#include <cuda_runtime.h>
#include <glog/logging.h>

namespace openpni::distributed::streaming::multi_gpu
{

namespace
{
    std::vector<uint32_t> resolveGpuIds(const TimeAlignerConfig &config)
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
                std::string("resolveGpuIds: cudaGetDeviceCount failed: ") +
                cudaGetErrorString(err));
        }

        std::vector<uint32_t> gpu_ids(static_cast<size_t>(device_count));
        for (int i = 0; i < device_count; ++i)
        {
            gpu_ids[static_cast<size_t>(i)] = static_cast<uint32_t>(i);
        }
        return gpu_ids;
    }
} // namespace

bool shouldUseCoincidenceMultiGpu(const TimeAlignerConfig &config)
{
    return config.enableMultiGpu;
}

CoincidenceMultiGpuEngineConfig makeCoincidenceMultiGpuEngineConfig(
    const TimeAlignerConfig &config)
{
    if (config.channelNum == 0 || config.crystalsPerChannel == 0)
    {
        throw std::invalid_argument(
            "makeCoincidenceMultiGpuEngineConfig: channelNum and crystalsPerChannel must be > 0");
    }

    CoincidenceMultiGpuEngineConfig engine_config;
    engine_config.protocol = config.coinProtocol;
    engine_config.crystal_nums_per_channel.assign(
        config.channelNum, config.crystalsPerChannel);
    engine_config.gpu_ids = resolveGpuIds(config);
    engine_config.instance_per_gpu = std::max<uint32_t>(1u, config.instancePerGpu);
    return engine_config;
}

CoincidenceMultiGpuEngine::~CoincidenceMultiGpuEngine()
{
    finalize();
}

bool CoincidenceMultiGpuEngine::initialize(const CoincidenceMultiGpuEngineConfig &config)
{
    if (initialized_)
    {
        return true;
    }

    if (config.crystal_nums_per_channel.empty())
    {
        LOG(ERROR) << "CoincidenceMultiGpuEngine: crystal_nums_per_channel is empty";
        return false;
    }

    if (config.gpu_ids.empty())
    {
        LOG(ERROR) << "CoincidenceMultiGpuEngine: no GPU ids configured";
        return false;
    }

    gpu_ids_ = config.gpu_ids;

    std::vector<S2CSPSCProcessor::ComputePtr> computes;
    computes.reserve(static_cast<size_t>(config.gpu_ids.size()) *
                     static_cast<size_t>(config.instance_per_gpu));

    for (const uint32_t gpu_id : config.gpu_ids)
    {
        for (uint32_t instance_id = 0; instance_id < config.instance_per_gpu; ++instance_id)
        {
            (void)instance_id;
            S2CComputeConfig compute_config{};
            compute_config.protocol = config.protocol;
            compute_config.crystal_nums_per_channel = config.crystal_nums_per_channel;
            compute_config.gpuId = static_cast<int>(gpu_id);
            computes.push_back(std::make_unique<S2CCompute>(compute_config));
        }
    }

    const size_t compute_instance_count = computes.size();

    processor_ = std::make_unique<S2CSPSCProcessor>(
        std::move(computes),
        config.ring_size,
        config.queue_cap,
        CoinResultPolicy{});

    initialized_ = true;
    LOG(INFO) << "CoincidenceMultiGpuEngine initialized with " << gpu_ids_.size()
              << " GPU(s), " << config.crystal_nums_per_channel.size() << " channels, "
              << compute_instance_count << " compute instance(s)";
    return true;
}

SegmentCoinResult CoincidenceMultiGpuEngine::processSinglesSync(
    std::span<const Single> singles, uint64_t carryCutoffTime_100fs)
{
    if (!initialized_ || !processor_)
    {
        throw std::runtime_error("CoincidenceMultiGpuEngine: not initialized");
    }

    // Release previous lease before submitting a new task so the ring slot can be reused.
    active_lease_ = S2CSPSCProcessor::OutputLease{};

    if (singles.empty())
    {
        return {};
    }

    S2CInput input{singles, carryCutoffTime_100fs};
    processor_->submit(&input);
    active_lease_ = processor_->next();

    if (active_lease_.failed())
    {
        active_lease_ = S2CSPSCProcessor::OutputLease{};
        throw std::runtime_error("CoincidenceMultiGpuEngine: compute failed");
    }

    if (!active_lease_)
    {
        active_lease_ = S2CSPSCProcessor::OutputLease{};
        throw std::runtime_error("CoincidenceMultiGpuEngine: empty result");
    }

    const auto &result = *active_lease_;
    const uint64_t prompt_count = result.actualPromptCount;
    const uint64_t delay_count = result.actualDelayCount;

    SegmentCoinResult out;
    out.promptCount = prompt_count;
    out.delayCount = delay_count;
    if (prompt_count > 0)
    {
        out.prompt = std::span<const Listmode>(
            result.prompt.Data(), static_cast<size_t>(prompt_count));
    }
    if (delay_count > 0)
    {
        out.delay = std::span<const Listmode>(
            result.delay.Data(), static_cast<size_t>(delay_count));
    }
    return out;
}

void CoincidenceMultiGpuEngine::finalize()
{
    active_lease_ = S2CSPSCProcessor::OutputLease{};
    processor_.reset();
    gpu_ids_.clear();
    initialized_ = false;
}

} // namespace openpni::distributed::streaming::multi_gpu
