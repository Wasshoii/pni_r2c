#include "core/streaming/multi_gpu/CoincidenceMultiGpuEngine.hpp"

#include "core/streaming/StreamingCoincidence.hpp"

#include <algorithm>
#include <condition_variable>
#include <mutex>
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
    engine_config.max_singles_capacity = config.maxSegmentSingles;

    const size_t gpu_count = std::max<size_t>(1, engine_config.gpu_ids.size());
    const size_t compute_n =
        gpu_count * static_cast<size_t>(engine_config.instance_per_gpu);
    const size_t depth = config.coinPipelineDepth == 0
                             ? compute_n
                             : std::max<size_t>(1, config.coinPipelineDepth);
    engine_config.ring_size = std::max({compute_n + 2, depth + 2, size_t{4}});
    engine_config.queue_cap = engine_config.ring_size;
    return engine_config;
}

namespace
{
    // maxSegmentSingles 由用户按显卡配置设定，这里给出一个可操作的越界提示：
    // 输入缓冲 + prompt/delay 输出上界（最坏情况下配对数与 singles 数同量级）。
    void warnIfSegmentExceedsVram(const CoincidenceMultiGpuEngineConfig &config,
                                  size_t compute_instances)
    {
        if (compute_instances == 0)
        {
            return;
        }

        if (config.max_singles_capacity == 0)
        {
            // 不设上界时单批规模只受水位推进速度约束。预编译 Coincidence 对单批 singles
            // 数存在上限（实测 9120 双节点数据在 5e5~1e6 之间就会触发非法访存），越过它
            // 是硬崩溃而非降级，所以这里必须提醒。
            LOG(WARNING) << "CoincidenceMultiGpuEngine: maxSegmentSingles=0（不限制单批"
                            "规模）；符合内核对单批 singles 数有上限，突发流量下可能触发"
                            "非法访存。建议按显存与数据率设一个显式上界（默认 262144）";
            return;
        }

        size_t freeBytes = 0;
        size_t totalBytes = 0;
        if (cudaMemGetInfo(&freeBytes, &totalBytes) != cudaSuccess)
        {
            return;
        }

        const size_t perInstance =
            config.max_singles_capacity *
            (sizeof(Single) + 2 * sizeof(Listmode));
        const size_t estimate = perInstance * compute_instances;

        // 内核自身还需要排序/前缀和等中间缓冲，留出一半余量再判定。
        if (estimate * 2 > freeBytes)
        {
            const size_t suggested =
                freeBytes / (2 * compute_instances *
                             (sizeof(Single) + 2 * sizeof(Listmode)));
            LOG(WARNING) << "CoincidenceMultiGpuEngine: maxSegmentSingles="
                         << config.max_singles_capacity << " 预计占用约 "
                         << (estimate / (1024 * 1024)) << " MiB，可用显存仅 "
                         << (freeBytes / (1024 * 1024)) << " MiB，可能 OOM；"
                         << "建议不超过 " << suggested;
        }
    }
} // namespace

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
    ring_size_ = std::max(config.ring_size, size_t{1});
    inflight_inputs_.assign(ring_size_, S2CInput{});
    submit_seq_ = 0;
    consume_seq_ = 0;
    in_flight_ = 0;

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
            compute_config.maxSinglesCapacity = config.max_singles_capacity;
            computes.push_back(std::make_unique<S2CCompute>(compute_config));
        }
    }

    const size_t compute_instance_count = computes.size();
    warnIfSegmentExceedsVram(config, compute_instance_count);

    processor_ = std::make_unique<S2CSPSCProcessor>(
        std::move(computes),
        config.ring_size,
        config.queue_cap,
        CoinResultPolicy{});

    initialized_ = true;
    LOG(INFO) << "CoincidenceMultiGpuEngine initialized with " << gpu_ids_.size()
              << " GPU(s), " << config.crystal_nums_per_channel.size() << " channels, "
              << compute_instance_count << " compute instance(s), ring=" << ring_size_;
    return true;
}

void CoincidenceMultiGpuEngine::submitSingles(
    std::span<const Single> singles, uint64_t carryCutoffTime_100fs,
    bool copyPrompt, bool copyDelay)
{
    if (!initialized_ || !processor_)
    {
        throw std::runtime_error("CoincidenceMultiGpuEngine: not initialized");
    }
    if (singles.empty())
    {
        return;
    }

    // 必须先等到同槽上一轮已经被 next() 收回，才能覆盖 inflight_inputs_。
    // 否则 worker 仍在读旧 span 时这里改掉 S2CInput，会提交错误数据。
    std::unique_lock<std::mutex> lock(submit_mu_);
    submit_cv_.wait(lock, [this]
                    { return in_flight_ < ring_size_; });
    const size_t idx = static_cast<size_t>(submit_seq_ % ring_size_);
    inflight_inputs_[idx] = S2CInput{singles, carryCutoffTime_100fs, copyPrompt, copyDelay};
    ++submit_seq_;
    ++in_flight_;
    lock.unlock();
    processor_->submit(&inflight_inputs_[idx]);
}

SegmentCoinResult CoincidenceMultiGpuEngine::nextResult()
{
    if (!initialized_ || !processor_)
    {
        throw std::runtime_error("CoincidenceMultiGpuEngine: not initialized");
    }

    active_lease_ = processor_->next();

    SegmentCoinResult out;
    if (active_lease_.failed())
    {
        out.failed = true;
        active_lease_ = S2CSPSCProcessor::OutputLease{};
        ++consume_seq_;
        {
            std::lock_guard<std::mutex> lock(submit_mu_);
            if (in_flight_ > 0)
            {
                --in_flight_;
            }
            submit_cv_.notify_one();
        }
        return out;
    }
    if (!active_lease_)
    {
        out.endOfStream = true;
        return out;
    }

    const auto &result = *active_lease_;
    out.promptCount = result.actualPromptCount;
    out.delayCount = result.actualDelayCount;
    const size_t in_idx = static_cast<size_t>(consume_seq_ % ring_size_);
    ++consume_seq_;
    const S2CInput &in = inflight_inputs_[in_idx];
    // 未回拷时主机缓冲是上一批残留，不能当成当前结果。
    if (in.copyPromptToHost && out.promptCount > 0 && result.prompt.Data() != nullptr)
    {
        out.prompt = std::span<const Listmode>(
            result.prompt.Data(), static_cast<size_t>(out.promptCount));
    }
    if (in.copyDelayToHost && out.delayCount > 0 && result.delay.Data() != nullptr)
    {
        out.delay = std::span<const Listmode>(
            result.delay.Data(), static_cast<size_t>(out.delayCount));
    }

    // 先读完本槽 S2CInput 再放行 submit 覆盖，避免 drain 与 extract 抢同一槽。
    {
        std::lock_guard<std::mutex> lock(submit_mu_);
        if (in_flight_ > 0)
        {
            --in_flight_;
        }
        submit_cv_.notify_one();
    }
    return out;
}

void CoincidenceMultiGpuEngine::releaseResult()
{
    active_lease_ = S2CSPSCProcessor::OutputLease{};
}

void CoincidenceMultiGpuEngine::signalNoMoreData()
{
    if (processor_)
    {
        processor_->signal_no_more_data();
    }
}

SegmentCoinResult CoincidenceMultiGpuEngine::processSinglesSync(
    std::span<const Single> singles, uint64_t carryCutoffTime_100fs,
    bool copyPrompt, bool copyDelay)
{
    releaseResult();
    if (singles.empty())
    {
        return {};
    }
    submitSingles(singles, carryCutoffTime_100fs, copyPrompt, copyDelay);
    auto out = nextResult();
    if (out.failed)
    {
        throw std::runtime_error("CoincidenceMultiGpuEngine: compute failed");
    }
    if (out.endOfStream)
    {
        throw std::runtime_error("CoincidenceMultiGpuEngine: empty result");
    }
    return out;
}

void CoincidenceMultiGpuEngine::finalize()
{
    active_lease_ = S2CSPSCProcessor::OutputLease{};
    processor_.reset();
    inflight_inputs_.clear();
    submit_seq_ = 0;
    consume_seq_ = 0;
    in_flight_ = 0;
    ring_size_ = 0;
    gpu_ids_.clear();
    initialized_ = false;
}

} // namespace openpni::distributed::streaming::multi_gpu
