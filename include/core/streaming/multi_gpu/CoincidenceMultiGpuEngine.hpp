#pragma once

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <vector>

#include <pni/core/CommonDataType.hpp>
#include <pni/node/misc/Coincidence.hpp>

#include "core/streaming/multi_gpu/S2CCompute.cuh"

namespace openpni::distributed::streaming
{
struct TimeAlignerConfig;
}

namespace openpni::distributed::streaming::multi_gpu
{

struct SegmentCoinResult
{
    // 未请求回拷时两个 span 为空，但 count 依然有效（取自设备端 span 长度）。
    std::span<const Listmode> prompt;
    std::span<const Listmode> delay;
    uint64_t promptCount = 0;
    uint64_t delayCount = 0;
    bool endOfStream = false;
    bool failed = false;
};

struct CoincidenceMultiGpuEngineConfig
{
    CoincidenceProtocol protocol;
    std::vector<uint32_t> crystal_nums_per_channel;
    std::vector<uint32_t> gpu_ids;
    uint32_t instance_per_gpu = 1;
    size_t ring_size = S2CSPSCProcessor::DEFAULT_RING_SIZE;
    size_t queue_cap = S2CSPSCProcessor::DEFAULT_QUEUE_CAP;
    // 单批 singles 上限（含 carry），用于预分配设备缓冲与显存校验。0 = 不预分配。
    size_t max_singles_capacity = 0;
};

class CoincidenceMultiGpuEngine
{
public:
    CoincidenceMultiGpuEngine() = default;
    ~CoincidenceMultiGpuEngine();

    CoincidenceMultiGpuEngine(const CoincidenceMultiGpuEngine &) = delete;
    CoincidenceMultiGpuEngine &operator=(const CoincidenceMultiGpuEngine &) = delete;

    bool initialize(const CoincidenceMultiGpuEngineConfig &config);

    // 提交一批 singles。span 必须保持有效直到对应的 nextResult() 返回
    // （worker 在 compute() 里读它做 H2D）。空输入会被忽略。
    void submitSingles(std::span<const Single> singles,
                       uint64_t carryCutoffTime_100fs = 0,
                       bool copyPrompt = true,
                       bool copyDelay = true);

    // 按提交序收回下一批结果。span 在 releaseResult() 或下一次 nextResult()
    // 之前有效。排空后 endOfStream=true。
    SegmentCoinResult nextResult();
    void releaseResult();
    void signalNoMoreData();

    // Lease must remain alive while returned spans are used.
    // copyPrompt/copyDelay 为 false 时跳过对应的 D2H，只回报数量。
    SegmentCoinResult processSinglesSync(std::span<const Single> singles,
                                         uint64_t carryCutoffTime_100fs = 0,
                                         bool copyPrompt = true,
                                         bool copyDelay = true);

    void finalize();

    size_t gpuCount() const noexcept { return gpu_ids_.size(); }
    size_t ringSize() const noexcept { return ring_size_; }

private:
    std::unique_ptr<S2CSPSCProcessor> processor_;
    typename S2CSPSCProcessor::OutputLease active_lease_;
    std::vector<S2CInput> inflight_inputs_;
    size_t ring_size_ = 0;
    uint64_t submit_seq_ = 0;
    uint64_t consume_seq_ = 0;
    size_t in_flight_ = 0;
    std::mutex submit_mu_;
    std::condition_variable submit_cv_;
    std::vector<uint32_t> gpu_ids_;
    bool initialized_ = false;
};

CoincidenceMultiGpuEngineConfig makeCoincidenceMultiGpuEngineConfig(
    const TimeAlignerConfig &config);

bool shouldUseCoincidenceMultiGpu(const TimeAlignerConfig &config);

} // namespace openpni::distributed::streaming::multi_gpu
