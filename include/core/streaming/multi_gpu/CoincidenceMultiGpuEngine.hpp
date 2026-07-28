#pragma once

#include <cstdint>
#include <memory>
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
    std::span<const Listmode> prompt;
    std::span<const Listmode> delay;
    uint64_t promptCount = 0;
    uint64_t delayCount = 0;
};

struct CoincidenceMultiGpuEngineConfig
{
    CoincidenceProtocol protocol;
    std::vector<uint32_t> crystal_nums_per_channel;
    std::vector<uint32_t> gpu_ids;
    uint32_t instance_per_gpu = 1;
    size_t ring_size = S2CSPSCProcessor::DEFAULT_RING_SIZE;
    size_t queue_cap = S2CSPSCProcessor::DEFAULT_QUEUE_CAP;
};

class CoincidenceMultiGpuEngine
{
public:
    CoincidenceMultiGpuEngine() = default;
    ~CoincidenceMultiGpuEngine();

    CoincidenceMultiGpuEngine(const CoincidenceMultiGpuEngine &) = delete;
    CoincidenceMultiGpuEngine &operator=(const CoincidenceMultiGpuEngine &) = delete;

    bool initialize(const CoincidenceMultiGpuEngineConfig &config);

    // Lease must remain alive while returned spans are used.
    SegmentCoinResult processSinglesSync(std::span<const Single> singles);

    void finalize();

    size_t gpuCount() const noexcept { return gpu_ids_.size(); }

private:
    std::unique_ptr<S2CSPSCProcessor> processor_;
    typename S2CSPSCProcessor::OutputLease active_lease_;
    std::vector<uint32_t> gpu_ids_;
    bool initialized_ = false;
};

CoincidenceMultiGpuEngineConfig makeCoincidenceMultiGpuEngineConfig(
    const TimeAlignerConfig &config);

bool shouldUseCoincidenceMultiGpu(const TimeAlignerConfig &config);

} // namespace openpni::distributed::streaming::multi_gpu
