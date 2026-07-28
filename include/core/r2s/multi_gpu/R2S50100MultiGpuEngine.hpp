#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include <pni/core/CommonDataType.hpp>
#include <pni/node/raw2singles/BDM50100/BDM50100Define.hpp>

#include "core/r2s/multi_gpu/R2S50100Compute.cuh"

namespace openpni::distributed::r2s
{
struct R2SProcessConfig;
}

namespace openpni::distributed::r2s::multi_gpu
{

struct SegmentSinglesResult
{
    std::span<const Single> span;
    uint64_t count = 0;
};

struct R2S50100MultiGpuEngineConfig
{
    openpni::device::bdm50100_v2::BDM50100R2SParams r2s_params;
    std::vector<std::string> local_calib_files;
    std::vector<uint32_t> gpu_ids;
    uint32_t instance_per_gpu = 1;
    long double max_input_gibits = 0.0L;
    float input_burst_tolerance_coef = 1.2f;
    size_t ring_size = R2S50100SPSCProcessor::DEFAULT_RING_SIZE;
    size_t queue_cap = R2S50100SPSCProcessor::DEFAULT_QUEUE_CAP;
};

class R2S50100MultiGpuEngine
{
public:
    R2S50100MultiGpuEngine() = default;
    ~R2S50100MultiGpuEngine();

    R2S50100MultiGpuEngine(const R2S50100MultiGpuEngine &) = delete;
    R2S50100MultiGpuEngine &operator=(const R2S50100MultiGpuEngine &) = delete;

    bool initialize(const R2S50100MultiGpuEngineConfig &config);

    SegmentSinglesResult processSegmentSync(const openpni::RawDataView &view);

    void finalize();

    size_t gpuCount() const noexcept { return gpu_ids_.size(); }

private:
    std::unique_ptr<R2S50100SPSCProcessor> processor_;
    std::vector<uint32_t> gpu_ids_;
    bool initialized_ = false;
};

R2S50100MultiGpuEngineConfig makeMultiGpuEngineConfig(
    const R2SProcessConfig &config,
    const std::vector<uint16_t> &channels_to_process);

bool shouldUseMultiGpu50100(const R2SProcessConfig &config);

} // namespace openpni::distributed::r2s::multi_gpu
