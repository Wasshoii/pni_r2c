#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include <cuda_runtime.h>

#include <pni/CudaSupport.hpp>
#include <pni/PnI-Config.hpp>
#include <pni/core/CommonDataType.hpp>
#include <pni/node/misc/Coincidence.hpp>
#include <pni/tools/CudaPtr.hpp>
#include <pni/tools/HostUniquePtr.hpp>

#include "core/r2s/multi_gpu/SPSCProcessor.hpp"

namespace openpni::distributed::streaming::multi_gpu
{

using openpni::distributed::r2s::multi_gpu::ICompute;
using openpni::distributed::r2s::multi_gpu::SPSCProcessor;

struct CoinResult
{
    CoinResult()
        : prompt(std::make_unique<openpni::detail::VAllocatorCUDAHost>()),
          delay(std::make_unique<openpni::detail::VAllocatorCUDAHost>()),
          actualPromptCount(0),
          actualDelayCount(0)
    {
    }

    openpni::tools::HostUniquePtr<Listmode> prompt;
    openpni::tools::HostUniquePtr<Listmode> delay;
    uint64_t actualPromptCount{0};
    uint64_t actualDelayCount{0};
};

/** MultiGPU 任务输入：singles + 可选跨段 cutoff（0 表示关闭）。 */
struct S2CInput
{
    std::span<const Single> singles;
    uint64_t carryCutoffTime_100fs = 0;

    bool empty() const noexcept { return singles.empty(); }
    size_t size() const noexcept { return singles.size(); }
};

struct CoinResultPolicy
{
    CoinResult make_result() const { return CoinResult{}; }

    void prepare_for_reuse(CoinResult &result, const S2CInput *) const noexcept
    {
        result.actualPromptCount = 0;
        result.actualDelayCount = 0;
    }
};

struct S2CComputeConfig
{
    CoincidenceProtocol protocol;
    std::vector<uint32_t> crystal_nums_per_channel;
    int gpuId = 0;
};

using IComputeS2C = ICompute<S2CInput, CoinResult>;

class S2CCompute : public IComputeS2C
{
public:
    using Data = S2CInput;
    using Result = CoinResult;

    explicit S2CCompute(const S2CComputeConfig &config);
    ~S2CCompute() override = default;

    S2CCompute(const S2CCompute &) = delete;
    S2CCompute &operator=(const S2CCompute &) = delete;
    S2CCompute(S2CCompute &&) = delete;
    S2CCompute &operator=(S2CCompute &&) = delete;

    void compute(const Data *data, Result *out) override;

    int gpu_id() const noexcept { return gpu_id_; }
    cudaStream_t stream() const noexcept;

private:
    int gpu_id_;
    CoincidenceProtocol protocol_;
    openpni::Coincidence coincidence_;
    openpni::detail::CudaUniquePointer<Single> d_singles_{"S2CCompute_singles"};
};

using S2CSPSCProcessor = SPSCProcessor<
    S2CInput,
    CoinResult,
    CoinResultPolicy>;

} // namespace openpni::distributed::streaming::multi_gpu
