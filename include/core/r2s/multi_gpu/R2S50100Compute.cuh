#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <cuda_runtime.h>

#include <pni/CudaSupport.hpp>
#include <pni/PnI-Config.hpp>
#include <pni/core/CommonDataType.hpp>
#include <pni/interface/SingleGenerator.hpp>
#include <pni/node/raw2singles/BDM50100/BDM50100Define.hpp>
#include <pni/node/raw2singles/BDM50100/BDM50100R2S.hpp>
#include <pni/node/raw2singles/BDM50100/BDM50100R2SArray.hpp>
#include <pni/tools/CudaPtr.hpp>
#include <pni/tools/HostUniquePtr.hpp>

#include "core/r2s/multi_gpu/DPacketsAsync.cuh"
#include "core/r2s/multi_gpu/SPSCProcessor.hpp"

namespace openpni::distributed::r2s::multi_gpu
{

struct SinglesResult
{
    SinglesResult()
        : singles(std::make_unique<openpni::detail::VAllocatorCUDAHost>()),
          d_singles{"R2S_d_singles"},
          actualSinglesCount(0)
    {
    }

    openpni::tools::HostUniquePtr<Single> singles;
    openpni::detail::CudaUniquePointer<Single> d_singles;
    int gpu_id = -1;
    uint64_t actualSinglesCount{0};
};

struct R2S50100SinglesResultPolicy
{
    explicit R2S50100SinglesResultPolicy(long double max_input_gibits = 0.0L) noexcept
        : max_input_gibits_(max_input_gibits)
    {
    }

    SinglesResult make_result() const
    {
        SinglesResult result;
        return result;
    }

    void prepare_for_reuse(SinglesResult &result, const openpni::RawDataView *) const noexcept
    {
        result.actualSinglesCount = 0;
        result.gpu_id = -1;
    }

private:
    static constexpr uint64_t bits_per_gibit = uint64_t{1} << 30;
    static constexpr uint64_t bits_per_input_packet =
        sizeof(openpni::device::bdm50100_v2::DataFrame50100_t) * 8ull;

    size_t reserve_singles_count() const noexcept
    {
        if (max_input_gibits_ <= 0.0L || bits_per_input_packet == 0)
        {
            return 0;
        }

        constexpr uint64_t max_size_t_u64 = static_cast<uint64_t>(std::numeric_limits<size_t>::max());
        constexpr uint64_t max_singles_per_packet = openpni::device::bdm50100_v2::MAX_SINGLE_NUM_PER_PACKET;
        constexpr uint64_t max_packet_count = max_size_t_u64 / max_singles_per_packet;

        const long double packet_count_exact =
            (max_input_gibits_ * static_cast<long double>(bits_per_gibit)) /
            static_cast<long double>(bits_per_input_packet);

        if (packet_count_exact >= static_cast<long double>(max_packet_count))
        {
            return std::numeric_limits<size_t>::max();
        }

        const uint64_t packet_count = static_cast<uint64_t>(packet_count_exact);

        if (packet_count > max_size_t_u64 / max_singles_per_packet)
        {
            return std::numeric_limits<size_t>::max();
        }

        return static_cast<size_t>(packet_count * max_singles_per_packet);
    }

    long double max_input_gibits_{0.0L};
};

using IComputeR2S = ICompute<RawDataView, SinglesResult>;

struct R2S50100ComputeConfig
{
    std::vector<std::string> local_calib_files;
    openpni::device::bdm50100_v2::BDM50100R2SParams r2s_params;
    int gpuId = 0;
};

class R2S50100Compute : public IComputeR2S
{
public:
    using Data = RawDataView;
    using Result = SinglesResult;

    explicit R2S50100Compute(const R2S50100ComputeConfig &config);
    ~R2S50100Compute() override = default;

    R2S50100Compute(const R2S50100Compute &) = delete;
    R2S50100Compute &operator=(const R2S50100Compute &) = delete;
    R2S50100Compute(R2S50100Compute &&) = delete;
    R2S50100Compute &operator=(R2S50100Compute &&) = delete;

    void compute(const Data *data, Result *out) override;

    int gpu_id() const noexcept { return gpu_id_; }
    cudaStream_t stream() const noexcept;

private:
    int gpu_id_;
    openpni::device::bdm50100_v2::BDM50100R2SArray array_;
    std::vector<std::unique_ptr<openpni::device::bdm50100_v2::BDM50100R2S>> channels_;
    DPacketsAsync d_packets_async_;
};

using R2S50100SPSCProcessor = SPSCProcessor<RawDataView, SinglesResult, R2S50100SinglesResultPolicy>;

} // namespace openpni::distributed::r2s::multi_gpu
