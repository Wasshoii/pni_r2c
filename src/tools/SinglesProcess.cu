// PnI-Config.hpp 必须在其他 PNI 头文件之前，定义 __PNI_CUDA_MACRO__ 等宏
#include <pni/PnI-Config.hpp>
#include "SinglesProcess.hpp"
#include <thrust/sort.h>
#include <thrust/execution_policy.h>
#include <thrust/device_ptr.h>
#include <thrust/remove.h>

namespace openpni::distributed::r2s
{

    struct OpSortSingles
    {
        __host__ __device__ bool operator()(
            Single const &a, Single const &b)
        {
            return a.timevalue_pico < b.timevalue_pico;
        }
    };

    struct OpEnergyOutsideWindow
    {
        float low;
        float high;

        __host__ __device__ bool operator()(Single const &s) const
        {
            const float e = s.energy;
            return !(e >= low && e <= high);
        }
    };

    void d_sortSinglesByTime_R2S(
        Single *d_singles, uint64_t singleCount)
    {
        if (singleCount == 0)
            return;

        auto ptr_begin = thrust::device_pointer_cast(d_singles);
        auto ptr_end = ptr_begin + singleCount;

        // Use thrust::sort with device policy
        // We can just use thrust::device which dispatches to CUDA
        // If we wanted to use a specific stream, we would use thrust::cuda::par.on(stream)
        // Assuming default stream is acceptable here as in typical R2S flow
        thrust::sort(thrust::device, ptr_begin, ptr_end, OpSortSingles());
    }

    uint64_t d_filterSinglesByEnergy_R2S(
        Single *d_singles,
        uint64_t singleCount,
        float low,
        float high)
    {
        if (singleCount == 0)
            return 0;

        auto ptr_begin = thrust::device_pointer_cast(d_singles);
        auto ptr_end = ptr_begin + singleCount;

        auto new_end = thrust::remove_if(
            thrust::device,
            ptr_begin,
            ptr_end,
            OpEnergyOutsideWindow{low, high});

        return static_cast<uint64_t>(new_end - ptr_begin);
    }

} // namespace openpni::experimental::node::impl
