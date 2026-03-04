#include "SinglesProcess.hpp"
#include <thrust/sort.h>
#include <thrust/execution_policy.h>
#include <thrust/device_ptr.h>
#include <pni/CudaPtr.hpp>

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

} // namespace openpni::experimental::node::impl
