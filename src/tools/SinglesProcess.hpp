#pragma once
#include <cstdint>
#include <pni/experimental/node/Coincidence.hpp>

namespace openpni::distributed::r2s
{
    /**
     * @brief Sort LocalSingle events by time on GPU
     *
     * @param d_singles Pointer to LocalSingle array in device memory
     * @param singleCount Number of elements
     */
    void d_sortSinglesByTime_R2S(openpni::experimental::interface::LocalSingle *d_singles, uint64_t singleCount);
}
