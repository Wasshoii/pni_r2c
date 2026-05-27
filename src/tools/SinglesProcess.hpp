#pragma once
#include <cstdint>
#include <pni/node/Coincidence.hpp>
#include <pni/core/CommonDataType.hpp>

namespace openpni::distributed::r2s
{
    /**
     * @brief Sort LocalSingle events by time on GPU
     *
     * @param d_singles Pointer to LocalSingle array in device memory
     * @param singleCount Number of elements
     */
    void d_sortSinglesByTime_R2S(Single *d_singles, uint64_t singleCount);

    /**
     * @brief Filter Single events by energy window on GPU (in-place compaction)
     *
     * @param d_singles Pointer to Single array in device memory
     * @param singleCount Number of elements
     * @param low Inclusive energy window lower bound
     * @param high Inclusive energy window upper bound
     * @return uint64_t Filtered element count
     */
    uint64_t d_filterSinglesByEnergy_R2S(
        Single *d_singles,
        uint64_t singleCount,
        float low,
        float high);
}
