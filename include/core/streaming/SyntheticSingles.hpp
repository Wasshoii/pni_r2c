#pragma once

#include <pni/core/CommonDataType.hpp>

#include <cstdint>
#include <vector>

namespace openpni::distributed::streaming
{

    struct SyntheticSpec
    {
        uint32_t nodeId = 0;
        uint32_t peerNodeId = 1;
        uint16_t localChannel = 0;
        uint16_t peerChannel = 1;
        uint16_t crystalIndex = 0;
        uint64_t promptPairs = 10000;
        uint64_t delayPairs = 10000;
        uint64_t delayTimePs = 2'000'000;
        float energyEv = 511000.0F;
        uint64_t startTime_100fs = 1'000'000'000ULL;
        uint64_t spacing_100fs = 10'000ULL; // 1 ns
    };

    struct SyntheticTruth
    {
        uint64_t promptPairs = 0;
        uint64_t delayPairs = 0;
        uint64_t singlesNode0 = 0;
        uint64_t singlesNode1 = 0;
    };

    inline uint64_t delayOffset_100fs(uint64_t delayTimePs)
    {
        return delayTimePs * 10ULL;
    }

    inline SyntheticTruth syntheticTruth(const SyntheticSpec &spec)
    {
        SyntheticTruth t;
        t.promptPairs = spec.promptPairs;
        t.delayPairs = spec.delayPairs;
        t.singlesNode0 = spec.promptPairs + spec.delayPairs;
        t.singlesNode1 = spec.promptPairs + spec.delayPairs;
        return t;
    }

    /** Singles for one node. Pairing assumes the peer generates matching timestamps. */
    inline std::vector<openpni::Single> generateSyntheticSingles(const SyntheticSpec &spec)
    {
        std::vector<openpni::Single> out;
        out.resize(spec.promptPairs + spec.delayPairs);
        const uint64_t delayOff = delayOffset_100fs(spec.delayTimePs);
        const bool isPeer = spec.nodeId == spec.peerNodeId || spec.nodeId > spec.peerNodeId;
        const uint16_t channel = isPeer ? spec.peerChannel : spec.localChannel;

        size_t i = 0;
        for (uint64_t p = 0; p < spec.promptPairs; ++p)
        {
            openpni::Single s{};
            s.channelIndex = channel;
            s.crystalIndex = spec.crystalIndex;
            s.timevalue_100fs = spec.startTime_100fs + p * spec.spacing_100fs;
            s.energy_ev = spec.energyEv;
            out[i++] = s;
        }
        for (uint64_t d = 0; d < spec.delayPairs; ++d)
        {
            openpni::Single s{};
            s.channelIndex = channel;
            s.crystalIndex = spec.crystalIndex;
            uint64_t t = spec.startTime_100fs + (spec.promptPairs + d) * spec.spacing_100fs;
            if (isPeer)
            {
                t += delayOff;
            }
            s.timevalue_100fs = t;
            s.energy_ev = spec.energyEv;
            out[i++] = s;
        }
        return out;
    }

} // namespace openpni::distributed::streaming
