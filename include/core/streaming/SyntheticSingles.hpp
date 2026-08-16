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

    inline uint64_t syntheticEventCount(const SyntheticSpec &spec)
    {
        return spec.promptPairs + spec.delayPairs;
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

    inline void fillSyntheticEvent(const SyntheticSpec &spec, uint64_t eventIndex, openpni::Single *out)
    {
        const uint64_t period = spec.promptPairs + spec.delayPairs;
        const bool isPeer = spec.nodeId == spec.peerNodeId || spec.nodeId > spec.peerNodeId;
        const uint16_t channel = isPeer ? spec.peerChannel : spec.localChannel;

        bool isPrompt = true;
        if (period > 0)
        {
            isPrompt = (eventIndex % period) < spec.promptPairs;
        }

        uint64_t t = spec.startTime_100fs + eventIndex * spec.spacing_100fs;
        if (!isPrompt && isPeer)
        {
            t += delayOffset_100fs(spec.delayTimePs);
        }

        out->channelIndex = channel;
        out->crystalIndex = spec.crystalIndex;
        out->timevalue_100fs = t;
        out->energy_ev = spec.energyEv;
    }

    /** Fill [startEventIndex, startEventIndex+count). Stream mode may pass unbounded indices. */
    inline size_t fillSyntheticChunk(
        const SyntheticSpec &spec,
        uint64_t startEventIndex,
        size_t count,
        std::vector<openpni::Single> *out)
    {
        if (!out)
        {
            return 0;
        }
        out->assign(count, openpni::Single{});
        for (size_t i = 0; i < count; ++i)
        {
            fillSyntheticEvent(spec, startEventIndex + i, &(*out)[i]);
        }
        return count;
    }

    /** Singles for one node. Pairing assumes the peer generates matching timestamps. */
    inline std::vector<openpni::Single> generateSyntheticSingles(const SyntheticSpec &spec)
    {
        std::vector<openpni::Single> out;
        fillSyntheticChunk(spec, 0, static_cast<size_t>(syntheticEventCount(spec)), &out);
        return out;
    }

} // namespace openpni::distributed::streaming
