/**
 * CPU-only checks for synthetic singles used by multi-host worker experiments.
 * No GPU / gRPC: pairing, chunk concat, and stream wrap must hold before RoCE.
 */

#include "core/streaming/SyntheticSingles.hpp"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <unordered_map>
#include <vector>

namespace streaming = openpni::distributed::streaming;

namespace
{
    bool fail(const char *msg)
    {
        std::cerr << "[FAIL] synthetic_singles: " << msg << "\n";
        return false;
    }

    bool sameEvent(const openpni::Single &a, const openpni::Single &b)
    {
        return a.channelIndex == b.channelIndex &&
               a.crystalIndex == b.crystalIndex &&
               a.timevalue_100fs == b.timevalue_100fs &&
               a.energy_ev == b.energy_ev;
    }

    bool testChunkConcatEqualsFull()
    {
        streaming::SyntheticSpec spec;
        spec.nodeId = 0;
        spec.peerNodeId = 1;
        spec.promptPairs = 100;
        spec.delayPairs = 40;
        const auto full = streaming::generateSyntheticSingles(spec);
        if (full.size() != streaming::syntheticEventCount(spec))
        {
            return fail("full size != prompt+delay");
        }

        std::vector<openpni::Single> acc;
        const size_t chunk = 7;
        for (uint64_t off = 0; off < full.size(); off += chunk)
        {
            const size_t n = std::min(chunk, full.size() - static_cast<size_t>(off));
            std::vector<openpni::Single> part;
            streaming::fillSyntheticChunk(spec, off, n, &part);
            acc.insert(acc.end(), part.begin(), part.end());
        }
        if (acc.size() != full.size())
        {
            return fail("chunk concat size mismatch");
        }
        for (size_t i = 0; i < full.size(); ++i)
        {
            if (!sameEvent(acc[i], full[i]))
            {
                return fail("chunk concat event mismatch");
            }
        }
        std::cout << "[PASS] chunk_concat_equals_full n=" << full.size() << "\n";
        return true;
    }

    bool testTwoNodePairing()
    {
        streaming::SyntheticSpec s0;
        s0.nodeId = 0;
        s0.peerNodeId = 1;
        s0.localChannel = 0;
        s0.peerChannel = 3;
        s0.promptPairs = 50;
        s0.delayPairs = 25;
        s0.delayTimePs = 2'000'000;
        streaming::SyntheticSpec s1 = s0;
        s1.nodeId = 1;

        const auto a = streaming::generateSyntheticSingles(s0);
        const auto b = streaming::generateSyntheticSingles(s1);
        if (a.size() != 75 || b.size() != 75)
        {
            return fail("pair vector size");
        }

        const uint64_t delayOff = streaming::delayOffset_100fs(s0.delayTimePs);
        std::unordered_map<uint64_t, int> node0Times;
        for (const auto &s : a)
        {
            node0Times[s.timevalue_100fs] += 1;
            if (s.channelIndex != s0.localChannel)
            {
                return fail("node0 channel");
            }
        }
        for (size_t i = 0; i < a.size(); ++i)
        {
            if (b[i].channelIndex != s0.peerChannel)
            {
                return fail("node1 channel");
            }
            if (i < s0.promptPairs)
            {
                if (a[i].timevalue_100fs != b[i].timevalue_100fs)
                {
                    return fail("prompt times must match across nodes");
                }
            }
            else
            {
                if (b[i].timevalue_100fs != a[i].timevalue_100fs + delayOff)
                {
                    return fail("delay peer offset");
                }
            }
        }

        int promptHits = 0;
        int delayHits = 0;
        for (size_t i = 0; i < b.size(); ++i)
        {
            if (i < s0.promptPairs)
            {
                promptHits += node0Times.count(b[i].timevalue_100fs) ? 1 : 0;
            }
            else if (b[i].timevalue_100fs >= delayOff)
            {
                delayHits += node0Times.count(b[i].timevalue_100fs - delayOff) ? 1 : 0;
            }
        }
        if (promptHits != static_cast<int>(s0.promptPairs) ||
            delayHits != static_cast<int>(s0.delayPairs))
        {
            std::cerr << "promptHits=" << promptHits << " delayHits=" << delayHits << "\n";
            return fail("software pairing counts");
        }
        std::cout << "[PASS] two_node_pairing prompt=" << promptHits
                  << " delay=" << delayHits << "\n";
        return true;
    }

    bool testStreamWrapMonotonic()
    {
        streaming::SyntheticSpec spec;
        spec.nodeId = 0;
        spec.peerNodeId = 1;
        spec.promptPairs = 8;
        spec.delayPairs = 4;
        const uint64_t period = streaming::syntheticEventCount(spec);

        std::vector<openpni::Single> first;
        std::vector<openpni::Single> second;
        streaming::fillSyntheticChunk(spec, 0, static_cast<size_t>(period), &first);
        streaming::fillSyntheticChunk(spec, period, static_cast<size_t>(period), &second);
        if (first.size() != period || second.size() != period)
        {
            return fail("stream wrap chunk size");
        }
        if (second.front().timevalue_100fs <= first.back().timevalue_100fs)
        {
            return fail("stream wrap time must keep increasing");
        }
        if (first[0].timevalue_100fs != spec.startTime_100fs)
        {
            return fail("first event time");
        }
        // Second cycle first event is prompt (slot 0) with index=period.
        if (second[0].timevalue_100fs !=
            spec.startTime_100fs + period * spec.spacing_100fs)
        {
            return fail("second cycle prompt time");
        }

        streaming::SyntheticSpec peer = spec;
        peer.nodeId = 1;
        std::vector<openpni::Single> peerSecond;
        streaming::fillSyntheticChunk(peer, period, static_cast<size_t>(period), &peerSecond);
        if (peerSecond[0].timevalue_100fs != second[0].timevalue_100fs)
        {
            return fail("stream wrap prompt still pairs");
        }
        const uint64_t delayOff = streaming::delayOffset_100fs(spec.delayTimePs);
        const size_t delaySlot = static_cast<size_t>(spec.promptPairs);
        if (peerSecond[delaySlot].timevalue_100fs !=
            second[delaySlot].timevalue_100fs + delayOff)
        {
            return fail("stream wrap delay still offset");
        }
        std::cout << "[PASS] stream_wrap_monotonic period=" << period << "\n";
        return true;
    }

    bool testChunkSpansPromptDelayBoundary()
    {
        streaming::SyntheticSpec spec;
        spec.nodeId = 0;
        spec.peerNodeId = 1;
        spec.promptPairs = 8;
        spec.delayPairs = 4;
        streaming::SyntheticSpec peer = spec;
        peer.nodeId = 1;

        const uint64_t start = spec.promptPairs - 2;
        const size_t count = 5;
        std::vector<openpni::Single> local;
        std::vector<openpni::Single> remote;
        streaming::fillSyntheticChunk(spec, start, count, &local);
        streaming::fillSyntheticChunk(peer, start, count, &remote);
        if (local.size() != count || remote.size() != count)
        {
            return fail("boundary chunk size");
        }
        const uint64_t delayOff = streaming::delayOffset_100fs(spec.delayTimePs);
        for (size_t i = 0; i < count; ++i)
        {
            const bool prompt = (start + i) < spec.promptPairs;
            if (prompt)
            {
                if (local[i].timevalue_100fs != remote[i].timevalue_100fs)
                {
                    return fail("boundary prompt times");
                }
            }
            else if (remote[i].timevalue_100fs != local[i].timevalue_100fs + delayOff)
            {
                return fail("boundary delay offset");
            }
        }
        std::cout << "[PASS] chunk_spans_prompt_delay_boundary\n";
        return true;
    }
} // namespace

int main()
{
    int rc = 0;
    if (!testChunkConcatEqualsFull())
        rc = 1;
    if (!testTwoNodePairing())
        rc = 1;
    if (!testStreamWrapMonotonic())
        rc = 1;
    if (!testChunkSpansPromptDelayBoundary())
        rc = 1;
    return rc;
}
