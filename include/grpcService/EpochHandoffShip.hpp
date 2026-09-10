#pragma once

#include "core/streaming/StreamingCoincidence.hpp"
#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "dataplane/rdma/RdmaWriteSender.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace openpni::distributed::streaming
{

    constexpr uint32_t kEpochShipDurMeta = 1;
    constexpr uint32_t kEpochShipDurCarry = 2;
    constexpr uint32_t kEpochShipDurTail = 3;

    /** Assemble durationMs 1/2/3 ship frames into an EpochHandoff. */
    class EpochShipAssembler
    {
    public:
        bool ingest(const openpni::distributed::dataplane::rdma::SlotChunkView &view);
        bool failed() const;
        bool isComplete() const;
        bool takeIfComplete(EpochHandoff *out);
        void reset();

        template <typename Rep, typename Period>
        bool waitUntilComplete(std::chrono::duration<Rep, Period> timeout)
        {
            std::unique_lock<std::mutex> lock(m_mu);
            return m_cv.wait_for(lock, timeout, [this]()
                                 { return m_failed || completeUnlocked(); });
        }

    private:
        bool completeUnlocked() const;

        mutable std::mutex m_mu;
        std::condition_variable m_cv;
        bool m_metaDone = false;
        bool m_failed = false;
        uint64_t m_epochId = 0;
        uint64_t m_cutWatermark_100fs = 0;
        uint64_t m_overlap_100fs = 0;
        uint64_t m_carryCount = 0;
        uint64_t m_tailCount = 0;
        std::vector<Single> m_carry;
        std::vector<TimestampedSingleChunk> m_tails;
        std::unordered_map<uint64_t, TimestampedSingleChunk> m_assembling;
    };

    /** Coin-A → Coin-B overlap+tail dataplane (InProcess memcpy or local RoCE). */
    bool sendEpochHandoffViaRdma(
        openpni::distributed::dataplane::rdma::RdmaWriteSender &tx,
        const EpochHandoff &handoff);

    /**
     * Handshake a dedicated ship QP (not the worker ingest path) and copy
     * `handoff` from A to B. `requireRoce` uses verbs when a device exists.
     */
    bool shipEpochHandoffViaDataplane(
        const EpochHandoff &handoff,
        EpochHandoff *out,
        bool requireRoce);

} // namespace openpni::distributed::streaming
