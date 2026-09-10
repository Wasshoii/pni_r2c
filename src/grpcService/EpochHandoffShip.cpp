#include "grpcService/EpochHandoffShip.hpp"

#include <cstring>
#include <glog/logging.h>

namespace openpni::distributed::streaming
{
    namespace rdma = openpni::distributed::dataplane::rdma;

    namespace
    {
        constexpr uint64_t kShipMagic = 0x4550484f43485348ull; // 'EPOCHSH'

#pragma pack(push, 1)
        struct EpochShipMeta
        {
            uint64_t magic = kShipMagic;
            uint64_t epochId = 0;
            uint64_t cutWatermark_100fs = 0;
            uint64_t overlap_100fs = 0;
            uint64_t carryCount = 0;
            uint64_t tailCount = 0;
        };
#pragma pack(pop)

        static_assert(sizeof(EpochShipMeta) % 16 == 0, "meta must pack into Single records");

        uint64_t nonemptyTailCount(const EpochHandoff &handoff)
        {
            uint64_t n = 0;
            for (const auto &chunk : handoff.tail)
            {
                if (chunk.remainingCount() > 0 && chunk.remainingData())
                {
                    ++n;
                }
            }
            return n;
        }
    } // namespace

    bool EpochShipAssembler::ingest(const rdma::SlotChunkView &view)
    {
        std::lock_guard<std::mutex> lock(m_mu);
        if (view.singlesCount == 0 || !view.singlesPacked)
        {
            return true;
        }
        const auto *src = static_cast<const Single *>(view.singlesPacked);
        if (view.durationMs == kEpochShipDurMeta)
        {
            if (view.singlesCount * sizeof(Single) < sizeof(EpochShipMeta))
            {
                m_failed = true;
                m_cv.notify_all();
                return false;
            }
            EpochShipMeta meta{};
            std::memcpy(static_cast<void *>(&meta), src, sizeof(meta));
            if (meta.magic != kShipMagic)
            {
                m_failed = true;
                m_cv.notify_all();
                return false;
            }
            m_epochId = meta.epochId;
            m_cutWatermark_100fs = meta.cutWatermark_100fs;
            m_overlap_100fs = meta.overlap_100fs;
            m_carryCount = meta.carryCount;
            m_tailCount = meta.tailCount;
            m_metaDone = true;
            m_cv.notify_all();
            return true;
        }
        auto &chunk = m_assembling[view.chunkId];
        if (chunk.singles.empty())
        {
            chunk.nodeId = static_cast<uint16_t>(view.computerClockMs);
            chunk.chunkId = view.chunkId;
            chunk.duration_ms = view.durationMs;
        }
        chunk.singles.insert(chunk.singles.end(), src, src + view.singlesCount);
        const bool eof = (view.flags & rdma::kSlotFlagEof) != 0;
        if (!eof)
        {
            return true;
        }
        chunk.updateTimeRange();
        if (view.durationMs == kEpochShipDurCarry)
        {
            m_carry = std::move(chunk.singles);
        }
        else if (view.durationMs == kEpochShipDurTail)
        {
            m_tails.push_back(std::move(chunk));
        }
        m_assembling.erase(view.chunkId);
        m_cv.notify_all();
        return true;
    }

    bool EpochShipAssembler::failed() const
    {
        std::lock_guard<std::mutex> lock(m_mu);
        return m_failed;
    }

    bool EpochShipAssembler::isComplete() const
    {
        std::lock_guard<std::mutex> lock(m_mu);
        return completeUnlocked();
    }

    bool EpochShipAssembler::completeUnlocked() const
    {
        return m_metaDone && !m_failed && m_carry.size() == m_carryCount &&
               m_tails.size() >= m_tailCount;
    }

    bool EpochShipAssembler::takeIfComplete(EpochHandoff *out)
    {
        if (!out)
        {
            return false;
        }
        std::lock_guard<std::mutex> lock(m_mu);
        if (!completeUnlocked())
        {
            return false;
        }
        out->epochId = m_epochId;
        out->cutWatermark_100fs = m_cutWatermark_100fs;
        out->overlap_100fs = m_overlap_100fs;
        out->carry = std::move(m_carry);
        out->tail = std::move(m_tails);
        m_metaDone = false;
        m_failed = false;
        m_epochId = 0;
        m_cutWatermark_100fs = 0;
        m_overlap_100fs = 0;
        m_carryCount = 0;
        m_tailCount = 0;
        m_assembling.clear();
        return true;
    }

    void EpochShipAssembler::reset()
    {
        std::lock_guard<std::mutex> lock(m_mu);
        m_metaDone = false;
        m_failed = false;
        m_epochId = 0;
        m_cutWatermark_100fs = 0;
        m_overlap_100fs = 0;
        m_carryCount = 0;
        m_tailCount = 0;
        m_carry.clear();
        m_tails.clear();
        m_assembling.clear();
    }

    bool sendEpochHandoffViaRdma(rdma::RdmaWriteSender &tx, const EpochHandoff &handoff)
    {
        EpochShipMeta meta;
        meta.epochId = handoff.epochId;
        meta.cutWatermark_100fs = handoff.cutWatermark_100fs;
        meta.overlap_100fs = handoff.overlap_100fs;
        meta.carryCount = handoff.carry.size();
        meta.tailCount = nonemptyTailCount(handoff);

        const uint32_t metaSingles =
            static_cast<uint32_t>(sizeof(meta) / sizeof(Single));
        if (!tx.sendPackedSingles(0, handoff.cutWatermark_100fs, kEpochShipDurMeta, &meta, metaSingles))
        {
            LOG(ERROR) << "epoch ship: meta send failed";
            return false;
        }
        uint64_t chunkId = 1;
        if (!handoff.carry.empty())
        {
            if (!tx.sendPackedSingles(
                    chunkId++,
                    handoff.cutWatermark_100fs,
                    kEpochShipDurCarry,
                    handoff.carry.data(),
                    static_cast<uint32_t>(handoff.carry.size())))
            {
                LOG(ERROR) << "epoch ship: carry send failed";
                return false;
            }
        }
        for (const auto &chunk : handoff.tail)
        {
            const uint32_t n = static_cast<uint32_t>(chunk.remainingCount());
            const Single *src = chunk.remainingData();
            if (n == 0 || !src)
            {
                continue;
            }
            if (!tx.sendPackedSingles(
                    chunkId++,
                    chunk.nodeId,
                    kEpochShipDurTail,
                    src,
                    n))
            {
                LOG(ERROR) << "epoch ship: tail send failed";
                return false;
            }
        }
        return true;
    }

    bool shipEpochHandoffViaDataplane(const EpochHandoff &handoff, EpochHandoff *out, bool requireRoce)
    {
        if (!out)
        {
            return false;
        }
        *out = EpochHandoff{};

        rdma::RdmaRecvServer::Config scfg;
        scfg.slotCount = 16;
        scfg.slotBytes = 256 * 1024;
        scfg.preferHugePages = false;
        scfg.pollSleepUs = 5;
        scfg.startPoller = true;
        scfg.forceInProcess = !requireRoce;
        scfg.requireRoce = requireRoce;

        rdma::RdmaWriteSender::Config wcfg;
        wcfg.nodeId = 0;
        wcfg.stagingSlotBytes = scfg.slotBytes;
        wcfg.preferHugePages = false;
        wcfg.forceInProcess = !requireRoce;
        wcfg.requireRoce = requireRoce;
        wcfg.txSlotCount = 2;

        rdma::RdmaRecvServer server(scfg);
        EpochShipAssembler assembler;
        server.setIngest([&](const rdma::SlotChunkView &view) -> bool
                         { return assembler.ingest(view); });

        server.start();
        auto session = server.ensureSession(0);
        if (!session)
        {
            LOG(ERROR) << "epoch ship: ensureSession failed";
            server.stop();
            return false;
        }
        rdma::RdmaWriteSender sender(wcfg);
        rdma::RdmaEndpointInfo local{};
        if (!sender.prepareLocalEndpoint(&local))
        {
            LOG(ERROR) << "epoch ship: prepareLocalEndpoint failed";
            server.stop();
            return false;
        }
        if (requireRoce && local.kind != rdma::DataPlaneKind::RdmaRoceV2)
        {
            LOG(ERROR) << "epoch ship: requireRoce but local endpoint is not RoCE";
            server.stop();
            return false;
        }
        if (!session->acceptRemote(local))
        {
            LOG(ERROR) << "epoch ship: acceptRemote failed";
            server.stop();
            return false;
        }
        if (!sender.connect(session->localEndpoint()))
        {
            LOG(ERROR) << "epoch ship: connect failed";
            server.stop();
            return false;
        }

        const bool sent = sendEpochHandoffViaRdma(sender, handoff);
        const bool okWait = assembler.waitUntilComplete(std::chrono::seconds(30));
        sender.close();
        server.stop();
        if (!sent || !okWait || assembler.failed() || !assembler.takeIfComplete(out))
        {
            return false;
        }
        return out->cutWatermark_100fs == handoff.cutWatermark_100fs &&
               out->carry.size() == handoff.carry.size() &&
               out->tail.size() == nonemptyTailCount(handoff);
    }

} // namespace openpni::distributed::streaming
