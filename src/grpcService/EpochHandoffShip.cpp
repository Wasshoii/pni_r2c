#include "grpcService/EpochHandoffShip.hpp"

#include "dataplane/rdma/SlotProtocol.hpp"

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <glog/logging.h>

namespace openpni::distributed::streaming
{
    namespace rdma = openpni::distributed::dataplane::rdma;

    namespace
    {
        constexpr uint64_t kShipMagic = 0x4550484f43485348ull; // 'EPOCHSH'
        constexpr uint32_t kDurMeta = 1;
        constexpr uint32_t kDurCarry = 2;
        constexpr uint32_t kDurTail = 3;

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
    } // namespace

    bool sendEpochHandoffViaRdma(rdma::RdmaWriteSender &tx, const EpochHandoff &handoff)
    {
        EpochShipMeta meta;
        meta.epochId = handoff.epochId;
        meta.cutWatermark_100fs = handoff.cutWatermark_100fs;
        meta.overlap_100fs = handoff.overlap_100fs;
        meta.carryCount = handoff.carry.size();
        meta.tailCount = handoff.tail.size();

        const uint32_t metaSingles =
            static_cast<uint32_t>(sizeof(meta) / sizeof(Single));
        if (!tx.sendPackedSingles(0, handoff.cutWatermark_100fs, kDurMeta, &meta, metaSingles))
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
                    kDurCarry,
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
                    kDurTail,
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

        std::mutex mu;
        std::condition_variable cv;
        bool metaDone = false;
        bool failed = false;
        EpochShipMeta meta{};
        std::vector<Single> carry;
        std::vector<TimestampedSingleChunk> tails;
        std::unordered_map<uint64_t, TimestampedSingleChunk> assembling;

        server.setIngest([&](const rdma::SlotChunkView &view) -> bool
                         {
            std::lock_guard<std::mutex> lock(mu);
            if (view.singlesCount == 0 || !view.singlesPacked)
            {
                return true;
            }
            const auto *src = static_cast<const Single *>(view.singlesPacked);
            if (view.durationMs == kDurMeta)
            {
                if (view.singlesCount * sizeof(Single) < sizeof(EpochShipMeta))
                {
                    failed = true;
                    cv.notify_all();
                    return false;
                }
                std::memcpy(static_cast<void *>(&meta), src, sizeof(meta));
                if (meta.magic != kShipMagic)
                {
                    failed = true;
                    cv.notify_all();
                    return false;
                }
                metaDone = true;
                cv.notify_all();
                return true;
            }
            auto &chunk = assembling[view.chunkId];
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
            if (view.durationMs == kDurCarry)
            {
                carry = std::move(chunk.singles);
            }
            else if (view.durationMs == kDurTail)
            {
                tails.push_back(std::move(chunk));
            }
            assembling.erase(view.chunkId);
            cv.notify_all();
            return true; });

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
        uint64_t expectedTails = 0;
        for (const auto &c : handoff.tail)
        {
            if (c.remainingCount() > 0)
            {
                ++expectedTails;
            }
        }
        const bool expectCarry = !handoff.carry.empty();
        {
            std::unique_lock<std::mutex> lock(mu);
            const bool ok = cv.wait_for(lock, std::chrono::seconds(8), [&]()
                                        {
                                            return failed || (metaDone && (!expectCarry || carry.size() == handoff.carry.size()) &&
                                                              tails.size() >= expectedTails);
                                        });
            sender.close();
            server.stop();
            if (!sent || !ok || failed || !metaDone)
            {
                return false;
            }
            out->epochId = meta.epochId;
            out->cutWatermark_100fs = meta.cutWatermark_100fs;
            out->overlap_100fs = meta.overlap_100fs;
            out->carry = std::move(carry);
            out->tail = std::move(tails);
        }
        return out->cutWatermark_100fs == handoff.cutWatermark_100fs &&
               out->carry.size() == handoff.carry.size() &&
               out->tail.size() == expectedTails;
    }

} // namespace openpni::distributed::streaming
