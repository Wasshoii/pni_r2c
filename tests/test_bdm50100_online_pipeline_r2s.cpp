/**
 * @file test_bdm50100_online_pipeline_r2s.cpp
 * @brief R2S-only TU for BDM50100 online pipeline test.
 */

#include "tests/test_bdm50100_online_pipeline_r2s.hpp"

#include "core/io/IOAdapter.hpp"
#include "core/r2s/R2S.hpp"

#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>
#include <span>

namespace bdm50100_online
{
    namespace r2s = openpni::distributed::r2s;
    namespace coreio = openpni::distributed::coreio;

    namespace
    {
        void applyBdm50100R2SParams(
            r2s::R2SProcessConfig &config,
            float energyLow_eV,
            float energyHigh_eV)
        {
            config.saveData2SingleFile = false;
            config.asyncFileWrite = false;
            config.sortDataByTime = true;
            config.progressLogInterval = 0;
            config.useEnergyCut = true;
            config.energyCutLow = energyLow_eV;
            config.energyCutHigh = energyHigh_eV;
            config.crossTalkEnabled = false;
        }

        class SinglesAccumulator
        {
        public:
            bool onSingles(std::span<r2s::Single const> singles)
            {
                std::vector<r2s::Single> hostSingles;
                try
                {
                    hostSingles = r2s::materializeSinglesOnHost(singles);
                }
                catch (const std::exception &e)
                {
                    std::cerr << "[SinglesAccumulator] materialize failed: " << e.what()
                              << std::endl;
                    return false;
                }

                uint64_t segmentXor = 0;
                for (const auto &s : hostSingles)
                {
                    segmentXor ^= hashSingle(s);
                }

                std::lock_guard<std::mutex> lock(m_mutex);
                m_digest.callbacks += 1;
                m_digest.totalSingles += static_cast<uint64_t>(hostSingles.size());
                m_digest.checksumXor ^= segmentXor;
                return true;
            }

            BaselineDigest snapshot() const
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                return m_digest;
            }

        private:
            static uint64_t hashSingle(const r2s::Single &s)
            {
                uint32_t energyBits = 0;
                std::memcpy(&energyBits, &s.energy_ev, sizeof(energyBits));

                uint64_t h = 1469598103934665603ULL;
                auto mix = [&h](uint64_t v)
                {
                    h ^= v;
                    h *= 1099511628211ULL;
                };

                mix(static_cast<uint64_t>(s.channelIndex));
                mix(static_cast<uint64_t>(s.crystalIndex));
                mix(static_cast<uint64_t>(s.timevalue_100fs));
                mix(static_cast<uint64_t>(energyBits));
                return h;
            }

            mutable std::mutex m_mutex;
            BaselineDigest m_digest;
        };
    } // namespace

    std::vector<std::string> collectBdmCalibrationFiles(const std::string &caliDir)
    {
        return r2s::collectCalibrationFiles(
            caliDir,
            {".bin"},
            true,
            "bdm_",
            ".bin");
    }

    BaselineResult runOfflineBaseline(
        const std::string &rawPath,
        const std::vector<std::string> &calibrationFiles,
        uint32_t segmentLimit,
        uint64_t packetLimit,
        float energyLow_eV,
        float energyHigh_eV,
        uint32_t chunkPackets)
    {
        BaselineResult result;
        const uint32_t chunkSize = std::max<uint32_t>(1, chunkPackets);

        try
        {
            coreio::RawDataFileReader input;
            input.Open(rawPath);
            const auto &header = input.Info();

            SinglesAccumulator accumulator;

            auto config = r2s::createBDM50100Config(
                rawPath,
                "Data/result/Bdm50100/online_baseline",
                calibrationFiles,
                "baseline_unused",
                {});
            applyBdm50100R2SParams(config, energyLow_eV, energyHigh_eV);
            config.onSinglesReady = nullptr;
            config.onSinglesSpanReady = [&accumulator](
                                            std::span<r2s::Single const> singles,
                                            uint64_t,
                                            uint32_t) -> bool
            {
                return accumulator.onSingles(singles);
            };

            r2s::R2SStreamProcessor processor(config);
            if (!processor.initialize(header.channelNum))
            {
                return result;
            }

            const uint32_t targetSegments =
                std::min<uint32_t>(segmentLimit, header.segmentNum);
            uint64_t remainingPackets =
                packetLimit == 0 ? std::numeric_limits<uint64_t>::max() : packetLimit;

            for (uint32_t i = 0; i < targetSegments && remainingPackets > 0; ++i)
            {
                auto segment = input.ReadSegment(i, i + 1);
                auto full = segment.View();
                if (!full.data || !full.length || !full.offset || !full.channel || !full.count)
                {
                    continue;
                }

                for (uint64_t begin = 0; begin < full.count && remainingPackets > 0;)
                {
                    const uint64_t n = std::min<uint64_t>(
                        chunkSize,
                        std::min<uint64_t>(full.count - begin, remainingPackets));

                    openpni::RawDataView view = full;
                    view.count = n;
                    // Sub-window: reuse parent arrays with packet index offset via temporary
                    // length/channel/offset slices would require owning buffers; instead
                    // process contiguous prefix by adjusting pointers.
                    view.length = full.length + begin;
                    view.channel = full.channel + begin;
                    view.offset = full.offset + begin;
                    view.clock_ms = full.clock_ms;
                    view.duration_ms = full.duration_ms;
                    view.channelNum = full.channelNum;

                    result.processedPackets += n;
                    remainingPackets -= n;
                    begin += n;
                    if (!processor.processSegment(view))
                    {
                        return result;
                    }
                    result.processedSegments += 1;
                }
            }

            result.success = processor.finalize();
            result.digest = accumulator.snapshot();
            return result;
        }
        catch (const std::exception &e)
        {
            std::cerr << "[Baseline] exception: " << e.what() << std::endl;
            return result;
        }
    }

    struct OnlineR2SBridge::Impl
    {
        std::unique_ptr<r2s::AsyncRawDataToR2SBridge> bridge;
        std::string singlesOutputDir;
    };

    OnlineR2SBridge::OnlineR2SBridge(
        const std::vector<std::string> &calibrationFiles,
        SinglesReadyFn onSingles,
        float energyLow_eV,
        float energyHigh_eV,
        const std::string &singlesOutputDir)
        : m_impl(std::make_unique<Impl>())
    {
        m_impl->singlesOutputDir = singlesOutputDir;

        auto r2sConfig = r2s::createBDM50100Config(
            "",
            singlesOutputDir,
            calibrationFiles,
            "online_unused",
            {});
        applyBdm50100R2SParams(r2sConfig, energyLow_eV, energyHigh_eV);
        // Persist singles for offline volume/path comparison; also stream to Coin.
        r2sConfig.saveData2SingleFile = true;
        r2sConfig.asyncFileWrite = true;
        r2sConfig.onSinglesReady = std::move(onSingles);

        r2s::AsyncRawDataToR2SBridge::Config bridgeConfig;
        bridgeConfig.leaseQueueCapacity = 2;
        bridgeConfig.blockWhenQueueFull = true;
        bridgeConfig.queueFullWarnEvery = 5000;

        std::cout << "[OnlineR2SBridge] handoff=ZeroCopyLease leaseQueueCapacity="
                  << bridgeConfig.leaseQueueCapacity
                  << " singlesOut=" << singlesOutputDir << std::endl;

        m_impl->bridge = std::make_unique<r2s::AsyncRawDataToR2SBridge>(
            r2sConfig,
            bridgeConfig);
    }

    OnlineR2SBridge::~OnlineR2SBridge() = default;

    bool OnlineR2SBridge::start(uint16_t channelCount)
    {
        return m_impl->bridge->start(channelCount);
    }

    bool OnlineR2SBridge::stop()
    {
        return m_impl->bridge->stop();
    }

    BridgeStats OnlineR2SBridge::stats() const
    {
        const auto s = m_impl->bridge->stats();
        BridgeStats out;
        out.enqueuedSegments = s.enqueuedSegments;
        out.processedSegments = s.processedSegments;
        out.droppedSegments = s.droppedSegments;
        out.enqueueFullHits = s.enqueueFullHits;
        out.healthy = s.healthy;
        return out;
    }

    void OnlineR2SBridge::setReleaseFn(std::function<void(uint64_t)> releaseFn)
    {
        m_impl->bridge->setReleaseFn(std::move(releaseFn));
    }

    std::function<bool(const openpni::RawDataView &)> OnlineR2SBridge::makeRawDataCallback()
    {
        return m_impl->bridge->makeRawDataCallback();
    }

    const std::string &OnlineR2SBridge::singlesOutputDir() const
    {
        return m_impl->singlesOutputDir;
    }

} // namespace bdm50100_online
