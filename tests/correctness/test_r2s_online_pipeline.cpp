/**
 * @file test_r2s_online_pipeline.cpp
 * @brief BDM50100 online pipeline: simulated Acq ring → R2S → Coin (generate outputs).
 *
 * Writes singles + coincidence LMFs and prints paths/volumes for external comparison.
 * No embedded baseline / correctness assertions.
 */

#include <pni/PnI-Config.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "core/io/IOAdapter.hpp"
#include "core/streaming/StreamingCoincidence.hpp"
#include "grpcNode/coinNode.hpp"
#include "grpcService/CoincidenceClient.hpp"
#include "tests/correctness/test_r2s_online_pipeline_r2s.hpp"

namespace
{
    namespace fs = std::filesystem;
    namespace grpcnode = openpni::distributed::grpcnode;
    namespace streaming = openpni::distributed::streaming;
    namespace coreio = openpni::distributed::coreio;

    constexpr float kEnergyLow_eV = 421000.0f;
    constexpr float kEnergyHigh_eV = 1000000.0f;
    constexpr int16_t kTimeWindow_ps = 2000;
    constexpr int kDelayTime_ps = 2000000;
    constexpr uint32_t kCrystalsPerChannel = 6 * 6 * 8; // 288
    constexpr size_t kRingPoolChunks = 4;               // > Bridge leaseQueueCapacity (2)

    struct ProgramOptions
    {
        std::string rawDir = "/media/lenovo/1TB/50100data/pni_res/NECR1";
        std::string caliDir = "/media/lenovo/1TB/50100data/pni_res/caliFile";
        std::string coinAddress = "127.0.0.1:50061";
        std::string coinOutputDir = "Data/result/Bdm50100/online_pipeline_coin";
        std::string singlesOutputDir = "Data/result/Bdm50100/online_pipeline_singles";
        // Print-only reference paths for external volume comparison (not loaded).
        std::string refSinglesDir;
        std::string refCoinDir;
        uint32_t maxFiles = 0;
        uint32_t maxSegments = 20;
        uint64_t maxPackets = 0; // 0 = unlimited
        uint32_t chunkPackets = 65536;
        uint32_t networkLatencyMarginMs = 100;
        uint32_t coinProcessingIntervalMs = 50;
        bool helpOnly = false;
    };

    void printHelp()
    {
        std::cout
            << "Usage: test_r2s_online_pipeline [options]\n"
            << "  Simulated Acq ring → zero-copy Bridge → R2S → Coin.\n"
            << "  Generates singles + coincidence LMFs; prints paths/volumes (no asserts).\n"
            << "  --raw-dir <dir>            NECR1 pniRaw-*.bin directory\n"
            << "  --cali-dir <dir>           Calibration directory (bdm_*.bin)\n"
            << "  --coin-address <host:port> CoinGrpcNode listen address\n"
            << "  --coin-output-dir <dir>    Coincidence LMF output directory\n"
            << "  --singles-output-dir <dir> Online R2S singles output directory\n"
            << "  --ref-singles-dir <dir>    Print-only: standalone singles dir to compare\n"
            << "  --ref-coin-dir <dir>       Print-only: standalone coin dir to compare\n"
            << "  --max-files <n>            Max raw files (default 1; 0=all)\n"
            << "  --max-segments <n>         Max file segments per file (default 20)\n"
            << "  --max-packets <n>          Max packets (default 0=all)\n"
            << "  --chunk-packets <n>       Packets per Bridge lease (default 65536)\n"
            << "  --network-latency-margin-ms <n>\n"
            << "                            Coin TimeAligner margin (default 100)\n"
            << "  --coin-processing-interval-ms <n>\n"
            << "                            Coin processing loop interval (default 50)\n"
            << "  --help / -h                Show this help\n";
    }

    bool parseArgs(int argc, char **argv, ProgramOptions &opts)
    {
        for (int i = 1; i < argc; ++i)
        {
            const std::string arg = argv[i];
            auto val = [&]() -> const char *
            {
                if (i + 1 >= argc)
                {
                    std::cerr << "Missing value for " << arg << std::endl;
                    return nullptr;
                }
                return argv[++i];
            };

            if (arg == "--help" || arg == "-h")
            {
                opts.helpOnly = true;
                return true;
            }
            if (arg == "--raw-dir")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.rawDir = v;
                continue;
            }
            if (arg == "--cali-dir")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.caliDir = v;
                continue;
            }
            if (arg == "--coin-address")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.coinAddress = v;
                continue;
            }
            if (arg == "--coin-output-dir")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.coinOutputDir = v;
                continue;
            }
            if (arg == "--singles-output-dir")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.singlesOutputDir = v;
                continue;
            }
            if (arg == "--ref-singles-dir")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.refSinglesDir = v;
                continue;
            }
            if (arg == "--ref-coin-dir")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.refCoinDir = v;
                continue;
            }
            if (arg == "--max-files")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.maxFiles = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--max-segments")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.maxSegments = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--max-packets")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.maxPackets = static_cast<uint64_t>(std::stoull(v));
                continue;
            }
            if (arg == "--chunk-packets")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.chunkPackets = std::max<uint32_t>(1, static_cast<uint32_t>(std::stoul(v)));
                continue;
            }
            if (arg == "--network-latency-margin-ms")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.networkLatencyMarginMs = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--coin-processing-interval-ms")
            {
                auto v = val();
                if (!v)
                    return false;
                opts.coinProcessingIntervalMs = static_cast<uint32_t>(std::stoul(v));
                continue;
            }

            std::cerr << "Unknown argument: " << arg << std::endl;
            return false;
        }
        return true;
    }

    struct DirVolume
    {
        uint64_t fileCount = 0;
        uint64_t totalBytes = 0;
    };

    DirVolume summarizeDir(const fs::path &dir, std::ostream &os)
    {
        DirVolume v;
        if (dir.empty() || !fs::exists(dir))
        {
            os << "  (missing) " << dir << std::endl;
            return v;
        }
        std::error_code ec;
        for (const auto &entry : fs::directory_iterator(dir, ec))
        {
            if (ec || !entry.is_regular_file())
            {
                continue;
            }
            const auto sz = entry.file_size(ec);
            if (ec)
            {
                continue;
            }
            v.fileCount += 1;
            v.totalBytes += static_cast<uint64_t>(sz);
            os << "  " << entry.path().filename().string() << "  " << sz << " bytes" << std::endl;
        }
        os << "  total: files=" << v.fileCount << " bytes=" << v.totalBytes << std::endl;
        return v;
    }

    /**
     * @brief Simulated Acq packet ring for zero-copy Bridge testing.
     *
     * Chunk pool (FIFO): fill from file → RawDataView into chunk memory → Bridge lease
     * → Release(packetCount) returns the oldest in-flight chunk to the free pool.
     */
    class SimulatedAcqRing
    {
    public:
        struct Stats
        {
            uint64_t writtenPackets = 0;
            uint64_t releasedPackets = 0;
            uint64_t stallWaits = 0;
            uint64_t leasedChunks = 0;
            uint64_t releasedChunks = 0;
            uint64_t peakInFlightChunks = 0;
            uint64_t spuriousReleases = 0;
        };

        explicit SimulatedAcqRing(size_t poolSize = kRingPoolChunks)
        {
            for (size_t i = 0; i < std::max<size_t>(2, poolSize); ++i)
            {
                free_.push_back(std::make_unique<Chunk>());
            }
        }

        /**
         * @brief Copy [begin, begin+count) packets from src into a free chunk and lease it.
         * @return RawDataView pointing into ring-owned memory (valid until matching Release).
         */
        std::optional<openpni::RawDataView> writeAndLease(
            const openpni::RawDataView &src,
            uint64_t begin,
            uint64_t count)
        {
            if (!src.data || !src.length || !src.offset || !src.channel || count == 0)
            {
                return std::nullopt;
            }
            if (begin + count > src.count)
            {
                return std::nullopt;
            }

            std::unique_ptr<Chunk> chunk;
            {
                std::unique_lock<std::mutex> lock(mu_);
                while (free_.empty() && !closed_)
                {
                    ++stats_.stallWaits;
                    cv_.wait(lock);
                }
                if (closed_ || free_.empty())
                {
                    return std::nullopt;
                }
                chunk = std::move(free_.front());
                free_.pop_front();
            }

            chunk->channelNum = src.channelNum;
            chunk->count = count;
            chunk->clock_ms = src.clock_ms;
            chunk->duration_ms = src.duration_ms;
            chunk->length.resize(count);
            chunk->channel.resize(count);
            chunk->offset.resize(count);

            uint64_t bytes = 0;
            for (uint64_t i = 0; i < count; ++i)
            {
                bytes += src.length[begin + i];
            }
            chunk->data.resize(bytes);

            uint64_t cursor = 0;
            for (uint64_t i = 0; i < count; ++i)
            {
                const uint16_t len = src.length[begin + i];
                const uint64_t off = src.offset[begin + i];
                std::memcpy(chunk->data.data() + cursor, src.data + off, len);
                chunk->length[i] = len;
                chunk->channel[i] = src.channel[begin + i];
                chunk->offset[i] = cursor;
                cursor += len;
            }

            openpni::RawDataView view{};
            view.data = chunk->data.data();
            view.length = chunk->length.data();
            view.offset = chunk->offset.data();
            view.channel = chunk->channel.data();
            view.count = chunk->count;
            view.clock_ms = chunk->clock_ms;
            view.duration_ms = chunk->duration_ms;
            view.channelNum = chunk->channelNum;

            {
                std::lock_guard<std::mutex> lock(mu_);
                stats_.writtenPackets += count;
                stats_.leasedChunks += 1;
                inFlight_.push_back(std::move(chunk));
                stats_.peakInFlightChunks =
                    std::max(stats_.peakInFlightChunks, static_cast<uint64_t>(inFlight_.size()));
            }

            return view;
        }

        void release(uint64_t packetCount)
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (inFlight_.empty())
            {
                std::cerr << "[SimulatedAcqRing] Release(" << packetCount
                          << ") with empty inFlight" << std::endl;
                ++stats_.spuriousReleases;
                return;
            }
            auto &front = inFlight_.front();
            if (front->count != packetCount)
            {
                std::cerr << "[SimulatedAcqRing] Release count mismatch: got " << packetCount
                          << " expected " << front->count << std::endl;
                ++stats_.spuriousReleases;
            }
            stats_.releasedPackets += front->count;
            stats_.releasedChunks += 1;
            free_.push_back(std::move(front));
            inFlight_.pop_front();
            cv_.notify_one();
        }

        std::function<void(uint64_t)> makeReleaseFn()
        {
            return [this](uint64_t packetCount)
            { release(packetCount); };
        }

        void close()
        {
            std::lock_guard<std::mutex> lock(mu_);
            closed_ = true;
            cv_.notify_all();
        }

        Stats stats() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return stats_;
        }

        uint64_t inFlightChunks() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return static_cast<uint64_t>(inFlight_.size());
        }

        bool drained() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return inFlight_.empty() && stats_.writtenPackets == stats_.releasedPackets;
        }

    private:
        struct Chunk
        {
            std::vector<uint8_t> data;
            std::vector<uint16_t> length;
            std::vector<uint64_t> offset;
            std::vector<uint16_t> channel;
            uint64_t count = 0;
            uint64_t clock_ms = 0;
            uint32_t duration_ms = 0;
            uint16_t channelNum = 0;
        };

        mutable std::mutex mu_;
        std::condition_variable cv_;
        std::deque<std::unique_ptr<Chunk>> free_;
        std::deque<std::unique_ptr<Chunk>> inFlight_;
        Stats stats_{};
        bool closed_ = false;
    };

    struct RawFileEntry
    {
        fs::path path;
        uint64_t clockMs = 0;
    };

    uint64_t parseRawClockMs(const std::string &filename)
    {
        // pniRaw-<clock>.bin
        constexpr std::string_view prefix = "pniRaw-";
        constexpr std::string_view suffix = ".bin";
        if (filename.size() <= prefix.size() + suffix.size())
        {
            return 0;
        }
        if (filename.compare(0, prefix.size(), prefix) != 0)
        {
            return 0;
        }
        if (filename.compare(filename.size() - suffix.size(), suffix.size(), suffix) != 0)
        {
            return 0;
        }
        try
        {
            return std::stoull(filename.substr(prefix.size(), filename.size() - prefix.size() - suffix.size()));
        }
        catch (...)
        {
            return 0;
        }
    }

    std::vector<RawFileEntry> collectRawFiles(const fs::path &rawDir)
    {
        std::vector<RawFileEntry> out;
        if (!fs::exists(rawDir))
        {
            return out;
        }
        for (const auto &entry : fs::directory_iterator(rawDir))
        {
            if (!entry.is_regular_file())
            {
                continue;
            }
            const auto name = entry.path().filename().string();
            if (name.rfind("pniRaw-", 0) != 0 || name.size() < 5 ||
                name.compare(name.size() - 4, 4, ".bin") != 0)
            {
                continue;
            }
            out.push_back(RawFileEntry{entry.path(), parseRawClockMs(name)});
        }
        std::sort(out.begin(), out.end(), [](const RawFileEntry &a, const RawFileEntry &b)
                  { return a.clockMs < b.clockMs || (a.clockMs == b.clockMs && a.path < b.path); });
        return out;
    }

    struct IngressResult
    {
        bool success = false;
        uint64_t fileSegments = 0;
        uint64_t chunksEnqueued = 0;
        uint64_t packetsWritten = 0;
    };

    IngressResult feedFilesThroughRing(
        const std::vector<RawFileEntry> &rawFiles,
        uint32_t maxSegments,
        uint64_t maxPackets,
        uint32_t chunkPackets,
        SimulatedAcqRing &ring,
        const std::function<bool(const openpni::RawDataView &)> &enqueue)
    {
        IngressResult result;
        const uint32_t chunkSize = std::max<uint32_t>(1, chunkPackets);
        uint64_t remaining =
            maxPackets == 0 ? std::numeric_limits<uint64_t>::max() : maxPackets;

        try
        {
            for (size_t fi = 0; fi < rawFiles.size() && remaining > 0; ++fi)
            {
                const auto &rawEntry = rawFiles[fi];
                std::cout << "[Online] Feeding file " << (fi + 1) << "/" << rawFiles.size()
                          << ": " << rawEntry.path.filename().string()
                          << " packet_budget=" << remaining << std::endl;

                coreio::RawDataFileReader input;
                input.Open(rawEntry.path.string());
                const auto &info = input.Info();
                const uint32_t segmentsToRead = std::min<uint32_t>(info.segmentNum, maxSegments);

                for (uint32_t seg = 0; seg < segmentsToRead && remaining > 0; ++seg)
                {
                    std::cout << "[Online] loading file-segment " << seg << "/" << segmentsToRead
                              << " ..." << std::flush;
                    const auto t0 = std::chrono::steady_clock::now();
                    auto segment = input.ReadSegment(seg, seg + 1);
                    auto src = segment.View();
                    const auto loadMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::steady_clock::now() - t0)
                                           .count();

                    if (!src.data || !src.length || !src.offset || !src.channel || !src.count)
                    {
                        std::cout << " empty (load " << loadMs << " ms)" << std::endl;
                        continue;
                    }

                    std::cout << " packets=" << src.count << " load=" << loadMs << " ms"
                              << " chunkPackets=" << chunkSize << std::endl;
                    result.fileSegments += 1;

                    for (uint64_t begin = 0; begin < src.count && remaining > 0;)
                    {
                        const uint64_t n = std::min<uint64_t>(
                            chunkSize, std::min<uint64_t>(src.count - begin, remaining));

                        auto viewOpt = ring.writeAndLease(src, begin, n);
                        if (!viewOpt)
                        {
                            std::cerr << "[Online] ring writeAndLease failed" << std::endl;
                            return result;
                        }

                        if (!enqueue(*viewOpt))
                        {
                            std::cerr << "[Online] bridge enqueue failed after packets="
                                      << result.packetsWritten << std::endl;
                            ring.release(n); // undo lease if enqueue rejected
                            return result;
                        }

                        result.packetsWritten += n;
                        result.chunksEnqueued += 1;
                        remaining -= n;
                        begin += n;

                        if (result.chunksEnqueued == 1 || result.chunksEnqueued % 8 == 0 ||
                            begin >= src.count || remaining == 0)
                        {
                            const auto rs = ring.stats();
                            std::cout << "[Online] progress packets=" << result.packetsWritten
                                      << " chunks=" << result.chunksEnqueued
                                      << " ring_inFlight=" << ring.inFlightChunks()
                                      << " written=" << rs.writtenPackets
                                      << " released=" << rs.releasedPackets
                                      << " stallWaits=" << rs.stallWaits << std::endl;
                        }
                    }
                }
            }

            result.success = result.packetsWritten > 0;
            return result;
        }
        catch (const std::exception &e)
        {
            std::cerr << "[Online] ingress exception: " << e.what() << std::endl;
            return result;
        }
    }
} // namespace

int main(int argc, char **argv)
{
    ProgramOptions opts;
    if (!parseArgs(argc, argv, opts))
    {
        printHelp();
        return 1;
    }
    if (opts.helpOnly)
    {
        printHelp();
        return 0;
    }

    std::cout << "====================================================" << std::endl;
    std::cout << "  BDM50100 Online Pipeline (generate coin / singles)" << std::endl;
    std::cout << "====================================================" << std::endl;
    std::cout << "rawDir            : " << opts.rawDir << std::endl;
    std::cout << "caliDir           : " << opts.caliDir << std::endl;
    std::cout << "coinAddress       : " << opts.coinAddress << std::endl;
    std::cout << "coinOutputDir     : " << opts.coinOutputDir << std::endl;
    std::cout << "singlesOutputDir  : " << opts.singlesOutputDir << std::endl;
    std::cout << "refSinglesDir     : " << (opts.refSinglesDir.empty() ? "(none)" : opts.refSinglesDir)
              << std::endl;
    std::cout << "refCoinDir        : " << (opts.refCoinDir.empty() ? "(none)" : opts.refCoinDir)
              << std::endl;
    std::cout << "maxFiles          : " << opts.maxFiles << std::endl;
    std::cout << "maxSegments       : " << opts.maxSegments << std::endl;
    std::cout << "maxPackets        : " << opts.maxPackets << std::endl;
    std::cout << "chunkPackets      : " << opts.chunkPackets << std::endl;
    std::cout << "netLatencyMarginMs: " << opts.networkLatencyMarginMs << std::endl;
    std::cout << "coinProcIntervalMs: " << opts.coinProcessingIntervalMs << std::endl;

    auto rawFiles = collectRawFiles(opts.rawDir);
    if (rawFiles.empty())
    {
        std::cerr << "[Online] No pniRaw-*.bin files found in: " << opts.rawDir << std::endl;
        return 1;
    }
    if (opts.maxFiles > 0 && rawFiles.size() > opts.maxFiles)
    {
        rawFiles.resize(opts.maxFiles);
    }

    const std::string firstRawPath = rawFiles.front().path.string();
    uint16_t channelCount = 0;
    uint32_t segmentNum = 0;
    try
    {
        coreio::RawDataFileReader probe;
        probe.Open(firstRawPath);
        const auto &info = probe.Info();
        channelCount = info.channelNum;
        segmentNum = info.segmentNum;
        std::cout << "[Online] First raw: " << firstRawPath
                  << " channelNum=" << channelCount
                  << " segmentNum=" << segmentNum << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << "[Online] Failed to open first raw file: " << e.what() << std::endl;
        return 1;
    }
    if (channelCount == 0)
    {
        std::cerr << "[Online] Invalid channelNum=0" << std::endl;
        return 1;
    }

    const auto calibrationFiles = bdm50100_online::collectBdmCalibrationFiles(opts.caliDir);
    if (calibrationFiles.empty())
    {
        std::cerr << "[Online] No calibration files in: " << opts.caliDir << std::endl;
        return 1;
    }
    std::cout << "[Online] Calibration files: " << calibrationFiles.size() << std::endl;

    std::error_code ec;
    fs::create_directories(opts.coinOutputDir, ec);
    if (ec)
    {
        std::cerr << "[Online] Failed to create coin output dir: " << ec.message() << std::endl;
        return 1;
    }
    fs::create_directories(opts.singlesOutputDir, ec);
    if (ec)
    {
        std::cerr << "[Online] Failed to create singles output dir: " << ec.message() << std::endl;
        return 1;
    }

    openpni::CoincidenceProtocol coinProtocol;
    coinProtocol.timeWindow_ps = kTimeWindow_ps;
    coinProtocol.delayTime_ps = kDelayTime_ps;
    coinProtocol.energyLower_eV = kEnergyLow_eV;
    coinProtocol.energyUpper_eV = kEnergyHigh_eV;

    streaming::TimeAlignerConfig alignerConfig;
    alignerConfig.outputDir = opts.coinOutputDir;
    alignerConfig.channelNum = channelCount;
    alignerConfig.crystalsPerChannel = kCrystalsPerChannel;
    alignerConfig.coinProtocol = coinProtocol;
    alignerConfig.enableMultiGpu = true;
    alignerConfig.processingIntervalMs = std::max<uint32_t>(1, opts.coinProcessingIntervalMs);
    alignerConfig.networkLatencyMargin_pico =
        static_cast<uint64_t>(opts.networkLatencyMarginMs) * 1'000'000'000ULL;
    alignerConfig.maxTotalMemoryBytes = 2ULL * 1024 * 1024 * 1024;

    grpcnode::CoinGrpcNode::InitOptions coinInit;
    coinInit.alignerConfig = alignerConfig;
    coinInit.listenAddress = opts.coinAddress;
    coinInit.expectedNodeCount = 1;
    coinInit.autoStartWhenAllRegistered = true;
    coinInit.startLeadTimeMs = 1000;
    coinInit.waitForStartDefaultTimeoutMs = 30000;
    coinInit.rejectStreamBeforeStart = true;

    grpcnode::CoinGrpcNode coinNode(coinInit);
    if (!coinNode.start())
    {
        std::cerr << "[Online] Failed to start CoinGrpcNode on " << opts.coinAddress << std::endl;
        return 1;
    }
    std::cout << "[Online] CoinGrpcNode listening on " << opts.coinAddress << std::endl;

    streaming::CoincidenceClientConfig coinClientConfig;
    coinClientConfig.serverAddress = opts.coinAddress;
    coinClientConfig.nodeId = 0;
    coinClientConfig.nodeAddress = "127.0.0.1";
    coinClientConfig.channelCount = channelCount;
    coinClientConfig.detectorType = "BDM50100";
    coinClientConfig.crystalsPerChannel = kCrystalsPerChannel;
    coinClientConfig.waitForStartSignal = true;
    coinClientConfig.waitForStartTimeoutMs = 60000;
    coinClientConfig.maxPendingChunks = 256;

    streaming::CoincidenceClient coinClient(coinClientConfig);
    if (!coinClient.start())
    {
        std::cerr << "[Online] Failed to start CoincidenceClient" << std::endl;
        coinNode.stop();
        return 1;
    }
    std::cout << "[Online] CoincidenceClient started" << std::endl;

    SimulatedAcqRing ring(kRingPoolChunks);

    bdm50100_online::OnlineR2SBridge bridge(
        calibrationFiles,
        [&coinClient](
            std::vector<openpni::Single> &&singles,
            uint64_t clockMs,
            uint32_t durationMs) -> bool
        {
            return coinClient.sendSingles(singles, clockMs, durationMs);
        },
        kEnergyLow_eV,
        kEnergyHigh_eV,
        opts.singlesOutputDir);

    bridge.setReleaseFn(ring.makeReleaseFn());

    if (!bridge.start(channelCount))
    {
        std::cerr << "[Online] Failed to start AsyncRawDataToR2SBridge" << std::endl;
        coinClient.stop();
        coinNode.stop();
        return 1;
    }

    auto enqueue = bridge.makeRawDataCallback();

    IngressResult ingress = feedFilesThroughRing(
        rawFiles,
        opts.maxSegments,
        opts.maxPackets,
        opts.chunkPackets,
        ring,
        enqueue);

    std::cout << "[Online] Ingress done success=" << (ingress.success ? "true" : "false")
              << " packets=" << ingress.packetsWritten
              << " chunks=" << ingress.chunksEnqueued
              << " fileSegments=" << ingress.fileSegments << std::endl;

    // Drain: bridge in-flight + ring releases.
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
        while (std::chrono::steady_clock::now() < deadline)
        {
            const auto bs = bridge.stats();
            const uint64_t inFlight =
                bs.enqueuedSegments >= bs.processedSegments
                    ? (bs.enqueuedSegments - bs.processedSegments)
                    : 0;
            if (inFlight == 0 && ring.drained())
            {
                break;
            }
            std::cout << "[Online] draining bridgeInFlight=" << inFlight
                      << " ringInFlight=" << ring.inFlightChunks()
                      << " enqueued=" << bs.enqueuedSegments
                      << " processed=" << bs.processedSegments << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    ring.close();
    const bool bridgeStopOk = bridge.stop();
    const auto bridgeStats = bridge.stats();
    const auto ringStats = ring.stats();

    coinClient.stop();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[Online] Stopping CoinGrpcNode (flushing aligner)..." << std::endl;
    std::cout.flush();
    coinNode.stop();
    std::cout << "[Online] CoinGrpcNode stopped" << std::endl;

    const uint64_t singlesSent = coinClient.getTotalSinglesSent();
    const auto &coinStats = coinNode.statistics();
    const uint64_t singlesReceived = coinStats.totalSinglesReceived.load();
    const uint64_t singlesProcessed = coinStats.totalSinglesProcessed.load();
    const uint64_t promptPairs = coinStats.totalPromptPairs.load();
    const uint64_t delayPairs = coinStats.totalDelayPairs.load();

    std::cout << "\n========== Pipeline Outputs ==========" << std::endl;
    std::cout << "Ingress          : success=" << (ingress.success ? "true" : "false")
              << " files=" << rawFiles.size()
              << " fileSegments=" << ingress.fileSegments
              << " chunks=" << ingress.chunksEnqueued
              << " packets=" << ingress.packetsWritten << std::endl;
    std::cout << "Ring             : written=" << ringStats.writtenPackets
              << " released=" << ringStats.releasedPackets
              << " peakInFlight=" << ringStats.peakInFlightChunks
              << " stallWaits=" << ringStats.stallWaits
              << " spuriousReleases=" << ringStats.spuriousReleases << std::endl;
    std::cout << "Bridge           : stop_ok=" << (bridgeStopOk ? "true" : "false")
              << " healthy=" << (bridgeStats.healthy ? "true" : "false")
              << " enqueued=" << bridgeStats.enqueuedSegments
              << " processed=" << bridgeStats.processedSegments
              << " dropped=" << bridgeStats.droppedSegments
              << " full_hits=" << bridgeStats.enqueueFullHits << std::endl;
    std::cout << "Singles counts   : client_sent=" << singlesSent
              << " coin_recv=" << singlesReceived
              << " coin_proc=" << singlesProcessed << std::endl;
    std::cout << "Coincidence      : promptPairs=" << promptPairs
              << " delayPairs=" << delayPairs << std::endl;

    std::cout << "\n--- Online singles dir ---\n"
              << opts.singlesOutputDir << std::endl;
    const auto onlineSinglesVol = summarizeDir(opts.singlesOutputDir, std::cout);
    std::cout << "--- Online coin dir ---\n"
              << opts.coinOutputDir << std::endl;
    const auto onlineCoinVol = summarizeDir(opts.coinOutputDir, std::cout);

    if (!opts.refSinglesDir.empty())
    {
        std::cout << "--- Ref singles dir (compare volume externally) ---\n"
                  << opts.refSinglesDir << std::endl;
        const auto refVol = summarizeDir(opts.refSinglesDir, std::cout);
        std::cout << "  volume_delta_bytes(online-ref)="
                  << static_cast<int64_t>(onlineSinglesVol.totalBytes) -
                         static_cast<int64_t>(refVol.totalBytes)
                  << std::endl;
    }
    else
    {
        std::cout << "--- Ref singles dir ---\n  (set --ref-singles-dir <standalone_singles_path>)\n";
    }

    if (!opts.refCoinDir.empty())
    {
        std::cout << "--- Ref coin dir (compare volume externally) ---\n"
                  << opts.refCoinDir << std::endl;
        const auto refVol = summarizeDir(opts.refCoinDir, std::cout);
        std::cout << "  volume_delta_bytes(online-ref)="
                  << static_cast<int64_t>(onlineCoinVol.totalBytes) -
                         static_cast<int64_t>(refVol.totalBytes)
                  << std::endl;
    }
    else
    {
        std::cout << "--- Ref coin dir ---\n  (set --ref-coin-dir <standalone_coin_path>)\n";
    }

    std::cout << "======================================\n" << std::endl;

    // Only fail on hard pipeline inability to produce outputs.
    if (!ingress.success || ingress.packetsWritten == 0)
    {
        std::cerr << "[Online] Pipeline did not ingest data" << std::endl;
        return 1;
    }
    if (!bridgeStopOk || !bridgeStats.healthy)
    {
        std::cerr << "[Online] Bridge failed" << std::endl;
        return 1;
    }

    std::cout << "[Online] DONE (outputs written; no correctness assert)" << std::endl;
    return 0;
}
