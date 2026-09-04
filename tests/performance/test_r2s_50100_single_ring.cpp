#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <cuda_runtime.h>
#include <pni/PnI-Config.hpp>
#include <pni/io/IO.hpp>

#include "core/io/IOAdapter.hpp"
#include "core/r2s/R2S.hpp"
#include "core/r2s/multi_gpu/HostCudaRegister.hpp"
#include "core/streaming/PackedSingle.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

namespace
{
    namespace fs = std::filesystem;
    namespace r2s = openpni::distributed::r2s;

    constexpr const char *kDefaultRawDir = "/media/lenovo/1TB/50100data/pni_res/NECR1";
    constexpr const char *kDefaultCaliDir = "/media/lenovo/1TB/50100data/pni_res/caliFile";
    constexpr uint64_t kBitsPerGibit = 1ull << 30;

    struct ProgramOptions
    {
        std::string rawDir = kDefaultRawDir;
        std::string caliDir = kDefaultCaliDir;
        uint32_t maxFiles = 0;
        uint32_t maxSegments = 0;
        uint32_t pipelineDepth = 2;
        bool pinnedPreload = false;
        bool helpOnly = false;
    };

    struct OwnedSegment
    {
        std::vector<uint8_t> data;
        std::vector<uint16_t> length;
        std::vector<uint64_t> offset;
        std::vector<uint16_t> channel;
        uint64_t clockMs = 0;
        uint32_t durationMs = 0;
        uint16_t channelNum = 0;
        uint64_t rawBytes = 0;

        openpni::RawDataView view()
        {
            openpni::RawDataView v{};
            v.data = data.empty() ? nullptr : data.data();
            v.length = length.empty() ? nullptr : length.data();
            v.offset = offset.empty() ? nullptr : offset.data();
            v.channel = channel.empty() ? nullptr : channel.data();
            v.count = length.size();
            v.clock_ms = clockMs;
            v.duration_ms = durationMs;
            v.channelNum = channelNum;
            return v;
        }
    };

    void printUsage(const char *argv0)
    {
        std::cerr
            << "Usage: " << argv0 << " [options]\n"
            << "  50100 single-ring (NECR1/930) R2S throughput via R2SStreamProcessor\n"
            << "  (multi-GPU processSegment; preload natural segments; no pass/fail gate)\n\n"
            << "Options:\n"
            << "  --raw-dir <dir>        Raw directory of pniRaw-*.bin (default NECR1)\n"
            << "  --cali-dir <dir>       Calibration directory of bdm_*.bin\n"
            << "  --max-files <n>        Cap raw files (0 = all)\n"
            << "  --max-segments <n>     Cap total segments after preload (0 = all)\n"
            << "  --pipeline-depth <n>   Multi-GPU in-flight segments (default 2)\n"
            << "  --pinned-preload       Pin preloaded segments (contrast only; default off)\n"
            << "  --help                 Show this help\n";
    }

    bool parseArgs(int argc, char **argv, ProgramOptions &opts)
    {
        for (int i = 1; i < argc; ++i)
        {
            const std::string arg = argv[i];
            auto needValue = [&](const char *name) -> std::string
            {
                if (i + 1 >= argc)
                {
                    throw std::runtime_error(std::string("Missing value for ") + name);
                }
                return argv[++i];
            };

            if (arg == "--help" || arg == "-h")
            {
                opts.helpOnly = true;
                printUsage(argv[0]);
                return true;
            }
            if (arg == "--raw-dir")
            {
                opts.rawDir = needValue("--raw-dir");
                continue;
            }
            if (arg == "--cali-dir")
            {
                opts.caliDir = needValue("--cali-dir");
                continue;
            }
            if (arg == "--max-files")
            {
                opts.maxFiles = static_cast<uint32_t>(std::stoul(needValue("--max-files")));
                continue;
            }
            if (arg == "--max-segments")
            {
                opts.maxSegments = static_cast<uint32_t>(std::stoul(needValue("--max-segments")));
                continue;
            }
            if (arg == "--pipeline-depth")
            {
                opts.pipelineDepth = std::max(1u, static_cast<uint32_t>(std::stoul(needValue("--pipeline-depth"))));
                continue;
            }
            if (arg == "--pinned-preload")
            {
                opts.pinnedPreload = true;
                continue;
            }

            std::cerr << "Unknown argument: " << arg << '\n';
            printUsage(argv[0]);
            return false;
        }
        return true;
    }

    int cudaDeviceCountOrSkip()
    {
        int count = 0;
        const cudaError_t err = cudaGetDeviceCount(&count);
        if (err != cudaSuccess || count <= 0)
        {
            std::cerr << "SKIP: no CUDA device available\n";
            return -1;
        }
        return count;
    }

    uint64_t dataSpanBytes(const openpni::RawDataView &view)
    {
        if (view.count == 0 || view.offset == nullptr || view.length == nullptr)
        {
            return 0;
        }

        uint64_t span = 0;
        for (uint64_t i = 0; i < view.count; ++i)
        {
            span = std::max(span, view.offset[i] + static_cast<uint64_t>(view.length[i]));
        }
        return span;
    }

    bool cloneView(const openpni::RawDataView &src, OwnedSegment &dst)
    {
        dst = {};
        dst.clockMs = src.clock_ms;
        dst.durationMs = src.duration_ms;
        dst.channelNum = src.channelNum;
        dst.rawBytes = dataSpanBytes(src);

        const auto count = static_cast<size_t>(src.count);
        if (count == 0)
        {
            return true;
        }
        if (src.data == nullptr || src.length == nullptr || src.offset == nullptr || src.channel == nullptr)
        {
            std::cerr << "Invalid RawDataView pointers (count=" << src.count << ")\n";
            return false;
        }

        dst.data.assign(src.data, src.data + dst.rawBytes);
        dst.length.assign(src.length, src.length + count);
        dst.offset.assign(src.offset, src.offset + count);
        dst.channel.assign(src.channel, src.channel + count);
        return true;
    }

    bool preloadSegments(
        const ProgramOptions &opts,
        std::vector<OwnedSegment> &out,
        uint16_t &channelNum)
    {
        out.clear();
        channelNum = 0;

        auto files = r2s::collectRawDataFiles(opts.rawDir);
        if (files.empty())
        {
            std::cerr << "SKIP: no pniRaw-*.bin under " << opts.rawDir << '\n';
            return false;
        }
        if (opts.maxFiles > 0 && files.size() > opts.maxFiles)
        {
            files.resize(opts.maxFiles);
        }

        for (const auto &entry : files)
        {
            if (opts.maxSegments > 0 && out.size() >= opts.maxSegments)
            {
                break;
            }

            openpni::distributed::coreio::RawDataFileReader reader;
            try
            {
                reader.Open(entry.path);
            }
            catch (const std::exception &e)
            {
                std::cerr << "Failed to open " << entry.path << ": " << e.what() << '\n';
                return false;
            }

            const uint16_t fileChannels = reader.Info().channelNum;
            if (channelNum == 0)
            {
                channelNum = fileChannels;
            }
            else if (fileChannels != channelNum)
            {
                std::cerr << "Channel count mismatch: " << entry.path
                          << " has " << fileChannels << ", expected " << channelNum << '\n';
                return false;
            }

            const uint32_t segmentNum = reader.SegmentNum();
            for (uint32_t seg = 0; seg < segmentNum; ++seg)
            {
                if (opts.maxSegments > 0 && out.size() >= opts.maxSegments)
                {
                    break;
                }

                const auto handle = reader.ReadSegment(seg, seg + 1);
                const auto view = handle.View();
                OwnedSegment owned;
                if (!cloneView(view, owned))
                {
                    return false;
                }
                if (owned.length.empty())
                {
                    continue;
                }
                if (opts.pinnedPreload && !owned.data.empty())
                {
                    static_cast<void>(openpni::distributed::r2s::multi_gpu::tryCudaHostRegister(
                        owned.data.data(), owned.data.size()));
                }
                out.push_back(std::move(owned));
            }
        }

        if (out.empty())
        {
            std::cerr << "SKIP: no non-empty segments preloaded from " << opts.rawDir << '\n';
            return false;
        }
        return true;
    }

    double percentileMs(std::vector<double> samples, double p)
    {
        if (samples.empty())
        {
            return 0.0;
        }
        std::sort(samples.begin(), samples.end());
        const double idx = (p / 100.0) * static_cast<double>(samples.size() - 1);
        const size_t lo = static_cast<size_t>(idx);
        const size_t hi = std::min(lo + 1, samples.size() - 1);
        const double frac = idx - static_cast<double>(lo);
        return samples[lo] * (1.0 - frac) + samples[hi] * frac;
    }
} // namespace

int main(int argc, char **argv)
{
    ProgramOptions opts;
    try
    {
        if (!parseArgs(argc, argv, opts))
        {
            return 1;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << '\n';
        return 1;
    }
    if (opts.helpOnly)
    {
        return 0;
    }

    const int gpuCount = cudaDeviceCountOrSkip();
    if (gpuCount < 0)
    {
        return 0;
    }

    if (!fs::is_directory(opts.rawDir))
    {
        std::cerr << "SKIP: raw dir missing: " << opts.rawDir << '\n';
        return 0;
    }
    if (!fs::is_directory(opts.caliDir))
    {
        std::cerr << "SKIP: calibration dir missing: " << opts.caliDir << '\n';
        return 0;
    }

    const auto caliFiles = r2s::collectCalibrationFiles(
        opts.caliDir, {".bin"}, true, "bdm_", ".bin");
    if (caliFiles.empty())
    {
        std::cerr << "SKIP: no bdm_*.bin under " << opts.caliDir << '\n';
        return 0;
    }

    std::vector<OwnedSegment> segments;
    uint16_t channelNum = 0;
    const auto preloadBegin = std::chrono::steady_clock::now();
    if (!preloadSegments(opts, segments, channelNum))
    {
        return 0;
    }
    const double preloadS = std::chrono::duration<double>(
                                std::chrono::steady_clock::now() - preloadBegin)
                                .count();

    uint64_t inputBytes = 0;
    uint64_t inputPackets = 0;
    uint64_t maxSegBytes = 0;
    for (const auto &seg : segments)
    {
        inputBytes += seg.rawBytes;
        inputPackets += seg.length.size();
        maxSegBytes = std::max(maxSegBytes, seg.rawBytes);
    }
    const long double inputGib =
        (static_cast<long double>(inputBytes) * 8.0L) / static_cast<long double>(kBitsPerGibit);
    const long double maxSegGib =
        (static_cast<long double>(maxSegBytes) * 8.0L) / static_cast<long double>(kBitsPerGibit);

    auto config = r2s::createBDM50100Config(
        segments.empty() ? std::string{} : opts.rawDir,
        "/tmp/r2c_r2s_50100_perf",
        caliFiles,
        "singles_50100_perf");
    config.enableMultiGpu = true;
    config.saveData2SingleFile = false;
    config.asyncFileWrite = false;
    config.progressLogInterval = 0;
    config.computePipelineDepth = opts.pipelineDepth;
    config.maxInputGibits = maxSegGib > 0.0L ? maxSegGib : 10.0L;

    namespace rdma = openpni::distributed::dataplane::rdma;
    const size_t maxSinglesPerTxSlot = rdma::maxSinglesPerSlot(rdma::kDefaultSlotBytes);
    constexpr size_t kTxSlotCount = 2;
    std::vector<uint8_t> txScratch(kTxSlotCount * rdma::kDefaultSlotBytes);
    static_cast<void>(openpni::distributed::r2s::multi_gpu::tryCudaHostRegister(
        txScratch.data(), txScratch.size()));
    cudaStream_t txFillStream = nullptr;
    cudaEvent_t txFillEvents[kTxSlotCount]{};
    int txFillDevice = -1;
    const auto destroyTxFill = [&]()
    {
        if (txFillDevice >= 0)
        {
            static_cast<void>(cudaSetDevice(txFillDevice));
        }
        for (auto &ev : txFillEvents)
        {
            if (ev)
            {
                static_cast<void>(cudaEventDestroy(ev));
                ev = nullptr;
            }
        }
        if (txFillStream)
        {
            static_cast<void>(cudaStreamDestroy(txFillStream));
            txFillStream = nullptr;
        }
        txFillDevice = -1;
    };
    const auto ensureTxFillStream = [&](int device) -> bool
    {
        if (device < 0)
        {
            return false;
        }
        if (txFillStream && txFillDevice == device)
        {
            return true;
        }
        destroyTxFill();
        if (cudaSetDevice(device) != cudaSuccess)
        {
            return false;
        }
        if (cudaStreamCreateWithFlags(&txFillStream, cudaStreamNonBlocking) != cudaSuccess ||
            !txFillStream)
        {
            return false;
        }
        for (size_t i = 0; i < kTxSlotCount; ++i)
        {
            if (cudaEventCreateWithFlags(&txFillEvents[i], cudaEventDisableTiming) != cudaSuccess ||
                !txFillEvents[i])
            {
                destroyTxFill();
                return false;
            }
        }
        txFillDevice = device;
        return true;
    };
    std::atomic<uint64_t> callbackSingles{0};
    std::atomic<uint64_t> callbackCount{0};
    std::atomic<uint64_t> fillNs{0};
    config.onSinglesSpanReady =
        [&](std::span<const openpni::Single> singles, uint64_t, uint32_t) -> bool
    {
        const auto fillBegin = std::chrono::steady_clock::now();
        const bool srcDevice = !singles.empty() && r2s::isDevicePointer(singles.data());
        int srcDeviceId = -1;
        if (srcDevice)
        {
            cudaPointerAttributes attr{};
            if (cudaPointerGetAttributes(&attr, singles.data()) == cudaSuccess && attr.device >= 0)
            {
                srcDeviceId = attr.device;
                const cudaError_t setErr = cudaSetDevice(srcDeviceId);
                if (setErr != cudaSuccess)
                {
                    std::cerr << "cudaSetDevice failed in TX fill: "
                              << cudaGetErrorString(setErr) << '\n';
                    return false;
                }
            }
        }

        struct PendingTxFill
        {
            size_t slot = 0;
            cudaEvent_t event = nullptr;
        };
        std::deque<PendingTxFill> pending;
        size_t offset = 0;
        size_t nextSlot = 0;
        const bool useAsync =
            srcDevice && srcDeviceId >= 0 && ensureTxFillStream(srcDeviceId);

        const auto enqueueDeviceSlice = [&]() -> bool
        {
            const size_t n = std::min(maxSinglesPerTxSlot, singles.size() - offset);
            const size_t slot = nextSlot % kTxSlotCount;
            uint8_t *dst = txScratch.data() + slot * rdma::kDefaultSlotBytes + rdma::kSlotHeaderBytes;
            const size_t bytes = n * openpni::distributed::streaming::kPackedSingleSize;
            const cudaError_t err = cudaMemcpyAsync(
                dst, singles.data() + offset, bytes, cudaMemcpyDeviceToHost, txFillStream);
            if (err != cudaSuccess)
            {
                std::cerr << "D2H into TX scratch failed: " << cudaGetErrorString(err) << '\n';
                return false;
            }
            const cudaError_t recErr = cudaEventRecord(txFillEvents[slot], txFillStream);
            if (recErr != cudaSuccess)
            {
                std::cerr << "cudaEventRecord failed in TX fill: "
                          << cudaGetErrorString(recErr) << '\n';
                return false;
            }
            pending.push_back(PendingTxFill{slot, txFillEvents[slot]});
            ++nextSlot;
            offset += n;
            return true;
        };

        if (useAsync)
        {
            while (offset < singles.size() || !pending.empty())
            {
                while (pending.size() < kTxSlotCount && offset < singles.size() &&
                       maxSinglesPerTxSlot > 0)
                {
                    if (!enqueueDeviceSlice())
                    {
                        if (txFillStream)
                        {
                            static_cast<void>(cudaStreamSynchronize(txFillStream));
                        }
                        return false;
                    }
                }
                if (!pending.empty())
                {
                    const cudaError_t waitErr = cudaEventSynchronize(pending.front().event);
                    if (waitErr != cudaSuccess)
                    {
                        std::cerr << "cudaEventSynchronize failed in TX fill: "
                                  << cudaGetErrorString(waitErr) << '\n';
                        static_cast<void>(cudaStreamSynchronize(txFillStream));
                        return false;
                    }
                    pending.pop_front();
                }
            }
        }
        else
        {
            while (offset < singles.size() && maxSinglesPerTxSlot > 0)
            {
                const size_t n = std::min(maxSinglesPerTxSlot, singles.size() - offset);
                const size_t slot = nextSlot % kTxSlotCount;
                uint8_t *dst =
                    txScratch.data() + slot * rdma::kDefaultSlotBytes + rdma::kSlotHeaderBytes;
                const size_t bytes = n * openpni::distributed::streaming::kPackedSingleSize;
                if (srcDevice)
                {
                    const cudaError_t err = cudaMemcpy(
                        dst, singles.data() + offset, bytes, cudaMemcpyDeviceToHost);
                    if (err != cudaSuccess)
                    {
                        std::cerr << "D2H into TX scratch failed: "
                                  << cudaGetErrorString(err) << '\n';
                        return false;
                    }
                }
                else
                {
                    std::memcpy(dst, singles.data() + offset, bytes);
                }
                ++nextSlot;
                offset += n;
            }
        }
        fillNs.fetch_add(
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                      std::chrono::steady_clock::now() - fillBegin)
                                      .count()),
            std::memory_order_relaxed);
        callbackCount.fetch_add(1, std::memory_order_relaxed);
        callbackSingles.fetch_add(singles.size(), std::memory_order_relaxed);
        return true;
    };

    std::cout << "===== test_r2s_50100_single_ring =====\n"
              << "raw-dir            : " << opts.rawDir << '\n'
              << "cali-dir           : " << opts.caliDir << '\n'
              << "cali files         : " << caliFiles.size() << '\n'
              << "CUDA devices       : " << gpuCount << '\n'
              << "enableMultiGpu     : " << (config.enableMultiGpu ? "true" : "false") << '\n'
              << "crossTalkEnabled   : " << (config.crossTalkEnabled ? "true" : "false") << '\n'
              << "sortDataByTime     : " << (config.sortDataByTime ? "true" : "false") << '\n'
              << "pipeline depth     : " << config.computePipelineDepth << '\n'
              << "maxInputGibits     : " << static_cast<double>(config.maxInputGibits) << '\n'
              << "pinned-preload     : " << (opts.pinnedPreload ? "true" : "false") << '\n'
              << "channelNum         : " << channelNum << '\n'
              << "preloaded segments : " << segments.size() << '\n'
              << std::flush;

    r2s::R2SStreamProcessor processor(config);
    if (!processor.initialize(channelNum))
    {
        std::cerr << "Failed to initialize R2SStreamProcessor\n";
        destroyTxFill();
        return 1;
    }

    std::vector<double> segmentMs;
    segmentMs.reserve(segments.size());
    uint64_t failedSegments = 0;

    const auto computeBegin = std::chrono::steady_clock::now();
    for (auto &seg : segments)
    {
        auto view = seg.view();
        const auto t0 = std::chrono::steady_clock::now();
        const bool ok = processor.processSegment(view);
        const double ms = std::chrono::duration<double, std::milli>(
                              std::chrono::steady_clock::now() - t0)
                              .count();
        segmentMs.push_back(ms);
        if (!ok)
        {
            ++failedSegments;
            break;
        }
    }
    const bool finalizeOk = processor.finalize();
    const double combinedS = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() - computeBegin)
                                 .count();

    if (!finalizeOk)
    {
        std::cerr << "R2SStreamProcessor::finalize failed\n";
        destroyTxFill();
        return 1;
    }
    if (failedSegments != 0)
    {
        std::cerr << "processSegment failed after " << (segmentMs.size() - 1) << " segments\n";
        destroyTxFill();
        return 1;
    }

    const double meanMs =
        segmentMs.empty()
            ? 0.0
            : std::accumulate(segmentMs.begin(), segmentMs.end(), 0.0) / static_cast<double>(segmentMs.size());
    const double p50 = percentileMs(segmentMs, 50.0);
    const double p99 = percentileMs(segmentMs, 99.0);
    const double fillS = static_cast<double>(fillNs.load()) / 1e9;
    const double r2sOnlyS = std::max(0.0, combinedS - fillS);
    const double gibCombined = combinedS > 0.0 ? static_cast<double>(inputGib) / combinedS : 0.0;
    const double gibR2s = r2sOnlyS > 0.0 ? static_cast<double>(inputGib) / r2sOnlyS : 0.0;
    const double segPerS = combinedS > 0.0 ? static_cast<double>(segments.size()) / combinedS : 0.0;
    const double singlesPerS =
        combinedS > 0.0 ? static_cast<double>(callbackSingles.load()) / combinedS : 0.0;
    const double payloadBytes =
        static_cast<double>(callbackSingles.load()) *
        static_cast<double>(openpni::distributed::streaming::kPackedSingleSize);
    const double payloadGib = payloadBytes * 8.0 / static_cast<double>(kBitsPerGibit);
    const double payloadFillGBs = fillS > 0.0 ? (payloadBytes / 1e9) / fillS : 0.0;

    std::cout << std::fixed << std::setprecision(3)
              << "===== Pref-style R2S summary (includes RDMA-like TX fill) =====\n"
              << "Preload time           : " << preloadS << " s\n"
              << "Combined wall          : " << combinedS << " s\n"
              << "TX-fill wall           : " << fillS << " s\n"
              << "R2S-only wall          : " << r2sOnlyS << " s\n"
              << "Segments               : " << segments.size() << '\n'
              << "Packets                : " << inputPackets << '\n'
              << "Input bytes            : " << inputBytes << '\n'
              << "Input traffic          : " << static_cast<double>(inputGib) << " Gib\n"
              << "Callback invocations   : " << callbackCount.load() << '\n'
              << "Singles (callback)     : " << callbackSingles.load() << '\n'
              << "Payload                : " << payloadGib << " Gib\n"
              << "TX-fill payload        : " << payloadFillGBs << " GB/s\n"
              << "Combined throughput    : " << gibCombined << " Gib/s\n"
              << "R2S-only throughput    : " << gibR2s << " Gib/s\n"
              << "Segment rate           : " << segPerS << " segment/s\n"
              << "Singles rate           : " << singlesPerS << " singles/s\n"
              << "Per-segment latency ms : mean=" << meanMs
              << " p50=" << p50 << " p99=" << p99 << '\n';
    destroyTxFill();
    return 0;
}
