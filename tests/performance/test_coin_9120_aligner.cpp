/**
 * @file test_coin_9120_aligner.cpp
 * @brief 9120 dual-node StreamingTimeAligner throughput (production path).
 *
 * Preloads .lsingle into memory, burst-pushes both node rings, reports
 * Combined / coin-kernel / align-extract / sink walls. No pass/fail gate.
 */

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <cuda_runtime.h>
#include <pni/PnI-Config.hpp>
#include <pni/io/ListmodeIO.hpp>

#include "core/streaming/PackedSingle.hpp"
#include "core/streaming/StreamingCoincidence.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"
#include "tests/correctness/data_9120_common.hpp"

namespace
{
    namespace fs = std::filesystem;
    namespace streaming = openpni::distributed::streaming;
    namespace rdma = openpni::distributed::dataplane::rdma;
    using r2c_test_9120::k9120DataRoot;
    using r2c_test_9120::k9120DelayTimePs;
    using r2c_test_9120::k9120EnergyLower_eV;
    using r2c_test_9120::k9120EnergyUpper_eV;
    using r2c_test_9120::k9120TimeWindowPs;

    constexpr uint64_t kBitsPerGibit = 1ull << 30;
    constexpr size_t kNodeCount = 2;
    const size_t kDefaultPushChunk = rdma::maxSinglesPerSlot(rdma::kDefaultSlotBytes);

    struct ProgramOptions
    {
        std::string dataRoot = k9120DataRoot;
        uint32_t maxFiles = 0;
        uint64_t maxSingles = 0;
        size_t pushChunk = kDefaultPushChunk;
        size_t maxChunks = 0;
        size_t maxSegment = 0;
        size_t minSegment = std::numeric_limits<size_t>::max(); // 未指定
        uint32_t minOverlapFactor = 0;
        double highWater = -1.0;
        int64_t maxLatencyMs = -1;
        size_t burstSkew = 0;
        size_t pipelineDepth = 0;
        bool writeLmf = true;
        bool helpOnly = false;
    };

    void printUsage(const char *argv0)
    {
        std::cerr
            << "Usage: " << argv0 << " [options]\n"
            << "  9120 dual-node coincidence throughput via StreamingTimeAligner\n"
            << "  (preload .lsingle; burst push; no pass/fail gate)\n\n"
            << "Options:\n"
            << "  --data-root <dir>        9120 root with pni_singles_node0/1 (default test_9120)\n"
            << "  --max-files <n>          Cap .lsingle files per node (0 = all)\n"
            << "  --max-singles <n>        Cap total singles across both nodes (0 = all)\n"
            << "  --push-chunk <n>         Singles per ring chunk (default one 4 MiB RDMA slot)\n"
            << "  --max-chunks <n>         Ring depth per node (0 = factory 9120, typically 1000)\n"
            << "  --max-segment <n>        Segment size cap incl. carry (0 = default 262144)\n"
            << "  --kernel-batch <n>       Deprecated alias for --max-segment\n"
            << "  --min-segment <n>        Batch-up threshold in singles (0 = no batching up)\n"
            << "  --min-overlap-factor <n> Min segment span in overlap windows (0 = default 4)\n"
            << "  --high-water <r>         Buffer occupancy ratio that forces a trigger\n"
            << "  --max-latency <ms>       Latency fallback trigger (0 = disable)\n"
            << "  --burst-skew <n>         Delay node1 by n chunks to exercise backpressure\n"
            << "  --pipeline-depth N       In-flight GPU segments (0 = derive from GPU count)\n"
            << "  --write-lmf              Write prompt/delay LMF (default; counted as sink wall)\n"
            << "  --no-write-lmf           Skip LMF write (kernel-only overlap measurement)\n"
            << "  --help                   Show this help\n";
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
            if (arg == "--data-root")
            {
                opts.dataRoot = needValue("--data-root");
                continue;
            }
            if (arg == "--max-files")
            {
                opts.maxFiles = static_cast<uint32_t>(std::stoul(needValue("--max-files")));
                continue;
            }
            if (arg == "--max-singles")
            {
                opts.maxSingles = std::stoull(needValue("--max-singles"));
                continue;
            }
            if (arg == "--push-chunk")
            {
                opts.pushChunk = static_cast<size_t>(std::stoull(needValue("--push-chunk")));
                if (opts.pushChunk == 0)
                {
                    throw std::runtime_error("--push-chunk must be > 0");
                }
                continue;
            }
            if (arg == "--max-chunks")
            {
                opts.maxChunks = static_cast<size_t>(std::stoull(needValue("--max-chunks")));
                continue;
            }
            if (arg == "--max-segment" || arg == "--kernel-batch")
            {
                opts.maxSegment = static_cast<size_t>(std::stoull(needValue(arg.c_str())));
                continue;
            }
            if (arg == "--min-segment")
            {
                opts.minSegment = static_cast<size_t>(std::stoull(needValue("--min-segment")));
                continue;
            }
            if (arg == "--min-overlap-factor")
            {
                opts.minOverlapFactor =
                    static_cast<uint32_t>(std::stoul(needValue("--min-overlap-factor")));
                continue;
            }
            if (arg == "--high-water")
            {
                opts.highWater = std::stod(needValue("--high-water"));
                continue;
            }
            if (arg == "--max-latency")
            {
                opts.maxLatencyMs = std::stoll(needValue("--max-latency"));
                continue;
            }
            if (arg == "--burst-skew")
            {
                opts.burstSkew = static_cast<size_t>(std::stoull(needValue("--burst-skew")));
                continue;
            }
            if (arg == "--pipeline-depth")
            {
                opts.pipelineDepth = static_cast<size_t>(std::stoull(needValue("--pipeline-depth")));
                continue;
            }
            if (arg == "--write-lmf")
            {
                opts.writeLmf = true;
                continue;
            }
            if (arg == "--no-write-lmf")
            {
                opts.writeLmf = false;
                continue;
            }
            throw std::runtime_error("Unknown argument: " + arg);
        }
        return true;
    }

    std::vector<std::string> collectSinglesFiles(const std::string &dir, uint32_t maxFiles)
    {
        std::vector<std::string> files;
        if (!fs::exists(dir) || !fs::is_directory(dir))
        {
            return files;
        }
        for (const auto &entry : fs::directory_iterator(dir))
        {
            if (entry.is_regular_file() && entry.path().extension() == ".lsingle")
            {
                files.push_back(entry.path().string());
            }
        }
        std::sort(files.begin(), files.end());
        if (maxFiles > 0 && files.size() > maxFiles)
        {
            files.resize(maxFiles);
        }
        return files;
    }

    std::vector<openpni::Single> readSinglesFromSegment(
        openpni::io::listmode::ListmodeFileSegment &segment)
    {
        const auto data = segment.GetHAnyData();
        if (!data.local_crystal_index1 || !data.channel_index1 || !data.absolute_timestamp1_100fs)
        {
            throw std::runtime_error("Single segment missing required fields");
        }

        std::vector<openpni::Single> singles(data.count);
        for (std::size_t i = 0; i < data.count; ++i)
        {
            singles[i].channelIndex = data.channel_index1[i];
            singles[i].crystalIndex = data.local_crystal_index1[i];
            singles[i].timevalue_100fs = data.absolute_timestamp1_100fs[i];
            singles[i].energy_ev = data.energy1 ? data.energy1[i] : 0.0f;
        }
        return singles;
    }

    bool preloadNodeChunks(
        const std::vector<std::string> &files,
        uint16_t nodeId,
        size_t pushChunk,
        uint64_t *remainingBudget,
        std::vector<streaming::TimestampedSingleChunk> *out)
    {
        out->clear();
        uint64_t nextChunkId = 0;
        for (const auto &filePath : files)
        {
            openpni::io::listmode::ListmodeFileInput inputFile;
            inputFile.Open(filePath);
            if (inputFile.Header().FileTypeName() !=
                openpni::io::listmode::fields::file_type_single_listmode)
            {
                continue;
            }

            for (uint32_t segIdx = 0; segIdx < inputFile.SegmentNum(); ++segIdx)
            {
                auto segment = inputFile.ReadSegment(segIdx);
                auto allSingles = readSinglesFromSegment(segment);
                if (allSingles.empty())
                {
                    continue;
                }

                for (size_t off = 0; off < allSingles.size(); off += pushChunk)
                {
                    if (remainingBudget != nullptr && *remainingBudget == 0)
                    {
                        return !out->empty();
                    }
                    size_t end = std::min(off + pushChunk, allSingles.size());
                    if (remainingBudget != nullptr)
                    {
                        const uint64_t room = *remainingBudget;
                        if (end - off > room)
                        {
                            end = off + static_cast<size_t>(room);
                        }
                    }

                    streaming::TimestampedSingleChunk chunk;
                    chunk.nodeId = nodeId;
                    chunk.chunkId = nextChunkId++;
                    chunk.computerClock_ms = segment.GetClockMs();
                    chunk.duration_ms = segment.GetDurationMs();
                    chunk.singles.assign(
                        allSingles.begin() + static_cast<std::ptrdiff_t>(off),
                        allSingles.begin() + static_cast<std::ptrdiff_t>(end));
                    chunk.updateTimeRange();
                    if (remainingBudget)
                    {
                        *remainingBudget -= chunk.singles.size();
                    }
                    out->push_back(std::move(chunk));
                    if (end < off + pushChunk)
                    {
                        return true;
                    }
                }
            }
        }
        return !out->empty();
    }

    void applyEnergyWindow(
        streaming::TimeAlignerConfig *cfg,
        const std::array<std::vector<streaming::TimestampedSingleChunk>, kNodeCount> &nodes)
    {
        uint64_t total = 0;
        uint64_t inEv = 0;
        uint64_t inKev = 0;
        constexpr uint64_t kProbe = 200000;
        for (const auto &chunks : nodes)
        {
            for (const auto &chunk : chunks)
            {
                for (const auto &s : chunk.singles)
                {
                    if (total >= kProbe)
                    {
                        break;
                    }
                    ++total;
                    if (s.energy_ev >= k9120EnergyLower_eV && s.energy_ev <= k9120EnergyUpper_eV)
                    {
                        ++inEv;
                    }
                    if (s.energy_ev >= k9120EnergyLower_eV / 1000.0f &&
                        s.energy_ev <= k9120EnergyUpper_eV / 1000.0f)
                    {
                        ++inKev;
                    }
                }
                if (total >= kProbe)
                {
                    break;
                }
            }
        }
        if (total > 0 && inEv == 0 && inKev > 0)
        {
            cfg->coinProtocol.energyLower_eV = k9120EnergyLower_eV / 1000.0f;
            cfg->coinProtocol.energyUpper_eV = k9120EnergyUpper_eV / 1000.0f;
            std::cout << "[Energy] Auto-switch window to keV scale: "
                      << cfg->coinProtocol.energyLower_eV << "~"
                      << cfg->coinProtocol.energyUpper_eV << '\n';
        }
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
        printUsage(argv[0]);
        return 1;
    }
    if (opts.helpOnly)
    {
        return 0;
    }

    int gpuCount = 0;
    const cudaError_t gpuSt = cudaGetDeviceCount(&gpuCount);
    if (gpuSt != cudaSuccess || gpuCount <= 0)
    {
        std::cout << "SKIP: no CUDA device\n";
        return 0;
    }

    const std::string node0Dir = opts.dataRoot + "/pni_singles_node0";
    const std::string node1Dir = opts.dataRoot + "/pni_singles_node1";
    auto files0 = collectSinglesFiles(node0Dir, opts.maxFiles);
    auto files1 = collectSinglesFiles(node1Dir, opts.maxFiles);
    if (files0.empty() || files1.empty())
    {
        std::cout << "SKIP: missing .lsingle under " << node0Dir << " or " << node1Dir << '\n';
        return 0;
    }

    std::array<std::vector<streaming::TimestampedSingleChunk>, kNodeCount> nodeChunks;
    const uint64_t perNodeBudget = opts.maxSingles == 0
                                       ? std::numeric_limits<uint64_t>::max()
                                       : std::max<uint64_t>(1, opts.maxSingles / kNodeCount);
    uint64_t budget0 = perNodeBudget;
    uint64_t budget1 = perNodeBudget;

    const auto preloadBegin = std::chrono::steady_clock::now();
    if (!preloadNodeChunks(files0, 0, opts.pushChunk, &budget0, &nodeChunks[0]) ||
        !preloadNodeChunks(files1, 1, opts.pushChunk, &budget1, &nodeChunks[1]))
    {
        std::cout << "SKIP: failed to preload singles from " << opts.dataRoot << '\n';
        return 0;
    }
    const double preloadS = std::chrono::duration<double>(
                                std::chrono::steady_clock::now() - preloadBegin)
                                .count();

    uint64_t inputSingles = 0;
    uint64_t inputChunks = 0;
    for (const auto &chunks : nodeChunks)
    {
        inputChunks += chunks.size();
        for (const auto &c : chunks)
        {
            inputSingles += c.singles.size();
        }
    }
    if (inputSingles == 0)
    {
        std::cout << "SKIP: preloaded zero singles\n";
        return 0;
    }

    openpni::CoincidenceProtocol coinProtocol;
    coinProtocol.timeWindow_ps = k9120TimeWindowPs;
    coinProtocol.delayTime_ps = k9120DelayTimePs;
    coinProtocol.energyLower_eV = k9120EnergyLower_eV;
    coinProtocol.energyUpper_eV = k9120EnergyUpper_eV;

    auto alignerConfig = streaming::createBDM50100_9120AlignerConfig(
        "/tmp/r2c_coin_9120_perf", coinProtocol);
    alignerConfig.enableMultiGpu = true;
    alignerConfig.savePrompt = opts.writeLmf;
    alignerConfig.saveDelay = opts.writeLmf;
    alignerConfig.processingIntervalMs = 0;
    alignerConfig.coinPipelineDepth = opts.pipelineDepth;
    if (opts.maxChunks > 0)
    {
        alignerConfig.maxChunksPerNode = opts.maxChunks;
    }
    if (opts.maxSegment > 0)
    {
        alignerConfig.maxSegmentSingles = opts.maxSegment;
    }
    if (opts.minSegment != std::numeric_limits<size_t>::max())
    {
        alignerConfig.minSegmentSingles = opts.minSegment;
    }
    if (opts.minOverlapFactor > 0)
    {
        alignerConfig.minSegmentOverlapFactor = opts.minOverlapFactor;
    }
    if (opts.highWater >= 0.0)
    {
        alignerConfig.bufferHighWaterRatio = opts.highWater;
    }
    if (opts.maxLatencyMs >= 0)
    {
        alignerConfig.maxProcessLatencyMs = static_cast<uint32_t>(opts.maxLatencyMs);
    }
    applyEnergyWindow(&alignerConfig, nodeChunks);

    std::cout << "===== test_coin_9120_aligner =====\n"
              << "data-root           : " << opts.dataRoot << '\n'
              << "node0 files         : " << files0.size() << '\n'
              << "node1 files         : " << files1.size() << '\n'
              << "CUDA devices        : " << gpuCount << '\n'
              << "enableMultiGpu      : " << (alignerConfig.enableMultiGpu ? "true" : "false") << '\n'
              << "write LMF           : " << (opts.writeLmf ? "true" : "false") << '\n'
              << "coinPipelineDepth   : " << alignerConfig.coinPipelineDepth
              << (alignerConfig.coinPipelineDepth == 0 ? " (derive from GPU count)" : "") << '\n'
              << "processingInterval  : " << alignerConfig.processingIntervalMs << " ms\n"
              << "push-chunk          : " << opts.pushChunk
              << " (RDMA slot max=" << kDefaultPushChunk << ")\n"
              << "maxChunksPerNode    : " << alignerConfig.maxChunksPerNode << '\n'
              << "maxSegmentSingles   : " << alignerConfig.maxSegmentSingles << '\n'
              << "minSegmentSingles   : " << alignerConfig.minSegmentSingles << '\n'
              << "minSegmentOverlapX  : " << alignerConfig.minSegmentOverlapFactor
              << " (span >= " << alignerConfig.minSegmentSpan_100fs() << " x100fs)\n"
              << "bufferHighWaterRatio: " << alignerConfig.bufferHighWaterRatio << '\n'
              << "maxProcessLatencyMs : " << alignerConfig.maxProcessLatencyMs << '\n'
              << "burst skew (chunks) : " << opts.burstSkew << '\n'
              << "preloaded chunks    : " << inputChunks << '\n'
              << "preloaded singles   : " << inputSingles << '\n'
              << std::flush;

    streaming::StreamingTimeAligner aligner(alignerConfig, kNodeCount);
    std::cout << "usingMultiGpu       : " << (aligner.usingMultiGpu() ? "true" : "false") << '\n'
              << "engine GPU count    : " << aligner.gpuCount() << '\n'
              << std::flush;

    auto *buf0 = aligner.getNodeBuffer(0);
    auto *buf1 = aligner.getNodeBuffer(1);
    if (buf0 == nullptr || buf1 == nullptr)
    {
        std::cerr << "missing node ring buffers\n";
        return 1;
    }

    const auto combinedBegin = std::chrono::steady_clock::now();
    aligner.start();

    std::atomic<bool> pushOk{true};
    std::atomic<bool> node0Done{false};
    const size_t node0ChunkCount = nodeChunks[0].size();
    std::thread t0([&] {
        for (auto &chunk : nodeChunks[0])
        {
            if (!buf0->push(std::move(chunk), 0))
            {
                pushOk.store(false);
                node0Done.store(true);
                return;
            }
        }
        node0Done.store(true);
    });
    std::thread t1([&] {
        // 让 node1 落后 node0 若干个 chunk，制造“一侧缓冲堆积、水位线推不动”的
        // 突发场景，用于走一遍压力触发与背压路径。
        // 目标不超过 node0 实际 chunk 数；node0 推完后也不再死等，避免 occupancy
        // 永远到不了 burstSkew 时把测试卡死。
        if (opts.burstSkew > 0)
        {
            const size_t want = std::min(opts.burstSkew, node0ChunkCount);
            while (pushOk.load() && !node0Done.load() && buf0->size() < want)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }
        }
        for (auto &chunk : nodeChunks[1])
        {
            if (!buf1->push(std::move(chunk), 0))
            {
                pushOk.store(false);
                return;
            }
        }
    });
    t0.join();
    t1.join();
    nodeChunks[0].clear();
    nodeChunks[1].clear();
    if (!pushOk.load())
    {
        std::cerr << "burst push failed\n";
        aligner.stop();
        return 1;
    }

    aligner.stop(true);
    const double combinedS = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() - combinedBegin)
                                 .count();
    if (aligner.hadError())
    {
        std::cerr << "StreamingTimeAligner reported a coincidence compute error\n";
        return 1;
    }

    const auto &stats = aligner.getStatistics();
    const double kernelS = static_cast<double>(stats.coinKernelNs.load()) / 1e9;
    const double extractS = static_cast<double>(stats.extractNs.load()) / 1e9;
    const double sinkS = static_cast<double>(stats.sinkNs.load()) / 1e9;
    const double drainWaitS = static_cast<double>(stats.drainWaitNs.load()) / 1e9;
    const double writeQueueWaitS = static_cast<double>(stats.writeQueueWaitNs.load()) / 1e9;
    const uint64_t prompt = stats.totalPromptPairs.load();
    const uint64_t delay = stats.totalDelayPairs.load();
    const uint64_t pairs = prompt + delay;
    const uint64_t processed = stats.totalSinglesProcessed.load();
    const uint64_t batches = stats.chunksProcessed.load();
    const uint64_t kernelBatches = stats.coinKernelBatches.load();
    const uint64_t carryTotal = stats.carrySinglesTotal.load();
    const double carryRatio =
        processed > 0 ? static_cast<double>(carryTotal) / static_cast<double>(processed) : 0.0;
    const uint64_t inputBytes = inputSingles * streaming::kPackedSingleSize;
    const long double inputGib =
        static_cast<long double>(inputBytes) * 8.0L / static_cast<long double>(kBitsPerGibit);
    const double gibCombined =
        combinedS > 0.0 ? static_cast<double>(inputGib) / combinedS : 0.0;
    const double gibKernel = kernelS > 0.0 ? static_cast<double>(inputGib) / kernelS : 0.0;
    const double singlesPerS =
        combinedS > 0.0 ? static_cast<double>(processed) / combinedS : 0.0;
    const double pairsPerS = combinedS > 0.0 ? static_cast<double>(pairs) / combinedS : 0.0;
    const double batchPerS = combinedS > 0.0 ? static_cast<double>(batches) / combinedS : 0.0;

    std::cout << std::fixed << std::setprecision(3)
              << "===== Pref-style coin summary (StreamingTimeAligner) =====\n"
              << "Preload time           : " << preloadS << " s\n"
              << "Combined wall          : " << combinedS << " s\n"
              << "Coin-kernel wall       : " << kernelS << " s\n"
              << "Align-extract wall     : " << extractS << " s\n"
              << "Drain wait             : " << drainWaitS << " s\n"
              << "Write queue wait       : " << writeQueueWaitS << " s\n"
              << "Sink wall              : " << sinkS << " s\n"
              << "Watermark extracts    : " << batches << '\n'
              << "Coin-kernel batches   : " << kernelBatches << '\n'
              << "Trigger by watermark  : " << stats.triggerByWatermark.load() << '\n'
              << "Trigger by pressure   : " << stats.triggerByPressure.load() << '\n'
              << "Trigger by deadline   : " << stats.triggerByDeadline.load() << '\n'
              << "Held by min duration  : " << stats.heldByMinDuration.load() << '\n'
              << "Watermark stalls      : " << stats.watermarkStallEvents.load() << '\n'
              << "Oversized segments    : " << stats.oversizedSegments.load() << '\n'
              << "Degraded segments     : " << stats.degradedSegments.load() << '\n'
              << "Peak node backlog     : " << stats.maxNodeBacklogChunks.load() << " chunks\n"
              << "Carry singles total   : " << carryTotal << '\n'
              << "Carry recompute ratio : " << carryRatio << '\n'
              << "Input singles          : " << inputSingles << '\n'
              << "Processed singles      : " << processed << '\n'
              << "Prompt pairs           : " << prompt << '\n'
              << "Delay pairs            : " << delay << '\n'
              << "Input bytes            : " << inputBytes << '\n'
              << "Input traffic          : " << static_cast<double>(inputGib) << " Gib\n"
              << "Combined throughput    : " << gibCombined << " Gib/s\n"
              << "Coin-kernel throughput : " << gibKernel << " Gib/s\n"
              << "Batch rate             : " << batchPerS << " batch/s\n"
              << "Singles rate           : " << singlesPerS << " singles/s\n"
              << "Pairs rate             : " << pairsPerS << " pairs/s\n";
    return 0;
}
