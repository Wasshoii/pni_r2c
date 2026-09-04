/**
 * @file test_listmode_write.cpp
 * @brief GPU-free Listmode write-path benchmark and durability check.
 *
 * Exercises ListmodeFileWriter plus an aligner-style write queue (cap 32,
 * optional segment merge to 262144 pairs). Does not require CUDA.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <pni/io/ListmodeIO.hpp>

#include "core/io/IOAdapter.hpp"

namespace
{
    namespace fs = std::filesystem;
    namespace coreio = openpni::distributed::coreio;
    using openpni::Listmode;
    using Clock = std::chrono::steady_clock;

    constexpr size_t kSegmentPairs = 262144;
    constexpr size_t kMergeTargetPairs = 262144;
    constexpr size_t kWriteQueueCap = 32;
    constexpr unsigned kIoQueueSize = 8;
    constexpr uint64_t kBurstPairs = 47'500'000;
    constexpr uint64_t kRealishPairs = 1'500'000;
    constexpr uint64_t kNsPerS = 1'000'000'000ull;

    struct ProgramOptions
    {
        std::string outDir = "/tmp/r2c_listmode_write";
        uint64_t burstPairs = kBurstPairs;
        uint64_t realishPairs = kRealishPairs;
        bool skipBurst = false;
        bool helpOnly = false;
    };

    void printUsage(const char *argv0)
    {
        std::cerr
            << "Usage: " << argv0 << " [options]\n"
            << "  Listmode write-path test (no GPU). Writes via ListmodeFileWriter\n"
            << "  plus an aligner-style queue, then fsync and read back.\n\n"
            << "Options:\n"
            << "  --out-dir <dir>     Output directory (default /tmp/r2c_listmode_write)\n"
            << "  --burst-pairs <n>   Burst pair count (default 47500000)\n"
            << "  --realish-pairs <n> Real-ish pair count (default 1500000)\n"
            << "  --quick             Skip the 47.5M burst case\n"
            << "  --help              Show this help\n";
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
            if (arg == "--out-dir")
            {
                opts.outDir = needValue("--out-dir");
                continue;
            }
            if (arg == "--burst-pairs")
            {
                opts.burstPairs = std::stoull(needValue("--burst-pairs"));
                continue;
            }
            if (arg == "--realish-pairs")
            {
                opts.realishPairs = std::stoull(needValue("--realish-pairs"));
                continue;
            }
            if (arg == "--quick")
            {
                opts.skipBurst = true;
                continue;
            }
            throw std::runtime_error("Unknown argument: " + arg);
        }
        return true;
    }

    double nsToS(uint64_t ns)
    {
        return static_cast<double>(ns) / static_cast<double>(kNsPerS);
    }

    uint64_t nsSince(Clock::time_point t0)
    {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count());
    }

    std::vector<Listmode> makeSegment(size_t n, uint32_t seed)
    {
        std::vector<Listmode> pairs(n);
        for (size_t i = 0; i < n; ++i)
        {
            pairs[i].channelIndex1 = 1;
            pairs[i].crystalIndex1 = static_cast<uint16_t>(i);
            pairs[i].channelIndex2 = 2;
            pairs[i].crystalIndex2 = static_cast<uint16_t>(i + 1);
            pairs[i].time1_2_100fs = static_cast<int32_t>(i & 0x7fff);
            pairs[i].timestamp_100us = seed + static_cast<uint32_t>(i);
        }
        return pairs;
    }

    uint64_t countListmodes(const std::string &path)
    {
        openpni::io::listmode::ListmodeFileInput in;
        in.Open(path);
        uint64_t n = 0;
        const uint32_t segs = in.SegmentNum();
        for (uint32_t i = 0; i < segs; ++i)
        {
            auto segment = in.ReadSegment(i, i + 1 < segs ? i + 1 : uint32_t(-1));
            n += segment.GetHListmodes().size();
        }
        return n;
    }

    struct WriteTimings
    {
        uint64_t enqueueWaitNs = 0;
        uint64_t setListmodesNs = 0;
        uint64_t appendNs = 0;
        uint64_t flushNs = 0;
        uint64_t readbackNs = 0;
        uint64_t totalNs = 0;
        size_t segmentsWritten = 0;
        bool failed = false;
    };

    struct QueueItem
    {
        std::vector<Listmode> pairs;
    };

    coreio::ListmodeWriterOptions makeOpts(uint64_t reservedBytes)
    {
        coreio::ListmodeWriterOptions opts;
        opts.io.reservedBytes = reservedBytes;
        opts.io.createPathIfNotExist = true;
        opts.io.enableOverrideExistingFile = true;
        opts.io.ioQueueSize = kIoQueueSize;
        opts.io.maxFileSizeBytes = 0;
        return opts;
    }

    WriteTimings runQueuedWrite(const std::string &dir,
                                const std::string &prefix,
                                uint64_t pairCount,
                                uint64_t reservedBytes)
    {
        WriteTimings t;
        const auto totalBegin = Clock::now();
        fs::create_directories(dir);

        coreio::RollingFileWriter<coreio::ListmodeFileWriter, coreio::ListmodeWriterOptions> rolling;
        if (!rolling.Open(dir, prefix, "lmf", makeOpts(reservedBytes),
                          [](coreio::ListmodeFileWriter &w, const std::string &path)
                          { w.Open(path); }))
        {
            t.failed = true;
            t.totalNs = nsSince(totalBegin);
            return t;
        }

        std::deque<QueueItem> queue;
        std::mutex mu;
        std::condition_variable cv;
        std::atomic<bool> stop{false};
        std::atomic<bool> writerFailed{false};

        std::thread writer([&]()
                           {
                               while (true)
                               {
                                   QueueItem item;
                                   {
                                       std::unique_lock lock(mu);
                                       cv.wait(lock, [&]
                                               { return !queue.empty() || stop.load(); });
                                       if (queue.empty() && stop.load())
                                       {
                                           break;
                                       }
                                       item = std::move(queue.front());
                                       queue.pop_front();
                                       while (!queue.empty())
                                       {
                                           auto &next = queue.front();
                                           if (item.pairs.size() + next.pairs.size() > kMergeTargetPairs)
                                           {
                                               break;
                                           }
                                           item.pairs.insert(item.pairs.end(),
                                                             std::make_move_iterator(next.pairs.begin()),
                                                             std::make_move_iterator(next.pairs.end()));
                                           queue.pop_front();
                                       }
                                   }
                                   cv.notify_one();
                                   if (item.pairs.empty())
                                   {
                                       continue;
                                   }
                                   const size_t n = item.pairs.size();
                                   const auto setBegin = Clock::now();
                                   openpni::io::listmode::ListmodeFileSegment segment;
                                   segment.SetListmodes(std::move(item.pairs));
                                   t.setListmodesNs += nsSince(setBegin);

                                   auto *writerPtr = rolling.RawHandle();
                                   const auto appendBegin = Clock::now();
                                   if (writerPtr == nullptr)
                                   {
                                       writerFailed.store(true);
                                       continue;
                                   }
                                   segment.SetClockMs(0);
                                   segment.SetDurationMs(0);
                                   writerPtr->RawHandle()->AppendSegment(std::move(segment));
                                   t.appendNs += nsSince(appendBegin);
                                   ++t.segmentsWritten;
                                   if ((rolling.GetStatus() & openpni::io::IOStatus_DiskSpaceNotEnough) != 0)
                                   {
                                       writerFailed.store(true);
                                   }
                                   (void)n;
                               }
                           });

        uint64_t remaining = pairCount;
        uint32_t seed = 1;
        while (remaining > 0)
        {
            const size_t n = static_cast<size_t>(std::min<uint64_t>(kSegmentPairs, remaining));
            auto pairs = makeSegment(n, seed++);
            remaining -= n;
            const auto waitBegin = Clock::now();
            {
                std::unique_lock lock(mu);
                cv.wait(lock, [&]
                        { return queue.size() < kWriteQueueCap || stop.load(); });
                if (stop.load() && queue.size() >= kWriteQueueCap)
                {
                    t.failed = true;
                    break;
                }
                queue.push_back(QueueItem{std::move(pairs)});
            }
            t.enqueueWaitNs += nsSince(waitBegin);
            cv.notify_one();
        }

        stop.store(true);
        cv.notify_all();
        writer.join();

        const auto flushBegin = Clock::now();
        rolling.Stop();
        t.flushNs = nsSince(flushBegin);
        t.failed = t.failed || writerFailed.load() ||
                   ((rolling.GetStatus() & openpni::io::IOStatus_DiskSpaceNotEnough) != 0);

        const std::string path = dir + "/" + prefix + ".lmf";
        if (!t.failed)
        {
            const auto readBegin = Clock::now();
            const uint64_t got = countListmodes(path);
            t.readbackNs = nsSince(readBegin);
            if (got != pairCount)
            {
                std::cerr << "readback mismatch: wrote " << pairCount << " got " << got
                          << " from " << path << '\n';
                t.failed = true;
            }
        }
        t.totalNs = nsSince(totalBegin);
        return t;
    }

    bool runSuccessCase(const std::string &name, const std::string &dir,
                        uint64_t pairs)
    {
        std::error_code ec;
        fs::remove_all(dir, ec);
        const auto t = runQueuedWrite(dir, "prompt", pairs, /*reservedBytes=*/1ull << 20);
        const double bytes = static_cast<double>(pairs) * sizeof(Listmode);
        const double appendS = nsToS(t.appendNs);
        const double flushS = nsToS(t.flushNs);
        const double totalS = nsToS(t.totalNs);
        const double mib = bytes / (1024.0 * 1024.0);
        std::cout << std::fixed << std::setprecision(3)
                  << "===== " << name << " =====\n"
                  << "pairs              : " << pairs << '\n'
                  << "segments           : " << t.segmentsWritten << '\n'
                  << "enqueue_wait_s     : " << nsToS(t.enqueueWaitNs) << '\n'
                  << "set_listmodes_s    : " << nsToS(t.setListmodesNs) << '\n'
                  << "append_block_s     : " << appendS
                  << "  (listmode ring wait; packing + unimode fwrite backpressure)\n"
                  << "flush_fsync_s      : " << flushS << '\n'
                  << "readback_s         : " << nsToS(t.readbackNs) << '\n'
                  << "total_s            : " << totalS << '\n'
                  << "payload_MiB        : " << mib << '\n'
                  << "append_MBps        : " << (appendS > 0 ? (bytes / 1e6) / appendS : 0.0) << '\n'
                  << "sink_MBps          : " << (totalS > 0 ? (bytes / 1e6) / totalS : 0.0) << '\n'
                  << "failed             : " << (t.failed ? "true" : "false") << '\n';
        if (t.failed)
        {
            std::cerr << name << " FAILED\n";
            return false;
        }
        if (t.setListmodesNs + t.appendNs > 0)
        {
            const double packFrac =
                static_cast<double>(t.setListmodesNs) /
                static_cast<double>(t.setListmodesNs + t.appendNs + t.flushNs);
            const double fwriteFrac =
                static_cast<double>(t.appendNs + t.flushNs) /
                static_cast<double>(t.setListmodesNs + t.appendNs + t.flushNs);
            std::cout << "setlistmodes_frac  : " << packFrac
                      << "\nfwrite_like_frac   : " << fwriteFrac
                      << "  (append_block + flush_fsync)\n";
        }
        return true;
    }

    bool runDiskFullCase(const std::string &dir)
    {
        std::error_code ec;
        fs::remove_all(dir, ec);
        const auto t = runQueuedWrite(dir, "prompt", kSegmentPairs,
                                      std::numeric_limits<uint64_t>::max() / 2);
        std::cout << "===== disk-full =====\n"
                  << "failed             : " << (t.failed ? "true" : "false") << '\n';
        if (!t.failed)
        {
            std::cerr << "disk-full path pretended to succeed\n";
            return false;
        }
        return true;
    }
} // namespace

int main(int argc, char **argv)
{
    ProgramOptions opts;
    try
    {
        if (!parseArgs(argc, argv, opts))
        {
            return EXIT_FAILURE;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << '\n';
        printUsage(argv[0]);
        return EXIT_FAILURE;
    }
    if (opts.helpOnly)
    {
        return EXIT_SUCCESS;
    }

    bool ok = true;
    ok = runSuccessCase("real-ish", opts.outDir + "/realish", opts.realishPairs) && ok;
    if (!opts.skipBurst)
    {
        ok = runSuccessCase("burst", opts.outDir + "/burst", opts.burstPairs) && ok;
    }
    ok = runDiskFullCase(opts.outDir + "/diskfull") && ok;
    std::cout << (ok ? "PASS\n" : "FAIL\n");
    return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
