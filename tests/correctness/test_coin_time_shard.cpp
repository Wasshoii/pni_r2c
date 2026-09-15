/**
 * Dual-aligner PET epoch handoff vs single-batch gold (9120 .lsingle).
 */

#include <pni/PnI-Config.hpp>

#include "core/streaming/StreamingCoincidence.hpp"
#include "tests/correctness/time_shard_9120.hpp"

#include <pni/io/IO.hpp>
#include <pni/io/ListmodeIO.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace openpni::distributed::streaming;
using namespace r2c_time_shard_9120;
using r2c_test_9120::k9120ChannelNum;
using r2c_test_9120::k9120CrystalsPerChannel;
namespace fs = std::filesystem;

int main()
{
    std::array<std::vector<TimestampedSingleChunk>, 2> nodeChunks;
    openpni::CoincidenceProtocol proto;
    if (!loadNodeChunks(&nodeChunks, &proto))
    {
        return 0;
    }

    const auto all = flattenSingles(nodeChunks);

    openpni::Coincidence coin;
    coin.setTotalCrystalNumOfEachChannel(
        std::vector<uint32_t>(k9120ChannelNum, k9120CrystalsPerChannel));
    openpni::tools::UniPtr<Single> devAll{"time_shard_gold"};
    devAll.CopyFromHost(std::span<const Single>(all));
    const auto gold = coin.getDListmode(std::vector<std::span<Single const>>{devAll.CudaRStdSpan()},
                                        proto, 0);
    const uint64_t goldPrompt = gold.prompt.size();
    const uint64_t goldDelay = gold.delay.size();
    std::cout << "gold prompt=" << goldPrompt << " delay=" << goldDelay << "\n";

    const std::string dirA = "/tmp/r2c_time_shard_A";
    const std::string dirB = "/tmp/r2c_time_shard_B";
    const std::string dirOne = "/tmp/r2c_time_shard_one";
    fs::remove_all(dirA);
    fs::remove_all(dirB);
    fs::remove_all(dirOne);

    auto cfgA = makeCfg(dirA, proto);
    const uint64_t W = pickCutWatermark(nodeChunks, cfgA);
    if (W == 0)
    {
        std::cerr << "could not pick cut watermark\n";
        return 1;
    }
    std::cout << "cut W=" << W << "\n";

    StreamingTimeAligner one(makeCfg(dirOne, proto), 2);
    one.start();
    if (!pushAll(&one, nodeChunks))
    {
        std::cerr << "push one failed\n";
        return 1;
    }
    one.stop(true);
    if (one.hadError())
    {
        std::cerr << "single-aligner stop path failed\n";
        return 1;
    }
    const auto &stOne = one.getStatistics();
    const uint64_t oneDelay = stOne.totalDelayPairs.load();
    const uint64_t onePrompt = stOne.totalPromptPairs.load();
    const uint64_t oneCarry = stOne.carrySinglesTotal.load();
    std::cout << "one-aligner prompt=" << onePrompt << " delay=" << oneDelay
              << " carry=" << oneCarry << "\n";
    if (oneDelay != goldDelay)
    {
        std::cerr << "single-aligner delay " << oneDelay << " != gold " << goldDelay << "\n";
        return 1;
    }
    if (onePrompt != goldPrompt)
    {
        std::cerr << "single-aligner prompt " << onePrompt << " != gold " << goldPrompt << "\n";
        return 1;
    }

    cfgA.epochId = 1;
    cfgA.coinId = 0;
    cfgA.epochT0_100fs = 0;
    cfgA.epochT1_100fs = W;
    cfgA.epochCut_100fs = W;
    StreamingTimeAligner alignerA(cfgA, 2);
    alignerA.start();
    if (!pushAll(&alignerA, nodeChunks))
    {
        std::cerr << "push A failed\n";
        return 1;
    }
    if (!waitWatermark(&alignerA, W, 60'000))
    {
        std::cerr << "watermark did not reach W (got " << alignerA.publishedWatermark() << ")\n";
        alignerA.stop(true);
        return 1;
    }
    if (!alignerA.completeEpoch(W, /*epochId=*/1))
    {
        std::cerr << "completeEpoch failed\n";
        alignerA.stop(true);
        return 1;
    }
    EpochHandoff handoff = alignerA.takeHandoff();
    alignerA.stop(true);
    if (alignerA.hadError())
    {
        std::cerr << "aligner A error\n";
        return 1;
    }
    if (W > 0 && handoff.carry.empty() && handoff.cutWatermark_100fs == 0)
    {
        std::cerr << "empty handoff\n";
        return 1;
    }
    std::cout << "handoff carry=" << handoff.carry.size() << " tailChunks=" << handoff.tail.size()
              << "\n";

    auto cfgB = makeCfg(dirB, proto);
    cfgB.coinId = 1;
    cfgB.epochId = 2;
    cfgB.epochT0_100fs = W;
    StreamingTimeAligner alignerB(cfgB, 2);
    if (!alignerB.applyHandoff(std::move(handoff)))
    {
        std::cerr << "applyHandoff failed\n";
        return 1;
    }
    if (alignerB.lastExtractedWatermark() != W)
    {
        std::cerr << "B lastWatermark " << alignerB.lastExtractedWatermark() << " != W\n";
        return 1;
    }
    alignerB.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    alignerB.stop(true);
    if (alignerB.hadError())
    {
        std::cerr << "aligner B error\n";
        return 1;
    }

    const auto &stA = alignerA.getStatistics();
    const auto &stB = alignerB.getStatistics();
    const uint64_t splitDelay = stA.totalDelayPairs.load() + stB.totalDelayPairs.load();
    const uint64_t splitPrompt = stA.totalPromptPairs.load() + stB.totalPromptPairs.load();
    const uint64_t splitCarry = stA.carrySinglesTotal.load() + stB.carrySinglesTotal.load();
    std::cout << "split prompt=" << splitPrompt << " delay=" << splitDelay
              << " carry=" << splitCarry << "\n";

    if (splitDelay != goldDelay)
    {
        std::cerr << "split delay " << splitDelay << " != gold " << goldDelay << "\n";
        return 1;
    }
    if (splitPrompt != goldPrompt)
    {
        std::cerr << "split prompt " << splitPrompt << " != gold " << goldPrompt << "\n";
        return 1;
    }

    const uint64_t lmfPrompt = mergeLmfByTime(dirA, dirB, "prompt");
    const uint64_t lmfDelay = mergeLmfByTime(dirA, dirB, "delay");
    if (lmfDelay != splitDelay)
    {
        std::cerr << "merged LMF delay count " << lmfDelay << " != stats " << splitDelay << "\n";
        return 1;
    }
    if (lmfPrompt != splitPrompt)
    {
        std::cerr << "merged LMF prompt count " << lmfPrompt << " != stats " << splitPrompt << "\n";
        return 1;
    }

    bool sawEpochName = false;
    if (fs::exists(dirA))
    {
        for (const auto &entry : fs::directory_iterator(dirA))
        {
            const auto name = entry.path().filename().string();
            if (name.find("epoch") != std::string::npos)
            {
                sawEpochName = true;
                break;
            }
        }
    }
    if (!sawEpochName)
    {
        std::cerr << "expected epoch metadata in A listmode names under " << dirA << "\n";
        return 1;
    }

    std::cout << "PASS: time-shard dual aligner (prompt/delay match gold)\n";
    return 0;
}
