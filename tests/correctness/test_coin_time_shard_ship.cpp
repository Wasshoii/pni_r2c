/**
 * Dual CoinGrpcNode production ship QP vs 9120 gold (GPU, not extractOnly).
 * All 1e5/node .lsingle go to A, then connectShipTo / cut / shipEpochTo.
 */

#include <pni/PnI-Config.hpp>

#include "core/streaming/StreamingCoincidence.hpp"
#include "grpcNode/coinNode.hpp"
#include "grpcService/CoincidenceClient.hpp"
#include "tests/correctness/time_shard_9120.hpp"

#include <glog/logging.h>

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
using r2c_test_9120::k9120ChannelsPerNode;
using r2c_test_9120::k9120CrystalsPerChannel;
namespace fs = std::filesystem;
namespace grpcnode = openpni::distributed::grpcnode;

namespace
{
    CoincidenceClientConfig makeClient(uint32_t nodeId,
                                       const std::string &addrA,
                                       const std::string &addrB)
    {
        CoincidenceClientConfig cc;
        cc.serverAddress = addrA;
        cc.nodeId = nodeId;
        cc.nodeAddress = "127.0.0.1";
        cc.channelCount = k9120ChannelsPerNode;
        cc.detectorType = "BDM50100_9120";
        cc.forceInProcess = true;
        cc.requireRoce = false;
        cc.waitForStartTimeoutMs = 60000;
        cc.heartbeatIntervalMs = 50;
        cc.coinId = 0;
        cc.activeCoinId = 0;
        cc.destinations = {{0, addrA}, {1, addrB}};
        return cc;
    }

    bool sendNodeChunks(CoincidenceClient *client,
                        const std::vector<TimestampedSingleChunk> &chunks)
    {
        for (const auto &chunk : chunks)
        {
            if (!client->sendSingles(chunk.singles, chunk.computerClock_ms, chunk.duration_ms))
            {
                return false;
            }
        }
        return true;
    }
} // namespace

int main(int argc, char **argv)
{
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    std::array<std::vector<TimestampedSingleChunk>, 2> nodeChunks;
    openpni::CoincidenceProtocol proto;
    if (!loadNodeChunks(&nodeChunks, &proto))
    {
        return 0;
    }

    const auto all = flattenSingles(nodeChunks);
    openpni::Coincidence goldCoin;
    goldCoin.setTotalCrystalNumOfEachChannel(
        std::vector<uint32_t>(k9120ChannelNum, k9120CrystalsPerChannel));
    openpni::tools::UniPtr<Single> devAll{"time_shard_ship_gold"};
    devAll.CopyFromHost(std::span<const Single>(all));
    const auto gold = goldCoin.getDListmode(
        std::vector<std::span<Single const>>{devAll.CudaRStdSpan()}, proto, 0);
    const uint64_t goldPrompt = gold.prompt.size();
    const uint64_t goldDelay = gold.delay.size();
    std::cout << "gold prompt=" << goldPrompt << " delay=" << goldDelay << "\n";

    const std::string dirA = "/tmp/r2c_time_shard_ship_A";
    const std::string dirB = "/tmp/r2c_time_shard_ship_B";
    fs::remove_all(dirA);
    fs::remove_all(dirB);

    auto cfgA = makeCfg(dirA, proto);
    const uint64_t W = pickCutWatermark(nodeChunks, cfgA);
    if (W == 0)
    {
        std::cerr << "could not pick cut watermark\n";
        return 1;
    }
    std::cout << "cut W=" << W << "\n";

    const std::string addrA = "127.0.0.1:51083";
    const std::string addrB = "127.0.0.1:51084";

    grpcnode::CoinGrpcNode::InitOptions initA;
    initA.alignerConfig = cfgA;
    initA.listenAddress = addrA;
    initA.expectedNodeCount = 2;
    initA.autoStartWhenAllRegistered = true;
    initA.startLeadTimeMs = 0;
    initA.forceInProcess = true;
    initA.coinId = 0;
    initA.enableTimeShard = true;
    initA.nextCoinId = 1;
    grpcnode::CoinGrpcNode::InitOptions initB = initA;
    initB.alignerConfig = makeCfg(dirB, proto);
    initB.listenAddress = addrB;
    initB.coinId = 1;
    initB.enableTimeShard = false;

    grpcnode::CoinGrpcNode coinA(initA);
    grpcnode::CoinGrpcNode coinB(initB);
    if (!coinA.start() || !coinB.start())
    {
        std::cerr << "dual CoinGrpcNode start failed\n";
        return 1;
    }

    auto c0 = makeClient(0, addrA, addrB);
    auto c1 = makeClient(1, addrA, addrB);
    CoincidenceClient client0(c0);
    CoincidenceClient client1(c1);
    std::thread t0([&] { client0.start(); });
    std::thread t1([&] { client1.start(); });
    t0.join();
    t1.join();
    if (!client0.isRunning() || !client1.isRunning())
    {
        std::cerr << "client start failed\n";
        client0.stop();
        client1.stop();
        coinA.stop();
        coinB.stop();
        return 1;
    }

    if (!sendNodeChunks(&client0, nodeChunks[0]) || !sendNodeChunks(&client1, nodeChunks[1]))
    {
        std::cerr << "send 9120 chunks to A failed\n";
        client0.stop();
        client1.stop();
        coinA.stop();
        coinB.stop();
        return 1;
    }

    if (!waitWatermark(&coinA.aligner(), W, 60'000))
    {
        std::cerr << "watermark did not reach W (got " << coinA.aligner().publishedWatermark()
                  << ")\n";
        client0.stop();
        client1.stop();
        coinA.stop();
        coinB.stop();
        return 1;
    }

    coinA.service().markNextCoinPrepared(true);
    if (!coinA.service().connectShipTo(addrB, 1))
    {
        std::cerr << "connectShipTo failed\n";
        client0.stop();
        client1.stop();
        coinA.stop();
        coinB.stop();
        return 1;
    }
    if (!coinA.service().cutEpochAtWatermark())
    {
        std::cerr << "cutEpochAtWatermark failed\n";
        client0.stop();
        client1.stop();
        coinA.stop();
        coinB.stop();
        return 1;
    }
    if (!coinA.service().shipEpochTo())
    {
        std::cerr << "shipEpochTo failed\n";
        client0.stop();
        client1.stop();
        coinA.stop();
        coinB.stop();
        return 1;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    if (coinB.aligner().isRunning())
    {
        coinB.aligner().stop(true);
    }
    client0.stop();
    client1.stop();
    coinA.stop();
    coinB.stop();

    const auto &stA = coinA.statistics();
    const auto &stB = coinB.statistics();
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

    std::cout << "PASS: time-shard production ship vs 9120 gold "
                 "(prompt/delay match gold)\n";
    return 0;
}
