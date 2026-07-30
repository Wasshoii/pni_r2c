/**
 * @file test_rdma_dataplane_loopback.cpp
 * @brief In-process RDMA/InProcess slot ring loopback (no IB device required).
 */

#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "dataplane/rdma/RdmaWriteSender.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include <pni/PnI-Config.hpp>
#include <pni/core/CommonDataType.hpp>

namespace rdma = openpni::distributed::dataplane::rdma;

int main()
{
    std::atomic<uint64_t> received{0};
    std::atomic<uint64_t> lastChunk{0};

    rdma::RdmaRecvServer::Config scfg;
    scfg.slotCount = 16;
    scfg.slotBytes = 256 * 1024; // 256 KiB slots for unit test
    scfg.preferHugePages = false;
    scfg.pollSleepUs = 5;

    rdma::RdmaRecvServer server(scfg);
    server.setIngest([&](const rdma::SlotChunkView &view) {
        received.fetch_add(view.singlesCount, std::memory_order_relaxed);
        lastChunk.store(view.chunkId, std::memory_order_relaxed);
        return true;
    });
    server.start();

    auto session = server.ensureSession(/*nodeId=*/0);
    if (!session)
    {
        std::cerr << "ensureSession failed\n";
        return 1;
    }
    const auto coinEp = session->localEndpoint();
    std::cout << "kind=" << static_cast<uint32_t>(coinEp.kind)
              << " slots=" << coinEp.slotCount
              << " stride=" << coinEp.slotStride
              << " handle=" << coinEp.inprocessHandle << std::endl;

    rdma::RdmaWriteSender::Config wcfg;
    wcfg.nodeId = 0;
    wcfg.stagingSlotBytes = scfg.slotBytes;
    wcfg.preferHugePages = false;
    rdma::RdmaWriteSender sender(wcfg);
    rdma::RdmaEndpointInfo local{};
    if (!sender.prepareLocalEndpoint(&local))
    {
        std::cerr << "prepareLocalEndpoint failed\n";
        return 1;
    }
    if (!sender.connect(coinEp))
    {
        std::cerr << "connect failed\n";
        return 1;
    }

    constexpr uint32_t kCount = 10000;
    std::vector<openpni::Single> singles(kCount);
    for (uint32_t i = 0; i < kCount; ++i)
    {
        singles[i].channelIndex = static_cast<uint16_t>(i % 100);
        singles[i].crystalIndex = 1;
        singles[i].timevalue_100fs = 1000ull + i;
        singles[i].energy = 511.0f;
    }

    const int rounds = 50;
    for (int r = 0; r < rounds; ++r)
    {
        if (!sender.sendPackedSingles(
                static_cast<uint64_t>(r),
                /*clock*/ 1000 + static_cast<uint64_t>(r),
                /*duration*/ 100,
                singles.data(),
                kCount))
        {
            std::cerr << "send failed at round " << r << "\n";
            return 1;
        }
    }

    const uint64_t expected = static_cast<uint64_t>(rounds) * kCount;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (received.load() < expected && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    server.stop();

    const uint64_t got = received.load();
    std::cout << "sent=" << expected << " received=" << got
              << " lastChunk=" << lastChunk.load() << std::endl;
    if (got != expected)
    {
        std::cerr << "FAIL count mismatch\n";
        return 1;
    }
    std::cout << "PASS RDMA/InProcess loopback\n";
    return 0;
}
