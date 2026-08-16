/**
 * @file test_rdma_dataplane_loopback.cpp
 * @brief Single-host RDMA dataplane correctness: InProcess (always) and
 *        same-process RNIC loopback (skipped if no verbs device / handshake fails).
 */

#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "dataplane/rdma/RdmaWriteSender.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <pni/PnI-Config.hpp>
#include <pni/core/CommonDataType.hpp>

namespace rdma = openpni::distributed::dataplane::rdma;

namespace
{

std::vector<openpni::Single> makeSingles(uint32_t count, uint64_t timeBase)
{
    std::vector<openpni::Single> singles(count);
    for (uint32_t i = 0; i < count; ++i)
    {
        singles[i].channelIndex = static_cast<uint16_t>(i % 100);
        singles[i].crystalIndex = 1;
        singles[i].timevalue_100fs = timeBase + i;
        singles[i].energy_ev = 511.0f;
    }
    return singles;
}

bool singlesEqual(const openpni::Single &a, const openpni::Single &b)
{
    return a.channelIndex == b.channelIndex && a.crystalIndex == b.crystalIndex &&
           a.timevalue_100fs == b.timevalue_100fs && a.energy_ev == b.energy_ev;
}

struct Loopback
{
    rdma::RdmaRecvServer::Config scfg;
    rdma::RdmaWriteSender::Config wcfg;
    std::unique_ptr<rdma::RdmaRecvServer> server;
    std::shared_ptr<rdma::RdmaNodeRecvSession> session;
    std::unique_ptr<rdma::RdmaWriteSender> sender;

    bool setup(bool requireRoce, uint32_t slotCount, size_t slotBytes, bool startPoller)
    {
        scfg = {};
        scfg.slotCount = slotCount;
        scfg.slotBytes = slotBytes;
        scfg.preferHugePages = false;
        scfg.pollSleepUs = 5;
        scfg.startPoller = startPoller;
        scfg.forceInProcess = !requireRoce;
        scfg.requireRoce = requireRoce;

        wcfg = {};
        wcfg.nodeId = 0;
        wcfg.stagingSlotBytes = slotBytes;
        wcfg.preferHugePages = false;
        wcfg.forceInProcess = !requireRoce;
        wcfg.requireRoce = requireRoce;
        wcfg.txSlotCount = 4;

        server = std::make_unique<rdma::RdmaRecvServer>(scfg);
        return true;
    }

    bool handshake()
    {
        session = server->ensureSession(0);
        if (!session)
        {
            std::cerr << "ensureSession failed\n";
            return false;
        }
        sender = std::make_unique<rdma::RdmaWriteSender>(wcfg);
        rdma::RdmaEndpointInfo local{};
        if (!sender->prepareLocalEndpoint(&local))
        {
            std::cerr << "prepareLocalEndpoint failed\n";
            return false;
        }
        if (wcfg.requireRoce && local.kind != rdma::DataPlaneKind::RdmaRoceV2)
        {
            std::cerr << "requireRoce but local kind is not RoCE\n";
            return false;
        }
        if (!session->acceptRemote(local))
        {
            std::cerr << "acceptRemote failed\n";
            return false;
        }
        const auto coinEp = session->localEndpoint();
        if (!sender->connect(coinEp))
        {
            std::cerr << "connect failed\n";
            return false;
        }
        return true;
    }

    void stop()
    {
        if (sender)
        {
            sender->close();
        }
        if (server)
        {
            server->stop();
        }
    }
};

bool waitReceived(const std::atomic<uint64_t> &got, uint64_t expected, int timeoutMs)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (got.load() < expected && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return got.load() >= expected;
}

bool testGolden(bool requireRoce)
{
    Loopback lb;
    if (!lb.setup(requireRoce, 16, 256 * 1024, true))
    {
        return false;
    }

    std::mutex mu;
    std::vector<openpni::Single> received;
    lb.server->setIngest([&](const rdma::SlotChunkView &view)
                         {
        std::lock_guard<std::mutex> lock(mu);
        const auto *src = static_cast<const openpni::Single *>(view.singlesPacked);
        received.insert(received.end(), src, src + view.singlesCount);
        return true; });
    lb.server->start();
    if (!lb.handshake())
    {
        lb.stop();
        return false;
    }

    auto sent = makeSingles(10000, 1000);
    const int rounds = 8;
    for (int r = 0; r < rounds; ++r)
    {
        if (!lb.sender->sendPackedSingles(static_cast<uint64_t>(r), 1000 + r, 100,
                                          sent.data(), static_cast<uint32_t>(sent.size())))
        {
            std::cerr << "golden send failed round=" << r << "\n";
            lb.stop();
            return false;
        }
    }

    const uint64_t expected = static_cast<uint64_t>(rounds) * sent.size();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (true)
    {
        size_t n = 0;
        {
            std::lock_guard<std::mutex> lock(mu);
            n = received.size();
        }
        if (n >= expected)
        {
            break;
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    lb.stop();
    std::lock_guard<std::mutex> lock(mu);
    if (received.size() != expected)
    {
        std::cerr << "golden count got=" << received.size() << " expected=" << expected << "\n";
        return false;
    }
    for (int r = 0; r < rounds; ++r)
    {
        for (size_t i = 0; i < sent.size(); ++i)
        {
            const auto &g = received[static_cast<size_t>(r) * sent.size() + i];
            if (!singlesEqual(g, sent[i]))
            {
                std::cerr << "golden payload mismatch round=" << r << " i=" << i << "\n";
                return false;
            }
        }
    }
    return true;
}

bool testCreditWrap(bool requireRoce)
{
    Loopback lb;
    constexpr uint32_t kSlots = 8;
    if (!lb.setup(requireRoce, kSlots, 64 * 1024, true))
    {
        return false;
    }
    std::atomic<uint64_t> received{0};
    lb.server->setIngest([&](const rdma::SlotChunkView &view)
                         {
        received.fetch_add(view.singlesCount);
        return true; });
    lb.server->start();
    if (!lb.handshake())
    {
        lb.stop();
        return false;
    }

    auto sent = makeSingles(32, 2000);
    const int rounds = static_cast<int>(kSlots * 3);
    for (int r = 0; r < rounds; ++r)
    {
        if (!lb.sender->sendPackedSingles(static_cast<uint64_t>(r), 1, 1,
                                          sent.data(), static_cast<uint32_t>(sent.size())))
        {
            std::cerr << "wrap send failed r=" << r << "\n";
            lb.stop();
            return false;
        }
    }
    const uint64_t expected = static_cast<uint64_t>(rounds) * sent.size();
    const bool ok = waitReceived(received, expected, 8000);
    lb.stop();
    if (!ok)
    {
        std::cerr << "wrap count got=" << received.load() << " expected=" << expected << "\n";
        return false;
    }
    return true;
}

bool testBackpressure(bool requireRoce)
{
    Loopback lb;
    constexpr uint32_t kSlots = 4;
    if (!lb.setup(requireRoce, kSlots, 64 * 1024, true))
    {
        return false;
    }
    std::atomic<uint64_t> received{0};
    lb.server->setIngest([&](const rdma::SlotChunkView &view)
                         {
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
        received.fetch_add(view.singlesCount);
        return true; });
    lb.server->start();
    if (!lb.handshake())
    {
        lb.stop();
        return false;
    }

    auto sent = makeSingles(16, 3000);
    const int rounds = static_cast<int>(kSlots * 2 + 2);
    for (int r = 0; r < rounds; ++r)
    {
        if (!lb.sender->sendPackedSingles(static_cast<uint64_t>(r), 1, 1,
                                          sent.data(), static_cast<uint32_t>(sent.size())))
        {
            std::cerr << "backpressure send failed r=" << r << "\n";
            lb.stop();
            return false;
        }
    }
    const uint64_t expected = static_cast<uint64_t>(rounds) * sent.size();
    const bool ok = waitReceived(received, expected, 15000);
    lb.stop();
    if (!ok)
    {
        std::cerr << "backpressure count got=" << received.load() << " expected=" << expected << "\n";
        return false;
    }
    return true;
}

bool testSofEof(bool requireRoce)
{
    Loopback lb;
    constexpr size_t kSlotBytes = 4 * 1024; // tiny slots to force multi-slot chunks
    if (!lb.setup(requireRoce, 8, kSlotBytes, true))
    {
        return false;
    }

    struct Rec
    {
        uint16_t flags = 0;
        uint64_t chunkId = 0;
        std::vector<openpni::Single> singles;
    };
    std::mutex mu;
    std::vector<Rec> recs;
    lb.server->setIngest([&](const rdma::SlotChunkView &view)
                         {
        Rec r;
        r.flags = view.flags;
        r.chunkId = view.chunkId;
        const auto *src = static_cast<const openpni::Single *>(view.singlesPacked);
        r.singles.assign(src, src + view.singlesCount);
        std::lock_guard<std::mutex> lock(mu);
        recs.push_back(std::move(r));
        return true; });
    lb.server->start();
    if (!lb.handshake())
    {
        lb.stop();
        return false;
    }

    const size_t maxPer = rdma::maxSinglesPerSlot(kSlotBytes);
    const uint32_t count = static_cast<uint32_t>(maxPer * 2 + 10);
    auto sent = makeSingles(count, 4000);
    if (!lb.sender->sendPackedSingles(7, 1, 1, sent.data(), count))
    {
        std::cerr << "sof/eof send failed\n";
        lb.stop();
        return false;
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (true)
    {
        size_t n = 0;
        {
            std::lock_guard<std::mutex> lock(mu);
            n = recs.size();
        }
        if (n >= 3)
        {
            break;
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    lb.stop();

    std::lock_guard<std::mutex> lock(mu);
    if (recs.size() < 2)
    {
        std::cerr << "sof/eof expected multi-slot got slots=" << recs.size() << "\n";
        return false;
    }
    if ((recs.front().flags & rdma::kSlotFlagSof) == 0)
    {
        std::cerr << "missing SOF\n";
        return false;
    }
    if ((recs.back().flags & rdma::kSlotFlagEof) == 0)
    {
        std::cerr << "missing EOF\n";
        return false;
    }
    std::vector<openpni::Single> concat;
    for (const auto &r : recs)
    {
        if (r.chunkId != 7)
        {
            std::cerr << "chunkId mismatch\n";
            return false;
        }
        if ((r.flags & rdma::kSlotFlagPartial) == 0)
        {
            std::cerr << "missing PARTIAL on multi-slot\n";
            return false;
        }
        concat.insert(concat.end(), r.singles.begin(), r.singles.end());
    }
    if (concat.size() != sent.size())
    {
        std::cerr << "sof/eof concat size " << concat.size() << " != " << sent.size() << "\n";
        return false;
    }
    for (size_t i = 0; i < sent.size(); ++i)
    {
        if (!singlesEqual(concat[i], sent[i]))
        {
            std::cerr << "sof/eof payload mismatch i=" << i << "\n";
            return false;
        }
    }
    return true;
}

bool testInvalidMagic()
{
    Loopback lb;
    if (!lb.setup(false, 4, 64 * 1024, false))
    {
        return false;
    }
    std::atomic<uint64_t> received{0};
    lb.server->setIngest([&](const rdma::SlotChunkView &view)
                         {
        received.fetch_add(view.singlesCount);
        return true; });
    if (!lb.handshake())
    {
        lb.stop();
        return false;
    }

    auto *hdr = lb.session->ring().slotHeader(0);
    auto *pl = lb.session->ring().slotPayload(0);
    auto sent = makeSingles(4, 1);
    std::memcpy(pl, sent.data(), sent.size() * sizeof(openpni::Single));
    rdma::clearSlotHeader(hdr);
    hdr->magic = 0xdeadbeef;
    hdr->singlesCount = 4;
    hdr->seq = 1;
    auto *note = &lb.session->ring().notifyBase()[0];
    note->slotIndex = 0;
    note->singlesCount = 4;
    std::atomic_thread_fence(std::memory_order_release);
    note->seq = 1;

    const int n = lb.session->pollOnce(8);
    lb.stop();
    if (n != 0 || received.load() != 0)
    {
        std::cerr << "invalid magic ingested n=" << n << " recv=" << received.load() << "\n";
        return false;
    }
    return true;
}

bool testWrongSeqIgnored()
{
    Loopback lb;
    if (!lb.setup(false, 4, 64 * 1024, false))
    {
        return false;
    }
    std::atomic<uint64_t> received{0};
    lb.server->setIngest([&](const rdma::SlotChunkView &view)
                         {
        received.fetch_add(view.singlesCount);
        return true; });
    if (!lb.handshake())
    {
        lb.stop();
        return false;
    }

    auto *hdr = lb.session->ring().slotHeader(0);
    auto *pl = lb.session->ring().slotPayload(0);
    auto sent = makeSingles(4, 1);
    std::memcpy(pl, sent.data(), sent.size() * sizeof(openpni::Single));
    rdma::clearSlotHeader(hdr);
    hdr->singlesCount = 4;
    hdr->seq = 1;
    auto *note = &lb.session->ring().notifyBase()[0];
    note->slotIndex = 0;
    note->singlesCount = 4;
    std::atomic_thread_fence(std::memory_order_release);
    note->seq = 99;

    const int n = lb.session->pollOnce(8);
    lb.stop();
    if (n != 0 || received.load() != 0)
    {
        std::cerr << "wrong seq ingested n=" << n << " recv=" << received.load() << "\n";
        return false;
    }
    return true;
}

bool testAcquireCommit(bool requireRoce)
{
    Loopback lb;
    if (!lb.setup(requireRoce, 8, 64 * 1024, true))
    {
        return false;
    }
    std::atomic<uint64_t> received{0};
    lb.server->setIngest([&](const rdma::SlotChunkView &view)
                         {
        received.fetch_add(view.singlesCount);
        return true; });
    lb.server->start();
    if (!lb.handshake())
    {
        lb.stop();
        return false;
    }

    auto sent = makeSingles(40, 5000);
    rdma::TxSlotLease lease{};
    if (!lb.sender->acquireTxSlot(&lease) || !lease.payload)
    {
        std::cerr << "acquireTxSlot failed\n";
        lb.stop();
        return false;
    }
    std::memcpy(lease.payload, sent.data(), sent.size() * sizeof(openpni::Single));
    rdma::SlotHeader hdr{};
    rdma::clearSlotHeader(&hdr);
    hdr.chunkId = 42;
    hdr.flags = static_cast<uint16_t>(rdma::kSlotFlagSof | rdma::kSlotFlagEof);
    if (!lb.sender->commitTxSlot(lease, hdr, static_cast<uint32_t>(sent.size())))
    {
        std::cerr << "commitTxSlot failed\n";
        lb.stop();
        return false;
    }
    const bool ok = waitReceived(received, sent.size(), 8000);
    lb.stop();
    if (!ok)
    {
        std::cerr << "acquire/commit got=" << received.load() << "\n";
        return false;
    }
    return true;
}

int runNamed(const char *name, bool (*fn)())
{
    std::cout << "  " << name << " ... " << std::flush;
    if (!fn())
    {
        std::cout << "FAIL\n";
        return 1;
    }
    std::cout << "PASS\n";
    return 0;
}

int runNamedRoce(const char *name, bool (*fn)(bool), bool requireRoce)
{
    std::cout << "  " << name << " ... " << std::flush;
    if (!fn(requireRoce))
    {
        std::cout << "FAIL\n";
        return 1;
    }
    std::cout << "PASS\n";
    return 0;
}

int runSuite(bool requireRoce)
{
    int rc = 0;
    rc |= runNamedRoce("golden", testGolden, requireRoce);
    rc |= runNamedRoce("credit_wrap", testCreditWrap, requireRoce);
    rc |= runNamedRoce("backpressure", testBackpressure, requireRoce);
    rc |= runNamedRoce("sof_eof", testSofEof, requireRoce);
    rc |= runNamedRoce("acquire_commit", testAcquireCommit, requireRoce);
    return rc;
}

} // namespace

int main()
{
    std::cout << "=== InProcess dataplane ===\n";
    int rc = runSuite(false);
    rc |= runNamed("invalid_magic", testInvalidMagic);
    rc |= runNamed("wrong_seq", testWrongSeqIgnored);

    if (rdma::RdmaDevice::hasVerbsDevice())
    {
        std::cout << "=== RNIC loopback ===\n";
        const int verbsRc = runSuite(true);
        if (verbsRc != 0)
        {
            std::cout << "SKIP remaining verbs assertions treated as FAIL (handshake or datapath)\n";
            rc |= verbsRc;
        }
    }
    else
    {
        std::cout << "SKIP RNIC loopback (no IB verbs device)\n";
    }

    if (rc == 0)
    {
        std::cout << "PASS RDMA dataplane loopback\n";
    }
    return rc;
}
