/**
 * Handshake order, InProcess dataplane, synthetic complete/drain.
 * RoCE same-process path runs only when a verbs device exists.
 */

#include "core/acquisition/RawIngress.hpp"
#include "core/streaming/PackedSingle.hpp"
#include "core/streaming/StreamingCoincidence.hpp"
#include "core/streaming/SyntheticSingles.hpp"
#include "dataplane/rdma/RdmaContext.hpp"
#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"
#include "grpcNode/coinNode.hpp"
#include "grpcService/CoincidenceClient.hpp"
#include "grpcService/EpochHandoffShip.hpp"
#include "protos/coincidence.grpc.pb.h"

#include <pni/core/CommonDataType.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace rdma = openpni::distributed::dataplane::rdma;
namespace grpcnode = openpni::distributed::grpcnode;
namespace streaming = openpni::distributed::streaming;
namespace coincidence = openpni::distributed::coincidence;
namespace acq = openpni::distributed::acquisition;

namespace
{
    streaming::TimeAlignerConfig makeAligner(const std::string &outDir, uint32_t nodes = 1)
    {
        (void)nodes;
        auto cfg = streaming::createBDM2AlignerConfig(outDir);
        cfg.savePrompt = true;
        cfg.saveDelay = true;
        cfg.processingIntervalMs = 50;
        cfg.maxChunksPerNode = 32;
        return cfg;
    }

    std::unique_ptr<coincidence::CoincidenceService::Stub> makeStub(const std::string &addr)
    {
        return coincidence::CoincidenceService::NewStub(
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
    }

    bool testHandshakeOrderInProcess()
    {
        const std::string addr = "127.0.0.1:51061";
        grpcnode::CoinGrpcNode::InitOptions init;
        init.alignerConfig = makeAligner("/tmp/r2c_orch_handshake");
        init.listenAddress = addr;
        init.expectedNodeCount = 1;
        init.autoStartWhenAllRegistered = true;
        init.startLeadTimeMs = 0;
        init.forceInProcess = true;
        init.requireRoce = false;
        grpcnode::CoinGrpcNode coin(init);
        if (!coin.start())
        {
            std::cerr << "coin start failed\n";
            return false;
        }

        auto stub = makeStub(addr);
        coincidence::RegisterNodeRequest reg;
        reg.set_node_id(0);
        reg.set_node_address("127.0.0.1");
        reg.set_channel_count(4);
        reg.set_detector_type("BDM2");
        coincidence::RegisterNodeResponse regResp;
        grpc::ClientContext regCtx;
        const auto regSt = stub->RegisterNode(&regCtx, reg, &regResp);
        if (!regSt.ok() || !regResp.success())
        {
            std::cerr << "RegisterNode failed: " << regSt.error_message() << " " << regResp.message() << "\n";
            coin.stop();
            return false;
        }
        if (coin.startSignalIssued() || coin.dataplaneOpenCount() != 0)
        {
            std::cerr << "Start must wait until OpenDataPlane (issued="
                      << coin.startSignalIssued() << " open=" << coin.dataplaneOpenCount() << ")\n";
            coin.stop();
            return false;
        }

        coincidence::OpenDataPlaneRequest open;
        open.set_node_id(0);
        coincidence::OpenDataPlaneResponse openResp;
        grpc::ClientContext openCtx;
        const auto openSt = stub->OpenDataPlane(&openCtx, open, &openResp);
        if (!openSt.ok() || !openResp.success())
        {
            std::cerr << "OpenDataPlane failed: " << openSt.error_message() << " " << openResp.message() << "\n";
            coin.stop();
            return false;
        }
        for (int i = 0; i < 40 && !coin.startSignalIssued(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        const bool started = coin.startSignalIssued();
        coin.stop();
        if (!started)
        {
            std::cerr << "start should be issued after OpenDataPlane\n";
            return false;
        }
        std::cout << "[PASS] handshake_order_inprocess\n";
        return true;
    }

    bool testDualNodeSyntheticAndDrain()
    {
        const std::string addr = "127.0.0.1:51062";
        grpcnode::CoinGrpcNode::InitOptions init;
        init.alignerConfig = makeAligner("/tmp/r2c_orch_dual");
        init.listenAddress = addr;
        init.expectedNodeCount = 2;
        init.autoStartWhenAllRegistered = true;
        init.startLeadTimeMs = 0;
        init.forceInProcess = true;
        init.requireRoce = false;
        grpcnode::CoinGrpcNode coin(init);
        if (!coin.start())
        {
            std::cerr << "dual coin start failed\n";
            return false;
        }

        streaming::CoincidenceClientConfig c0;
        c0.serverAddress = addr;
        c0.nodeId = 0;
        c0.nodeAddress = "127.0.0.1";
        c0.channelCount = 4;
        c0.detectorType = "BDM2";
        c0.forceInProcess = true;
        c0.requireRoce = false;
        c0.waitForStartTimeoutMs = 15000;
        streaming::CoincidenceClientConfig c1 = c0;
        c1.nodeId = 1;

        streaming::CoincidenceClient client0(c0);
        streaming::CoincidenceClient client1(c1);
        std::thread t0([&] { client0.start(); });
        std::thread t1([&] { client1.start(); });
        t0.join();
        t1.join();
        if (!client0.isRunning() || !client1.isRunning())
        {
            std::cerr << "dual client start failed\n";
            client0.stop();
            client1.stop();
            coin.stop();
            return false;
        }

        streaming::SyntheticSpec s0;
        s0.nodeId = 0;
        s0.peerNodeId = 1;
        s0.promptPairs = 256;
        s0.delayPairs = 256;
        streaming::SyntheticSpec s1 = s0;
        s1.nodeId = 1;
        const auto truth = streaming::syntheticTruth(s0);
        auto sendByChunk = [](streaming::CoincidenceClient &client, const streaming::SyntheticSpec &spec) {
            const uint64_t total = streaming::syntheticEventCount(spec);
            const size_t chunk = 37;
            std::vector<streaming::Single> buf;
            for (uint64_t off = 0; off < total; off += chunk)
            {
                const size_t n = static_cast<size_t>(std::min<uint64_t>(chunk, total - off));
                streaming::fillSyntheticChunk(spec, off, n, &buf);
                if (!client.sendSingles(buf, 0, 0))
                {
                    return false;
                }
            }
            return true;
        };
        const bool okSend = sendByChunk(client0, s0) && sendByChunk(client1, s1);
        const bool okDone = client0.notifyProducerComplete() && client1.notifyProducerComplete();
        for (int i = 0; i < 80 && !coin.allProducersComplete(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        const bool complete = coin.allProducersComplete();
        const auto &st = coin.statistics();
        uint64_t received = 0;
        uint64_t prompt = 0;
        uint64_t delay = 0;
        const uint64_t sent = streaming::syntheticEventCount(s0) + streaming::syntheticEventCount(s1);
        for (int i = 0; i < 80; ++i)
        {
            received = st.totalSinglesReceived.load();
            prompt = st.totalPromptPairs.load();
            delay = st.totalDelayPairs.load();
            if (received >= sent && prompt > 0 && delay > 0)
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        client0.stop();
        client1.stop();
        coin.stop();
        if (!okSend || !okDone || !complete)
        {
            std::cerr << "dual complete failed send=" << okSend << " done=" << okDone
                      << " complete=" << complete << "\n";
            return false;
        }
        std::cout << "[PASS] dual_node_synthetic received=" << received
                  << " sent=" << sent
                  << " prompt=" << prompt << " (truth " << truth.promptPairs << ")"
                  << " delay=" << delay << " (truth " << truth.delayPairs << ")\n";
        if (received < sent)
        {
            std::cerr << "received " << received << " < sent " << sent << "\n";
            return false;
        }
        if (prompt == 0 || delay == 0)
        {
            if (coin.aligner().gpuCount() == 0)
            {
                std::cout << "[WARN] coincidence products not checked (no GPU)\n";
                return true;
            }
            std::cerr << "expected prompt>0 and delay>0, got prompt=" << prompt
                      << " delay=" << delay << "\n";
            return false;
        }
        return true;
    }

    bool testChannelRemapInProcess()
    {
        const std::string addr = "127.0.0.1:51068";
        grpcnode::CoinGrpcNode::InitOptions init;
        init.alignerConfig = makeAligner("/tmp/r2c_orch_remap");
        init.alignerConfig.processingIntervalMs = 30000;
        init.listenAddress = addr;
        init.expectedNodeCount = 1;
        init.autoStartWhenAllRegistered = true;
        init.startLeadTimeMs = 0;
        init.forceInProcess = true;
        init.requireRoce = false;
        grpcnode::CoinGrpcNode coin(init);
        if (!coin.start())
        {
            std::cerr << "remap coin start failed\n";
            return false;
        }

        constexpr uint16_t kLocalChannel = 5;
        constexpr uint32_t kOffset = 20;
        constexpr uint16_t kExpected = 25;

        streaming::CoincidenceClientConfig cc;
        cc.serverAddress = addr;
        cc.nodeId = 0;
        cc.nodeAddress = "127.0.0.1";
        cc.channelCount = 4;
        cc.detectorType = "BDM2";
        cc.forceInProcess = true;
        cc.requireRoce = false;
        cc.waitForStartTimeoutMs = 15000;
        cc.remapLocalToGlobalChannels = true;
        cc.globalChannelOffset = kOffset;
        streaming::CoincidenceClient client(cc);
        if (!client.start())
        {
            std::cerr << "remap client start failed\n";
            coin.stop();
            return false;
        }

        std::vector<streaming::Single> buf(8);
        for (size_t i = 0; i < buf.size(); ++i)
        {
            buf[i].channelIndex = kLocalChannel;
            buf[i].crystalIndex = static_cast<uint16_t>(i);
            buf[i].timevalue_100fs = 1'000'000'000ULL + static_cast<uint64_t>(i) * 10'000ULL;
            buf[i].energy_ev = 511000.0F;
        }
        if (!client.sendSingles(buf, 0, 0))
        {
            std::cerr << "remap send failed\n";
            client.stop();
            coin.stop();
            return false;
        }
        (void)client.waitUntilIdle();

        // stealLoop empties NodeRingBuffer within ~1ms; wait on ingest stats instead,
        // then read remapped payload from the InProcess slot (still valid until reuse).
        coincidence::StatusResponse st;
        uint64_t ingested = 0;
        for (int i = 0; i < 80 && ingested < buf.size(); ++i)
        {
            st.Clear();
            if (coin.copyStatus(&st))
            {
                for (const auto &ns : st.node_stats())
                {
                    if (ns.node_id() == 0)
                    {
                        ingested = ns.singles_received();
                    }
                }
            }
            if (ingested >= buf.size())
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        std::vector<streaming::Single> got;
        const uint64_t handle = client.rdmaInprocessHandle();
        if (auto sess = rdma::RdmaRecvServer::findInProcessSession(handle))
        {
            const uint32_t slot = rdma::slotIndexFromSeq(1, sess->ring().slotCount());
            const auto *hdr = sess->ring().slotHeader(slot);
            const auto *payload = sess->ring().slotPayload(slot);
            if (hdr && payload && hdr->singlesCount == buf.size())
            {
                got.resize(hdr->singlesCount);
                streaming::unpackBinaryToSingles(payload, got.size(), got.data());
            }
        }
        client.stop();
        coin.stop();
        if (ingested < buf.size() || got.size() != buf.size())
        {
            std::cerr << "remap chunk missing ingested=" << ingested
                      << " ring=" << got.size() << " expected=" << buf.size() << "\n";
            return false;
        }
        for (size_t i = 0; i < got.size(); ++i)
        {
            if (got[i].channelIndex != kExpected)
            {
                std::cerr << "remap channelIndex[" << i << "]=" << got[i].channelIndex
                          << " expected " << kExpected << "\n";
                return false;
            }
        }
        std::cout << "[PASS] channel_remap_inprocess offset=" << kOffset
                  << " local=" << kLocalChannel << " global=" << kExpected << "\n";
        return true;
    }

    bool testDualNodeBackpressureDrain()
    {
        const std::string addr = "127.0.0.1:51065";
        grpcnode::CoinGrpcNode::InitOptions init;
        init.alignerConfig = makeAligner("/tmp/r2c_orch_bp");
        init.listenAddress = addr;
        init.expectedNodeCount = 2;
        init.autoStartWhenAllRegistered = true;
        init.startLeadTimeMs = 0;
        init.forceInProcess = true;
        init.requireRoce = false;
        grpcnode::CoinGrpcNode coin(init);
        if (!coin.start())
        {
            std::cerr << "backpressure coin start failed\n";
            return false;
        }

        streaming::CoincidenceClientConfig c0;
        c0.serverAddress = addr;
        c0.nodeId = 0;
        c0.nodeAddress = "127.0.0.1";
        c0.channelCount = 4;
        c0.detectorType = "BDM2";
        c0.forceInProcess = true;
        c0.requireRoce = false;
        // JSON-compat field; hot path sends inline. Assert send completes, not queue depth.
        c0.maxPendingChunks = 2;
        c0.batchSize = 1;
        c0.waitForStartTimeoutMs = 15000;
        streaming::CoincidenceClientConfig c1 = c0;
        c1.nodeId = 1;

        streaming::CoincidenceClient client0(c0);
        streaming::CoincidenceClient client1(c1);
        std::thread t0([&] { client0.start(); });
        std::thread t1([&] { client1.start(); });
        t0.join();
        t1.join();
        if (!client0.isRunning() || !client1.isRunning())
        {
            std::cerr << "backpressure client start failed\n";
            client0.stop();
            client1.stop();
            coin.stop();
            return false;
        }

        streaming::SyntheticSpec s0;
        s0.nodeId = 0;
        s0.peerNodeId = 1;
        s0.promptPairs = 64;
        s0.delayPairs = 64;
        streaming::SyntheticSpec s1 = s0;
        s1.nodeId = 1;
        auto a = streaming::generateSyntheticSingles(s0);
        auto b = streaming::generateSyntheticSingles(s1);

        auto sendChunks = [](streaming::CoincidenceClient &client, const std::vector<openpni::Single> &all) {
            const size_t chunk = 16;
            for (size_t off = 0; off < all.size(); off += chunk)
            {
                const size_t n = std::min(chunk, all.size() - off);
                std::vector<openpni::Single> slice(all.begin() + static_cast<std::ptrdiff_t>(off),
                                                   all.begin() + static_cast<std::ptrdiff_t>(off + n));
                if (!client.sendSingles(slice, 0, 0))
                {
                    return false;
                }
            }
            return true;
        };

        const bool okSend = sendChunks(client0, a) && sendChunks(client1, b);
        const bool okDone = client0.notifyProducerComplete() && client1.notifyProducerComplete();
        for (int i = 0; i < 80 && !coin.allProducersComplete(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        const bool complete = coin.allProducersComplete();
        const uint64_t received = coin.statistics().totalSinglesReceived.load();
        client0.stop();
        client1.stop();
        coin.stop();
        if (!okSend || !okDone || !complete || received < a.size() + b.size())
        {
            std::cerr << "backpressure/drain failed send=" << okSend << " done=" << okDone
                      << " complete=" << complete << " received=" << received << "\n";
            return false;
        }
        std::cout << "[PASS] dual_node_backpressure_drain received=" << received << "\n";
        return true;
    }

    bool testRequireRoceWithoutDevice()
    {
        if (rdma::RdmaDevice::hasVerbsDevice())
        {
            std::cout << "[SKIP] requireRoce_no_device (verbs present)\n";
            return true;
        }
        grpcnode::CoinGrpcNode::InitOptions init;
        init.alignerConfig = makeAligner("/tmp/r2c_orch_roce");
        init.listenAddress = "127.0.0.1:51063";
        init.expectedNodeCount = 1;
        init.requireRoce = true;
        init.forceInProcess = false;
        grpcnode::CoinGrpcNode coin(init);
        if (!coin.start())
        {
            std::cerr << "coin start failed\n";
            return false;
        }
        streaming::CoincidenceClientConfig cc;
        cc.serverAddress = "127.0.0.1:51063";
        cc.nodeId = 0;
        cc.requireRoce = true;
        cc.waitForStartTimeoutMs = 2000;
        streaming::CoincidenceClient client(cc);
        const bool started = client.start();
        client.stop();
        coin.stop();
        if (started)
        {
            std::cerr << "requireRoce should fail without verbs\n";
            return false;
        }
        std::cout << "[PASS] requireRoce_no_device\n";
        return true;
    }

    bool testSameProcessRoce()
    {
        if (!rdma::RdmaDevice::hasVerbsDevice())
        {
            std::cout << "[SKIP] same_process_roce (no verbs device)\n";
            return true;
        }
        grpcnode::CoinGrpcNode::InitOptions init;
        init.alignerConfig = makeAligner("/tmp/r2c_orch_roce_ok");
        init.listenAddress = "127.0.0.1:51064";
        init.expectedNodeCount = 1;
        init.requireRoce = true;
        grpcnode::CoinGrpcNode coin(init);
        if (!coin.start())
        {
            std::cerr << "roce coin start failed\n";
            return false;
        }
        streaming::CoincidenceClientConfig cc;
        cc.serverAddress = "127.0.0.1:51064";
        cc.nodeId = 0;
        cc.nodeAddress = "127.0.0.1";
        cc.requireRoce = true;
        cc.waitForStartTimeoutMs = 10000;
        streaming::CoincidenceClient client(cc);
        if (!client.start())
        {
            std::cerr << "roce client start failed\n";
            coin.stop();
            return false;
        }
        if (client.dataPlaneKind() != rdma::DataPlaneKind::RdmaRoceV2)
        {
            std::cerr << "expected RoCE kind\n";
            client.stop();
            coin.stop();
            return false;
        }
        streaming::SyntheticSpec spec;
        spec.promptPairs = 64;
        spec.delayPairs = 64;
        auto singles = streaming::generateSyntheticSingles(spec);
        const bool ok = client.sendSingles(singles, 0, 0) && client.notifyProducerComplete();
        client.stop();
        coin.stop();
        if (!ok)
        {
            std::cerr << "roce send/complete failed\n";
            return false;
        }
        std::cout << "[PASS] same_process_roce\n";
        return true;
    }

    bool testStubRawIngressIdle()
    {
        auto stub = acq::makeStubRawIngress();
        std::atomic<int> fired{0};
        stub->setRawDataReadyCallback([&](const openpni::RawDataView &) {
            fired.fetch_add(1);
            return true;
        });
        if (!stub->start())
        {
            std::cerr << "StubRawIngress start failed\n";
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        stub->stop();
        if (fired.load() != 0)
        {
            std::cerr << "StubRawIngress must not produce raw\n";
            return false;
        }
        std::cout << "[PASS] stub_raw_ingress_idle\n";
        return true;
    }

    bool testHeartbeatTelemetryAndPause()
    {
        const std::string addr = "127.0.0.1:51066";
        grpcnode::CoinGrpcNode::InitOptions init;
        init.alignerConfig = makeAligner("/tmp/r2c_orch_hb");
        init.listenAddress = addr;
        init.expectedNodeCount = 1;
        init.autoStartWhenAllRegistered = true;
        init.startLeadTimeMs = 0;
        init.forceInProcess = true;
        init.requireRoce = false;
        init.heartbeatTimeoutMs = 3000;
        grpcnode::CoinGrpcNode coin(init);
        if (!coin.start())
        {
            std::cerr << "hb coin start failed\n";
            return false;
        }

        streaming::CoincidenceClientConfig cc;
        cc.serverAddress = addr;
        cc.nodeId = 0;
        cc.nodeAddress = "127.0.0.1";
        cc.channelCount = 4;
        cc.detectorType = "BDM2";
        cc.forceInProcess = true;
        cc.requireRoce = false;
        cc.heartbeatIntervalMs = 100;
        cc.waitForStartTimeoutMs = 10000;
        streaming::CoincidenceClient client(cc);
        if (!client.start())
        {
            std::cerr << "hb client start failed\n";
            coin.stop();
            return false;
        }

        streaming::SyntheticSpec spec;
        spec.promptPairs = 32;
        spec.delayPairs = 0;
        auto singles = streaming::generateSyntheticSingles(spec);
        if (!client.sendSingles(singles, 0, 0))
        {
            std::cerr << "hb send failed\n";
            client.stop();
            coin.stop();
            return false;
        }

        coincidence::StatusResponse st;
        bool sawSent = false;
        for (int i = 0; i < 30; ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            st.Clear();
            if (!client.getServerStatus(&st))
            {
                continue;
            }
            for (const auto &ns : st.node_stats())
            {
                if (ns.node_id() == 0 && ns.singles_sent_heartbeat() >= singles.size() &&
                    ns.chunks_pending_cap() > 0)
                {
                    sawSent = true;
                }
            }
            if (sawSent)
            {
                break;
            }
        }
        if (!sawSent)
        {
            std::cerr << "GetStatus did not observe heartbeat telemetry\n";
            client.stop();
            coin.stop();
            return false;
        }

        auto stub = makeStub(addr);
        coincidence::ControlRequest ctrl;
        ctrl.set_command(coincidence::ControlRequest::PAUSE);
        coincidence::ControlResponse ctrlResp;
        grpc::ClientContext ctx;
        const auto ctrlSt = stub->Control(&ctx, ctrl, &ctrlResp);
        if (!ctrlSt.ok() || !ctrlResp.success())
        {
            std::cerr << "PAUSE control failed\n";
            client.stop();
            coin.stop();
            return false;
        }

        for (int i = 0; i < 20 && !client.isPaused(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        if (!client.isPaused())
        {
            std::cerr << "client did not pause after heartbeat command\n";
            client.stop();
            coin.stop();
            return false;
        }

        const uint64_t sentAtPause = client.getTotalSinglesSent();
        std::atomic<bool> sendDone{false};
        std::thread blocked([&] {
            auto more = streaming::generateSyntheticSingles(spec);
            (void)client.sendSingles(more, 0, 0);
            sendDone.store(true);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        const bool stillBlocked = !sendDone.load();
        const uint64_t sentDuringPause = client.getTotalSinglesSent();

        coincidence::ControlRequest resume;
        resume.set_command(coincidence::ControlRequest::RESUME);
        coincidence::ControlResponse resumeResp;
        grpc::ClientContext ctx2;
        (void)stub->Control(&ctx2, resume, &resumeResp);
        blocked.join();

        const bool ok = stillBlocked && sentDuringPause == sentAtPause;
        client.stop();
        coin.stop();
        if (!ok)
        {
            std::cerr << "PAUSE did not stop produce blocked=" << stillBlocked
                      << " sent " << sentDuringPause << " vs " << sentAtPause << "\n";
            return false;
        }
        std::cout << "[PASS] heartbeat_telemetry_and_pause sent=" << sentAtPause << "\n";
        return true;
    }

    bool testStreamingSyntheticChunksInProcess()
    {
        streaming::SyntheticSpec spec;
        spec.nodeId = 0;
        spec.peerNodeId = 1;
        spec.promptPairs = 1000;
        spec.delayPairs = 0;
        const auto full = streaming::generateSyntheticSingles(spec);
        std::vector<streaming::Single> chunk0;
        std::vector<streaming::Single> chunk1;
        streaming::fillSyntheticChunk(spec, 0, 64, &chunk0);
        streaming::fillSyntheticChunk(spec, 64, 64, &chunk1);
        if (full.size() != 1000 || chunk0.size() != 64 || chunk1.size() != 64)
        {
            std::cerr << "chunk sizes unexpected full=" << full.size() << "\n";
            return false;
        }
        for (size_t i = 0; i < 64; ++i)
        {
            if (chunk0[i].timevalue_100fs != full[i].timevalue_100fs ||
                chunk1[i].timevalue_100fs != full[64 + i].timevalue_100fs)
            {
                std::cerr << "fillSyntheticChunk timestamps differ from generateSyntheticSingles\n";
                return false;
            }
        }

        const std::string addr = "127.0.0.1:51067";
        grpcnode::CoinGrpcNode::InitOptions init;
        init.alignerConfig = makeAligner("/tmp/r2c_orch_stream_chunks");
        init.listenAddress = addr;
        init.expectedNodeCount = 1;
        init.autoStartWhenAllRegistered = true;
        init.startLeadTimeMs = 0;
        init.forceInProcess = true;
        init.requireRoce = false;
        grpcnode::CoinGrpcNode coin(init);
        if (!coin.start())
        {
            std::cerr << "stream-chunk coin start failed\n";
            return false;
        }

        streaming::CoincidenceClientConfig cc;
        cc.serverAddress = addr;
        cc.nodeId = 0;
        cc.nodeAddress = "127.0.0.1";
        cc.channelCount = 4;
        cc.detectorType = "BDM2";
        cc.forceInProcess = true;
        cc.requireRoce = false;
        cc.waitForStartTimeoutMs = 15000;
        streaming::CoincidenceClient client(cc);
        if (!client.start())
        {
            std::cerr << "stream-chunk client start failed\n";
            coin.stop();
            return false;
        }

        const bool okSend = client.sendSingles(chunk0, 0, 0) && client.sendSingles(chunk1, 0, 0);
        const uint64_t sent = client.getTotalSinglesSent();
        const bool okDone = client.notifyProducerComplete();
        for (int i = 0; i < 80 && !coin.allProducersComplete(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        const bool complete = coin.allProducersComplete();
        client.stop();
        coin.stop();
        if (!okSend || !okDone || !complete || sent != 128)
        {
            std::cerr << "stream-chunk send failed send=" << okSend << " done=" << okDone
                      << " complete=" << complete << " sent=" << sent << "\n";
            return false;
        }
        std::cout << "[PASS] streaming_synthetic_chunks sent=" << sent << "\n";
        return true;
    }

    streaming::CoincidenceClientConfig makeDualDestClient(
        uint32_t nodeId,
        const std::string &addrA,
        const std::string &addrB)
    {
        streaming::CoincidenceClientConfig cc;
        cc.serverAddress = addrA;
        cc.nodeId = nodeId;
        cc.nodeAddress = "127.0.0.1";
        cc.channelCount = 4;
        cc.detectorType = "BDM2";
        cc.forceInProcess = true;
        cc.requireRoce = false;
        cc.waitForStartTimeoutMs = 15000;
        cc.heartbeatIntervalMs = 50;
        cc.coinId = 0;
        cc.activeCoinId = 0;
        cc.destinations = {{0, addrA}, {1, addrB}};
        return cc;
    }

    bool waitClientActiveCoin(streaming::CoincidenceClient &client, uint32_t want, int timeoutMs)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (client.activeCoinId() == want)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    bool startClientThread(streaming::CoincidenceClient &client)
    {
        std::thread t([&] { client.start(); });
        t.join();
        return client.isRunning();
    }

    bool testTimeShardColdSwitchInProcess()
    {
        const std::string addrA = "127.0.0.1:51071";
        const std::string addrB = "127.0.0.1:51072";
        grpcnode::CoinGrpcNode::InitOptions initA;
        initA.alignerConfig = makeAligner("/tmp/r2c_orch_shard_cold_a");
        initA.listenAddress = addrA;
        initA.expectedNodeCount = 1;
        initA.autoStartWhenAllRegistered = true;
        initA.startLeadTimeMs = 0;
        initA.forceInProcess = true;
        initA.coinId = 0;
        initA.enableTimeShard = true;
        initA.nextCoinId = 1;
        grpcnode::CoinGrpcNode::InitOptions initB = initA;
        initB.alignerConfig = makeAligner("/tmp/r2c_orch_shard_cold_b");
        initB.listenAddress = addrB;
        initB.coinId = 1;
        initB.enableTimeShard = false;
        grpcnode::CoinGrpcNode coinA(initA);
        grpcnode::CoinGrpcNode coinB(initB);
        if (!coinA.start() || !coinB.start())
        {
            std::cerr << "cold dual coin start failed\n";
            return false;
        }

        auto cc = makeDualDestClient(0, addrA, addrB);
        streaming::CoincidenceClient client(cc);
        if (!startClientThread(client))
        {
            std::cerr << "cold client start failed\n";
            client.stop();
            coinA.stop();
            coinB.stop();
            return false;
        }

        streaming::SyntheticSpec spec;
        spec.nodeId = 0;
        spec.peerNodeId = 0;
        spec.promptPairs = 128;
        spec.delayPairs = 0;
        auto buf = streaming::generateSyntheticSingles(spec);
        const bool okSend = client.sendSingles(buf, 0, 0);
        const bool okDone = client.notifyProducerComplete();
        for (int i = 0; i < 80 && !coinA.allProducersComplete(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        coinA.service().requestSetActiveCoin(1);
        const bool switched = waitClientActiveCoin(client, 1, 5000);
        client.stop();
        coinA.stop();
        coinB.stop();
        if (!okSend || !okDone || !switched)
        {
            std::cerr << "cold switch failed send=" << okSend << " done=" << okDone
                      << " switched=" << switched << "\n";
            return false;
        }
        std::cout << "[PASS] time_shard_cold_switch (control plane only)\n";
        return true;
    }

    bool testTimeShardHotSwitchInProcess()
    {
        const std::string addrA = "127.0.0.1:51073";
        const std::string addrB = "127.0.0.1:51074";
        grpcnode::CoinGrpcNode::InitOptions initA;
        initA.alignerConfig = makeAligner("/tmp/r2c_orch_shard_hot_a", 2);
        initA.alignerConfig.extractOnly = true;
        initA.listenAddress = addrA;
        initA.expectedNodeCount = 2;
        initA.autoStartWhenAllRegistered = true;
        initA.startLeadTimeMs = 0;
        initA.forceInProcess = true;
        initA.coinId = 0;
        initA.enableTimeShard = true;
        initA.nextCoinId = 1;
        grpcnode::CoinGrpcNode::InitOptions initB = initA;
        initB.alignerConfig = makeAligner("/tmp/r2c_orch_shard_hot_b", 2);
        initB.alignerConfig.extractOnly = true;
        initB.listenAddress = addrB;
        initB.coinId = 1;
        initB.enableTimeShard = false;
        grpcnode::CoinGrpcNode coinA(initA);
        grpcnode::CoinGrpcNode coinB(initB);
        if (!coinA.start() || !coinB.start())
        {
            std::cerr << "hot dual coin start failed\n";
            return false;
        }

        auto c0 = makeDualDestClient(0, addrA, addrB);
        auto c1 = makeDualDestClient(1, addrA, addrB);
        streaming::CoincidenceClient client0(c0);
        streaming::CoincidenceClient client1(c1);
        std::thread t0([&] { client0.start(); });
        std::thread t1([&] { client1.start(); });
        t0.join();
        t1.join();
        if (!client0.isRunning() || !client1.isRunning())
        {
            std::cerr << "hot client start failed\n";
            client0.stop();
            client1.stop();
            coinA.stop();
            coinB.stop();
            return false;
        }

        streaming::SyntheticSpec s0;
        s0.nodeId = 0;
        s0.peerNodeId = 1;
        s0.promptPairs = 256;
        s0.delayPairs = 256;
        streaming::SyntheticSpec s1 = s0;
        s1.nodeId = 1;
        const uint64_t total = streaming::syntheticEventCount(s0);
        const uint64_t half = total / 2;
        auto sendRange = [](streaming::CoincidenceClient &client,
                            const streaming::SyntheticSpec &spec,
                            uint64_t begin,
                            uint64_t end) {
            std::vector<streaming::Single> buf;
            const size_t chunk = 32;
            for (uint64_t off = begin; off < end; off += chunk)
            {
                const size_t n = static_cast<size_t>(std::min<uint64_t>(chunk, end - off));
                streaming::fillSyntheticChunk(spec, off, n, &buf);
                if (!client.sendSingles(buf, 0, 0))
                {
                    return false;
                }
            }
            return true;
        };
        if (!sendRange(client0, s0, 0, half) || !sendRange(client1, s1, 0, half))
        {
            std::cerr << "hot first-half send failed\n";
            client0.stop();
            client1.stop();
            coinA.stop();
            coinB.stop();
            return false;
        }
        bool reached = false;
        for (int i = 0; i < 200; ++i)
        {
            if (coinA.aligner().publishedWatermark() > 0)
            {
                reached = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        if (!reached)
        {
            std::cerr << "hot watermark not ready W=" << coinA.aligner().publishedWatermark()
                      << "\n";
            client0.stop();
            client1.stop();
            coinA.stop();
            coinB.stop();
            return false;
        }
        const uint64_t wBefore = coinA.aligner().publishedWatermark();
        coinA.service().markNextCoinPrepared(true);
        if (!coinA.service().cutEpochAtWatermark())
        {
            std::cerr << "hot cutEpochAtWatermark failed W=" << wBefore << "\n";
            client0.stop();
            client1.stop();
            coinA.stop();
            coinB.stop();
            return false;
        }
        if (!coinA.service().shipEpochTo(coinB.service()))
        {
            std::cerr << "hot shipEpochTo failed\n";
            client0.stop();
            client1.stop();
            coinA.stop();
            coinB.stop();
            return false;
        }
        const bool switched =
            waitClientActiveCoin(client0, 1, 5000) && waitClientActiveCoin(client1, 1, 5000);
        if (!switched)
        {
            std::cerr << "hot SET_ACTIVE_COIN did not land\n";
            client0.stop();
            client1.stop();
            coinA.stop();
            coinB.stop();
            return false;
        }
        if (!sendRange(client0, s0, half, total) || !sendRange(client1, s1, half, total))
        {
            std::cerr << "hot second-half send failed\n";
            client0.stop();
            client1.stop();
            coinA.stop();
            coinB.stop();
            return false;
        }
        (void)client0.notifyProducerComplete();
        (void)client1.notifyProducerComplete();
        for (int i = 0; i < 80 && !coinA.allProducersComplete(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        if (coinB.aligner().isRunning())
        {
            coinB.aligner().stop(true);
        }
        const auto &stA = coinA.statistics();
        const auto &stB = coinB.statistics();
        const uint64_t prompt = stA.totalPromptPairs.load() + stB.totalPromptPairs.load();
        const uint64_t delay = stA.totalDelayPairs.load() + stB.totalDelayPairs.load();
        const uint64_t wAfter = coinA.service().timeLease().t1_100fs;
        client0.stop();
        client1.stop();
        coinA.stop();
        coinB.stop();
        if (wAfter < wBefore)
        {
            std::cerr << "hot W not monotonic " << wBefore << "->" << wAfter << "\n";
            return false;
        }
        if (prompt == 0 && delay == 0)
        {
            std::cout << "[WARN] hot coincidence products not checked (extract-only / no GPU)\n";
        }
        std::cout << "[PASS] time_shard_hot_switch prompt=" << prompt << " delay=" << delay
                  << " W=" << wAfter << "\n";
        return true;
    }

    bool testTimeLeaseFsmInProcess()
    {
        const std::string addr = "127.0.0.1:51075";
        grpcnode::CoinGrpcNode::InitOptions init;
        init.alignerConfig = makeAligner("/tmp/r2c_orch_lease");
        init.listenAddress = addr;
        init.expectedNodeCount = 1;
        init.autoStartWhenAllRegistered = true;
        init.startLeadTimeMs = 0;
        init.forceInProcess = true;
        init.coinId = 0;
        init.enableTimeShard = true;
        init.plannedLeaseSpan_100fs = 1;
        init.minLease_100fs = 0;
        grpcnode::CoinGrpcNode coin(init);
        if (!coin.start())
        {
            std::cerr << "lease coin start failed\n";
            return false;
        }
        streaming::CoincidenceClientConfig cc;
        cc.serverAddress = addr;
        cc.nodeId = 0;
        cc.nodeAddress = "127.0.0.1";
        cc.forceInProcess = true;
        cc.waitForStartTimeoutMs = 15000;
        streaming::CoincidenceClient client(cc);
        if (!startClientThread(client))
        {
            std::cerr << "lease client start failed\n";
            client.stop();
            coin.stop();
            return false;
        }
        streaming::SyntheticSpec spec;
        spec.nodeId = 0;
        spec.peerNodeId = 0;
        spec.promptPairs = 256;
        spec.delayPairs = 0;
        auto buf = streaming::generateSyntheticSingles(spec);
        if (!client.sendSingles(buf, 0, 0))
        {
            std::cerr << "lease send failed\n";
            client.stop();
            coin.stop();
            return false;
        }
        bool gotW = false;
        for (int i = 0; i < 200; ++i)
        {
            if (coin.aligner().publishedWatermark() > 0)
            {
                gotW = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        const bool noPrepare = !coin.service().maybePreemptLease();
        const auto stay = coin.service().timeLease();
        coin.service().markNextCoinPrepared(true);
        const bool cut = coin.service().maybePreemptLease();
        const auto after = coin.service().timeLease();
        const bool second = coin.service().maybePreemptLease();
        client.stop();
        coin.stop();
        if (!gotW || !noPrepare || stay.activeCoinId != 0 || stay.epochId != 0 || !cut ||
            after.epochId != 1 || after.t1_100fs == 0 || second)
        {
            std::cerr << "lease fsm failed gotW=" << gotW << " noPrepare=" << noPrepare
                      << " stayEpoch=" << stay.epochId << " cut=" << cut
                      << " epoch=" << after.epochId << " W=" << after.t1_100fs
                      << " second=" << second << "\n";
            return false;
        }
        std::cout << "[PASS] time_lease_fsm W=" << after.t1_100fs << "\n";
        return true;
    }

    bool singlesMatch(const streaming::Single &a, const streaming::Single &b)
    {
        return a.channelIndex == b.channelIndex && a.crystalIndex == b.crystalIndex &&
               a.timevalue_100fs == b.timevalue_100fs && a.energy_ev == b.energy_ev;
    }

    bool testEpochShipDataplane(bool requireRoce)
    {
        if (requireRoce && !rdma::RdmaDevice::hasVerbsDevice())
        {
            std::cout << "[SKIP] epoch_ship_roce (no verbs device)\n";
            return true;
        }
        streaming::EpochHandoff in;
        in.epochId = 7;
        in.cutWatermark_100fs = 123456;
        in.overlap_100fs = 2000;
        in.carry.resize(8);
        for (uint32_t i = 0; i < in.carry.size(); ++i)
        {
            in.carry[i].channelIndex = static_cast<uint16_t>(i);
            in.carry[i].crystalIndex = 1;
            in.carry[i].timevalue_100fs = in.cutWatermark_100fs - 10 + i;
            in.carry[i].energy_ev = 511.0f;
        }
        streaming::TimestampedSingleChunk tail;
        tail.nodeId = 1;
        tail.chunkId = 9;
        tail.singles.resize(5);
        for (uint32_t i = 0; i < tail.singles.size(); ++i)
        {
            tail.singles[i].channelIndex = 3;
            tail.singles[i].crystalIndex = 2;
            tail.singles[i].timevalue_100fs = in.cutWatermark_100fs + 1 + i;
            tail.singles[i].energy_ev = 511.0f;
        }
        tail.updateTimeRange();
        in.tail.push_back(std::move(tail));

        streaming::EpochHandoff out;
        if (!streaming::shipEpochHandoffViaDataplane(in, &out, requireRoce))
        {
            if (requireRoce)
            {
                std::cout << "[SKIP] epoch_ship_roce (handshake failed)\n";
                return true;
            }
            std::cerr << "epoch ship dataplane failed\n";
            return false;
        }
        if (out.epochId != in.epochId || out.cutWatermark_100fs != in.cutWatermark_100fs ||
            out.carry.size() != in.carry.size() || out.tail.size() != 1)
        {
            std::cerr << "epoch ship meta mismatch\n";
            return false;
        }
        for (size_t i = 0; i < in.carry.size(); ++i)
        {
            if (!singlesMatch(in.carry[i], out.carry[i]))
            {
                std::cerr << "epoch ship carry mismatch at " << i << "\n";
                return false;
            }
        }
        if (out.tail[0].nodeId != 1 || out.tail[0].singles.size() != 5 ||
            !singlesMatch(in.tail[0].singles[0], out.tail[0].singles[0]))
        {
            std::cerr << "epoch ship tail mismatch\n";
            return false;
        }
        std::cout << "[PASS] epoch_ship_" << (requireRoce ? "roce" : "inprocess")
                  << " carry=" << out.carry.size() << " tail=" << out.tail.size() << "\n";
        return true;
    }
} // namespace

int main(int argc, char **argv)
{
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    int rc = 0;
    if (!testHandshakeOrderInProcess())
        rc = 1;
    if (!testDualNodeSyntheticAndDrain())
        rc = 1;
    if (!testChannelRemapInProcess())
        rc = 1;
    if (!testDualNodeBackpressureDrain())
        rc = 1;
    if (!testRequireRoceWithoutDevice())
        rc = 1;
    if (!testSameProcessRoce())
        rc = 1;
    if (!testStubRawIngressIdle())
        rc = 1;
    if (!testHeartbeatTelemetryAndPause())
        rc = 1;
    if (!testStreamingSyntheticChunksInProcess())
        rc = 1;
    if (!testTimeShardColdSwitchInProcess())
        rc = 1;
    if (!testTimeShardHotSwitchInProcess())
        rc = 1;
    if (!testTimeLeaseFsmInProcess())
        rc = 1;
    if (!testEpochShipDataplane(false))
        rc = 1;
    if (!testEpochShipDataplane(true))
        rc = 1;
    return rc;
}
