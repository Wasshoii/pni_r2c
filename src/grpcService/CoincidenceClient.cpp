#include "grpcService/CoincidenceClient.hpp"
#include "core/r2s/multi_gpu/HostCudaRegister.hpp"
#include "core/streaming/PackedSingle.hpp"
#include "dataplane/rdma/ProtoConvert.hpp"
#include "dataplane/rdma/SlotProtocol.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <deque>
#include <iostream>
#include <limits>
#include <span>
#include <thread>
#include <utility>
#include <cuda_runtime.h>
#include <glog/logging.h>

namespace openpni::distributed::streaming
{
    namespace rdma = openpni::distributed::dataplane::rdma;

    namespace
    {
    bool singlesOnDevice(const void *ptr)
    {
        if (ptr == nullptr)
        {
            return false;
        }
        cudaPointerAttributes attr{};
        const cudaError_t st = cudaPointerGetAttributes(&attr, ptr);
        if (st != cudaSuccess)
        {
            static_cast<void>(cudaGetLastError());
            return false;
        }
#if CUDART_VERSION >= 10000
        return attr.type == cudaMemoryTypeDevice;
#else
        return attr.memoryType == cudaMemoryTypeDevice;
#endif
    }

    bool copySinglesIntoTxPayload(Single *dst, const Single *src, uint32_t n, bool srcDevice)
    {
        const size_t bytes = static_cast<size_t>(n) * kPackedSingleSize;
        if (srcDevice)
        {
            const cudaError_t err = cudaMemcpy(dst, src, bytes, cudaMemcpyDeviceToHost);
            if (err != cudaSuccess)
            {
                LOG(ERROR) << "D2H into TX slot failed: " << cudaGetErrorString(err);
                return false;
            }
            return true;
        }
        std::memcpy(dst, src, bytes);
        return true;
    }

    rdma::SlotHeader makeTxSlotHeader(
        uint64_t chunkId,
        uint64_t computerClock_ms,
        uint32_t duration_ms,
        bool first,
        bool last,
        bool partial)
    {
        rdma::SlotHeader hdr{};
        rdma::clearSlotHeader(&hdr);
        hdr.chunkId = chunkId;
        hdr.computerClockMs = computerClock_ms;
        hdr.durationMs = duration_ms;
        hdr.flags = 0;
        if (first)
        {
            hdr.flags = static_cast<uint16_t>(hdr.flags | rdma::kSlotFlagSof);
        }
        if (last)
        {
            hdr.flags = static_cast<uint16_t>(hdr.flags | rdma::kSlotFlagEof);
        }
        if (partial)
        {
            hdr.flags = static_cast<uint16_t>(hdr.flags | rdma::kSlotFlagPartial);
        }
        return hdr;
    }

    struct PendingTxFill
    {
        rdma::TxSlotLease lease{};
        uint32_t n = 0;
        uint32_t absOffset = 0;
        bool first = false;
        bool last = false;
        bool partial = false;
        cudaEvent_t event = nullptr;
    };
    } // namespace

    CoincidenceClient::CoincidenceClient(const CoincidenceClientConfig &config)
        : m_config(config), m_activeCoinId(config.activeCoinId)
    {
        m_channel = grpc::CreateChannel(
            config.serverAddress,
            grpc::InsecureChannelCredentials());
        m_stub = coincidence::CoincidenceService::NewStub(m_channel);
    }

    CoincidenceClient::~CoincidenceClient()
    {
        stop();
    }

    bool CoincidenceClient::start()
    {
        if (m_running.exchange(true))
        {
            LOG(WARNING) << "Already running";
            return false;
        }

        std::cout << "[CoincidenceClient] handshake=reg node=" << m_config.nodeId << std::endl;
        if (!registerNode())
        {
            m_running = false;
            return false;
        }

        std::cout << "[CoincidenceClient] handshake=dp node=" << m_config.nodeId << std::endl;
        if (!openRdmaDataPlane())
        {
            m_running = false;
            return false;
        }

        if (m_config.waitForStartSignal)
        {
            std::cout << "[CoincidenceClient] handshake=wait node=" << m_config.nodeId << std::endl;
            if (!waitForServerStartSignal(m_config.waitForStartTimeoutMs))
            {
                m_running = false;
                return false;
            }
        }

        m_heartbeatThread = std::thread([this]()
                                        { heartbeatLoop(); });

        std::cout << "[CoincidenceClient] handshake=run node=" << m_config.nodeId << std::endl;
        LOG(INFO) << "Started RDMA client for node " << m_config.nodeId;
        return true;
    }

    void CoincidenceClient::stop()
    {
        if (!m_running.exchange(false))
        {
            return;
        }

        m_cv.notify_all();

        if (m_heartbeatThread.joinable())
        {
            m_heartbeatThread.join();
        }

        destroyTxD2hResources();

        if (m_rdmaSender)
        {
            if (m_rdmaSender->txCudaRegistered() && m_rdmaSender->txStagingBase() != nullptr)
            {
                static_cast<void>(cudaHostUnregister(m_rdmaSender->txStagingBase()));
                m_rdmaSender->setTxCudaRegistered(false);
            }
            m_rdmaSender->close();
            m_rdmaSender.reset();
        }
        for (auto &kv : m_idleSenders)
        {
            if (kv.second)
            {
                kv.second->close();
            }
        }
        m_idleSenders.clear();

        LOG(INFO) << "Stopped";
    }

    bool CoincidenceClient::openRdmaDataPlane()
    {
        if (m_config.destinations.empty())
        {
            if (!openOneDataPlane(m_config.serverAddress, m_config.coinId))
            {
                return false;
            }
            m_activeCoinId.store(m_config.coinId, std::memory_order_release);
            m_connected = true;
            return true;
        }
        for (const auto &d : m_config.destinations)
        {
            if (!openOneDataPlane(d.address, d.coinId))
            {
                return false;
            }
        }
        setActiveCoinId(m_config.activeCoinId);
        m_connected = m_rdmaSender != nullptr;
        return m_connected.load();
    }

    bool CoincidenceClient::openOneDataPlane(const std::string &address, uint32_t coinId)
    {
        rdma::RdmaWriteSender::Config sc;
        sc.nodeId = m_config.nodeId;
        sc.deviceName = m_config.rdmaDeviceName;
        sc.requireRoce = m_config.requireRoce;
        sc.forceInProcess = m_config.forceInProcess;
        sc.preferHugePages = false;
        sc.gidIndex = m_config.gidIndex;
        if (m_config.txSlotCount > 0)
        {
            sc.txSlotCount = m_config.txSlotCount;
        }
        auto sender = std::make_unique<rdma::RdmaWriteSender>(std::move(sc));

        rdma::RdmaEndpointInfo localEp{};
        if (!sender->prepareLocalEndpoint(&localEp))
        {
            LOG(ERROR) << "prepareLocalEndpoint failed";
            return false;
        }
        if (m_config.requireRoce && localEp.kind != rdma::DataPlaneKind::RdmaRoceV2)
        {
            LOG(ERROR) << "requireRoce but local endpoint is not RoCE";
            return false;
        }

        std::shared_ptr<grpc::Channel> channel = m_channel;
        std::unique_ptr<coincidence::CoincidenceService::Stub> extraStub;
        coincidence::CoincidenceService::Stub *stub = m_stub.get();
        if (address != m_config.serverAddress)
        {
            channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
            extraStub = coincidence::CoincidenceService::NewStub(channel);
            stub = extraStub.get();
        }

        grpc::ClientContext context;
        coincidence::OpenDataPlaneRequest req;
        req.set_node_id(m_config.nodeId);
        if (m_config.requestedSlotCount > 0)
        {
            req.set_requested_slot_count(m_config.requestedSlotCount);
        }
        if (m_config.requestedSlotBytes > 0)
        {
            req.set_requested_slot_bytes(m_config.requestedSlotBytes);
        }
        rdma::fillProtoEndpoint(localEp, req.mutable_node_endpoint());

        coincidence::OpenDataPlaneResponse resp;
        grpc::Status status = stub->OpenDataPlane(&context, req, &resp);
        if (!status.ok() || !resp.success())
        {
            LOG(ERROR) << "OpenDataPlane failed: "
                       << (status.ok() ? resp.message() : status.error_message());
            return false;
        }

        const auto coinEp = rdma::fromProtoEndpoint(resp.coin_endpoint());
        if (m_config.requireRoce && coinEp.kind != rdma::DataPlaneKind::RdmaRoceV2)
        {
            LOG(ERROR) << "requireRoce but coin endpoint is InProcess";
            return false;
        }
        if (!sender->connect(coinEp))
        {
            LOG(ERROR) << "RDMA connect failed";
            return false;
        }
        if (sender->kind() == rdma::DataPlaneKind::RdmaRoceV2)
        {
            const bool registered = openpni::distributed::r2s::multi_gpu::tryCudaHostRegister(
                sender->txStagingBase(), sender->txStagingBytes());
            sender->setTxCudaRegistered(registered);
            if (!registered)
            {
                LOG(ERROR) << "cudaHostRegister TX arena failed; RoCE device D2H requires "
                              "pinned TX node="
                           << m_config.nodeId;
                return false;
            }
        }
        LOG(INFO) << "RDMA dataplane connected node=" << m_config.nodeId
                  << " coinId=" << coinId
                  << " kind=" << static_cast<uint32_t>(sender->kind());
        if (!m_rdmaSender)
        {
            m_rdmaSender = std::move(sender);
            m_activeCoinId.store(coinId, std::memory_order_release);
        }
        else
        {
            m_idleSenders[coinId] = std::move(sender);
        }
        return true;
    }

    uint32_t CoincidenceClient::activeCoinId() const
    {
        return m_activeCoinId.load(std::memory_order_acquire);
    }

    void CoincidenceClient::setActiveCoinId(uint32_t coinId)
    {
        std::lock_guard<std::mutex> senderLock(m_senderMutex);
        const uint32_t cur = m_activeCoinId.load(std::memory_order_acquire);
        if (cur == coinId && m_rdmaSender)
        {
            return;
        }
        if (m_rdmaSender)
        {
            m_idleSenders[cur] = std::move(m_rdmaSender);
        }
        auto it = m_idleSenders.find(coinId);
        if (it == m_idleSenders.end())
        {
            LOG(ERROR) << "setActiveCoinId: no session for coin " << coinId;
            return;
        }
        m_rdmaSender = std::move(it->second);
        m_idleSenders.erase(it);
        m_activeCoinId.store(coinId, std::memory_order_release);
        LOG(INFO) << "active coin switched to " << coinId << " node=" << m_config.nodeId;
    }

    void CoincidenceClient::destroyTxD2hDeviceResources(TxD2hResources *res)
    {
        if (!res)
        {
            return;
        }
        if (res->device >= 0)
        {
            static_cast<void>(cudaSetDevice(res->device));
        }
        for (void *ev : res->events)
        {
            if (ev)
            {
                static_cast<void>(cudaEventDestroy(static_cast<cudaEvent_t>(ev)));
            }
        }
        res->events.clear();
        if (res->stream)
        {
            static_cast<void>(cudaStreamDestroy(static_cast<cudaStream_t>(res->stream)));
            res->stream = nullptr;
        }
        res->device = -1;
    }

    void CoincidenceClient::destroyTxD2hResources()
    {
        for (auto &[device, res] : m_txD2hByDevice)
        {
            (void)device;
            destroyTxD2hDeviceResources(&res);
        }
        m_txD2hByDevice.clear();
    }

    bool CoincidenceClient::ensureTxD2hStream(int device)
    {
        if (device < 0)
        {
            return false;
        }
        auto it = m_txD2hByDevice.find(device);
        if (it != m_txD2hByDevice.end() && it->second.stream && !it->second.events.empty())
        {
            const cudaError_t setErr = cudaSetDevice(device);
            if (setErr != cudaSuccess)
            {
                LOG(ERROR) << "cudaSetDevice failed for TX D2H stream: "
                           << cudaGetErrorString(setErr);
                return false;
            }
            return true;
        }
        if (it != m_txD2hByDevice.end())
        {
            destroyTxD2hDeviceResources(&it->second);
            m_txD2hByDevice.erase(it);
        }

        const cudaError_t setErr = cudaSetDevice(device);
        if (setErr != cudaSuccess)
        {
            LOG(ERROR) << "cudaSetDevice failed for TX D2H stream: " << cudaGetErrorString(setErr);
            return false;
        }
        cudaStream_t stream = nullptr;
        const cudaError_t stErr =
            cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking);
        if (stErr != cudaSuccess || !stream)
        {
            LOG(ERROR) << "cudaStreamCreate failed for TX D2H: " << cudaGetErrorString(stErr);
            return false;
        }
        TxD2hResources res;
        res.device = device;
        res.stream = stream;
        const uint32_t nEvents = std::max<uint32_t>(
            2, m_rdmaSender ? m_rdmaSender->txStagingSlotCount() : 2);
        res.events.resize(nEvents, nullptr);
        for (uint32_t i = 0; i < nEvents; ++i)
        {
            cudaEvent_t ev = nullptr;
            const cudaError_t evErr =
                cudaEventCreateWithFlags(&ev, cudaEventDisableTiming);
            if (evErr != cudaSuccess || !ev)
            {
                LOG(ERROR) << "cudaEventCreate failed for TX D2H: " << cudaGetErrorString(evErr);
                destroyTxD2hDeviceResources(&res);
                return false;
            }
            res.events[i] = ev;
        }
        m_txD2hByDevice[device] = std::move(res);
        const uint64_t creates = m_txD2hStreamCreates.fetch_add(1, std::memory_order_relaxed) + 1;
        LOG(INFO) << "TX D2H stream created device=" << device << " creates=" << creates;
        return true;
    }

    bool CoincidenceClient::remapTxSlotPayload(Single *dst, uint32_t n, uint32_t absIndex)
    {
        if (!dst || !m_config.remapLocalToGlobalChannels)
        {
            return true;
        }
        for (uint32_t i = 0; i < n; ++i)
        {
            const uint64_t globalChannel =
                static_cast<uint64_t>(dst[i].channelIndex) +
                static_cast<uint64_t>(m_config.globalChannelOffset);
            if (globalChannel > static_cast<uint64_t>(std::numeric_limits<uint16_t>::max()))
            {
                LOG(ERROR) << "remapped channel index overflow: " << globalChannel;
                return false;
            }
            dst[i].channelIndex = static_cast<uint16_t>(globalChannel);
            if (absIndex + i == 0 && !m_remapSampleLogged.exchange(true))
            {
                LOG(INFO) << "remap sample node=" << m_config.nodeId
                          << " global_channel=" << dst[i].channelIndex;
            }
        }
        return true;
    }

    bool CoincidenceClient::remapChannels(std::vector<Single> *singles)
    {
        if (!singles || !m_config.remapLocalToGlobalChannels)
        {
            return true;
        }

        for (size_t i = 0; i < singles->size(); ++i)
        {
            auto &s = (*singles)[i];
            const uint64_t globalChannel = static_cast<uint64_t>(s.channelIndex) +
                                           static_cast<uint64_t>(m_config.globalChannelOffset);
            if (globalChannel > static_cast<uint64_t>(std::numeric_limits<uint16_t>::max()))
            {
                LOG(ERROR) << "remapped channel index overflow: " << globalChannel;
                return false;
            }

            const uint16_t localChannel = s.channelIndex;
            s.channelIndex = static_cast<uint16_t>(globalChannel);

            if (i == 0 && !m_remapSampleLogged.exchange(true))
            {
                LOG(INFO) << "remap sample node=" << m_config.nodeId
                          << " local_channel=" << localChannel
                          << " global_channel=" << s.channelIndex;
            }
        }
        return true;
    }

    bool CoincidenceClient::waitIfPausedOrStopped()
    {
        if (!m_paused.load(std::memory_order_acquire) &&
            !m_stopProduce.load(std::memory_order_acquire))
        {
            return m_running.load();
        }
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cv.wait(lock, [this]()
                  {
                      return !m_paused.load(std::memory_order_acquire) ||
                             m_stopProduce.load(std::memory_order_acquire) ||
                             !m_running.load();
                  });
        return m_running.load() && !m_stopProduce.load(std::memory_order_acquire);
    }

    bool CoincidenceClient::useRoceTxFill() const
    {
        return m_rdmaSender &&
               m_rdmaSender->kind() == rdma::DataPlaneKind::RdmaRoceV2;
    }

    bool CoincidenceClient::sendPackedOnCallerThread(
        uint64_t chunkId,
        uint64_t computerClock_ms,
        uint32_t duration_ms,
        const Single *src,
        uint32_t count)
    {
        if (!m_rdmaSender)
        {
            return false;
        }
        if (count == 0)
        {
            return true;
        }
        if (!src)
        {
            return false;
        }
        std::lock_guard<std::mutex> senderLock(m_senderMutex);
        if (!m_rdmaSender)
        {
            return false;
        }
        const bool ok = m_rdmaSender->sendPackedSingles(
            chunkId, computerClock_ms, duration_ms, src, count);
        m_connected = ok;
        return ok;
    }

    bool CoincidenceClient::fillRoceTxAndCommit(
        std::span<const Single> singles,
        uint64_t computerClock_ms,
        uint32_t duration_ms)
    {
        if (singles.empty())
        {
            return true;
        }
        std::lock_guard<std::mutex> senderLock(m_senderMutex);
        if (!m_rdmaSender)
        {
            return false;
        }

        const size_t maxPerSlot = rdma::maxSinglesPerSlot(m_rdmaSender->slotStride());
        if (maxPerSlot == 0)
        {
            return false;
        }

        const uint64_t chunkId = m_chunkIdCounter++;
        const uint32_t total = static_cast<uint32_t>(singles.size());
        const bool srcDevice = singlesOnDevice(singles.data());
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
                    LOG(ERROR) << "cudaSetDevice failed before TX D2H: " << cudaGetErrorString(setErr);
                    return false;
                }
            }
        }

        cudaStream_t d2hStream = nullptr;
        std::vector<void *> *d2hEvents = nullptr;
        const bool useAsyncD2h =
            srcDevice && srcDeviceId >= 0 && ensureTxD2hStream(srcDeviceId);
        if (useAsyncD2h)
        {
            auto it = m_txD2hByDevice.find(srcDeviceId);
            if (it != m_txD2hByDevice.end() && it->second.stream && !it->second.events.empty())
            {
                d2hStream = static_cast<cudaStream_t>(it->second.stream);
                d2hEvents = &it->second.events;
            }
        }

        const auto abortPending = [this, d2hStream](std::deque<PendingTxFill> *q)
        {
            if (d2hStream)
            {
                static_cast<void>(cudaStreamSynchronize(d2hStream));
            }
            if (!q)
            {
                return;
            }
            for (auto &p : *q)
            {
                m_rdmaSender->abortTxSlot(p.lease);
            }
            q->clear();
        };
        const auto commitFilled = [&](PendingTxFill &p) -> bool
        {
            auto *dst = reinterpret_cast<Single *>(p.lease.payload);
            if (!remapTxSlotPayload(dst, p.n, p.absOffset))
            {
                return false;
            }
            const rdma::SlotHeader hdr = makeTxSlotHeader(
                chunkId, computerClock_ms, duration_ms, p.first, p.last, p.partial);
            if (!m_rdmaSender->commitTxSlot(p.lease, hdr, p.n))
            {
                LOG(ERROR) << "commitTxSlot failed node=" << m_config.nodeId;
                m_connected = false;
                return false;
            }
            m_totalSinglesSent += p.n;
            return true;
        };

        if (!d2hStream || !d2hEvents)
        {
            uint32_t offset = 0;
            bool first = true;
            while (offset < total)
            {
                if (!waitIfPausedOrStopped())
                {
                    return false;
                }

                rdma::TxSlotLease lease{};
                if (!m_rdmaSender->acquireTxSlot(&lease) || !lease.payload)
                {
                    LOG(ERROR) << "acquireTxSlot failed node=" << m_config.nodeId;
                    return false;
                }

                uint32_t n = static_cast<uint32_t>(
                    std::min(maxPerSlot, static_cast<size_t>(total - offset)));
                const size_t capSingles = lease.payloadCapacity / kPackedSingleSize;
                if (capSingles == 0)
                {
                    m_rdmaSender->abortTxSlot(lease);
                    return false;
                }
                n = static_cast<uint32_t>(std::min<size_t>(n, capSingles));

                auto *dst = reinterpret_cast<Single *>(lease.payload);
                if (!copySinglesIntoTxPayload(dst, singles.data() + offset, n, srcDevice))
                {
                    m_rdmaSender->abortTxSlot(lease);
                    return false;
                }
                PendingTxFill p{};
                p.lease = lease;
                p.n = n;
                p.absOffset = offset;
                p.first = first;
                p.last = (offset + n >= total);
                p.partial = (n < total);
                if (!commitFilled(p))
                {
                    m_rdmaSender->abortTxSlot(lease);
                    return false;
                }
                first = false;
                offset += n;
            }
            return true;
        }

        auto *stream = d2hStream;
        auto &events = *d2hEvents;
        const uint32_t slotCount = std::max<uint32_t>(1, m_rdmaSender->txStagingSlotCount());
        const size_t depth = static_cast<size_t>(slotCount);
        std::deque<PendingTxFill> pending;
        uint32_t offset = 0;
        bool first = true;

        const auto enqueueSlice = [&]() -> bool
        {
            if (!waitIfPausedOrStopped())
            {
                return false;
            }
            rdma::TxSlotLease lease{};
            if (!m_rdmaSender->acquireTxSlot(&lease) || !lease.payload)
            {
                LOG(ERROR) << "acquireTxSlot failed node=" << m_config.nodeId;
                return false;
            }
            uint32_t n = static_cast<uint32_t>(
                std::min(maxPerSlot, static_cast<size_t>(total - offset)));
            const size_t capSingles = lease.payloadCapacity / kPackedSingleSize;
            if (capSingles == 0 || lease.localIndex >= events.size() ||
                !events[lease.localIndex])
            {
                m_rdmaSender->abortTxSlot(lease);
                return false;
            }
            n = static_cast<uint32_t>(std::min<size_t>(n, capSingles));
            auto *dst = reinterpret_cast<Single *>(lease.payload);
            const size_t bytes = static_cast<size_t>(n) * kPackedSingleSize;
            const cudaError_t copyErr = cudaMemcpyAsync(
                dst, singles.data() + offset, bytes, cudaMemcpyDeviceToHost, stream);
            if (copyErr != cudaSuccess)
            {
                LOG(ERROR) << "cudaMemcpyAsync D2H into TX failed: "
                           << cudaGetErrorString(copyErr);
                m_rdmaSender->abortTxSlot(lease);
                return false;
            }
            auto *ev = static_cast<cudaEvent_t>(events[lease.localIndex]);
            const cudaError_t recErr = cudaEventRecord(ev, stream);
            if (recErr != cudaSuccess)
            {
                LOG(ERROR) << "cudaEventRecord failed: " << cudaGetErrorString(recErr);
                static_cast<void>(cudaStreamSynchronize(stream));
                m_rdmaSender->abortTxSlot(lease);
                return false;
            }
            PendingTxFill p{};
            p.lease = lease;
            p.n = n;
            p.absOffset = offset;
            p.first = first;
            p.last = (offset + n >= total);
            p.partial = (n < total);
            p.event = ev;
            pending.push_back(p);
            first = false;
            offset += n;
            return true;
        };

        while (offset < total || !pending.empty())
        {
            while (pending.size() < depth && offset < total)
            {
                if (!enqueueSlice())
                {
                    abortPending(&pending);
                    return false;
                }
            }
            if (!pending.empty())
            {
                PendingTxFill &p = pending.front();
                const cudaError_t waitErr = cudaEventSynchronize(p.event);
                if (waitErr != cudaSuccess || !commitFilled(p))
                {
                    if (waitErr != cudaSuccess)
                    {
                        LOG(ERROR) << "cudaEventSynchronize failed: "
                                   << cudaGetErrorString(waitErr);
                    }
                    abortPending(&pending);
                    return false;
                }
                pending.pop_front();
            }
        }
        return true;
    }

    bool CoincidenceClient::sendSingles(
        const std::vector<Single> &singles,
        uint64_t computerClock_ms,
        uint32_t duration_ms)
    {
        return sendSingles(std::span<const Single>(singles), computerClock_ms, duration_ms);
    }

    bool CoincidenceClient::sendSingles(
        std::span<const Single> singles,
        uint64_t computerClock_ms,
        uint32_t duration_ms)
    {
        if (!m_running.load() || !waitIfPausedOrStopped())
        {
            return false;
        }
        m_sendInFlight.store(true, std::memory_order_release);
        bool ok = false;
        if (useRoceTxFill())
        {
            ok = fillRoceTxAndCommit(singles, computerClock_ms, duration_ms);
        }
        else if (singles.empty())
        {
            ok = true;
        }
        else
        {
            std::vector<Single> host;
            const Single *src = singles.data();
            uint32_t count = static_cast<uint32_t>(singles.size());
            const bool srcDevice = singlesOnDevice(singles.data());
            if (srcDevice || m_config.remapLocalToGlobalChannels)
            {
                host.resize(singles.size());
                if (srcDevice)
                {
                    cudaPointerAttributes attr{};
                    if (cudaPointerGetAttributes(&attr, singles.data()) == cudaSuccess &&
                        attr.device >= 0)
                    {
                        const cudaError_t setErr = cudaSetDevice(attr.device);
                        if (setErr != cudaSuccess)
                        {
                            LOG(ERROR) << "cudaSetDevice failed before InProcess D2H: "
                                       << cudaGetErrorString(setErr);
                            m_sendInFlight.store(false, std::memory_order_release);
                            m_cv.notify_all();
                            return false;
                        }
                    }
                }
                if (!copySinglesIntoTxPayload(
                        host.data(), singles.data(), count, srcDevice))
                {
                    m_sendInFlight.store(false, std::memory_order_release);
                    m_cv.notify_all();
                    return false;
                }
                if (!remapChannels(&host))
                {
                    m_sendInFlight.store(false, std::memory_order_release);
                    m_cv.notify_all();
                    return false;
                }
                src = host.data();
            }
            const uint64_t chunkId = m_chunkIdCounter++;
            ok = sendPackedOnCallerThread(
                chunkId, computerClock_ms, duration_ms, src, count);
            if (ok)
            {
                m_totalSinglesSent += count;
            }
        }
        m_sendInFlight.store(false, std::memory_order_release);
        m_cv.notify_all();
        return ok;
    }

    bool CoincidenceClient::sendSingles(
        std::vector<Single> &&singles,
        uint64_t computerClock_ms,
        uint32_t duration_ms)
    {
        if (!m_running.load() || !waitIfPausedOrStopped())
        {
            return false;
        }
        if (useRoceTxFill())
        {
            return sendSingles(std::span<const Single>(singles), computerClock_ms, duration_ms);
        }
        if (!remapChannels(&singles))
        {
            return false;
        }
        m_sendInFlight.store(true, std::memory_order_release);
        const uint32_t count = static_cast<uint32_t>(singles.size());
        const uint64_t chunkId = m_chunkIdCounter++;
        const bool ok = sendPackedOnCallerThread(
            chunkId, computerClock_ms, duration_ms, singles.data(), count);
        if (ok)
        {
            m_totalSinglesSent += count;
        }
        m_sendInFlight.store(false, std::memory_order_release);
        m_cv.notify_all();
        return ok;
    }

    bool CoincidenceClient::sendSinglesView(
        std::span<const Single> singles,
        uint64_t computerClock_ms,
        uint32_t duration_ms)
    {
        if (m_config.remapLocalToGlobalChannels ||
            (!singles.empty() && singlesOnDevice(singles.data())))
        {
            return sendSingles(singles, computerClock_ms, duration_ms);
        }
        if (!m_running.load() || !waitIfPausedOrStopped())
        {
            return false;
        }
        if (useRoceTxFill())
        {
            m_sendInFlight.store(true, std::memory_order_release);
            const bool ok = fillRoceTxAndCommit(singles, computerClock_ms, duration_ms);
            m_sendInFlight.store(false, std::memory_order_release);
            m_cv.notify_all();
            return ok;
        }

        m_sendInFlight.store(true, std::memory_order_release);
        const uint32_t count = static_cast<uint32_t>(singles.size());
        const uint64_t chunkId = m_chunkIdCounter++;
        const bool ok = sendPackedOnCallerThread(
            chunkId, computerClock_ms, duration_ms,
            singles.empty() ? nullptr : singles.data(), count);
        if (ok)
        {
            m_totalSinglesSent += count;
        }
        m_sendInFlight.store(false, std::memory_order_release);
        m_cv.notify_all();
        return ok;
    }

    bool CoincidenceClient::getServerStatus(coincidence::StatusResponse *response)
    {
        grpc::ClientContext context;
        coincidence::StatusRequest request;
        request.set_include_node_stats(true);

        grpc::Status status = m_stub->GetStatus(&context, request, response);
        return status.ok();
    }

    bool CoincidenceClient::waitUntilIdle()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cv.wait(lock, [this]()
                  { return !m_sendInFlight.load() || !m_running.load(); });
        return m_running.load();
    }

    bool CoincidenceClient::notifyProducerComplete()
    {
        waitUntilIdle();
        m_producerComplete.store(true, std::memory_order_release);

        grpc::ClientContext context;
        coincidence::NotifyProducerCompleteRequest request;
        request.set_node_id(m_config.nodeId);
        request.set_singles_sent(m_totalSinglesSent.load());
        coincidence::NotifyProducerCompleteResponse response;
        grpc::Status status = m_stub->NotifyProducerComplete(&context, request, &response);
        if (!status.ok() || !response.success())
        {
            LOG(ERROR) << "NotifyProducerComplete failed: "
                       << (status.ok() ? response.message() : status.error_message());
            return false;
        }
        LOG(INFO) << "Producer complete node=" << m_config.nodeId
                  << " singles_sent=" << m_totalSinglesSent.load()
                  << " all_complete=" << (response.all_complete() ? "true" : "false");
        return true;
    }

    rdma::DataPlaneKind CoincidenceClient::dataPlaneKind() const
    {
        return m_rdmaSender ? m_rdmaSender->kind() : rdma::DataPlaneKind::InProcess;
    }

    uint64_t CoincidenceClient::getTotalSinglesSent() const
    {
        return m_totalSinglesSent.load();
    }

    size_t CoincidenceClient::getPendingMessageCount() const
    {
        return m_rdmaSender ? static_cast<size_t>(m_rdmaSender->txSlotsBusy()) : 0;
    }

    bool CoincidenceClient::isRunning() const
    {
        return m_running.load();
    }

    bool CoincidenceClient::isConnected() const
    {
        return m_connected.load();
    }

    bool CoincidenceClient::isPaused() const
    {
        return m_paused.load(std::memory_order_acquire);
    }

    bool CoincidenceClient::stopProduceRequested() const
    {
        return m_stopProduce.load(std::memory_order_acquire);
    }

    uint64_t CoincidenceClient::lastRttMs() const
    {
        return m_lastRttMs.load(std::memory_order_acquire);
    }

    coincidence::SourceState CoincidenceClient::sourceState() const
    {
        return currentSourceState();
    }

    uint32_t CoincidenceClient::rdmaSlotCount() const
    {
        return m_rdmaSender ? m_rdmaSender->slotCount() : 0;
    }

    uint32_t CoincidenceClient::txStagingSlotCount() const
    {
        return m_rdmaSender ? m_rdmaSender->txStagingSlotCount() : 0;
    }

    uint64_t CoincidenceClient::rdmaInprocessHandle() const
    {
        return m_rdmaSender ? m_rdmaSender->inprocessHandle() : 0;
    }

    uint64_t CoincidenceClient::rdmaSlotsInFlight() const
    {
        return m_rdmaSender ? m_rdmaSender->slotsInFlight() : 0;
    }

    uint32_t CoincidenceClient::rdmaCreditRemaining() const
    {
        return m_rdmaSender ? m_rdmaSender->creditRemaining() : 0;
    }

    uint64_t CoincidenceClient::txD2hStreamCreateCount() const
    {
        return m_txD2hStreamCreates.load(std::memory_order_relaxed);
    }

    void CoincidenceClient::setTelemetryHook(std::function<WorkerTelemetry()> hook)
    {
        std::lock_guard<std::mutex> lock(m_telemetryMutex);
        m_telemetryHook = std::move(hook);
    }

    bool CoincidenceClient::waitForServerStartSignal(uint32_t timeoutMs)
    {
        const uint64_t startTs = nowMs();

        while (m_running.load())
        {
            grpc::ClientContext context;
            coincidence::WaitForStartRequest request;
            request.set_node_id(m_config.nodeId);
            request.set_timeout_ms(m_config.waitForStartRpcTimeoutMs);

            coincidence::WaitForStartResponse response;
            grpc::Status status = m_stub->WaitForStart(&context, request, &response);

            if (status.ok() && response.success() && response.start_signal_issued())
            {
                LOG(INFO) << "Start signal received (wall-clock wait skipped; PET time is in singles)";
                return true;
            }

            if (!status.ok())
            {
                LOG(ERROR) << "WaitForStart RPC failed: " << status.error_message();
            }

            if (timeoutMs > 0)
            {
                const uint64_t elapsed = nowMs() - startTs;
                if (elapsed >= timeoutMs)
                {
                    LOG(ERROR) << "WaitForStart timed out after " << elapsed << " ms";
                    return false;
                }
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(m_config.waitForStartRetryIntervalMs));
        }

        return false;
    }

    uint64_t CoincidenceClient::nowMs()
    {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
    }

    bool CoincidenceClient::registerNode()
    {
        grpc::ClientContext context;
        coincidence::RegisterNodeRequest request;
        request.set_node_id(m_config.nodeId);
        request.set_node_address(m_config.nodeAddress);
        request.set_channel_count(m_config.channelCount);
        request.set_detector_type(m_config.detectorType);

        coincidence::RegisterNodeResponse response;
        grpc::Status status = m_stub->RegisterNode(&context, request, &response);

        if (status.ok() && response.success())
        {
            LOG(INFO) << "Node " << m_config.nodeId << " registered successfully";
            m_connected = true;
            return true;
        }

        LOG(ERROR) << "Failed to register node: "
                   << (status.ok() ? response.message() : status.error_message());
        return false;
    }

    void CoincidenceClient::heartbeatLoop()
    {
        while (m_running.load())
        {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(m_config.heartbeatIntervalMs == 0
                                              ? 1000u
                                              : m_config.heartbeatIntervalMs));

            if (!m_running.load())
            {
                break;
            }

            grpc::ClientContext context;
            coincidence::HeartbeatRequest request;
            request.set_node_id(m_config.nodeId);
            const uint64_t sendTs = nowMs();
            request.set_timestamp_ms(sendTs);
            request.set_singles_sent(m_totalSinglesSent.load());
            request.set_producer_complete(m_producerComplete.load(std::memory_order_acquire));
            fillHeartbeatTelemetry(&request);

            coincidence::HeartbeatResponse response;
            grpc::Status status = m_stub->Heartbeat(&context, request, &response);

            if (!status.ok())
            {
                m_connected = false;
                LOG(WARNING) << "Heartbeat failed: " << status.error_message();
                continue;
            }

            m_connected = true;
            if (response.echo_timestamp_ms() != 0)
            {
                const uint64_t now = nowMs();
                if (now >= response.echo_timestamp_ms())
                {
                    m_lastRttMs.store(now - response.echo_timestamp_ms(), std::memory_order_release);
                }
            }
            applyProducerCommand(response.command(), response.active_coin_id());
        }
    }

    coincidence::SourceState CoincidenceClient::currentSourceState() const
    {
        if (m_producerComplete.load(std::memory_order_acquire))
        {
            return coincidence::SOURCE_STATE_COMPLETE;
        }
        if (m_paused.load(std::memory_order_acquire))
        {
            return coincidence::SOURCE_STATE_PAUSED;
        }
        if (m_running.load(std::memory_order_acquire))
        {
            return coincidence::SOURCE_STATE_RUNNING;
        }
        return coincidence::SOURCE_STATE_IDLE;
    }

    void CoincidenceClient::fillHeartbeatTelemetry(coincidence::HeartbeatRequest *request)
    {
        request->set_source_state(currentSourceState());
        request->set_chunks_pending(getPendingMessageCount());
        request->set_chunks_pending_cap(
            m_rdmaSender ? m_rdmaSender->txStagingSlotCount() : 0u);
        request->set_last_rtt_ms(m_lastRttMs.load(std::memory_order_acquire));
        if (m_rdmaSender)
        {
            request->set_rdma_slot_count(m_rdmaSender->slotCount());
            request->set_rdma_slots_in_flight(m_rdmaSender->slotsInFlight());
            request->set_rdma_credit_remaining(m_rdmaSender->creditRemaining());
        }
        WorkerTelemetry tel;
        {
            std::lock_guard<std::mutex> lock(m_telemetryMutex);
            if (m_telemetryHook)
            {
                tel = m_telemetryHook();
            }
        }
        request->set_r2s_singles_out(tel.r2sSinglesOut);
        request->set_r2s_lease_used(tel.r2sLeaseUsed);
        request->set_r2s_lease_cap(tel.r2sLeaseCap);
        request->set_acq_packets_total(tel.acqPacketsTotal);
        request->set_acq_bytes_total(tel.acqBytesTotal);
        request->set_acq_running(tel.acqRunning);
    }

    void CoincidenceClient::applyProducerCommand(coincidence::ProducerCommand command,
                                                 uint32_t activeCoinId)
    {
        const bool switchCoin =
            command == coincidence::CMD_SET_ACTIVE_COIN ||
            (!m_config.destinations.empty() &&
             activeCoinId != m_activeCoinId.load(std::memory_order_acquire));
        if (switchCoin)
        {
            setActiveCoinId(activeCoinId);
        }
        switch (command)
        {
        case coincidence::CMD_SET_ACTIVE_COIN:
            break;
        case coincidence::CMD_PAUSE_PRODUCE:
            if (!m_paused.exchange(true))
            {
                LOG(INFO) << "Producer paused node=" << m_config.nodeId;
            }
            m_cv.notify_all();
            break;
        case coincidence::CMD_START_PRODUCE:
            if (m_paused.exchange(false))
            {
                LOG(INFO) << "Producer resumed node=" << m_config.nodeId;
            }
            m_cv.notify_all();
            break;
        case coincidence::CMD_STOP_PRODUCE:
        case coincidence::CMD_DRAIN:
            if (m_producerComplete.load(std::memory_order_acquire))
            {
                break;
            }
            if (m_stopProduce.exchange(true))
            {
                break;
            }
            LOG(INFO) << "Producer stop requested node=" << m_config.nodeId;
            m_paused.store(false, std::memory_order_release);
            m_cv.notify_all();
            (void)notifyProducerComplete();
            break;
        case coincidence::CMD_NONE:
        default:
            break;
        }
    }

} // namespace openpni::distributed::streaming
