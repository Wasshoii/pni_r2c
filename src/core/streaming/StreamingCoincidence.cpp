#include "core/streaming/StreamingCoincidence.hpp"

#include "core/streaming/multi_gpu/CoincidenceMultiGpuEngine.hpp"

#include <algorithm>
#include <chrono>
#include <iterator>
#include <limits>
#include <string>
#include <utility>
#include <glog/logging.h>

namespace openpni::distributed::streaming
{

    SharedMemoryPool::SharedMemoryPool(size_t maxMemoryBytes)
        : m_maxMemoryBytes(maxMemoryBytes), m_usedMemoryBytes(0)
    {
    }

    bool SharedMemoryPool::tryAllocate(size_t bytes, uint32_t timeoutMs)
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        auto canAllocate = [this, bytes]()
        {
            return m_usedMemoryBytes + bytes <= m_maxMemoryBytes || m_closed;
        };

        if (timeoutMs == 0)
        {
            m_cvAvailable.wait(lock, canAllocate);
        }
        else
        {
            if (!m_cvAvailable.wait_for(lock,
                                        std::chrono::milliseconds(timeoutMs),
                                        canAllocate))
            {
                return false;
            }
        }

        if (m_closed)
        {
            return false;
        }

        m_usedMemoryBytes += bytes;
        m_peakMemoryBytes = std::max(m_peakMemoryBytes, m_usedMemoryBytes);
        m_totalAllocations++;
        return true;
    }

    void SharedMemoryPool::release(size_t bytes)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (bytes > m_usedMemoryBytes)
        {
            LOG(WARNING) << "[SharedMemoryPool] releasing more than allocated ("
                         << bytes << " > " << m_usedMemoryBytes << ")";
            m_usedMemoryBytes = 0;
        }
        else
        {
            m_usedMemoryBytes -= bytes;
        }
        m_cvAvailable.notify_all();
    }

    size_t SharedMemoryPool::getUsedMemory() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_usedMemoryBytes;
    }

    size_t SharedMemoryPool::getPeakMemory() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_peakMemoryBytes;
    }

    size_t SharedMemoryPool::getMaxMemory() const
    {
        return m_maxMemoryBytes;
    }

    double SharedMemoryPool::getUsageRatio() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return static_cast<double>(m_usedMemoryBytes) / m_maxMemoryBytes;
    }

    size_t SharedMemoryPool::getTotalAllocations() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_totalAllocations;
    }

    void SharedMemoryPool::close()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_closed = true;
        m_cvAvailable.notify_all();
    }

    SharedMemoryPool::MemoryStatus SharedMemoryPool::getStatus() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return MemoryStatus{
            m_usedMemoryBytes,
            m_maxMemoryBytes,
            static_cast<double>(m_usedMemoryBytes) / m_maxMemoryBytes};
    }

    void SharedMemoryPool::printStatus() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        LOG(INFO) << "[SharedMemoryPool] Status\n"
                  << "  Max memory:   " << (m_maxMemoryBytes / 1024.0 / 1024.0) << " MB\n"
                  << "  Used memory:  " << (m_usedMemoryBytes / 1024.0 / 1024.0) << " MB ("
                  << (100.0 * m_usedMemoryBytes / m_maxMemoryBytes) << "%)\n"
                  << "  Peak memory:  " << (m_peakMemoryBytes / 1024.0 / 1024.0) << " MB\n"
                  << "  Allocations:  " << m_totalAllocations;
    }

    void TimestampedSingleChunk::updateTimeRange()
    {
        if (singles.empty())
        {
            minTime_pico = UINT64_MAX;
            maxTime_pico = 0;
            return;
        }

        minTime_pico = UINT64_MAX;
        maxTime_pico = 0;
        for (const auto &s : singles)
        {
            minTime_pico = std::min(minTime_pico, s.timevalue_100fs);
            maxTime_pico = std::max(maxTime_pico, s.timevalue_100fs);
        }
    }

    size_t TimestampedSingleChunk::memorySize() const
    {
        return sizeof(TimestampedSingleChunk) +
               singles.capacity() * sizeof(Single);
    }

    size_t TimestampedSingleChunk::singlesMemorySize() const
    {
        return singles.capacity() * sizeof(Single);
    }

    bool TimestampedSingleChunk::operator<(const TimestampedSingleChunk &other) const
    {
        return minTime_pico < other.minTime_pico;
    }

    NodeRingBuffer::NodeRingBuffer(uint16_t nodeId, size_t maxChunks, SharedMemoryPool *memoryPool)
        : m_nodeId(nodeId), m_maxChunks(maxChunks), m_memoryPool(memoryPool)
    {
    }

    bool NodeRingBuffer::push(TimestampedSingleChunk &&chunk, uint32_t timeoutMs)
    {
        size_t chunkMemory = chunk.memorySize();

        if (m_memoryPool)
        {
            if (!m_memoryPool->tryAllocate(chunkMemory, timeoutMs))
            {
                LOG(WARNING) << "[NodeRingBuffer] Node " << m_nodeId
                             << " failed to allocate memory for chunk "
                             << chunk.chunkId;
                return false;
            }
        }

        std::unique_lock<std::mutex> lock(m_mutex);

        auto canPush = [this]()
        { return m_buffer.size() < m_maxChunks || m_closed; };

        if (timeoutMs == 0)
        {
            m_cvNotFull.wait(lock, canPush);
        }
        else
        {
            if (!m_cvNotFull.wait_for(lock, std::chrono::milliseconds(timeoutMs), canPush))
            {
                if (m_memoryPool)
                {
                    m_memoryPool->release(chunkMemory);
                }
                LOG(WARNING) << "[NodeRingBuffer] Node " << m_nodeId
                             << " push timeout for chunk " << chunk.chunkId;
                return false;
            }
        }

        if (m_closed)
        {
            if (m_memoryPool)
            {
                m_memoryPool->release(chunkMemory);
            }
            return false;
        }

        if (m_expectedChunkId > 0 && chunk.chunkId != m_expectedChunkId)
        {
            m_outOfOrderCount++;
            LOG(WARNING) << "[NodeRingBuffer] Node " << m_nodeId
                         << " received chunk " << chunk.chunkId
                         << ", expected " << m_expectedChunkId
                         << " (out of order or gap)";
        }
        m_expectedChunkId = chunk.chunkId + 1;

        chunk.updateTimeRange();
        m_maxEventTimeReceived = std::max(m_maxEventTimeReceived, chunk.maxTime_pico);
        m_bufferMemoryBytes += chunkMemory;
        m_lastPushTime = std::chrono::steady_clock::now();

        if (m_buffer.empty() || chunk.minTime_pico >= m_buffer.back().minTime_pico)
        {
            m_buffer.push_back(std::move(chunk));
        }
        else
        {
            auto it = std::lower_bound(m_buffer.begin(), m_buffer.end(), chunk);
            m_buffer.insert(it, std::move(chunk));
            m_reorderedCount++;
        }

        m_cvNotEmpty.notify_one();

        // 在锁外通知处理线程，避免它被唤醒后立刻又阻塞在本缓冲的锁上。
        lock.unlock();
        if (m_pushObserver)
        {
            m_pushObserver();
        }
        return true;
    }

    void NodeRingBuffer::setPushObserver(std::function<void()> observer)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_pushObserver = std::move(observer);
    }

    const TimestampedSingleChunk *NodeRingBuffer::front() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_buffer.empty() ? nullptr : &m_buffer.front();
    }

    std::optional<TimestampedSingleChunk> NodeRingBuffer::pop()
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        m_cvNotEmpty.wait(lock, [this]()
                          { return !m_buffer.empty() || m_closed; });

        if (m_buffer.empty())
        {
            return std::nullopt;
        }

        auto chunk = std::move(m_buffer.front());
        size_t chunkMemory = chunk.memorySize();
        m_buffer.pop_front();
        m_bufferMemoryBytes -= chunkMemory;

        if (m_memoryPool)
        {
            lock.unlock();
            m_memoryPool->release(chunkMemory);
        }

        m_cvNotFull.notify_one();
        return chunk;
    }

    std::optional<TimestampedSingleChunk> NodeRingBuffer::tryPop()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_buffer.empty())
        {
            return std::nullopt;
        }

        auto chunk = std::move(m_buffer.front());
        size_t chunkMemory = chunk.memorySize();
        m_buffer.pop_front();
        m_bufferMemoryBytes -= chunkMemory;

        if (m_memoryPool)
        {
            m_memoryPool->release(chunkMemory);
        }

        m_cvNotFull.notify_one();
        return chunk;
    }

    std::vector<Single> NodeRingBuffer::extractSinglesBefore(uint64_t boundary)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        std::vector<Single> result;
        size_t releasedMemory = 0;

        while (!m_buffer.empty())
        {
            auto &frontChunk = m_buffer.front();

            if (frontChunk.maxTime_pico <= boundary)
            {
                size_t chunkMemory = frontChunk.memorySize();
                result.insert(result.end(),
                              std::make_move_iterator(frontChunk.singles.begin()),
                              std::make_move_iterator(frontChunk.singles.end()));
                m_buffer.pop_front();

                if (chunkMemory <= m_bufferMemoryBytes)
                {
                    m_bufferMemoryBytes -= chunkMemory;
                }
                else
                {
                    m_bufferMemoryBytes = 0;
                }
                releasedMemory += chunkMemory;
            }
            else if (frontChunk.minTime_pico <= boundary)
            {
                auto splitPoint = std::upper_bound(
                    frontChunk.singles.begin(),
                    frontChunk.singles.end(),
                    boundary,
                    [](uint64_t bound, const Single &s)
                    {
                        return bound < s.timevalue_100fs;
                    });

                if (splitPoint != frontChunk.singles.begin())
                {
                    result.insert(result.end(),
                                  std::make_move_iterator(frontChunk.singles.begin()),
                                  std::make_move_iterator(splitPoint));

                    frontChunk.singles.erase(frontChunk.singles.begin(), splitPoint);
                    frontChunk.updateTimeRange();
                }

                if (frontChunk.singles.empty())
                {
                    size_t remainingMemory = frontChunk.memorySize();
                    m_buffer.pop_front();

                    if (remainingMemory <= m_bufferMemoryBytes)
                    {
                        m_bufferMemoryBytes -= remainingMemory;
                    }
                    else
                    {
                        m_bufferMemoryBytes = 0;
                    }
                    releasedMemory += remainingMemory;
                }

                break;
            }
            else
            {
                break;
            }
        }

        if (m_memoryPool && releasedMemory > 0)
        {
            lock.unlock();
            m_memoryPool->release(releasedMemory);
        }

        if (!result.empty())
        {
            m_cvNotFull.notify_all();
        }
        return result;
    }

    size_t NodeRingBuffer::extractSinglesBeforeInto(uint64_t boundary, Single *dst, size_t cap,
                                                    bool *truncated)
    {
        if (dst == nullptr)
        {
            return 0;
        }

        std::unique_lock<std::mutex> lock(m_mutex);
        size_t written = 0;
        size_t releasedMemory = 0;

        auto releaseFront = [&]()
        {
            const size_t chunkMemory = m_buffer.front().memorySize();
            m_buffer.pop_front();
            m_bufferMemoryBytes =
                chunkMemory <= m_bufferMemoryBytes ? m_bufferMemoryBytes - chunkMemory : 0;
            releasedMemory += chunkMemory;
        };

        while (!m_buffer.empty() && written < cap)
        {
            auto &frontChunk = m_buffer.front();

            if (frontChunk.minTime_pico > boundary)
            {
                break;
            }

            // 整块可取时直接拷走；否则只取时间前缀。
            auto takeEnd = frontChunk.singles.end();
            if (frontChunk.maxTime_pico > boundary)
            {
                takeEnd = std::upper_bound(
                    frontChunk.singles.begin(), frontChunk.singles.end(), boundary,
                    [](uint64_t bound, const Single &s)
                    { return bound < s.timevalue_100fs; });
            }

            size_t available = static_cast<size_t>(
                std::distance(frontChunk.singles.begin(), takeEnd));
            if (available > cap - written)
            {
                available = cap - written;
                takeEnd = frontChunk.singles.begin() +
                          static_cast<std::ptrdiff_t>(available);
                if (truncated)
                {
                    *truncated = true;
                }
            }

            if (available > 0)
            {
                std::copy(frontChunk.singles.begin(), takeEnd, dst + written);
                written += available;
            }

            const bool wholeChunk =
                takeEnd == frontChunk.singles.end() && frontChunk.maxTime_pico <= boundary;
            if (wholeChunk)
            {
                releaseFront();
                continue;
            }

            if (available > 0)
            {
                frontChunk.singles.erase(frontChunk.singles.begin(), takeEnd);
                frontChunk.updateTimeRange();
            }
            if (frontChunk.singles.empty())
            {
                releaseFront();
            }
            break;
        }

        // 容量耗尽但边界内仍有残留：调用方必须知道，否则推进边界会漏配。
        if (truncated && written >= cap && !m_buffer.empty() &&
            m_buffer.front().minTime_pico <= boundary)
        {
            *truncated = true;
        }

        if (m_memoryPool && releasedMemory > 0)
        {
            lock.unlock();
            m_memoryPool->release(releasedMemory);
        }

        if (written > 0)
        {
            m_cvNotFull.notify_all();
        }
        return written;
    }

    uint64_t NodeRingBuffer::chunkBoundaryWithin(uint64_t boundary, size_t budget,
                                                 size_t *outCount) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        uint64_t bound = 0;
        size_t count = 0;

        for (const auto &chunk : m_buffer)
        {
            if (chunk.maxTime_pico > boundary)
            {
                break;
            }
            if (count + chunk.singles.size() > budget)
            {
                break;
            }
            count += chunk.singles.size();
            bound = chunk.maxTime_pico;
        }

        if (outCount)
        {
            *outCount = count;
        }
        return bound;
    }

    double NodeRingBuffer::occupancyRatio() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_maxChunks == 0)
        {
            return 0.0;
        }
        return static_cast<double>(m_buffer.size()) / static_cast<double>(m_maxChunks);
    }

    uint64_t NodeRingBuffer::lastPushAgeMs() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_lastPushTime.has_value())
        {
            return UINT64_MAX;
        }
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - *m_lastPushTime)
                .count());
    }

    size_t NodeRingBuffer::countSinglesBefore(uint64_t boundary) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        size_t n = 0;
        for (const auto &chunk : m_buffer)
        {
            if (chunk.maxTime_pico <= boundary)
            {
                n += chunk.singles.size();
                continue;
            }
            if (chunk.minTime_pico <= boundary)
            {
                auto splitPoint = std::upper_bound(
                    chunk.singles.begin(),
                    chunk.singles.end(),
                    boundary,
                    [](uint64_t bound, const Single &s)
                    {
                        return bound < s.timevalue_100fs;
                    });
                n += static_cast<size_t>(
                    std::distance(chunk.singles.begin(), splitPoint));
            }
            break;
        }
        return n;
    }

    std::vector<TimestampedSingleChunk> NodeRingBuffer::extractCompleteBefore(uint64_t boundary)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::vector<TimestampedSingleChunk> result;

        size_t releasedBytes = 0;
        while (!m_buffer.empty() && m_buffer.front().maxTime_pico <= boundary)
        {
            releasedBytes += m_buffer.front().memorySize();
            result.push_back(std::move(m_buffer.front()));
            m_buffer.pop_front();
        }

        if (releasedBytes > 0)
        {
            m_bufferMemoryBytes -= releasedBytes;
            if (m_memoryPool)
            {
                m_memoryPool->release(releasedBytes);
            }
        }

        if (!result.empty())
        {
            m_cvNotFull.notify_one();
        }
        return result;
    }

    uint64_t NodeRingBuffer::getFrontMinTime() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_buffer.empty() ? UINT64_MAX : m_buffer.front().minTime_pico;
    }

    uint64_t NodeRingBuffer::getMaxEventTime() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_maxEventTimeReceived;
    }

    size_t NodeRingBuffer::getOutOfOrderCount() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_outOfOrderCount;
    }

    size_t NodeRingBuffer::getReorderedCount() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_reorderedCount;
    }

    bool NodeRingBuffer::empty() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_buffer.empty();
    }

    size_t NodeRingBuffer::size() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_buffer.size();
    }

    uint16_t NodeRingBuffer::nodeId() const
    {
        return m_nodeId;
    }

    void NodeRingBuffer::close()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_closed = true;
        m_cvNotEmpty.notify_all();
        m_cvNotFull.notify_all();
    }

    bool NodeRingBuffer::isClosed() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_closed;
    }

    size_t NodeRingBuffer::getBufferMemoryBytes() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_bufferMemoryBytes;
    }

    uint64_t TimeAlignerConfig::getTotalSafetyMargin() const
    {
        const uint64_t coinWindow_ps = static_cast<uint64_t>(coinProtocol.timeWindow_ps);
        const uint64_t delayWindow_ps = static_cast<uint64_t>(coinProtocol.delayTime_ps);
        // Watermark 与 singles 时间戳同为 100fs；ps 配置值需 ×10
        return networkLatencyMargin_pico * 10
               + std::max(coinWindow_ps, delayWindow_ps) * 10;
    }

    uint64_t TimeAlignerConfig::overlapLength_100fs() const
    {
        const uint64_t coinWindow_ps = static_cast<uint64_t>(coinProtocol.timeWindow_ps);
        const uint64_t delayWindow_ps = static_cast<uint64_t>(coinProtocol.delayTime_ps);
        return (coinWindow_ps + delayWindow_ps) * 10ull;
    }

    uint64_t TimeAlignerConfig::minSegmentSpan_100fs() const
    {
        const uint64_t factor = std::max<uint32_t>(1u, minSegmentOverlapFactor);
        return overlapLength_100fs() * factor;
    }

    void ProcessingStatistics::reset()
    {
        totalSinglesReceived = 0;
        totalSinglesProcessed = 0;
        totalPromptPairs = 0;
        totalDelayPairs = 0;
        chunksProcessed = 0;
        avgProcessingTime_ms = 0;
        currentTimeBoundary_pico = 0;
        coinKernelNs = 0;
        extractNs = 0;
        sinkNs = 0;
        drainWaitNs = 0;
        writeQueueWaitNs = 0;
        coinKernelBatches = 0;
        triggerByWatermark = 0;
        triggerByPressure = 0;
        triggerByDeadline = 0;
        watermarkStallEvents = 0;
        degradedSegments = 0;
        heldByMinDuration = 0;
        oversizedSegments = 0;
        carrySinglesTotal = 0;
        maxNodeBacklogChunks = 0;
    }

    namespace
    {
        uint64_t nsSince(std::chrono::steady_clock::time_point begin)
        {
            return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - begin)
                    .count());
        }

        bool earlier(const Single &a, const Single &b)
        {
            return a.timevalue_100fs < b.timevalue_100fs;
        }

        // 把若干条各自有序的段归并成一条全局有序序列。段数等于节点数（通常 2），
        // 每元素 k 次比较，比对整批 std::sort 的 O(n log n) 便宜得多。
        void mergeSortedRuns(const Single *src,
                             const std::vector<std::pair<size_t, size_t>> &runs,
                             Single *dst)
        {
            if (runs.empty())
            {
                return;
            }
            if (runs.size() == 1)
            {
                std::copy(src + runs[0].first, src + runs[0].second, dst);
                return;
            }
            if (runs.size() == 2)
            {
                std::merge(src + runs[0].first, src + runs[0].second,
                           src + runs[1].first, src + runs[1].second, dst, earlier);
                return;
            }

            std::vector<size_t> cursor(runs.size());
            for (size_t i = 0; i < runs.size(); ++i)
            {
                cursor[i] = runs[i].first;
            }

            size_t out = 0;
            while (true)
            {
                size_t pick = runs.size();
                uint64_t pickTime = 0;
                for (size_t i = 0; i < runs.size(); ++i)
                {
                    if (cursor[i] >= runs[i].second)
                    {
                        continue;
                    }
                    const uint64_t t = src[cursor[i]].timevalue_100fs;
                    if (pick == runs.size() || t < pickTime)
                    {
                        pick = i;
                        pickTime = t;
                    }
                }
                if (pick == runs.size())
                {
                    break;
                }
                dst[out++] = src[cursor[pick]++];
            }
        }
    } // namespace

    StreamingTimeAligner::StreamingTimeAligner(const TimeAlignerConfig &config, size_t nodeCount)
        : m_config(config), m_nodeCount(nodeCount)
    {
        if (config.useMemoryPool)
        {
            m_memoryPool = std::make_unique<SharedMemoryPool>(config.maxTotalMemoryBytes);
            LOG(INFO) << "[StreamingTimeAligner] Memory pool enabled: "
                      << (config.maxTotalMemoryBytes / (1024 * 1024)) << " MB limit";
        }

        SharedMemoryPool *poolPtr = m_memoryPool.get();
        for (size_t i = 0; i < nodeCount; ++i)
        {
            m_nodeBuffers.push_back(
                std::make_unique<NodeRingBuffer>(i, config.maxChunksPerNode, poolPtr));
            // 数据到达即唤醒处理线程；是否真的开工由处理线程的触发判定决定，
            // 这样所有策略集中在一处。
            m_nodeBuffers.back()->setPushObserver(
                [this]()
                {
                    m_wakeSeq.fetch_add(1, std::memory_order_release);
                    m_wakeCv.notify_one();
                });
        }

        m_mergeRuns.reserve(nodeCount);

        std::vector<uint32_t> crystalNumOfEachChannel(
            config.channelNum, config.crystalsPerChannel);

        m_useMultiGpu = multi_gpu::shouldUseCoincidenceMultiGpu(config);
        if (m_useMultiGpu)
        {
            try
            {
                auto engineConfig = multi_gpu::makeCoincidenceMultiGpuEngineConfig(config);
                m_multiGpuEngine = std::make_unique<multi_gpu::CoincidenceMultiGpuEngine>();
                if (!m_multiGpuEngine->initialize(engineConfig))
                {
                    LOG(WARNING) << "[StreamingTimeAligner] Multi-GPU engine init failed, "
                                 << "falling back to legacy single-GPU coincidence";
                    m_multiGpuEngine.reset();
                    m_useMultiGpu = false;
                }
            }
            catch (const std::exception &e)
            {
                LOG(WARNING) << "[StreamingTimeAligner] Multi-GPU engine setup failed: "
                             << e.what() << "; falling back to legacy single-GPU coincidence";
                m_multiGpuEngine.reset();
                m_useMultiGpu = false;
            }
        }

        if (!m_useMultiGpu)
        {
            m_coinNode.setTotalCrystalNumOfEachChannel(crystalNumOfEachChannel);
        }

        LOG(INFO) << "[StreamingTimeAligner] Initialized with " << nodeCount
                  << " nodes, " << config.channelNum << " channels, multiGpu="
                  << (m_useMultiGpu ? "true" : "false")
                  << (m_useMultiGpu && m_multiGpuEngine
                          ? (", ring=" + std::to_string(m_multiGpuEngine->ringSize()) +
                             ", pipelineDepth=" +
                             std::to_string(config.coinPipelineDepth))
                          : std::string());
        initInputSlots();
    }

    StreamingTimeAligner::~StreamingTimeAligner()
    {
        stop();
        if (m_multiGpuEngine)
        {
            m_multiGpuEngine->finalize();
            m_multiGpuEngine.reset();
        }
    }

    NodeRingBuffer *StreamingTimeAligner::getNodeBuffer(uint16_t nodeId)
    {
        if (nodeId >= m_nodeBuffers.size())
        {
            return nullptr;
        }
        return m_nodeBuffers[nodeId].get();
    }

    size_t StreamingTimeAligner::gpuCount() const noexcept
    {
        return m_multiGpuEngine ? m_multiGpuEngine->gpuCount() : 0;
    }

    void StreamingTimeAligner::start()
    {
        if (m_running.exchange(true))
        {
            LOG(WARNING) << "[StreamingTimeAligner] Already running";
            return;
        }

        initializeOutput();
        m_writeStop.store(false, std::memory_order_release);
        if (m_useMultiGpu && m_multiGpuEngine)
        {
            m_writerThread = std::thread([this]()
                                         { writerLoop(); });
            m_drainThread = std::thread([this]()
                                        { drainLoop(); });
        }
        m_processorThread = std::thread([this]()
                                        { processingLoop(); });
        LOG(INFO) << "[StreamingTimeAligner] Started";
    }

    void StreamingTimeAligner::stop(bool waitForCompletion)
    {
        (void)waitForCompletion;

        if (!m_running.exchange(false))
        {
            return;
        }

        if (m_memoryPool)
        {
            m_memoryPool->close();
        }

        for (auto &buf : m_nodeBuffers)
        {
            buf->close();
        }

        // 让处理线程从 wait_for 里立刻醒来，进入收尾流程。
        m_wakeSeq.fetch_add(1, std::memory_order_release);
        m_wakeCv.notify_all();

        if (m_processorThread.joinable())
        {
            m_processorThread.join();
        }

        if (m_useMultiGpu && m_multiGpuEngine)
        {
            m_multiGpuEngine->signalNoMoreData();
        }
        if (m_drainThread.joinable())
        {
            m_drainThread.join();
        }
        stopWriterAndJoin();

        finalizeOutput();
        if (m_multiGpuEngine)
        {
            m_multiGpuEngine->finalize();
        }
        LOG(INFO) << "[StreamingTimeAligner] Stopped";
    }

    SharedMemoryPool::MemoryStatus StreamingTimeAligner::getMemoryStatus() const
    {
        if (m_memoryPool)
        {
            return m_memoryPool->getStatus();
        }
        return SharedMemoryPool::MemoryStatus{0, 0, 0.0};
    }

    void StreamingTimeAligner::initializeOutput()
    {
        fs::create_directories(m_config.outputDir);
        const uint32_t totalCrystals = m_config.channelNum * m_config.crystalsPerChannel;

        if (m_config.savePrompt)
        {
            openpni::distributed::coreio::ListmodeWriterOptions opts;
            opts.totalCrystals = totalCrystals;
            opts.io.maxFileSizeBytes = m_config.listmodeMaxFileSizeBytes;
            opts.io.enableOverrideExistingFile = m_config.listmodeOverwriteExisting;
            // maxFileSizeBytes == 0 时文件名与历史行为完全一致："{outputDir}/prompt.lmf"
            m_promptOpened = m_promptWriter.Open(
                m_config.outputDir, "prompt", "lmf", opts,
                [](openpni::distributed::coreio::ListmodeFileWriter &w, const std::string &path)
                {
                    w.Open(path);
                });
        }

        if (m_config.saveDelay)
        {
            openpni::distributed::coreio::ListmodeWriterOptions opts;
            opts.totalCrystals = totalCrystals;
            opts.io.maxFileSizeBytes = m_config.listmodeMaxFileSizeBytes;
            opts.io.enableOverrideExistingFile = m_config.listmodeOverwriteExisting;
            // maxFileSizeBytes == 0 时文件名与历史行为完全一致："{outputDir}/delay.lmf"
            m_delayOpened = m_delayWriter.Open(
                m_config.outputDir, "delay", "lmf", opts,
                [](openpni::distributed::coreio::ListmodeFileWriter &w, const std::string &path)
                {
                    w.Open(path);
                });
        }
    }

    void StreamingTimeAligner::finalizeOutput()
    {
        std::lock_guard<std::mutex> lock(m_outputMutex);
        m_promptWriter.Stop();
        m_delayWriter.Stop();
        m_promptOpened = false;
        m_delayOpened = false;
    }

    uint64_t StreamingTimeAligner::calculateWatermark()
    {
        uint64_t globalMinMaxTime = UINT64_MAX;
        size_t nodesWithData = 0;
        size_t stalledNodes = 0;

        for (const auto &buf : m_nodeBuffers)
        {
            const uint64_t nodeMaxTime = buf->getMaxEventTime();
            if (nodeMaxTime == 0)
            {
                // 从未收到过任何数据：无时间信息可用，bypass 也无从谈起。
                continue;
            }

            if (m_config.allowStalledNodeBypass &&
                buf->lastPushAgeMs() > m_config.nodeStallTimeoutMs)
            {
                stalledNodes++;
                continue;
            }

            globalMinMaxTime = std::min(globalMinMaxTime, nodeMaxTime);
            nodesWithData++;
        }

        m_watermarkDegraded = false;

        if (nodesWithData + stalledNodes < m_nodeCount || globalMinMaxTime == UINT64_MAX)
        {
            return 0;
        }

        if (stalledNodes > 0)
        {
            // 该段不再包含停滞节点的数据，跨节点对齐精度已降级。
            m_watermarkDegraded = true;
        }

        const uint64_t safetyMargin = m_config.getTotalSafetyMargin();

        if (globalMinMaxTime > safetyMargin)
        {
            return globalMinMaxTime - safetyMargin;
        }
        return 0;
    }

    size_t StreamingTimeAligner::countPendingBefore(uint64_t boundary) const
    {
        size_t n = 0;
        for (const auto &buf : m_nodeBuffers)
        {
            n += buf->countSinglesBefore(boundary);
        }
        return n;
    }

    bool StreamingTimeAligner::anyNodeAboveHighWater() const
    {
        const double ratio = m_config.bufferHighWaterRatio;
        if (ratio <= 0.0)
        {
            return false;
        }

        for (const auto &buf : m_nodeBuffers)
        {
            if (buf->occupancyRatio() >= ratio)
            {
                return true;
            }
        }
        return m_memoryPool && m_memoryPool->getUsageRatio() >= ratio;
    }

    uint64_t StreamingTimeAligner::clampSegmentBoundary(uint64_t watermark, size_t pending,
                                                        size_t *outCount)
    {
        // 不变式 3：carry 与新数据合计受 maxSegmentSingles 约束，故预算先扣掉 carry。
        // 这一步是显存上界真正生效的关键——只按新数据切分会让内核收到
        // maxSegmentSingles + carry 条，悄悄突破用户设定的上限。
        size_t budget = std::numeric_limits<size_t>::max();
        if (m_config.maxSegmentSingles != 0)
        {
            const size_t carryCount = m_carrySingles.size();
            if (m_config.maxSegmentSingles > carryCount)
            {
                budget = m_config.maxSegmentSingles - carryCount;
            }
            else
            {
                budget = 1;
                LOG_EVERY_N(WARNING, 64)
                    << "[StreamingTimeAligner] carry (" << carryCount
                    << ") 已占满 maxSegmentSingles (" << m_config.maxSegmentSingles
                    << ")，请调大 maxSegmentSingles 或调小 minSegmentOverlapFactor";
            }
        }

        // 不变式 2：段跨度不得小于硬下界。
        const uint64_t minSpan = std::max<uint64_t>(1, m_config.minSegmentSpan_100fs());
        const uint64_t minBound = m_lastWatermark > UINT64_MAX - minSpan
                                      ? UINT64_MAX
                                      : m_lastWatermark + minSpan;

        // 常见稳态：待处理量不超预算，直接取水位线，零次探测。
        if (pending <= budget)
        {
            if (outCount)
            {
                *outCount = pending;
            }
            return watermark;
        }

        // 搜索下界要取「上次边界」与「缓冲中最早事件」的较大者。时间戳是绝对 PET 时间
        // （量级 1e17），首轮 m_lastWatermark 还是 0，只按它播种会让插值落在数据起点之前，
        // 二分要耗掉很多轮才爬到有数据的区间——低轮询频率下就表现为水位线迟迟不前进。
        uint64_t earliest = UINT64_MAX;
        for (const auto &buf : m_nodeBuffers)
        {
            earliest = std::min(earliest, buf->getFrontMinTime());
        }

        uint64_t lo = m_lastWatermark;
        if (earliest != UINT64_MAX && earliest > lo)
        {
            lo = earliest;
        }
        uint64_t hi = watermark;
        if (lo >= hi)
        {
            if (outCount)
            {
                *outCount = pending;
            }
            return watermark;
        }

        // 硬下界同样要以「有数据的起点」为基准，否则 m_lastWatermark 远落后于数据时
        // （首轮为 0）会切出一串空段，只是在空转。
        const uint64_t floorBound =
            earliest != UINT64_MAX
                ? std::min(watermark, std::max(minBound, earliest + minSpan))
                : std::min(minBound, watermark);

        const uint64_t searchLo = lo;
        uint64_t best = 0;
        size_t bestCount = 0;

        uint64_t probe = lo + static_cast<uint64_t>(
                                  static_cast<long double>(hi - lo) *
                                  (static_cast<long double>(budget) /
                                   static_cast<long double>(pending)));
        probe = std::clamp(probe, lo, hi);

        constexpr int kMaxProbes = 12;
        for (int iter = 0; iter < kMaxProbes; ++iter)
        {
            const size_t count = countPendingBefore(probe);
            if (count <= budget)
            {
                if (probe >= best)
                {
                    best = probe;
                    bestCount = count;
                }
                if (probe == hi)
                {
                    break;
                }
                lo = probe + 1;
            }
            else
            {
                if (probe == 0)
                {
                    break;
                }
                hi = probe - 1;
            }

            if (lo > hi)
            {
                break;
            }
            probe = lo + (hi - lo) / 2;
        }

        // 向下吸附到整 chunk 边界，避开抽取时的 erase memmove。
        // 边界只会变小，因此不会破坏预算约束。
        if (best > floorBound)
        {
            uint64_t snap = UINT64_MAX;
            for (const auto &buf : m_nodeBuffers)
            {
                size_t chunkCount = 0;
                snap = std::min(snap, buf->chunkBoundaryWithin(best, budget, &chunkCount));
            }
            // 损失不超过一半跨度才值得吸附。
            if (snap != UINT64_MAX && snap > floorBound &&
                (snap - searchLo) * 2 >= (best - searchLo))
            {
                best = snap;
                bestCount = countPendingBefore(best);
            }
        }

        // 应用硬下界：宁可本段超预算，也不让段跨度塌到重叠窗以下——
        // 否则 carry 占比失控，且抽取边界会失去单调性。
        if (best < floorBound)
        {
            best = floorBound;
            bestCount = countPendingBefore(best);
            if (bestCount > budget)
            {
                m_stats.oversizedSegments.fetch_add(1, std::memory_order_relaxed);
                LOG_EVERY_N(WARNING, 64)
                    << "[StreamingTimeAligner] 一个重叠窗内的数据量 (" << bestCount
                    << ") 已超过显存预算 (" << budget
                    << ")，本段放宽预算以保证正确性；请调大 maxSegmentSingles";
            }
        }

        if (outCount)
        {
            *outCount = bestCount;
        }
        return best;
    }

    void StreamingTimeAligner::processingLoop()
    {
        // 本循环贯穿三条不变式：
        //  1. 压力触发与延迟兜底只改变“何时”处理，绝不改变“处理到哪里”。抽取边界恒
        //     <= calculateWatermark()，跨节点对齐语义与触发策略完全解耦。
        //  2. 抽取边界恒 >= m_lastWatermark + minSegmentSpan_100fs()（停机 flush 除外），
        //     保证 carry 占比有界、边界严格单调。
        //  3. 送入内核的批 = carry + 新数据，两者合计受 maxSegmentSingles 约束。
        uint64_t lastWakeSeq = m_wakeSeq.load(std::memory_order_acquire);
        auto lastProcessTime = std::chrono::steady_clock::now();
        auto lastStallWarn = std::chrono::steady_clock::now() -
                             std::chrono::hours(1);

        auto hasBufferedSingles = [this]()
        {
            for (const auto &buf : m_nodeBuffers)
            {
                if (buf && !buf->empty())
                {
                    return true;
                }
            }
            return false;
        };

        // 上一段被预算钳掉了尾巴（或缓冲仍在高水位）时置位：说明水位线之内还有数据可
        // 立即处理，必须连续抽取而不是回去睡 processingIntervalMs，否则单轮只能吞下
        // maxSegmentSingles 条，积压永远追不上。
        bool moreReadyNow = false;

        while (m_running.load() || hasBufferedSingles())
        {
            // 事件驱动：有节点 push 就提前醒来，否则最多等 processingIntervalMs。
            if (!moreReadyNow)
            {
                std::unique_lock<std::mutex> lock(m_wakeMutex);
                m_wakeCv.wait_for(
                    lock, std::chrono::milliseconds(m_config.processingIntervalMs),
                    [this, lastWakeSeq]()
                    {
                        return m_wakeSeq.load(std::memory_order_acquire) != lastWakeSeq ||
                               !m_running.load();
                    });
                lastWakeSeq = m_wakeSeq.load(std::memory_order_acquire);
            }
            moreReadyNow = false;

            const auto startTime = std::chrono::high_resolution_clock::now();
            const auto extractBegin = std::chrono::steady_clock::now();

            const uint64_t watermark = calculateWatermark();
            const bool pressure = anyNodeAboveHighWater();

            if (watermark == 0 || watermark <= m_lastWatermark)
            {
                // 缓冲已到高水位却推不动水位线，说明某节点滞后。此时不能越过水位线抽取
                // （那会破坏跨节点对齐），只能继续对上游背压并告警。
                if (pressure)
                {
                    m_stats.watermarkStallEvents.fetch_add(1, std::memory_order_relaxed);
                    const auto now = std::chrono::steady_clock::now();
                    if (std::chrono::duration_cast<std::chrono::milliseconds>(
                            now - lastStallWarn)
                            .count() >= static_cast<int64_t>(m_config.nodeStallWarnMs))
                    {
                        lastStallWarn = now;
                        LOG(WARNING) << "[StreamingTimeAligner] 缓冲高水位但水位线无法前进，"
                                     << "正在对上游背压（某节点数据滞后）";
                    }
                }
                m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);
                if (!m_running.load())
                {
                    break;
                }
                continue;
            }

            // 不变式 2：段跨度硬下界，压力与延迟兜底都不能突破。
            if (watermark - m_lastWatermark < m_config.minSegmentSpan_100fs())
            {
                m_stats.heldByMinDuration.fetch_add(1, std::memory_order_relaxed);
                m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);
                if (!m_running.load())
                {
                    break; // 余量交给 flushRemaining 收尾
                }
                continue;
            }

            const size_t pending = countPendingBefore(watermark);
            const bool stopping = !m_running.load();
            const bool deadline =
                m_config.maxProcessLatencyMs > 0 &&
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - lastProcessTime)
                        .count() >= static_cast<int64_t>(m_config.maxProcessLatencyMs);
            const bool enough = pending >= m_config.minSegmentSingles;

            if (pending > 0 && !enough && !pressure && !deadline && !stopping)
            {
                // 攒批：数据量不足且无压力、未到期，等下一轮。
                m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);
                continue;
            }

            size_t segmentCount = 0;
            const uint64_t boundary =
                clampSegmentBoundary(watermark, pending, &segmentCount);

            if (boundary <= m_lastWatermark)
            {
                // 不变式 2 保证不会走到这里；真发生了就不能强推边界（会跳过数据）。
                LOG_EVERY_N(ERROR, 64)
                    << "[StreamingTimeAligner] 抽取边界未前进 (" << boundary
                    << " <= " << m_lastWatermark << ")，跳过本轮";
                m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);
                continue;
            }

            if (segmentCount == 0)
            {
                // 边界区间内没有任何 single：直接推进边界，不会漏掉数据。
                if (!m_carrySingles.empty())
                {
                    updateCarrySingles(std::span<const Single>(m_carrySingles), boundary);
                }
                m_lastWatermark = boundary;
                m_stats.currentTimeBoundary_pico = boundary;
                m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);
                moreReadyNow = boundary < watermark;
                continue;
            }

            if (enough)
            {
                m_stats.triggerByWatermark.fetch_add(1, std::memory_order_relaxed);
            }
            else if (pressure)
            {
                m_stats.triggerByPressure.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                m_stats.triggerByDeadline.fetch_add(1, std::memory_order_relaxed);
            }
            if (m_watermarkDegraded)
            {
                m_stats.degradedSegments.fetch_add(1, std::memory_order_relaxed);
            }

            m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);

            if (!processSegment(boundary, segmentCount))
            {
                break;
            }

            m_stats.chunksProcessed++;
            m_stats.currentTimeBoundary_pico = m_lastWatermark;
            lastProcessTime = std::chrono::steady_clock::now();
            moreReadyNow = m_lastWatermark < watermark;

            size_t backlog = 0;
            for (const auto &buf : m_nodeBuffers)
            {
                backlog = std::max(backlog, buf->size());
            }
            uint64_t prevPeak = m_stats.maxNodeBacklogChunks.load(std::memory_order_relaxed);
            while (backlog > prevPeak &&
                   !m_stats.maxNodeBacklogChunks.compare_exchange_weak(
                       prevPeak, backlog, std::memory_order_relaxed))
            {
            }

            const auto endTime = std::chrono::high_resolution_clock::now();
            const double elapsed_ms =
                std::chrono::duration<double, std::milli>(endTime - startTime).count();
            const double currentAvg = m_stats.avgProcessingTime_ms.load();
            m_stats.avgProcessingTime_ms = currentAvg * 0.9 + elapsed_ms * 0.1;

            if (m_processFailed.load(std::memory_order_acquire))
            {
                break;
            }
        }

        if (!m_processFailed.load(std::memory_order_acquire))
        {
            flushRemaining();
        }
    }

    uint64_t StreamingTimeAligner::overlapLength_100fs() const
    {
        return m_config.overlapLength_100fs();
    }

    void StreamingTimeAligner::ensureStagingCapacity(size_t elements)
    {
        if (elements <= m_stageCapacity)
        {
            return;
        }

        // pinned 内存分配很慢，一次多留些余量，避免边界抖动导致反复重分配。
        size_t want = elements + elements / 8 + 4096;
        if (m_config.maxSegmentSingles != 0)
        {
            want = std::max(want, m_config.maxSegmentSingles +
                                      m_config.maxSegmentSingles / 8 + 4096);
        }

        m_stageMerged.ResetPointer(want);
        if (m_nodeBuffers.size() > 1)
        {
            m_stageRaw.ResetPointer(want);
        }
        m_stageCapacity = want;
    }

    void StreamingTimeAligner::initInputSlots()
    {
        if (!m_useMultiGpu || !m_multiGpuEngine)
        {
            return;
        }

        const size_t n = std::max<size_t>(1, m_multiGpuEngine->ringSize());
        m_inputSlotCapacity = m_config.maxSegmentSingles == 0
                                  ? size_t{262144}
                                  : std::max<size_t>(2, m_config.maxSegmentSingles);
        m_inputSlots.clear();
        m_inputSlots.reserve(n);
        {
            std::lock_guard<std::mutex> lock(m_slotMutex);
            m_freeSlots.clear();
            for (size_t i = 0; i < n; ++i)
            {
                m_inputSlots.emplace_back();
                m_inputSlots.back().buffer.ResetPointer(m_inputSlotCapacity);
                m_freeSlots.push_back(i);
            }
        }
        m_writeQueueCap = n;
        {
            std::lock_guard<std::mutex> lock(m_submittedMutex);
            m_submittedSlots.clear();
        }
        {
            std::lock_guard<std::mutex> lock(m_writeMutex);
            m_writeQueue.clear();
        }
    }

    size_t StreamingTimeAligner::extractAndMergeInto(Single *dest, size_t destCap,
                                                     uint64_t boundary, bool *truncated)
    {
        if (dest == nullptr || destCap == 0)
        {
            if (truncated)
            {
                *truncated = true;
            }
            return 0;
        }

        if (m_nodeBuffers.size() == 1)
        {
            return m_nodeBuffers[0]->extractSinglesBeforeInto(
                boundary, dest, destCap, truncated);
        }

        Single *raw = m_stageRaw.Data();
        if (raw == nullptr)
        {
            if (truncated)
            {
                *truncated = true;
            }
            return 0;
        }

        m_mergeRuns.clear();
        size_t offset = 0;
        for (auto &buf : m_nodeBuffers)
        {
            const size_t n = buf->extractSinglesBeforeInto(
                boundary, raw + offset, destCap - offset, truncated);
            if (n > 0)
            {
                m_mergeRuns.emplace_back(offset, offset + n);
            }
            offset += n;
        }
        mergeSortedRuns(raw, m_mergeRuns, dest);
        return offset;
    }

    size_t StreamingTimeAligner::acquireInputSlot()
    {
        std::unique_lock<std::mutex> lock(m_slotMutex);
        m_slotCv.wait(lock, [this]
                      {
                          return !m_freeSlots.empty() ||
                                 m_processFailed.load(std::memory_order_acquire);
                      });
        if (m_freeSlots.empty())
        {
            return std::numeric_limits<size_t>::max();
        }
        const size_t idx = m_freeSlots.front();
        m_freeSlots.pop_front();
        return idx;
    }

    void StreamingTimeAligner::releaseInputSlot(size_t idx)
    {
        {
            std::lock_guard<std::mutex> lock(m_slotMutex);
            m_freeSlots.push_back(idx);
        }
        m_slotCv.notify_one();
    }

    bool StreamingTimeAligner::submitOwnedSlot(size_t slotIdx, size_t count,
                                               uint64_t carryCutoff, uint64_t carryBound)
    {
        Single *data = m_inputSlots[slotIdx].buffer.Data();
        if (data == nullptr || count == 0)
        {
            releaseInputSlot(slotIdx);
            return count == 0;
        }
        const std::span<const Single> batch(data, count);
        const auto carryBegin = std::chrono::steady_clock::now();
        updateCarrySingles(batch, carryBound);
        m_stats.extractNs.fetch_add(nsSince(carryBegin), std::memory_order_relaxed);
        m_lastWatermark = carryBound;

        try
        {
            m_multiGpuEngine->submitSingles(
                batch, carryCutoff, m_promptOpened, m_delayOpened);
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "[StreamingTimeAligner] submitSingles failed: " << e.what();
            m_processFailed.store(true, std::memory_order_release);
            releaseInputSlot(slotIdx);
            m_slotCv.notify_all();
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(m_submittedMutex);
            m_submittedSlots.push_back(slotIdx);
        }
        m_submittedCv.notify_one();
        return true;
    }

    bool StreamingTimeAligner::submitCopiedSpan(std::span<const Single> sorted,
                                                uint64_t carryCutoff, uint64_t carryBound)
    {
        const size_t slotIdx = acquireInputSlot();
        if (slotIdx == std::numeric_limits<size_t>::max())
        {
            return false;
        }

        if (sorted.size() > m_inputSlotCapacity)
        {
            m_inputSlots[slotIdx].buffer.ResetPointer(sorted.size());
        }
        std::copy(sorted.begin(), sorted.end(), m_inputSlots[slotIdx].buffer.Data());
        return submitOwnedSlot(slotIdx, sorted.size(), carryCutoff, carryBound);
    }

    bool StreamingTimeAligner::processSegment(uint64_t boundary, size_t expectedCount)
    {
        const auto extractBegin = std::chrono::steady_clock::now();

        const size_t carryCount = m_carrySingles.size();
        const size_t totalEst = carryCount + expectedCount;
        const bool useSlot =
            m_useMultiGpu && m_multiGpuEngine &&
            m_inputSlotCapacity > 0 && totalEst <= m_inputSlotCapacity;

        if (useSlot)
        {
            const size_t slotIdx = acquireInputSlot();
            if (slotIdx == std::numeric_limits<size_t>::max())
            {
                return false;
            }

            Single *merged = m_inputSlots[slotIdx].buffer.Data();
            if (m_nodeBuffers.size() > 1)
            {
                ensureStagingCapacity(std::max(expectedCount, m_inputSlotCapacity));
            }
            if (carryCount > 0)
            {
                std::copy(m_carrySingles.begin(), m_carrySingles.end(), merged);
            }

            bool truncated = false;
            const size_t newRoom =
                m_inputSlotCapacity > carryCount ? m_inputSlotCapacity - carryCount : 0;
            const size_t newCount =
                extractAndMergeInto(merged + carryCount, newRoom, boundary, &truncated);

            if (truncated)
            {
                LOG(ERROR) << "[StreamingTimeAligner] 暂存区不足以容纳边界内的全部 singles"
                           << "（预估 " << expectedCount << "），"
                           << "请增大 networkLatencyMargin_pico 或检查数据乱序程度";
                m_processFailed.store(true, std::memory_order_release);
                releaseInputSlot(slotIdx);
                m_slotCv.notify_all();
                return false;
            }

            m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);

            if (newCount == 0)
            {
                if (carryCount > 0)
                {
                    updateCarrySingles(std::span<const Single>(merged, carryCount), boundary);
                }
                m_lastWatermark = boundary;
                releaseInputSlot(slotIdx);
                return true;
            }

            m_stats.totalSinglesReceived += newCount;
            const uint64_t carryCutoff = carryCount == 0 ? 0 : m_lastWatermark;
            m_stats.carrySinglesTotal.fetch_add(carryCount, std::memory_order_relaxed);
            if (!submitOwnedSlot(slotIdx, carryCount + newCount, carryCutoff, boundary))
            {
                return false;
            }
            m_stats.totalSinglesProcessed += newCount;
            return true;
        }

        ensureStagingCapacity(carryCount + expectedCount);

        Single *merged = m_stageMerged.Data();
        const size_t newRoom = m_stageCapacity - carryCount;

        // carry 恒是本批的时间前缀：carry ⊆ (prevBound - overlap, prevBound]，而新抽出的
        // 数据恒 > prevBound（<= prevBound 的都已在上一段取走）。因此只需归并各节点段，
        // 再把 carry 原样放到最前面即可得到全局时序有序的批。
        if (carryCount > 0)
        {
            std::copy(m_carrySingles.begin(), m_carrySingles.end(), merged);
        }

        bool truncated = false;
        const size_t newCount =
            extractAndMergeInto(merged + carryCount, newRoom, boundary, &truncated);

        if (truncated)
        {
            // 抽取量超出了按水位线预估的条数，只可能是安全裕量没能覆盖乱序到达。
            // 继续下去会让部分数据错过配对，这里必须显式失败而不是静默降级。
            LOG(ERROR) << "[StreamingTimeAligner] 暂存区不足以容纳边界内的全部 singles"
                       << "（预估 " << expectedCount << "），"
                       << "请增大 networkLatencyMargin_pico 或检查数据乱序程度";
            m_processFailed.store(true, std::memory_order_release);
            return false;
        }

        m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);

        if (newCount == 0)
        {
            if (carryCount > 0)
            {
                updateCarrySingles(std::span<const Single>(merged, carryCount), boundary);
            }
            m_lastWatermark = boundary;
            return true;
        }

        m_stats.totalSinglesReceived += newCount;
        return processSinglesRange(std::span<Single>(merged, carryCount + newCount),
                                   boundary);
    }

    bool StreamingTimeAligner::processSinglesRange(std::span<Single> sorted,
                                                   uint64_t finalCarryBound,
                                                   bool enforceMinDuration)
    {
        const size_t total = sorted.size();
        if (total == 0)
        {
            return true;
        }

        // 不变式 3：carry + 新数据合计不得超过 maxSegmentSingles。
        const size_t hardCap = m_config.maxSegmentSingles == 0
                                   ? std::numeric_limits<size_t>::max()
                                   : std::max<size_t>(2, m_config.maxSegmentSingles);

        if (total <= hardCap)
        {
            const size_t carryCount = m_carrySingles.size();
            const uint64_t carryCutoff = carryCount == 0 ? 0 : m_lastWatermark;
            m_stats.carrySinglesTotal.fetch_add(carryCount, std::memory_order_relaxed);

            if (m_useMultiGpu && m_multiGpuEngine)
            {
                if (!submitCopiedSpan(std::span<const Single>(sorted), carryCutoff,
                                      finalCarryBound != 0 ? finalCarryBound
                                                           : sorted.back().timevalue_100fs))
                {
                    return false;
                }
                m_stats.totalSinglesProcessed += total - carryCount;
                return true;
            }

            processCoincidence(std::span<const Single>(sorted), carryCutoff);
            if (m_processFailed.load(std::memory_order_acquire))
            {
                return false;
            }
            m_stats.totalSinglesProcessed += total - carryCount;

            const uint64_t carryBound = finalCarryBound != 0
                                            ? finalCarryBound
                                            : sorted.back().timevalue_100fs;
            const auto carryBegin = std::chrono::steady_clock::now();
            updateCarrySingles(std::span<const Single>(sorted), carryBound);
            m_stats.extractNs.fetch_add(nsSince(carryBegin), std::memory_order_relaxed);
            m_lastWatermark = carryBound;
            return true;
        }

        // 超预算路径：只在“段时长硬下界与显存预算冲突”或停机 flush 一次性倒空缓冲时
        // 走到。按时间前缀切，切点同时满足“子批 ≤ 预算”与（可选）“切点 ≥ 硬下界”；
        // 两界冲突时以下界优先并计入 oversizedSegments。
        LOG(INFO) << "[StreamingTimeAligner] 分段 " << total
                  << " 条超出 maxSegmentSingles (" << hardCap << ")，按时间前缀切分";

        const uint64_t minSpan = std::max<uint64_t>(1, m_config.minSegmentSpan_100fs());
        std::vector<Single> scratch;
        size_t offset = m_carrySingles.size(); // 跳过 sorted 中的 carry 前缀
        while (offset < total)
        {
            const size_t carryCount = m_carrySingles.size();
            const size_t room = hardCap > carryCount ? hardCap - carryCount : 1;
            const size_t remaining = total - offset;
            size_t nBudget = std::min(room, remaining);

            // 同一时间戳的事件必须留在同一子批：否则它们会分别落在 carry 与新数据里，
            // 而 t == cutoff 的新事件会被内核当作尾部保留抑制掉，导致漏配。
            auto extendSameTime = [&](size_t n)
            {
                while (offset + n < total &&
                       sorted[offset + n].timevalue_100fs ==
                           sorted[offset + n - 1].timevalue_100fs)
                {
                    ++n;
                }
                return n;
            };
            nBudget = extendSameTime(nBudget);

            size_t n = nBudget;
            if (enforceMinDuration && remaining > nBudget)
            {
                const uint64_t prevCutoff = m_lastWatermark;
                const uint64_t minBound = prevCutoff > UINT64_MAX - minSpan
                                              ? UINT64_MAX
                                              : prevCutoff + minSpan;
                const auto first = sorted.begin() + static_cast<std::ptrdiff_t>(offset);
                const auto it = std::lower_bound(
                    first, sorted.end(), minBound,
                    [](const Single &s, uint64_t bound)
                    { return s.timevalue_100fs < bound; });
                size_t nMin = remaining;
                if (it != sorted.end())
                {
                    nMin = static_cast<size_t>(
                               std::distance(first, it)) +
                           1;
                    nMin = extendSameTime(nMin);
                }
                if (nMin > nBudget)
                {
                    n = nMin;
                    m_stats.oversizedSegments.fetch_add(1, std::memory_order_relaxed);
                    LOG_EVERY_N(WARNING, 64)
                        << "[StreamingTimeAligner] 子批硬下界需要 " << nMin
                        << " 条新数据，超出预算 " << nBudget
                        << "；本段放宽预算以保证正确性";
                }
            }

            scratch.clear();
            scratch.reserve(carryCount + n);
            scratch.insert(scratch.end(), m_carrySingles.begin(), m_carrySingles.end());
            scratch.insert(scratch.end(), sorted.begin() + static_cast<std::ptrdiff_t>(offset),
                           sorted.begin() + static_cast<std::ptrdiff_t>(offset + n));

            const uint64_t carryCutoff = carryCount == 0 ? 0 : m_lastWatermark;
            m_stats.carrySinglesTotal.fetch_add(carryCount, std::memory_order_relaxed);

            const uint64_t carryBound = (offset + n >= total && finalCarryBound != 0)
                                            ? finalCarryBound
                                            : scratch.back().timevalue_100fs;

            if (m_useMultiGpu && m_multiGpuEngine)
            {
                if (!submitCopiedSpan(std::span<const Single>(scratch), carryCutoff, carryBound))
                {
                    return false;
                }
            }
            else
            {
                processCoincidence(std::span<const Single>(scratch), carryCutoff);
                if (m_processFailed.load(std::memory_order_acquire))
                {
                    return false;
                }
                updateCarrySingles(std::span<const Single>(scratch), carryBound);
                m_lastWatermark = carryBound;
            }
            m_stats.totalSinglesProcessed += n;

            offset += n;
        }
        return true;
    }

    void StreamingTimeAligner::updateCarrySingles(std::span<const Single> sortedBatch,
                                                  uint64_t watermark)
    {
        // sortedBatch 按时间有序，carry 恰是它的一段尾部切片，二分定位即可，
        // 无需线性扫描整批。sortedBatch 可能指向 m_carrySingles 自身，因此先构建
        // 临时容器、最后再整体移动赋值。
        std::vector<Single> nextCarry;
        if (sortedBatch.empty() || watermark == 0)
        {
            m_carrySingles = std::move(nextCarry);
            return;
        }

        const uint64_t overlap = overlapLength_100fs();
        const uint64_t carryBegin = watermark > overlap ? watermark - overlap : 0;

        const auto byTime = [](uint64_t bound, const Single &s)
        { return bound < s.timevalue_100fs; };

        // 下界取闭区间：恰好落在 watermark - overlap 上的事件仍可能与下一段开头的
        // 事件构成延迟符合，漏掉它会让配对结果随分段方式漂移。多带的事件都 <= cutoff，
        // 只会与新数据配对，不会重复计数。
        const auto first = std::lower_bound(sortedBatch.begin(), sortedBatch.end(),
                                            carryBegin,
                                            [](const Single &s, uint64_t bound)
                                            { return s.timevalue_100fs < bound; });
        const auto last = std::upper_bound(first, sortedBatch.end(), watermark, byTime);

        if (first < last)
        {
            nextCarry.assign(first, last);
        }
        m_carrySingles = std::move(nextCarry);
    }

    void StreamingTimeAligner::processCoincidence(
        std::span<const Single> singles, uint64_t carryCutoffTime_100fs)
    {
        if (singles.empty())
        {
            return;
        }
        try
        {
            // 多 GPU 流水线由抽取线程 submit、收回线程 next，禁止在这里同步 next。
            // 不要在这里 Clear()：UniPtr::Clear 释放了 host/cuda 指针却没有把
            // m_capacityHost / m_capacityCuda 归零，下一次 CheckHostCapacity 认为容量够用
            // 而不再分配，HostWPtr() 于是返回空指针。UniPtr::CopyFromHost 本身就是
            // Resize + std::copy，批大小逐批变化也能正确处理。
            const auto kernelBegin = std::chrono::steady_clock::now();
            m_singleBuffer.CopyFromHost(std::span<const Single>(singles));

            std::vector<std::span<Single const>> inputList;
            inputList.push_back(m_singleBuffer.CudaRStdSpan());

            auto [prompt, delay] = m_coinNode.getDListmode(
                inputList, m_config.coinProtocol, carryCutoffTime_100fs);
            m_stats.coinKernelNs.fetch_add(nsSince(kernelBegin), std::memory_order_relaxed);
            m_stats.coinKernelBatches.fetch_add(1, std::memory_order_relaxed);

            m_stats.totalPromptPairs += prompt.size();
            m_stats.totalDelayPairs += delay.size();

            if (!prompt.empty() && m_promptOpened)
            {
                const auto sinkBegin = std::chrono::steady_clock::now();
                saveCoincidenceResult(m_promptWriter, prompt);
                m_stats.sinkNs.fetch_add(nsSince(sinkBegin), std::memory_order_relaxed);
            }

            if (!delay.empty() && m_delayOpened)
            {
                const auto sinkBegin = std::chrono::steady_clock::now();
                saveCoincidenceResult(m_delayWriter, delay);
                m_stats.sinkNs.fetch_add(nsSince(sinkBegin), std::memory_order_relaxed);
            }
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "[StreamingTimeAligner] Coincidence error: " << e.what();
            m_processFailed.store(true, std::memory_order_release);
        }
    }

    void StreamingTimeAligner::saveCoincidenceResult(
        openpni::distributed::coreio::RollingFileWriter<
            openpni::distributed::coreio::ListmodeFileWriter,
            openpni::distributed::coreio::ListmodeWriterOptions> &output,
        std::span<Listmode const> coins,
        bool alreadyOnHost)
    {
        if (coins.empty())
        {
            return;
        }

        std::span<Listmode const> hostBuf;
        if (alreadyOnHost)
        {
            hostBuf = coins;
        }
        else
        {
            m_coinHostBuffer.resize(coins.size());
            openpni::detail::copy_from_device_to_host(m_coinHostBuffer.data(), coins);
            hostBuf = std::span<Listmode const>(m_coinHostBuffer);
        }

        // 估算本次写入字节数，用于分卷阈值判断（仅在 listmodeMaxFileSizeBytes > 0 时生效）
        constexpr size_t ESTIMATED_LISTMODE_BYTES = 16;
        const uint64_t sizeEstimate = hostBuf.size() * ESTIMATED_LISTMODE_BYTES;

        std::lock_guard<std::mutex> lock(m_outputMutex);
        output.AppendSegment(sizeEstimate, hostBuf, 0, 0);
    }

    void StreamingTimeAligner::enqueueWrite(std::vector<Listmode> &&prompt,
                                            std::vector<Listmode> &&delay)
    {
        if (prompt.empty() && delay.empty())
        {
            return;
        }

        const auto waitBegin = std::chrono::steady_clock::now();
        {
            std::unique_lock<std::mutex> lock(m_writeMutex);
            m_writeCv.wait(lock, [this]
                           { return m_writeQueue.size() < m_writeQueueCap || m_writeStop.load(); });
            if (m_writeStop.load(std::memory_order_acquire) &&
                m_writeQueue.size() >= m_writeQueueCap)
            {
                LOG(ERROR) << "[StreamingTimeAligner] write queue stopped while full";
                m_processFailed.store(true, std::memory_order_release);
                return;
            }
            m_writeQueue.push_back(ListmodeWriteItem{std::move(prompt), std::move(delay)});
        }
        m_stats.writeQueueWaitNs.fetch_add(nsSince(waitBegin), std::memory_order_relaxed);
        m_writeCv.notify_one();
    }

    void StreamingTimeAligner::stopWriterAndJoin()
    {
        m_writeStop.store(true, std::memory_order_release);
        m_writeCv.notify_all();
        if (m_writerThread.joinable())
        {
            m_writerThread.join();
        }
    }

    void StreamingTimeAligner::drainLoop()
    {
        if (!m_multiGpuEngine)
        {
            return;
        }

        while (true)
        {
            multi_gpu::SegmentCoinResult result;
            const auto waitBegin = std::chrono::steady_clock::now();
            try
            {
                result = m_multiGpuEngine->nextResult();
            }
            catch (const std::exception &e)
            {
                LOG(ERROR) << "[StreamingTimeAligner] nextResult failed: " << e.what();
                m_processFailed.store(true, std::memory_order_release);
                m_slotCv.notify_all();
                m_wakeCv.notify_all();
                break;
            }
            const uint64_t waited = nsSince(waitBegin);
            m_stats.drainWaitNs.fetch_add(waited, std::memory_order_relaxed);
            m_stats.coinKernelNs.fetch_add(waited, std::memory_order_relaxed);

            if (result.endOfStream)
            {
                break;
            }

            size_t slotIdx = std::numeric_limits<size_t>::max();
            {
                std::unique_lock<std::mutex> lock(m_submittedMutex);
                m_submittedCv.wait(lock, [this]
                                   { return !m_submittedSlots.empty(); });
                slotIdx = m_submittedSlots.front();
                m_submittedSlots.pop_front();
            }

            if (result.failed)
            {
                LOG(ERROR) << "[StreamingTimeAligner] coincidence compute failed";
                m_processFailed.store(true, std::memory_order_release);
                m_multiGpuEngine->releaseResult();
                releaseInputSlot(slotIdx);
                m_slotCv.notify_all();
                m_wakeCv.notify_all();
                continue;
            }

            m_stats.totalPromptPairs += result.promptCount;
            m_stats.totalDelayPairs += result.delayCount;
            m_stats.coinKernelBatches.fetch_add(1, std::memory_order_relaxed);

            std::vector<Listmode> prompt;
            std::vector<Listmode> delay;
            if (!result.prompt.empty())
            {
                prompt.assign(result.prompt.begin(), result.prompt.end());
            }
            if (!result.delay.empty())
            {
                delay.assign(result.delay.begin(), result.delay.end());
            }

            m_multiGpuEngine->releaseResult();
            releaseInputSlot(slotIdx);
            enqueueWrite(std::move(prompt), std::move(delay));
        }
    }

    void StreamingTimeAligner::writerLoop()
    {
        while (true)
        {
            ListmodeWriteItem item;
            {
                std::unique_lock<std::mutex> lock(m_writeMutex);
                m_writeCv.wait(lock, [this]
                               {
                                   return !m_writeQueue.empty() ||
                                          m_writeStop.load(std::memory_order_acquire);
                               });
                if (m_writeQueue.empty() && m_writeStop.load(std::memory_order_acquire))
                {
                    break;
                }
                item = std::move(m_writeQueue.front());
                m_writeQueue.pop_front();
            }
            m_writeCv.notify_one();

            const auto sinkBegin = std::chrono::steady_clock::now();
            if (!item.prompt.empty() && m_promptOpened)
            {
                saveCoincidenceResult(m_promptWriter, item.prompt, true);
            }
            if (!item.delay.empty() && m_delayOpened)
            {
                saveCoincidenceResult(m_delayWriter, item.delay, true);
            }
            m_stats.sinkNs.fetch_add(nsSince(sinkBegin), std::memory_order_relaxed);
        }
    }

    void StreamingTimeAligner::flushRemaining()
    {
        LOG(INFO) << "[StreamingTimeAligner] Flushing remaining data...";

        const auto extractBegin = std::chrono::steady_clock::now();
        std::vector<Single> remaining;

        for (auto &buf : m_nodeBuffers)
        {
            while (auto chunk = buf->tryPop())
            {
                remaining.insert(remaining.end(),
                                 std::make_move_iterator(chunk->singles.begin()),
                                 std::make_move_iterator(chunk->singles.end()));
            }
        }

        if (remaining.empty())
        {
            m_carrySingles.clear();
            m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);
            LOG(INFO) << "[StreamingTimeAligner] Flush complete (no remaining singles)";
            return;
        }

        LOG(INFO) << "[StreamingTimeAligner] Processing " << remaining.size()
                  << " remaining singles (+ carry " << m_carrySingles.size()
                  << ")";
        m_stats.totalSinglesReceived += remaining.size();

        // 停机收尾：段时长硬下界在这里不适用，否则尾部数据永远处理不掉。
        // 各节点的段被简单串接，需要整体排序后才能拼在 carry 之后。
        std::sort(remaining.begin(), remaining.end(), earlier);

        std::vector<Single> batch;
        batch.reserve(m_carrySingles.size() + remaining.size());
        batch.insert(batch.end(), m_carrySingles.begin(), m_carrySingles.end());
        batch.insert(batch.end(), remaining.begin(), remaining.end());
        remaining.clear();
        remaining.shrink_to_fit();

        m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);

        if (!processSinglesRange(std::span<Single>(batch), 0, /*enforceMinDuration=*/false))
        {
            LOG(ERROR) << "[StreamingTimeAligner] Stopping flush after coincidence failure";
            return;
        }

        m_carrySingles.clear();
        LOG(INFO) << "[StreamingTimeAligner] Flush complete";
    }

    TimeAlignerConfig createBDM2AlignerConfig(
        const std::string &outputDir,
        const openpni::CoincidenceProtocol &coinProtocol)
    {
        TimeAlignerConfig config;
        config.outputDir = outputDir;
        config.channelNum = 48;
        config.crystalsPerChannel = 169 * 4;
        config.coinProtocol = coinProtocol;
        config.enableMultiGpu = true;
        return config;
    }

    TimeAlignerConfig createBDM50100_9120AlignerConfig(
        const std::string &outputDir,
        const openpni::CoincidenceProtocol &coinProtocol)
    {
        TimeAlignerConfig config;
        config.outputDir = outputDir;
        config.channelNum = 576;
        config.crystalsPerChannel = 6 * 6 * 8;
        config.coinProtocol = coinProtocol;
        config.maxChunksPerNode = 1000;
        config.maxTotalMemoryBytes = 8ULL * 1024 * 1024 * 1024;
        config.processingIntervalMs = 100;
        config.enableMultiGpu = true;
        return config;
    }

} // namespace openpni::distributed::streaming
