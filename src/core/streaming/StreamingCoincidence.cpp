#include "core/streaming/StreamingCoincidence.hpp"

#include "core/streaming/multi_gpu/CoincidenceMultiGpuEngine.hpp"

#include <algorithm>
#include <chrono>
#include <iterator>
#include <limits>
#include <optional>
#include <string>
#include <thread>
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
        if (remainingEmpty())
        {
            minTime_pico = UINT64_MAX;
            maxTime_pico = 0;
            return;
        }

        minTime_pico = UINT64_MAX;
        maxTime_pico = 0;
        const size_t begin = remainingBegin();
        for (size_t i = begin; i < singles.size(); ++i)
        {
            minTime_pico = std::min(minTime_pico, singles[i].timevalue_100fs);
            maxTime_pico = std::max(maxTime_pico, singles[i].timevalue_100fs);
        }
    }

    void TimestampedSingleChunk::refreshTimeRangeFromSortedEnds()
    {
        if (remainingEmpty())
        {
            minTime_pico = UINT64_MAX;
            maxTime_pico = 0;
            return;
        }
        minTime_pico = singles[remainingBegin()].timevalue_100fs;
        maxTime_pico = singles.back().timevalue_100fs;
    }

    void TimestampedSingleChunk::fillTimeRangePreferSortedEnds()
    {
        consumed = remainingBegin();
        if (remainingEmpty())
        {
            minTime_pico = UINT64_MAX;
            maxTime_pico = 0;
            return;
        }
        const uint64_t first = singles[consumed].timevalue_100fs;
        const uint64_t last = singles.back().timevalue_100fs;
        if (first <= last)
        {
            minTime_pico = first;
            maxTime_pico = last;
            return;
        }
        updateTimeRange();
    }

    void TimestampedSingleChunk::consumePrefix(size_t n)
    {
        const size_t take = std::min(n, remainingCount());
        consumed = remainingBegin() + take;
        refreshTimeRangeFromSortedEnds();
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

        chunk.fillTimeRangePreferSortedEnds();
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

    std::optional<TimestampedSingleChunk> NodeRingBuffer::stealWholeFront(uint64_t boundary)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (!m_buffer.empty() && m_buffer.front().remainingEmpty())
        {
            TimestampedSingleChunk empty = std::move(m_buffer.front());
            m_buffer.pop_front();
            const size_t mem = empty.memorySize();
            m_bufferMemoryBytes =
                mem <= m_bufferMemoryBytes ? m_bufferMemoryBytes - mem : 0;
            lock.unlock();
            dropStolenChunk(std::move(empty));
            lock.lock();
        }
        if (m_buffer.empty() || m_buffer.front().minTime_pico > boundary)
        {
            return std::nullopt;
        }

        const size_t memBefore = m_buffer.front().memorySize();
        TimestampedSingleChunk stolen = std::move(m_buffer.front());
        m_buffer.pop_front();
        m_bufferMemoryBytes =
            memBefore <= m_bufferMemoryBytes ? m_bufferMemoryBytes - memBefore : 0;
        lock.unlock();
        m_cvNotFull.notify_all();
        return stolen;
    }

    std::vector<TimestampedSingleChunk> NodeRingBuffer::stealWholeFronts(size_t maxCount)
    {
        std::vector<TimestampedSingleChunk> out;
        if (maxCount == 0)
        {
            return out;
        }
        out.reserve(maxCount);

        std::unique_lock<std::mutex> lock(m_mutex);
        const size_t canTake = std::min(maxCount, m_buffer.size());
        out.reserve(canTake);
        while (out.size() < maxCount)
        {
            while (!m_buffer.empty() && m_buffer.front().remainingEmpty())
            {
                TimestampedSingleChunk empty = std::move(m_buffer.front());
                m_buffer.pop_front();
                const size_t mem = empty.memorySize();
                m_bufferMemoryBytes =
                    mem <= m_bufferMemoryBytes ? m_bufferMemoryBytes - mem : 0;
                lock.unlock();
                dropStolenChunk(std::move(empty));
                lock.lock();
            }
            if (m_buffer.empty())
            {
                break;
            }

            const size_t memBefore = m_buffer.front().memorySize();
            out.push_back(std::move(m_buffer.front()));
            m_buffer.pop_front();
            m_bufferMemoryBytes =
                memBefore <= m_bufferMemoryBytes ? m_bufferMemoryBytes - memBefore : 0;
        }
        lock.unlock();
        if (!out.empty())
        {
            m_cvNotFull.notify_all();
        }
        return out;
    }

    void NodeRingBuffer::reinsertFront(TimestampedSingleChunk &&chunk)
    {
        if (chunk.remainingEmpty())
        {
            dropStolenChunk(std::move(chunk));
            return;
        }

        const size_t mem = chunk.memorySize();
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_buffer.push_front(std::move(chunk));
            m_bufferMemoryBytes += mem;
        }
        m_cvNotEmpty.notify_one();
    }

    void NodeRingBuffer::dropStolenChunk(TimestampedSingleChunk &&chunk)
    {
        const size_t mem = chunk.memorySize();
        chunk.singles = {};
        if (m_memoryPool && mem > 0)
        {
            m_memoryPool->release(mem);
        }
    }

    bool NodeRingBuffer::peekFront(uint64_t *minTime, uint64_t *maxTime, size_t *count) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_buffer.empty() || m_buffer.front().remainingEmpty())
        {
            return false;
        }
        if (minTime)
        {
            *minTime = m_buffer.front().minTime_pico;
        }
        if (maxTime)
        {
            *maxTime = m_buffer.front().maxTime_pico;
        }
        if (count)
        {
            *count = m_buffer.front().remainingCount();
        }
        return true;
    }

    std::vector<Single> NodeRingBuffer::stealFront(uint64_t boundary, size_t budget)
    {
        if (budget == 0)
        {
            return {};
        }

        auto stolenChunk = stealWholeFront(boundary);
        if (!stolenChunk)
        {
            return {};
        }

        auto timeEnd = stolenChunk->singles.end();
        if (stolenChunk->maxTime_pico > boundary)
        {
            timeEnd = std::upper_bound(
                stolenChunk->singles.begin(), stolenChunk->singles.end(), boundary,
                [](uint64_t bound, const Single &s)
                { return bound < s.timevalue_100fs; });
        }

        const size_t available = static_cast<size_t>(
            std::distance(stolenChunk->singles.begin(), timeEnd));
        if (available == 0)
        {
            reinsertFront(std::move(*stolenChunk));
            return {};
        }

        size_t take = std::min(budget, available);
        while (take < available &&
               stolenChunk->singles[take].timevalue_100fs ==
                   stolenChunk->singles[take - 1].timevalue_100fs)
        {
            ++take;
        }

        std::vector<Single> stolen;
        if (take == stolenChunk->singles.size())
        {
            stolen.swap(stolenChunk->singles);
            dropStolenChunk(std::move(*stolenChunk));
            return stolen;
        }

        stolen.assign(stolenChunk->singles.begin(),
                      stolenChunk->singles.begin() + static_cast<std::ptrdiff_t>(take));
        stolenChunk->singles.erase(
            stolenChunk->singles.begin(),
            stolenChunk->singles.begin() + static_cast<std::ptrdiff_t>(take));
        stolenChunk->refreshTimeRangeFromSortedEnds();
        reinsertFront(std::move(*stolenChunk));
        return stolen;
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

    size_t NodeRingBuffer::countReadySingles(uint64_t boundary) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        size_t n = 0;
        for (const auto &chunk : m_buffer)
        {
            if (chunk.maxTime_pico <= boundary)
            {
                n += chunk.remainingCount();
                continue;
            }
            if (chunk.minTime_pico <= boundary)
            {
                n += chunk.remainingCount();
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
        stealNs = 0;
        mergeNs = 0;
        watermarkNs = 0;
        slotWaitNs = 0;
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

        struct EarlierByTime
        {
            bool operator()(const Single &a, const Single &b) const noexcept
            {
                return a.timevalue_100fs < b.timevalue_100fs;
            }
        };

        size_t countLeTime(const Single *b, size_t n, uint64_t t)
        {
            if (n == 0 || b == nullptr)
            {
                return 0;
            }
            auto it = std::upper_bound(
                b, b + n, t,
                [](uint64_t bound, const Single &s)
                { return bound < s.timevalue_100fs; });
            return static_cast<size_t>(it - b);
        }

        uint64_t firstTimeGe(const Single *b, size_t n, uint64_t t)
        {
            if (n == 0 || b == nullptr)
            {
                return UINT64_MAX;
            }
            auto it = std::lower_bound(
                b, b + n, t,
                [](const Single &s, uint64_t bound)
                { return s.timevalue_100fs < bound; });
            return it == b + n ? UINT64_MAX : it->timevalue_100fs;
        }

        // 与逐元素 k-way 停点相同：取满 budget 且 lastT>=floorBound，并收齐 lastT 上的并列。
        // 有序区间上用二分找切点，避免热路径再扫一遍 262k。
        void planLayerTakeN(const std::vector<TimestampedSingleChunk> &layer,
                            const std::vector<char> &has,
                            const std::vector<size_t> &off,
                            const std::vector<size_t> &limit,
                            size_t nNodes, size_t alreadyTaken, size_t budget,
                            uint64_t floorBound, uint64_t *lastT,
                            std::vector<size_t> *takeN, size_t *got)
        {
            takeN->assign(nNodes, 0);
            *got = 0;
            const size_t prefixK = alreadyTaken < budget ? budget - alreadyTaken : 0;

            auto view = [&](size_t i) -> const Single *
            {
                return layer[i].singles.data() + off[i];
            };

            auto countLe = [&](uint64_t t) -> size_t
            {
                size_t n = 0;
                for (size_t i = 0; i < nNodes; ++i)
                {
                    if (has[i])
                    {
                        n += countLeTime(view(i), limit[i], t);
                    }
                }
                return n;
            };

            size_t nAvail = 0;
            uint64_t lo = UINT64_MAX;
            uint64_t hi = 0;
            for (size_t i = 0; i < nNodes; ++i)
            {
                if (!has[i] || limit[i] == 0)
                {
                    continue;
                }
                nAvail += limit[i];
                lo = std::min(lo, view(i)[0].timevalue_100fs);
                hi = std::max(hi, view(i)[limit[i] - 1].timevalue_100fs);
            }
            if (nAvail == 0)
            {
                return;
            }

            const size_t k = std::max(prefixK, size_t{1});
            uint64_t tK = hi;
            if (k <= nAvail)
            {
                uint64_t blo = lo;
                uint64_t bhi = hi;
                while (blo < bhi)
                {
                    const uint64_t mid = blo + (bhi - blo) / 2;
                    if (countLe(mid) >= k)
                    {
                        bhi = mid;
                    }
                    else if (mid == UINT64_MAX)
                    {
                        break;
                    }
                    else
                    {
                        blo = mid + 1;
                    }
                }
                tK = blo;
            }

            uint64_t tStop = tK;
            if (tK < floorBound)
            {
                tStop = UINT64_MAX;
                for (size_t i = 0; i < nNodes; ++i)
                {
                    if (!has[i] || limit[i] == 0)
                    {
                        continue;
                    }
                    tStop = std::min(tStop, firstTimeGe(view(i), limit[i], floorBound));
                }
                if (tStop == UINT64_MAX)
                {
                    for (size_t i = 0; i < nNodes; ++i)
                    {
                        if (has[i])
                        {
                            (*takeN)[i] = limit[i];
                            *got += limit[i];
                        }
                    }
                    *lastT = hi;
                    return;
                }
            }

            for (size_t i = 0; i < nNodes; ++i)
            {
                if (!has[i])
                {
                    continue;
                }
                (*takeN)[i] = countLeTime(view(i), limit[i], tStop);
                *got += (*takeN)[i];
            }
            *lastT = tStop;
        }

        // 把若干条各自有序的段归并成一条全局有序序列。段数等于节点数（通常 2），
        // 每元素 k 次比较，比对整批 std::sort 的 O(n log n) 便宜得多。
        void mergeSortedViews(const std::vector<std::pair<const Single *, const Single *>> &runs,
                              Single *dst)
        {
            if (runs.empty())
            {
                return;
            }
            if (runs.size() == 1)
            {
                std::copy(runs[0].first, runs[0].second, dst);
                return;
            }
            if (runs.size() == 2)
            {
                std::merge(runs[0].first, runs[0].second,
                           runs[1].first, runs[1].second, dst, EarlierByTime{});
                return;
            }

            std::vector<const Single *> cursor(runs.size());
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
                    const uint64_t t = cursor[i]->timevalue_100fs;
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
                dst[out++] = *cursor[pick]++;
            }
        }
    } // namespace

    bool StreamingTimeAligner::StolenLane::peekFront(uint64_t *minTime, uint64_t *maxTime,
                                                     size_t *count) const
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (chunks.empty() || chunks.front().remainingEmpty())
        {
            return false;
        }
        if (minTime)
        {
            *minTime = chunks.front().minTime_pico;
        }
        if (maxTime)
        {
            *maxTime = chunks.front().maxTime_pico;
        }
        if (count)
        {
            *count = chunks.front().remainingCount();
        }
        return true;
    }

    std::optional<TimestampedSingleChunk>
    StreamingTimeAligner::StolenLane::popWholeFront(uint64_t boundary)
    {
        std::lock_guard<std::mutex> lock(mutex);
        while (!chunks.empty() && chunks.front().remainingEmpty())
        {
            chunks.pop_front();
        }
        if (chunks.empty() || chunks.front().minTime_pico > boundary)
        {
            return std::nullopt;
        }
        TimestampedSingleChunk stolen = std::move(chunks.front());
        chunks.pop_front();
        const size_t n = stolen.remainingCount();
        singles = singles >= n ? singles - n : 0;
        cvNotFull.notify_one();
        return stolen;
    }

    std::vector<TimestampedSingleChunk>
    StreamingTimeAligner::StolenLane::popCompleteBefore(uint64_t bound, size_t maxSingles)
    {
        std::vector<TimestampedSingleChunk> out;
        size_t takenSingles = 0;
        {
            std::lock_guard<std::mutex> lock(mutex);
            while (!chunks.empty())
            {
                if (chunks.front().remainingEmpty())
                {
                    chunks.pop_front();
                    continue;
                }
                const auto &front = chunks.front();
                if (front.minTime_pico > bound || front.maxTime_pico > bound)
                {
                    break;
                }
                const size_t n = front.remainingCount();
                if (takenSingles > 0 && maxSingles != std::numeric_limits<size_t>::max() &&
                    takenSingles + n > maxSingles)
                {
                    break;
                }
                TimestampedSingleChunk stolen = std::move(chunks.front());
                chunks.pop_front();
                singles = singles >= n ? singles - n : 0;
                takenSingles += n;
                out.push_back(std::move(stolen));
            }
        }
        if (!out.empty())
        {
            cvNotFull.notify_all();
        }
        return out;
    }

    std::optional<TimestampedSingleChunk> StreamingTimeAligner::StolenLane::tryPop()
    {
        std::lock_guard<std::mutex> lock(mutex);
        while (!chunks.empty() && chunks.front().remainingEmpty())
        {
            chunks.pop_front();
        }
        if (chunks.empty())
        {
            return std::nullopt;
        }
        TimestampedSingleChunk stolen = std::move(chunks.front());
        chunks.pop_front();
        const size_t n = stolen.remainingCount();
        singles = singles >= n ? singles - n : 0;
        cvNotFull.notify_one();
        return stolen;
    }

    void StreamingTimeAligner::StolenLane::reinsertFront(TimestampedSingleChunk &&chunk)
    {
        if (chunk.remainingEmpty())
        {
            return;
        }
        const size_t n = chunk.remainingCount();
        {
            std::lock_guard<std::mutex> lock(mutex);
            chunks.push_front(std::move(chunk));
            singles += n;
        }
        cvNotEmpty.notify_one();
    }

    void StreamingTimeAligner::StolenLane::pushBack(TimestampedSingleChunk &&chunk, size_t cap,
                                                    const std::atomic<bool> &stop)
    {
        if (chunk.remainingEmpty())
        {
            return;
        }
        const size_t n = chunk.remainingCount();
        {
            std::unique_lock<std::mutex> lock(mutex);
            cvNotFull.wait(lock, [&]
                           { return chunks.size() < cap || stop.load(std::memory_order_acquire); });
            chunks.push_back(std::move(chunk));
            singles += n;
        }
        cvNotEmpty.notify_one();
    }

    bool StreamingTimeAligner::StolenLane::empty() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return chunks.empty();
    }

    size_t StreamingTimeAligner::StolenLane::size() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return chunks.size();
    }

    size_t StreamingTimeAligner::StolenLane::countReadySingles(uint64_t boundary) const
    {
        std::lock_guard<std::mutex> lock(mutex);
        size_t n = 0;
        for (const auto &chunk : chunks)
        {
            if (chunk.minTime_pico > boundary)
            {
                break;
            }
            n += chunk.remainingCount();
        }
        return n;
    }

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
        m_stolenLanes.reserve(nodeCount);
        for (size_t i = 0; i < nodeCount; ++i)
        {
            m_stolenLanes.push_back(std::make_unique<StolenLane>());
            m_nodeBuffers.push_back(
                std::make_unique<NodeRingBuffer>(i, config.maxChunksPerNode, poolPtr));
            // 数据到达即唤醒处理线程与 steal 工人；是否真的开工由处理线程的触发判定决定，
            // 这样所有策略集中在一处。
            m_nodeBuffers.back()->setPushObserver(
                [this]()
                {
                    wakeStealAndCoord();
                });
        }

        m_mergeRuns.reserve(nodeCount);

        std::vector<uint32_t> crystalNumOfEachChannel(
            config.channelNum, config.crystalsPerChannel);

        const bool skipGpu = config.extractOnly || config.stealOnly;
        m_useMultiGpu = !skipGpu && multi_gpu::shouldUseCoincidenceMultiGpu(config);
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
                  << (config.extractOnly ? ", extractOnly" : "")
                  << (config.stealOnly ? ", stealOnly" : "")
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
        m_stealStop.store(false, std::memory_order_release);
        if (m_useMultiGpu && m_multiGpuEngine)
        {
            m_writerThread = std::thread([this]()
                                         { writerLoop(); });
            m_drainThread = std::thread([this]()
                                        { drainLoop(); });
        }
        startStealWorkers();
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

        // 先停 steal：把环里剩余 chunk 抽进 deque。协调线程此时仍在跑，避免 deque
        // 满时 steal 与 stop 互相等待。
        m_stealStop.store(true, std::memory_order_release);
        wakeStealAndCoord();
        joinStealWorkers();

        // 让处理线程从 wait_for 里立刻醒来，抽空 deque 后 flush。
        wakeStealAndCoord();

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
            opts.io.ioQueueSize = std::max(1u, m_config.listmodeIoQueueSize);
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
            opts.io.ioQueueSize = std::max(1u, m_config.listmodeIoQueueSize);
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
        const auto failed =
            (m_promptWriter.GetStatus() & openpni::io::IOStatus_DiskSpaceNotEnough) != 0 ||
            (m_delayWriter.GetStatus() & openpni::io::IOStatus_DiskSpaceNotEnough) != 0;
        if (failed)
        {
            LOG(ERROR) << "[StreamingTimeAligner] listmode flush/fsync reported disk/IO failure";
            m_processFailed.store(true, std::memory_order_release);
        }
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

    size_t StreamingTimeAligner::countReadyBefore(uint64_t boundary) const
    {
        size_t n = 0;
        for (const auto &lane : m_stolenLanes)
        {
            n += lane->countReadySingles(boundary);
        }
        for (const auto &buf : m_nodeBuffers)
        {
            n += buf->countReadySingles(boundary);
        }
        return n;
    }

    bool StreamingTimeAligner::hasStolenChunks() const
    {
        for (const auto &lane : m_stolenLanes)
        {
            if (!lane->empty())
            {
                return true;
            }
        }
        return false;
    }

    void StreamingTimeAligner::wakeStealAndCoord()
    {
        m_wakeSeq.fetch_add(1, std::memory_order_release);
        m_wakeCv.notify_all();
        m_stealCv.notify_all();
        for (auto &lane : m_stolenLanes)
        {
            if (lane)
            {
                lane->cvNotFull.notify_all();
                lane->cvNotEmpty.notify_all();
            }
        }
    }

    void StreamingTimeAligner::publishWatermark(uint64_t watermark)
    {
        uint64_t prev = m_publishedWatermark.load(std::memory_order_relaxed);
        while (watermark > prev &&
               !m_publishedWatermark.compare_exchange_weak(
                   prev, watermark, std::memory_order_release, std::memory_order_relaxed))
        {
        }
        if (watermark > prev)
        {
            m_stealCv.notify_all();
        }
    }

    bool StreamingTimeAligner::waitStolenReady(uint64_t watermark)
    {
        while (true)
        {
            bool anyReady = false;
            bool waitingOnSteal = false;
            const bool stealAlive = m_stealAlive.load(std::memory_order_acquire) > 0;
            for (size_t i = 0; i < m_stolenLanes.size(); ++i)
            {
                uint64_t stolenMin = 0;
                const bool stolenOk =
                    m_stolenLanes[i]->peekFront(&stolenMin, nullptr, nullptr) &&
                    stolenMin <= watermark;
                if (stolenOk)
                {
                    anyReady = true;
                    continue;
                }
                const bool inFlight =
                    m_stolenLanes[i]->inFlight.load(std::memory_order_acquire);
                uint64_t ringMin = 0;
                const bool ringHas =
                    m_nodeBuffers[i]->peekFront(&ringMin, nullptr, nullptr) &&
                    ringMin <= watermark;
                if (inFlight || (ringHas && stealAlive))
                {
                    waitingOnSteal = true;
                }
            }
            if (waitingOnSteal)
            {
                const uint64_t seq = m_wakeSeq.load(std::memory_order_acquire);
                std::unique_lock<std::mutex> lock(m_wakeMutex);
                m_wakeCv.wait_for(
                    lock, std::chrono::milliseconds(1),
                    [this, seq]()
                    {
                        return m_wakeSeq.load(std::memory_order_acquire) != seq ||
                               m_stealStop.load(std::memory_order_acquire) ||
                               !m_running.load(std::memory_order_acquire);
                    });
                continue;
            }
            return anyReady;
        }
    }

    void StreamingTimeAligner::startStealWorkers()
    {
        m_stealAlive.store(0, std::memory_order_release);
        for (size_t i = 0; i < m_stolenLanes.size(); ++i)
        {
            m_stolenLanes[i]->thread = std::thread([this, i]()
                                                   { stealLoop(i); });
        }
    }

    void StreamingTimeAligner::joinStealWorkers()
    {
        for (auto &lane : m_stolenLanes)
        {
            if (lane->thread.joinable())
            {
                lane->thread.join();
            }
        }
    }

    void StreamingTimeAligner::stealLoop(size_t nodeIdx)
    {
        m_stealAlive.fetch_add(1, std::memory_order_acq_rel);
        auto *buf = m_nodeBuffers[nodeIdx].get();
        auto &lane = *m_stolenLanes[nodeIdx];
        const size_t cap = std::max<size_t>(1, m_config.stolenDequeCap);

        while (true)
        {
            const bool stopping = m_stealStop.load(std::memory_order_acquire);
            const size_t queued = lane.size();
            if (!stopping && queued >= cap)
            {
                std::unique_lock<std::mutex> lock(lane.mutex);
                lane.cvNotFull.wait_for(
                    lock, std::chrono::milliseconds(1),
                    [&lane, cap, this]()
                    {
                        return lane.chunks.size() < cap ||
                               m_stealStop.load(std::memory_order_acquire);
                    });
                continue;
            }
            if (buf->empty())
            {
                if (stopping)
                {
                    break;
                }
                std::unique_lock<std::mutex> lock(m_stealMutex);
                m_stealCv.wait_for(
                    lock, std::chrono::milliseconds(1),
                    [this, buf]()
                    {
                        return m_stealStop.load(std::memory_order_acquire) ||
                               !buf->empty();
                    });
                continue;
            }

            const size_t room = stopping
                                    ? std::max<size_t>(1, buf->size() + cap)
                                    : (queued < cap ? cap - queued : 0);
            if (room == 0)
            {
                continue;
            }

            const auto stealBegin = std::chrono::steady_clock::now();
            lane.inFlight.store(true, std::memory_order_release);
            auto stolen = buf->stealWholeFronts(room);
            if (stolen.empty())
            {
                lane.inFlight.store(false, std::memory_order_release);
                wakeStealAndCoord();
                continue;
            }

            if (m_config.stealOnly)
            {
                for (auto &chunk : stolen)
                {
                    m_stats.totalSinglesReceived += chunk.remainingCount();
                    m_stats.totalSinglesProcessed += chunk.remainingCount();
                    buf->dropStolenChunk(std::move(chunk));
                }
                lane.inFlight.store(false, std::memory_order_release);
                m_stats.stealNs.fetch_add(nsSince(stealBegin), std::memory_order_relaxed);
                wakeStealAndCoord();
                continue;
            }

            for (auto &chunk : stolen)
            {
                lane.pushBack(std::move(chunk), cap, m_stealStop);
            }
            lane.inFlight.store(false, std::memory_order_release);
            m_stats.stealNs.fetch_add(nsSince(stealBegin), std::memory_order_relaxed);
            wakeStealAndCoord();
        }

        lane.inFlight.store(false, std::memory_order_release);
        m_stealAlive.fetch_sub(1, std::memory_order_acq_rel);
        wakeStealAndCoord();
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
            return hasStolenChunks();
        };

        auto stealStillRunning = [this]()
        {
            return m_stealAlive.load(std::memory_order_acquire) > 0;
        };

        // 上一段被预算钳掉了尾巴（或缓冲仍在高水位）时置位：说明水位线之内还有数据可
        // 立即处理，必须连续抽取而不是回去睡 processingIntervalMs，否则单轮只能吞下
        // maxSegmentSingles 条，积压永远追不上。
        bool moreReadyNow = false;

        while (m_running.load() || hasBufferedSingles() || stealStillRunning())
        {
            // 事件驱动：有节点 push / steal 入队就提前醒来，否则最多等 processingIntervalMs。
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
            const auto watermarkBegin = std::chrono::steady_clock::now();

            const uint64_t watermark = calculateWatermark();
            if (watermark > 0)
            {
                publishWatermark(watermark);
            }
            const bool pressure = anyNodeAboveHighWater();

            auto stopIdleToFlush = [&]()
            {
                return !m_running.load() && !stealStillRunning();
            };

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
                m_stats.watermarkNs.fetch_add(nsSince(watermarkBegin), std::memory_order_relaxed);
                if (stopIdleToFlush())
                {
                    break;
                }
                continue;
            }

            // 不变式 2：段跨度硬下界，压力与延迟兜底都不能突破。
            if (watermark - m_lastWatermark < m_config.minSegmentSpan_100fs())
            {
                m_stats.heldByMinDuration.fetch_add(1, std::memory_order_relaxed);
                m_stats.watermarkNs.fetch_add(nsSince(watermarkBegin), std::memory_order_relaxed);
                if (stopIdleToFlush())
                {
                    break; // 余量交给 flushRemaining 收尾
                }
                continue;
            }

            const size_t pending = countReadyBefore(watermark);
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
                m_stats.watermarkNs.fetch_add(nsSince(watermarkBegin), std::memory_order_relaxed);
                continue;
            }

            m_stats.watermarkNs.fetch_add(nsSince(watermarkBegin), std::memory_order_relaxed);

            if (pending > 0)
            {
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
            }

            const uint64_t recBefore = m_stats.totalSinglesReceived.load(std::memory_order_relaxed);
            if (!processSegment(watermark))
            {
                break;
            }

            const bool extracted =
                m_stats.totalSinglesReceived.load(std::memory_order_relaxed) > recBefore;
            m_stats.currentTimeBoundary_pico = m_lastWatermark;
            moreReadyNow = m_lastWatermark < watermark;
            if (!extracted)
            {
                continue;
            }

            m_stats.chunksProcessed++;
            lastProcessTime = std::chrono::steady_clock::now();

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
        m_stageCapacity = want;
    }

    void StreamingTimeAligner::initInputSlots()
    {
        size_t n = 0;
        if (m_config.extractOnly && !m_config.stealOnly)
        {
            // 不建 GPU 工人；抽取墙走 pageable m_extractHost，不占 pinned 槽。
            n = 0;
            const size_t cap = m_config.maxSegmentSingles == 0
                                   ? size_t{262144}
                                   : std::max<size_t>(2, m_config.maxSegmentSingles);
            if (m_extractHost.size() < cap + 64)
            {
                m_extractHost.resize(cap + 64);
            }
        }
        else if (m_useMultiGpu && m_multiGpuEngine)
        {
            n = std::max<size_t>(1, m_multiGpuEngine->ringSize());
        }
        else
        {
            return;
        }

        const size_t nSlots = n;
        m_inputSlotCapacity = m_config.maxSegmentSingles == 0
                                  ? size_t{262144}
                                  : std::max<size_t>(2, m_config.maxSegmentSingles);
        m_inputSlots.clear();
        m_inputSlots.reserve(nSlots);
        {
            std::lock_guard<std::mutex> lock(m_slotMutex);
            m_freeSlots.clear();
            for (size_t i = 0; i < nSlots; ++i)
            {
                m_inputSlots.emplace_back();
                m_inputSlots.back().buffer.ResetPointer(m_inputSlotCapacity);
                m_freeSlots.push_back(i);
            }
        }
        m_writeQueueCap = std::max(nSlots, m_config.listmodeWriteQueueCap);
        {
            std::lock_guard<std::mutex> lock(m_submittedMutex);
            m_submittedSlots.clear();
        }
        {
            std::lock_guard<std::mutex> lock(m_writeMutex);
            m_writeQueue.clear();
        }
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
        const auto slotBegin = std::chrono::steady_clock::now();
        const size_t slotIdx = acquireInputSlot();
        m_stats.slotWaitNs.fetch_add(nsSince(slotBegin), std::memory_order_relaxed);
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

    bool StreamingTimeAligner::processSegment(uint64_t watermark)
    {
        if (m_config.stealOnly)
        {
            m_lastWatermark = watermark;
            m_stats.currentTimeBoundary_pico = watermark;
            return true;
        }

        waitStolenReady(watermark);
        const auto extractBegin = std::chrono::steady_clock::now();

        const size_t nNodes = m_nodeBuffers.size();
        const size_t carryCount = m_carrySingles.size();

        size_t budget = std::numeric_limits<size_t>::max();
        if (m_config.maxSegmentSingles != 0)
        {
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

        const uint64_t minSpan = std::max<uint64_t>(1, m_config.minSegmentSpan_100fs());
        const uint64_t minBoundFromLast = m_lastWatermark > UINT64_MAX - minSpan
                                              ? UINT64_MAX
                                              : m_lastWatermark + minSpan;
        uint64_t earliest = UINT64_MAX;
        auto peekEarliest = [&](auto &&peekFn)
        {
            uint64_t mn = 0;
            if (peekFn(&mn, nullptr, nullptr) && mn <= watermark)
            {
                earliest = std::min(earliest, mn);
            }
        };
        for (auto &lane : m_stolenLanes)
        {
            peekEarliest([&](uint64_t *mn, uint64_t *, size_t *)
                         { return lane->peekFront(mn, nullptr, nullptr); });
        }
        if (earliest == UINT64_MAX)
        {
            for (const auto &buf : m_nodeBuffers)
            {
                peekEarliest([&](uint64_t *mn, uint64_t *, size_t *)
                             { return buf->peekFront(mn, nullptr, nullptr); });
            }
        }
        const uint64_t floorBound =
            earliest != UINT64_MAX
                ? std::min(watermark,
                           std::max(minBoundFromLast,
                                    earliest > UINT64_MAX - minSpan
                                        ? UINT64_MAX
                                        : earliest + minSpan))
                : std::min(minBoundFromLast, watermark);

        struct KeptBatch
        {
            TimestampedSingleChunk chunk;
            size_t takeN = 0;
            uint16_t node = 0;
            bool poolOwned = true;
        };
        struct PeekMeta
        {
            bool ok = false;
            uint64_t minT = UINT64_MAX;
            uint64_t maxT = 0;
            size_t n = 0;
        };

        std::vector<KeptBatch> kept;
        size_t taken = 0;
        uint64_t endTime = m_lastWatermark;
        bool exceededBudget = false;

        auto currentRoom = [&]() -> size_t
        {
            if (endTime < floorBound)
            {
                return std::numeric_limits<size_t>::max();
            }
            return taken < budget ? budget - taken : 0;
        };

        while (true)
        {
            const size_t roomNow = currentRoom();
            if (roomNow == 0)
            {
                break;
            }
            const uint64_t extractCap = watermark;
            // 每个节点要么 deque 上已有 <=W 的队头，要么环/在途确认没有。禁止在
            // 某节点仍把更早事件藏在环里时，先抽另一节点的后续块。
            if (!waitStolenReady(extractCap))
            {
                break;
            }

            std::vector<PeekMeta> peeks(nNodes);
            size_t pick = nNodes;
            uint64_t pickMin = UINT64_MAX;
            for (size_t i = 0; i < nNodes; ++i)
            {
                uint64_t mn = 0;
                uint64_t mx = 0;
                size_t nn = 0;
                if (!m_stolenLanes[i]->peekFront(&mn, &mx, &nn))
                {
                    continue;
                }
                peeks[i] = PeekMeta{true, mn, mx, nn};
                if (mn <= extractCap && mn < pickMin)
                {
                    pick = i;
                    pickMin = mn;
                }
            }
            if (pick == nNodes)
            {
                break;
            }

            uint64_t otherMin = UINT64_MAX;
            for (size_t i = 0; i < nNodes; ++i)
            {
                if (i == pick || !peeks[i].ok || peeks[i].minT > extractCap)
                {
                    continue;
                }
                otherMin = std::min(otherMin, peeks[i].minT);
            }

            const PeekMeta &p = peeks[pick];
            const bool whollyBeforeOthers = p.maxT <= extractCap && p.maxT <= otherMin;
            const size_t roomBudget = taken < budget ? budget - taken : 0;
            const size_t segCap = m_config.maxSegmentSingles == 0
                                      ? std::numeric_limits<size_t>::max()
                                      : m_config.maxSegmentSingles;
            // 低于 floorBound 时允许整块拿走不超过 maxSegment 的 chunk；禁止 SIZE_MAX 吞 5M 块。
            const size_t wholeLimit = endTime < floorBound
                                          ? std::max(roomBudget, segCap)
                                          : roomBudget;
            const bool takeWhole = whollyBeforeOthers && p.n <= wholeLimit && p.n <= roomNow;

            if (takeWhole)
            {
                const uint64_t wholeCap = otherMin == UINT64_MAX ? extractCap
                                                                : std::min(extractCap, otherMin);
                const size_t batchRoom = wholeLimit;
                auto batch = m_stolenLanes[pick]->popCompleteBefore(wholeCap, batchRoom);
                if (batch.empty())
                {
                    auto chunk = m_stolenLanes[pick]->popWholeFront(extractCap);
                    if (!chunk)
                    {
                        break;
                    }
                    batch.push_back(std::move(*chunk));
                }
                bool tookAny = false;
                for (auto &chunk : batch)
                {
                    const size_t n = chunk.remainingCount();
                    if (n == 0)
                    {
                        m_nodeBuffers[pick]->dropStolenChunk(std::move(chunk));
                        continue;
                    }
                    taken += n;
                    endTime = std::max(endTime, chunk.maxTime_pico);
                    if (taken > budget)
                    {
                        exceededBudget = true;
                    }
                    const bool owned = chunk.poolOwned;
                    kept.push_back(KeptBatch{
                        std::move(chunk), n, static_cast<uint16_t>(pick), owned});
                    tookAny = true;
                }
                if (!tookAny)
                {
                    continue;
                }
                continue;
            }

            std::vector<size_t> stealSet;
            if (whollyBeforeOthers)
            {
                stealSet.push_back(pick);
            }
            else
            {
                for (size_t i = 0; i < nNodes; ++i)
                {
                    if (peeks[i].ok && peeks[i].minT <= extractCap)
                    {
                        stealSet.push_back(i);
                    }
                }
            }

            std::vector<TimestampedSingleChunk> layer(nNodes);
            std::vector<char> has(nNodes, 0);
            for (size_t idx : stealSet)
            {
                auto chunk = m_stolenLanes[idx]->popWholeFront(extractCap);
                if (!chunk)
                {
                    continue;
                }
                if (chunk->remainingEmpty())
                {
                    m_nodeBuffers[idx]->dropStolenChunk(std::move(*chunk));
                    continue;
                }
                layer[idx] = std::move(*chunk);
                has[idx] = 1;
            }

            std::vector<size_t> limit(nNodes, 0);
            std::vector<size_t> off(nNodes, 0);
            for (size_t i = 0; i < nNodes; ++i)
            {
                if (!has[i])
                {
                    continue;
                }
                off[i] = layer[i].remainingBegin();
                auto &s = layer[i].singles;
                if (layer[i].maxTime_pico <= extractCap)
                {
                    limit[i] = layer[i].remainingCount();
                }
                else
                {
                    auto it = std::upper_bound(
                        s.begin() + static_cast<std::ptrdiff_t>(off[i]), s.end(), extractCap,
                        [](uint64_t bound, const Single &e)
                        { return bound < e.timevalue_100fs; });
                    limit[i] = static_cast<size_t>(
                        std::distance(s.begin() + static_cast<std::ptrdiff_t>(off[i]), it));
                }
            }

            std::vector<size_t> takeOf(nNodes, 0);
            size_t got = 0;
            uint64_t lastT = endTime;
            planLayerTakeN(layer, has, off, limit, nNodes, taken, budget, floorBound,
                           &lastT, &takeOf, &got);

            if (got == 0)
            {
                for (size_t i = 0; i < nNodes; ++i)
                {
                    if (has[i])
                    {
                        m_stolenLanes[i]->reinsertFront(std::move(layer[i]));
                    }
                }
                break;
            }

            if (taken + got > budget && lastT < floorBound)
            {
                exceededBudget = true;
            }
            taken += got;
            endTime = lastT;

            for (size_t i = 0; i < nNodes; ++i)
            {
                if (!has[i])
                {
                    continue;
                }
                const size_t takeN = takeOf[i];
                auto &ch = layer[i];
                if (takeN == 0)
                {
                    m_stolenLanes[i]->reinsertFront(std::move(ch));
                    continue;
                }
                const bool owned = ch.poolOwned;
                kept.push_back(KeptBatch{
                    std::move(ch), takeN, static_cast<uint16_t>(i), owned});
            }

            if (endTime >= floorBound && taken >= budget)
            {
                break;
            }
        }

        if (exceededBudget)
        {
            m_stats.oversizedSegments.fetch_add(1, std::memory_order_relaxed);
            LOG_EVERY_N(WARNING, 64)
                << "[StreamingTimeAligner] 一个重叠窗内的数据量已超过显存预算 ("
                << budget << ")，本段放宽预算以保证正确性；请调大 maxSegmentSingles";
        }

        bool moreAtOrBefore = false;
        for (const auto &b : kept)
        {
            if (b.takeN < b.chunk.remainingCount())
            {
                moreAtOrBefore = true;
                break;
            }
        }
        if (!moreAtOrBefore)
        {
            for (size_t i = 0; i < nNodes; ++i)
            {
                if (m_stolenLanes[i]->inFlight.load(std::memory_order_acquire))
                {
                    moreAtOrBefore = true;
                    break;
                }
                uint64_t mn = 0;
                if (m_stolenLanes[i]->peekFront(&mn, nullptr, nullptr) && mn <= watermark)
                {
                    moreAtOrBefore = true;
                    break;
                }
                if (m_nodeBuffers[i]->peekFront(&mn, nullptr, nullptr) && mn <= watermark)
                {
                    moreAtOrBefore = true;
                    break;
                }
            }
        }
        // 节点未到齐或本段被预算截断时，只能推到实际抽到的 endTime。
        // taken==0 且仍有残留时不得把 lastWatermark 跳到全水位。
        const uint64_t carryBound = !moreAtOrBefore
                                        ? watermark
                                        : (taken == 0 ? m_lastWatermark : endTime);

        auto releaseKept = [&](bool commit)
        {
            for (auto &b : kept)
            {
                if (commit)
                {
                    b.chunk.consumePrefix(b.takeN);
                }
                if (!b.chunk.remainingEmpty())
                {
                    m_stolenLanes[b.node]->reinsertFront(std::move(b.chunk));
                }
                else if (b.poolOwned)
                {
                    m_nodeBuffers[b.node]->dropStolenChunk(std::move(b.chunk));
                }
            }
            kept.clear();
        };

        m_stats.extractNs.fetch_add(nsSince(extractBegin), std::memory_order_relaxed);

        if (taken == 0)
        {
            if (!m_carrySingles.empty())
            {
                const auto carryBegin = std::chrono::steady_clock::now();
                updateCarrySingles(std::span<const Single>(m_carrySingles), carryBound);
                m_stats.extractNs.fetch_add(nsSince(carryBegin), std::memory_order_relaxed);
            }
            m_lastWatermark = carryBound;
            m_stats.currentTimeBoundary_pico = m_lastWatermark;
            return true;
        }

        const size_t total = carryCount + taken;
        const bool preferSlot = m_inputSlotCapacity > 0 &&
                                total <= m_inputSlotCapacity + 64 &&
                                (m_useMultiGpu && m_multiGpuEngine);

        auto mergeInto = [&](Single *dest)
        {
            if (carryCount > 0)
            {
                std::copy(m_carrySingles.begin(), m_carrySingles.end(), dest);
            }
            std::vector<std::vector<std::pair<const Single *, const Single *>>> byNode(nNodes);
            for (const auto &b : kept)
            {
                const size_t n = std::min(b.takeN, b.chunk.remainingCount());
                if (n == 0)
                {
                    continue;
                }
                const Single *p = b.chunk.singles.data() + b.chunk.remainingBegin();
                byNode[b.node].emplace_back(p, p + n);
            }
            std::vector<std::pair<const Single *, const Single *>> runs;
            std::vector<std::vector<Single>> collapsed;
            runs.reserve(nNodes);
            for (size_t i = 0; i < nNodes; ++i)
            {
                auto &vs = byNode[i];
                if (vs.empty())
                {
                    continue;
                }
                if (vs.size() == 1)
                {
                    runs.push_back(vs[0]);
                    continue;
                }
                // 同节点多块时间相接，先收成一条 run，再两路 std::merge。
                size_t runN = 0;
                for (const auto &v : vs)
                {
                    runN += static_cast<size_t>(v.second - v.first);
                }
                collapsed.emplace_back();
                auto &buf = collapsed.back();
                buf.resize(runN);
                Single *w = buf.data();
                for (const auto &v : vs)
                {
                    w = std::copy(v.first, v.second, w);
                }
                runs.emplace_back(buf.data(), buf.data() + buf.size());
            }
            mergeSortedViews(runs, dest + carryCount);
        };

        auto runMerge = [&](Single *dest)
        {
            const auto mergeBegin = std::chrono::steady_clock::now();
            mergeInto(dest);
            const uint64_t mergeElapsed = nsSince(mergeBegin);
            m_stats.mergeNs.fetch_add(mergeElapsed, std::memory_order_relaxed);
            m_stats.extractNs.fetch_add(mergeElapsed, std::memory_order_relaxed);
            releaseKept(true);
        };

        if (m_config.extractOnly)
        {
            if (m_extractHost.size() < total)
            {
                m_extractHost.resize(total);
            }
            runMerge(m_extractHost.data());
            m_stats.totalSinglesReceived += taken;
            m_stats.carrySinglesTotal.fetch_add(carryCount, std::memory_order_relaxed);
            const auto carryBegin = std::chrono::steady_clock::now();
            updateCarrySingles(std::span<const Single>(m_extractHost.data(), total),
                               carryBound);
            m_stats.extractNs.fetch_add(nsSince(carryBegin), std::memory_order_relaxed);
            m_lastWatermark = carryBound;
            m_stats.totalSinglesProcessed += taken;
            return true;
        }

        if (preferSlot)
        {
            const auto slotBegin = std::chrono::steady_clock::now();
            const size_t slotIdx = acquireInputSlot();
            m_stats.slotWaitNs.fetch_add(nsSince(slotBegin), std::memory_order_relaxed);
            if (slotIdx == std::numeric_limits<size_t>::max())
            {
                releaseKept(false);
                return false;
            }

            if (total > m_inputSlotCapacity)
            {
                m_inputSlots[slotIdx].buffer.ResetPointer(total);
            }
            runMerge(m_inputSlots[slotIdx].buffer.Data());

            m_stats.totalSinglesReceived += taken;
            const uint64_t carryCutoff = carryCount == 0 ? 0 : m_lastWatermark;
            m_stats.carrySinglesTotal.fetch_add(carryCount, std::memory_order_relaxed);
            if (!submitOwnedSlot(slotIdx, total, carryCutoff, carryBound))
            {
                return false;
            }
            m_stats.totalSinglesProcessed += taken;
            return true;
        }

        ensureStagingCapacity(total);
        Single *merged = m_stageMerged.Data();
        runMerge(merged);

        m_stats.totalSinglesReceived += taken;
        return processSinglesRange(std::span<Single>(merged, total), carryBound);
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

            if (!m_config.extractOnly && !m_config.stealOnly)
            {
                processCoincidence(std::span<const Single>(sorted), carryCutoff);
                if (m_processFailed.load(std::memory_order_acquire))
                {
                    return false;
                }
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

            // 内核在大约 5e5–1e6 条时会 CUDA 非法访问。GPU 提交不得超过硬预算。
            if (m_useMultiGpu && m_multiGpuEngine && n > hardCap)
            {
                n = nBudget;
                n = extendSameTime(n);
                if (n > hardCap + 64)
                {
                    n = hardCap;
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
            else if (!m_config.extractOnly && !m_config.stealOnly)
            {
                processCoincidence(std::span<const Single>(scratch), carryCutoff);
                if (m_processFailed.load(std::memory_order_acquire))
                {
                    return false;
                }
                updateCarrySingles(std::span<const Single>(scratch), carryBound);
                m_lastWatermark = carryBound;
            }
            else
            {
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

        if (alreadyOnHost)
        {
            std::vector<Listmode> host(coins.begin(), coins.end());
            saveCoincidenceResult(output, std::move(host));
            return;
        }

        m_coinHostBuffer.resize(coins.size());
        openpni::detail::copy_from_device_to_host(m_coinHostBuffer.data(), coins);
        saveCoincidenceResult(output, std::move(m_coinHostBuffer));
    }

    void StreamingTimeAligner::saveCoincidenceResult(
        openpni::distributed::coreio::RollingFileWriter<
            openpni::distributed::coreio::ListmodeFileWriter,
            openpni::distributed::coreio::ListmodeWriterOptions> &output,
        std::vector<Listmode> &&coins)
    {
        if (coins.empty())
        {
            return;
        }

        constexpr size_t ESTIMATED_LISTMODE_BYTES = 16;
        const uint64_t sizeEstimate = coins.size() * ESTIMATED_LISTMODE_BYTES;

        std::lock_guard<std::mutex> lock(m_outputMutex);
        if ((output.GetStatus() & openpni::io::IOStatus_DiskSpaceNotEnough) != 0)
        {
            LOG(ERROR) << "[StreamingTimeAligner] listmode output already failed; refusing further writes";
            m_processFailed.store(true, std::memory_order_release);
            return;
        }
        const bool ok = output.AppendSegment(sizeEstimate, std::move(coins), 0, 0);
        if (!ok || (output.GetStatus() & openpni::io::IOStatus_DiskSpaceNotEnough) != 0)
        {
            LOG(ERROR) << "[StreamingTimeAligner] listmode AppendSegment failed";
            m_processFailed.store(true, std::memory_order_release);
        }
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
        constexpr size_t kMergeTargetPairs = 262144;
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
                while (!m_writeQueue.empty())
                {
                    auto &next = m_writeQueue.front();
                    if (item.prompt.size() + next.prompt.size() > kMergeTargetPairs ||
                        item.delay.size() + next.delay.size() > kMergeTargetPairs)
                    {
                        break;
                    }
                    item.prompt.insert(item.prompt.end(),
                                       std::make_move_iterator(next.prompt.begin()),
                                       std::make_move_iterator(next.prompt.end()));
                    item.delay.insert(item.delay.end(),
                                      std::make_move_iterator(next.delay.begin()),
                                      std::make_move_iterator(next.delay.end()));
                    m_writeQueue.pop_front();
                }
            }
            m_writeCv.notify_one();

            const auto sinkBegin = std::chrono::steady_clock::now();
            if (!item.prompt.empty() && m_promptOpened)
            {
                saveCoincidenceResult(m_promptWriter, std::move(item.prompt));
            }
            if (!item.delay.empty() && m_delayOpened)
            {
                saveCoincidenceResult(m_delayWriter, std::move(item.delay));
            }
            m_stats.sinkNs.fetch_add(nsSince(sinkBegin), std::memory_order_relaxed);
        }
    }

    void StreamingTimeAligner::flushRemaining()
    {
        LOG(INFO) << "[StreamingTimeAligner] Flushing remaining data...";

        const auto extractBegin = std::chrono::steady_clock::now();
        std::vector<Single> remaining;

        // join steal 之后水位内数据应已在 deque；环只收安全边际内残留。

        for (size_t i = 0; i < m_stolenLanes.size(); ++i)
        {
            while (auto chunk = m_stolenLanes[i]->tryPop())
            {
                const size_t n = chunk->remainingCount();
                if (n > 0)
                {
                    const Single *p = chunk->remainingData();
                    remaining.insert(remaining.end(), p, p + n);
                }
                if (chunk->poolOwned)
                {
                    m_nodeBuffers[i]->dropStolenChunk(std::move(*chunk));
                }
            }
        }

        for (auto &buf : m_nodeBuffers)
        {
            while (auto chunk = buf->tryPop())
            {
                const size_t n = chunk->remainingCount();
                if (n > 0)
                {
                    const Single *p = chunk->remainingData();
                    remaining.insert(remaining.end(), p, p + n);
                }
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
