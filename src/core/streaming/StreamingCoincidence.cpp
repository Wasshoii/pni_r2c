#include "core/streaming/StreamingCoincidence.hpp"

#include <iterator>
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
        return true;
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
        const uint64_t coinWindow_pico = static_cast<uint64_t>(coinProtocol.timeWindow_ps);
        const uint64_t delayWindow_pico = static_cast<uint64_t>(coinProtocol.delayTime_ps);
        return networkLatencyMargin_pico + std::max(coinWindow_pico, delayWindow_pico);
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
        for (size_t i = 0; i < nodeCount; ++i)
        {
            m_nodeBuffers.push_back(
                std::make_unique<NodeRingBuffer>(i, config.maxChunksPerNode, poolPtr));
        }

        std::vector<uint32_t> crystalNumOfEachChannel(
            config.channelNum, config.crystalsPerChannel);
        m_coinNode.setTotalCrystalNumOfEachChannel(crystalNumOfEachChannel);

        LOG(INFO) << "[StreamingTimeAligner] Initialized with " << nodeCount
                  << " nodes, " << config.channelNum << " channels";
    }

    StreamingTimeAligner::~StreamingTimeAligner()
    {
        stop();
    }

    NodeRingBuffer *StreamingTimeAligner::getNodeBuffer(uint16_t nodeId)
    {
        if (nodeId >= m_nodeBuffers.size())
        {
            return nullptr;
        }
        return m_nodeBuffers[nodeId].get();
    }

    void StreamingTimeAligner::start()
    {
        if (m_running.exchange(true))
        {
            LOG(WARNING) << "[StreamingTimeAligner] Already running";
            return;
        }

        initializeOutput();
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

        if (m_processorThread.joinable())
        {
            m_processorThread.join();
        }

        finalizeOutput();
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
            opts.backend = openpni::distributed::coreio::IOBackendContext::Get().listmodeWriter;
            opts.totalCrystals = totalCrystals;
            m_promptWriter = std::make_unique<openpni::distributed::coreio::ListmodeFileWriter>(std::move(opts));
            m_promptWriter->Open(m_config.outputDir + "/prompt.lmf");
        }

        if (m_config.saveDelay)
        {
            openpni::distributed::coreio::ListmodeWriterOptions opts;
            opts.backend = openpni::distributed::coreio::IOBackendContext::Get().listmodeWriter;
            opts.totalCrystals = totalCrystals;
            m_delayWriter = std::make_unique<openpni::distributed::coreio::ListmodeFileWriter>(std::move(opts));
            m_delayWriter->Open(m_config.outputDir + "/delay.lmf");
        }
    }

    void StreamingTimeAligner::finalizeOutput()
    {
        std::lock_guard<std::mutex> lock(m_outputMutex);
        m_promptWriter.reset();
        m_delayWriter.reset();
    }

    uint64_t StreamingTimeAligner::calculateWatermark() const
    {
        uint64_t globalMinMaxTime = UINT64_MAX;
        size_t nodesWithData = 0;

        for (const auto &buf : m_nodeBuffers)
        {
            uint64_t nodeMaxTime = buf->getMaxEventTime();
            if (nodeMaxTime > 0)
            {
                globalMinMaxTime = std::min(globalMinMaxTime, nodeMaxTime);
                nodesWithData++;
            }
        }

        if (nodesWithData < m_nodeCount || globalMinMaxTime == UINT64_MAX)
        {
            return 0;
        }

        const uint64_t safetyMargin = m_config.getTotalSafetyMargin();

        if (globalMinMaxTime > safetyMargin)
        {
            return globalMinMaxTime - safetyMargin;
        }
        return 0;
    }

    void StreamingTimeAligner::processingLoop()
    {
        size_t consecutiveEmptyRounds = 0;
        const size_t maxEmptyRounds = 100;
        uint64_t lastWatermark = 0;

        std::this_thread::sleep_for(
            std::chrono::milliseconds(m_config.processingIntervalMs));

        while (m_running.load())
        {
            auto startTime = std::chrono::high_resolution_clock::now();

            uint64_t watermark = calculateWatermark();

            if (watermark == 0 || watermark <= lastWatermark)
            {
                consecutiveEmptyRounds++;
                if (consecutiveEmptyRounds > maxEmptyRounds && !m_running.load())
                {
                    break;
                }
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(m_config.processingIntervalMs));
                continue;
            }

            consecutiveEmptyRounds = 0;

            std::vector<Single> allSingles;

            for (auto &buf : m_nodeBuffers)
            {
                auto singles = buf->extractSinglesBefore(watermark);
                if (!singles.empty())
                {
                    allSingles.insert(allSingles.end(),
                                      std::make_move_iterator(singles.begin()),
                                      std::make_move_iterator(singles.end()));
                }
            }

            if (!allSingles.empty())
            {
                m_stats.totalSinglesReceived += allSingles.size();
                processCoincidence(allSingles);

                m_stats.totalSinglesProcessed += allSingles.size();
                m_stats.chunksProcessed++;
            }

            lastWatermark = watermark;
            m_stats.currentTimeBoundary_pico = watermark;

            auto endTime = std::chrono::high_resolution_clock::now();
            double elapsed_ms = std::chrono::duration<double, std::milli>(
                                    endTime - startTime)
                                    .count();
            double currentAvg = m_stats.avgProcessingTime_ms.load();
            m_stats.avgProcessingTime_ms = currentAvg * 0.9 + elapsed_ms * 0.1;
        }

        flushRemaining();
    }

    void StreamingTimeAligner::processCoincidence(const std::vector<Single> &singles)
    {
        if (singles.empty())
        {
            return;
        }
        try
        {
            m_singleBuffer.CopyFromHost(std::span<const Single>(singles));

            std::vector<std::span<Single const>> inputList;
            inputList.push_back(m_singleBuffer.CudaRStdSpan());

            auto [prompt, delay] = m_coinNode.getDListmode(inputList, m_config.coinProtocol);

            if (!prompt.empty() && m_promptWriter)
            {
                saveCoincidenceResult(*m_promptWriter, prompt);
                m_stats.totalPromptPairs += prompt.size();
            }

            if (!delay.empty() && m_delayWriter)
            {
                saveCoincidenceResult(*m_delayWriter, delay);
                m_stats.totalDelayPairs += delay.size();
            }
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "[StreamingTimeAligner] Coincidence error: " << e.what();
        }
    }

    void StreamingTimeAligner::saveCoincidenceResult(
        openpni::distributed::coreio::ListmodeFileWriter &output,
        std::span<Listmode const> coins)
    {
        if (coins.empty())
        {
            return;
        }

        m_coinBuffer.CopyFromCuda(coins);
        auto hostBuf = m_coinBuffer.HostRStdSpan();

        std::lock_guard<std::mutex> lock(m_outputMutex);
        output.AppendSegment(hostBuf, 0, 0);
    }

    void StreamingTimeAligner::flushRemaining()
    {
        LOG(INFO) << "[StreamingTimeAligner] Flushing remaining data...";

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

        if (!remaining.empty())
        {
            LOG(INFO) << "[StreamingTimeAligner] Processing " << remaining.size()
                      << " remaining singles...";
            m_stats.totalSinglesReceived += remaining.size();
            processCoincidence(remaining);
            m_stats.totalSinglesProcessed += remaining.size();
        }

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
        return config;
    }

} // namespace openpni::distributed::streaming
