#include "core/streaming/StreamingCoincidence.hpp"

#include "core/streaming/multi_gpu/CoincidenceMultiGpuEngine.hpp"

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
        const uint64_t coinWindow_ps = static_cast<uint64_t>(coinProtocol.timeWindow_ps);
        const uint64_t delayWindow_ps = static_cast<uint64_t>(coinProtocol.delayTime_ps);
        // Watermark 与 singles 时间戳同为 100fs；ps 配置值需 ×10
        return networkLatencyMargin_pico * 10
               + std::max(coinWindow_ps, delayWindow_ps) * 10;
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
                  << (m_useMultiGpu ? "true" : "false");
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

            std::vector<Single> newSingles;

            for (auto &buf : m_nodeBuffers)
            {
                auto singles = buf->extractSinglesBefore(watermark);
                if (!singles.empty())
                {
                    newSingles.insert(newSingles.end(),
                                      std::make_move_iterator(singles.begin()),
                                      std::make_move_iterator(singles.end()));
                }
            }

            if (!newSingles.empty())
            {
                const uint64_t carryCutoff =
                    m_carrySingles.empty() ? 0 : m_lastWatermark;
                const size_t newCount = newSingles.size();

                std::vector<Single> batch;
                batch.reserve(m_carrySingles.size() + newCount);
                batch.insert(batch.end(), m_carrySingles.begin(), m_carrySingles.end());
                batch.insert(batch.end(),
                             std::make_move_iterator(newSingles.begin()),
                             std::make_move_iterator(newSingles.end()));
                newSingles.clear();

                m_stats.totalSinglesReceived += newCount;
                processCoincidence(batch, carryCutoff);
                m_stats.totalSinglesProcessed += newCount;
                m_stats.chunksProcessed++;

                updateCarrySingles(batch, watermark);
            }
            else if (!m_carrySingles.empty())
            {
                // 水位推进但无新事件：按新窗口裁剪尾部，避免陈旧 carry。
                updateCarrySingles(m_carrySingles, watermark);
            }

            m_lastWatermark = watermark;
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

    uint64_t StreamingTimeAligner::overlapLength_100fs() const
    {
        const uint64_t coinWindow_ps =
            static_cast<uint64_t>(m_config.coinProtocol.timeWindow_ps);
        const uint64_t delayWindow_ps =
            static_cast<uint64_t>(m_config.coinProtocol.delayTime_ps);
        return (coinWindow_ps + delayWindow_ps) * 10ull;
    }

    void StreamingTimeAligner::updateCarrySingles(
        const std::vector<Single> &processedSingles, uint64_t watermark)
    {
        std::vector<Single> nextCarry;
        if (processedSingles.empty() || watermark == 0)
        {
            m_carrySingles = std::move(nextCarry);
            return;
        }

        const uint64_t overlap = overlapLength_100fs();
        const uint64_t carryBegin =
            watermark > overlap ? watermark - overlap : 0;

        nextCarry.reserve(processedSingles.size() / 8 + 8);
        for (const auto &s : processedSingles)
        {
            if (s.timevalue_100fs > carryBegin && s.timevalue_100fs <= watermark)
            {
                nextCarry.push_back(s);
            }
        }
        m_carrySingles = std::move(nextCarry);
    }

    void StreamingTimeAligner::processCoincidence(
        const std::vector<Single> &singles, uint64_t carryCutoffTime_100fs)
    {
        if (singles.empty())
        {
            return;
        }
        try
        {
            if (m_useMultiGpu && m_multiGpuEngine)
            {
                const auto result = m_multiGpuEngine->processSinglesSync(
                    std::span<const Single>(singles), carryCutoffTime_100fs);

                if (!result.prompt.empty() && m_promptOpened)
                {
                    saveCoincidenceResult(m_promptWriter, result.prompt, true);
                    m_stats.totalPromptPairs += result.promptCount;
                }

                if (!result.delay.empty() && m_delayOpened)
                {
                    saveCoincidenceResult(m_delayWriter, result.delay, true);
                    m_stats.totalDelayPairs += result.delayCount;
                }
                return;
            }

            // UniPtr keeps CUDA capacity from the largest batch; SyncToCuda then calls
            // cuda_unique_ptr::CopyFromHost which requires exact element-count match.
            // Clear() before copy so a smaller subsequent batch does not hit
            // "size mismatch when copying from host, expected N, got M".
            m_singleBuffer.Clear();
            m_singleBuffer.CopyFromHost(std::span<const Single>(singles));

            std::vector<std::span<Single const>> inputList;
            inputList.push_back(m_singleBuffer.CudaRStdSpan());

            auto [prompt, delay] = m_coinNode.getDListmode(
                inputList, m_config.coinProtocol, carryCutoffTime_100fs);

            if (!prompt.empty() && m_promptOpened)
            {
                saveCoincidenceResult(m_promptWriter, prompt);
                m_stats.totalPromptPairs += prompt.size();
            }

            if (!delay.empty() && m_delayOpened)
            {
                saveCoincidenceResult(m_delayWriter, delay);
                m_stats.totalDelayPairs += delay.size();
            }
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "[StreamingTimeAligner] Coincidence error: " << e.what();
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
            m_coinBuffer.CopyFromCuda(coins);
            hostBuf = m_coinBuffer.HostRStdSpan();
        }

        // 估算本次写入字节数，用于分卷阈值判断（仅在 listmodeMaxFileSizeBytes > 0 时生效）
        constexpr size_t ESTIMATED_LISTMODE_BYTES = 16;
        const uint64_t sizeEstimate = hostBuf.size() * ESTIMATED_LISTMODE_BYTES;

        std::lock_guard<std::mutex> lock(m_outputMutex);
        output.AppendSegment(sizeEstimate, hostBuf, 0, 0);
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

        if (remaining.empty())
        {
            m_carrySingles.clear();
            LOG(INFO) << "[StreamingTimeAligner] Flush complete (no remaining singles)";
            return;
        }

        constexpr size_t kFlushBatch = 65536;
        LOG(INFO) << "[StreamingTimeAligner] Processing " << remaining.size()
                  << " remaining singles (+ carry " << m_carrySingles.size()
                  << ") in batches of " << kFlushBatch << "...";
        m_stats.totalSinglesReceived += remaining.size();

        // Sort remaining by time so flush sub-batches form contiguous time ranges.
        std::sort(remaining.begin(), remaining.end(),
                  [](const Single &a, const Single &b) {
                      return a.timevalue_100fs < b.timevalue_100fs;
                  });

        uint64_t prevCutoff = m_lastWatermark;
        size_t batchIndex = 0;
        for (size_t offset = 0; offset < remaining.size(); offset += kFlushBatch)
        {
            const size_t n = std::min(kFlushBatch, remaining.size() - offset);
            std::vector<Single> newBatch(
                std::make_move_iterator(remaining.begin() + static_cast<std::ptrdiff_t>(offset)),
                std::make_move_iterator(remaining.begin() + static_cast<std::ptrdiff_t>(offset + n)));

            uint64_t batchMaxTime = 0;
            for (const auto &s : newBatch)
            {
                batchMaxTime = std::max(batchMaxTime, s.timevalue_100fs);
            }

            const uint64_t carryCutoff = m_carrySingles.empty() ? 0 : prevCutoff;

            std::vector<Single> batch;
            batch.reserve(m_carrySingles.size() + newBatch.size());
            batch.insert(batch.end(), m_carrySingles.begin(), m_carrySingles.end());
            batch.insert(batch.end(),
                         std::make_move_iterator(newBatch.begin()),
                         std::make_move_iterator(newBatch.end()));

            ++batchIndex;
            LOG(INFO) << "[StreamingTimeAligner] Flush batch " << batchIndex
                      << "/" << ((remaining.size() + kFlushBatch - 1) / kFlushBatch)
                      << " size=" << batch.size() << " (new=" << n
                      << ", carry=" << (batch.size() - n) << ")";

            processCoincidence(batch, carryCutoff);
            m_stats.totalSinglesProcessed += n;

            updateCarrySingles(batch, batchMaxTime);
            prevCutoff = batchMaxTime;
            m_lastWatermark = batchMaxTime;
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
