#pragma once

#include <pni/PnI-Config.hpp>

#include <cstdint>
#include <pni/io/IO.hpp>
#include "../io/IOAdapter.hpp"
#include <pni/node/misc/Coincidence.hpp>
#include <pni/tools/Parallel.hpp>
#include <pni/tools/CudaPtr.hpp>
#include <pni/tools/UniPtr.hpp>

#include <vector>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <memory>
#include <span>
#include <optional>
#include <chrono>
#include <algorithm>
#include <filesystem>
#include <iostream>

namespace openpni::distributed::streaming::multi_gpu
{
    class CoincidenceMultiGpuEngine;
}

namespace openpni::distributed::streaming
{
    using Single = openpni::Single;
    namespace fs = std::filesystem;

    class SharedMemoryPool
    {
    public:
        struct MemoryStatus
        {
            size_t usedBytes;
            size_t maxBytes;
            double usageRatio;
        };

        explicit SharedMemoryPool(size_t maxMemoryBytes = 1ULL * 1024 * 1024 * 1024);

        bool tryAllocate(size_t bytes, uint32_t timeoutMs = 0);
        void release(size_t bytes);

        size_t getUsedMemory() const;
        size_t getPeakMemory() const;
        size_t getMaxMemory() const;
        double getUsageRatio() const;
        size_t getTotalAllocations() const;

        void close();
        MemoryStatus getStatus() const;
        void printStatus() const;

    private:
        size_t m_maxMemoryBytes;
        size_t m_usedMemoryBytes;
        size_t m_peakMemoryBytes = 0;
        size_t m_totalAllocations = 0;
        bool m_closed = false;
        mutable std::mutex m_mutex;
        std::condition_variable m_cvAvailable;
    };

    struct TimestampedSingleChunk
    {
        uint16_t nodeId = 0;
        uint64_t chunkId = 0;
        uint64_t computerClock_ms = 0;
        uint32_t duration_ms = 0;
        std::vector<Single> singles;

        uint64_t minTime_pico = UINT64_MAX;
        uint64_t maxTime_pico = 0;

        void updateTimeRange();
        size_t memorySize() const;
        size_t singlesMemorySize() const;
        bool operator<(const TimestampedSingleChunk &other) const;
    };

    class NodeRingBuffer
    {
    public:
        explicit NodeRingBuffer(uint16_t nodeId, size_t maxChunks = 100,
                                SharedMemoryPool *memoryPool = nullptr);

        bool push(TimestampedSingleChunk &&chunk, uint32_t timeoutMs = 0);

        const TimestampedSingleChunk *front() const;
        std::optional<TimestampedSingleChunk> pop();
        std::optional<TimestampedSingleChunk> tryPop();

        std::vector<Single> extractSinglesBefore(uint64_t boundary);
        std::vector<TimestampedSingleChunk> extractCompleteBefore(uint64_t boundary);

        uint64_t getFrontMinTime() const;
        uint64_t getMaxEventTime() const;

        size_t getOutOfOrderCount() const;
        size_t getReorderedCount() const;

        bool empty() const;
        size_t size() const;

        uint16_t nodeId() const;

        void close();
        bool isClosed() const;

        size_t getBufferMemoryBytes() const;

    private:
        uint16_t m_nodeId;
        size_t m_maxChunks;
        SharedMemoryPool *m_memoryPool;
        mutable std::mutex m_mutex;
        std::condition_variable m_cvNotEmpty;
        std::condition_variable m_cvNotFull;

        std::deque<TimestampedSingleChunk> m_buffer;

        bool m_closed = false;
        uint64_t m_expectedChunkId = 0;
        size_t m_outOfOrderCount = 0;
        size_t m_reorderedCount = 0;
        uint64_t m_maxEventTimeReceived = 0;
        size_t m_bufferMemoryBytes = 0;
    };

    struct TimeAlignerConfig
    {
        // 网络延迟安全边际，单位皮秒（与 CoincidenceProtocol 一致）；getTotalSafetyMargin 内转为 100fs
        uint64_t networkLatencyMargin_pico = 5'000'000'000;

        openpni::CoincidenceProtocol coinProtocol; // Coincidence 协议配置，包含时间窗口、能量窗口等参数
        uint16_t channelNum = 0;                   // 总通道数
        uint32_t crystalsPerChannel = 0;

        std::string outputDir;
        bool savePrompt = true;
        bool saveDelay = true;

        // Listmode 输出（prompt.lmf / delay.lmf）写盘策略：maxFileSizeBytes 为 0 表示不分卷
        // （单文件，默认行为，与既有 Coincidence 输出保持一致）
        uint64_t listmodeMaxFileSizeBytes = 0;
        bool listmodeOverwriteExisting = true;

        size_t maxChunksPerNode = 100;       // 每个节点的 RingBuffer 大小（单位：Chunk 数量）
        uint32_t processingIntervalMs = 200; // 处理循环的时间间隔，单位毫秒

        size_t maxTotalMemoryBytes = 2ULL * 1024 * 1024 * 1024; // 内存池最大容量，单位字节（默认 2 GB）
        bool useMemoryPool = true;

        // 默认使用多 GPU 任务并行引擎（gpuIds 为空时自动检测全部 GPU）。
        // 设 enableMultiGpu=false 可临时回退单 GPU Coincidence 路径。
        bool enableMultiGpu = true;
        std::vector<uint32_t> gpuIds;
        uint32_t instancePerGpu = 1;

        uint64_t getTotalSafetyMargin() const;
    };

    struct ProcessingStatistics
    {
        std::atomic<uint64_t> totalSinglesReceived{0};
        std::atomic<uint64_t> totalSinglesProcessed{0};
        std::atomic<uint64_t> totalPromptPairs{0};
        std::atomic<uint64_t> totalDelayPairs{0};
        std::atomic<uint64_t> chunksProcessed{0};
        std::atomic<double> avgProcessingTime_ms{0};
        std::atomic<uint64_t> currentTimeBoundary_pico{0};

        void reset();
    };

    class StreamingTimeAligner
    {
    public:
        StreamingTimeAligner(const TimeAlignerConfig &config, size_t nodeCount);
        ~StreamingTimeAligner();

        NodeRingBuffer *getNodeBuffer(uint16_t nodeId);

        void start();
        void stop(bool waitForCompletion = true);

        const ProcessingStatistics &getStatistics() const { return m_stats; }
        bool isRunning() const { return m_running.load(); }
        size_t getNodeCount() const { return m_nodeCount; }

        SharedMemoryPool::MemoryStatus getMemoryStatus() const;

    private:
        void initializeOutput();
        void finalizeOutput();
        uint64_t calculateWatermark() const;
        void processingLoop();
        void processCoincidence(const std::vector<Single> &singles, uint64_t carryCutoffTime_100fs);
        void updateCarrySingles(const std::vector<Single> &processedSingles, uint64_t watermark);
        uint64_t overlapLength_100fs() const;
        void saveCoincidenceResult(
            openpni::distributed::coreio::RollingFileWriter<
                openpni::distributed::coreio::ListmodeFileWriter,
                openpni::distributed::coreio::ListmodeWriterOptions> &output,
            std::span<Listmode const> coins,
            bool alreadyOnHost = false);
        void flushRemaining();

        TimeAlignerConfig m_config;
        size_t m_nodeCount;

        std::vector<std::unique_ptr<NodeRingBuffer>> m_nodeBuffers;

        openpni::Coincidence m_coinNode;
        openpni::tools::UniPtr<Single> m_singleBuffer{"StreamingTimeAligner_singles"};
        openpni::tools::UniPtr<Listmode> m_coinBuffer{"StreamingTimeAligner_coins"};

        std::unique_ptr<multi_gpu::CoincidenceMultiGpuEngine> m_multiGpuEngine;
        bool m_useMultiGpu = false;

        // 上一段尾部保留（长度见 overlapLength_100fs），供下一段隔段匹配。
        std::vector<Single> m_carrySingles;
        uint64_t m_lastWatermark = 0;

        openpni::distributed::coreio::RollingFileWriter<
            openpni::distributed::coreio::ListmodeFileWriter,
            openpni::distributed::coreio::ListmodeWriterOptions>
            m_promptWriter;
        openpni::distributed::coreio::RollingFileWriter<
            openpni::distributed::coreio::ListmodeFileWriter,
            openpni::distributed::coreio::ListmodeWriterOptions>
            m_delayWriter;
        bool m_promptOpened = false;
        bool m_delayOpened = false;
        std::mutex m_outputMutex;

        std::thread m_processorThread;
        std::atomic<bool> m_running{false};

        std::unique_ptr<SharedMemoryPool> m_memoryPool;

        ProcessingStatistics m_stats;
    };

    TimeAlignerConfig createBDM2AlignerConfig(
        const std::string &outputDir,
        const openpni::CoincidenceProtocol &coinProtocol = {});

    TimeAlignerConfig createBDM50100_9120AlignerConfig(
        const std::string &outputDir,
        const openpni::CoincidenceProtocol &coinProtocol = {});

} // namespace openpni::distributed::streaming
