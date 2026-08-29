#pragma once

#include <pni/PnI-Config.hpp>

#include <cstdint>
#include <pni/io/IO.hpp>
#include "../io/IOAdapter.hpp"
#include <pni/node/misc/Coincidence.hpp>
#include <pni/tools/Parallel.hpp>
#include <pni/tools/CudaPtr.hpp>
#include <pni/tools/HostUniquePtr.hpp>
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
#include <functional>
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
        // 与 extractSinglesBefore 语义相同，但直接写入调用方缓冲（通常是 pinned 暂存区），
        // 省掉一次中间 vector。返回实际写入的元素数；dst 容量不足时按 cap 截断，
        // 未取走的部分留在环形缓冲里等待下一轮。
        size_t extractSinglesBeforeInto(uint64_t boundary, Single *dst, size_t cap,
                                        bool *truncated = nullptr);
        std::vector<TimestampedSingleChunk> extractCompleteBefore(uint64_t boundary);
        size_t countSinglesBefore(uint64_t boundary) const;
        // 返回不超过 boundary 且累计条数不超过 budget 的最大 chunk 边界时间。
        // outCount 回填该边界下的条数。没有任何整 chunk 可取时返回 0。
        uint64_t chunkBoundaryWithin(uint64_t boundary, size_t budget, size_t *outCount) const;

        uint64_t getFrontMinTime() const;
        uint64_t getMaxEventTime() const;

        size_t getOutOfOrderCount() const;
        size_t getReorderedCount() const;

        bool empty() const;
        size_t size() const;
        size_t maxChunks() const noexcept { return m_maxChunks; }
        double occupancyRatio() const;
        // 距最近一次成功 push 的墙钟毫秒数；从未收到数据时返回 UINT64_MAX。
        uint64_t lastPushAgeMs() const;

        uint16_t nodeId() const;

        void close();
        bool isClosed() const;

        size_t getBufferMemoryBytes() const;

        // push() 成功后（已释放内部锁）触发，用于唤醒处理线程。
        // 必须在任何 push 之前完成设置，运行期间不得再改。
        void setPushObserver(std::function<void()> observer);

    private:
        uint16_t m_nodeId;
        size_t m_maxChunks;
        SharedMemoryPool *m_memoryPool;
        mutable std::mutex m_mutex;
        std::condition_variable m_cvNotEmpty;
        std::condition_variable m_cvNotFull;
        std::function<void()> m_pushObserver;

        std::deque<TimestampedSingleChunk> m_buffer;

        bool m_closed = false;
        uint64_t m_expectedChunkId = 0;
        size_t m_outOfOrderCount = 0;
        size_t m_reorderedCount = 0;
        uint64_t m_maxEventTimeReceived = 0;
        size_t m_bufferMemoryBytes = 0;
        std::optional<std::chrono::steady_clock::time_point> m_lastPushTime;
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

        size_t maxChunksPerNode = 100; // 每个节点的 RingBuffer 大小（单位：Chunk 数量）
        // 处理循环“无事件时”的最长空转等待，单位毫秒。有数据推入时由条件变量提前唤醒，
        // 因此这里不再是固定节拍，只是兜底轮询周期。
        uint32_t processingIntervalMs = 200;

        // ---- 数据段大小上界（显存约束，用户按显卡配置设置）----
        // 单次送入 Coincidence 内核的 singles 上限，carry + 新数据合计受此约束。
        // 默认一个 4 MiB RDMA 槽；0 = 不限制（可能 OOM）。
        size_t maxSegmentSingles = 262144;

        // ---- 数据段下界 ----
        // 软下界：攒批下限，避免频繁发起小内核。数据量不足且无缓冲压力、未到延迟兜底时
        // 继续攒。0 = 不攒批。缓冲高水位或延迟兜底可以突破它。
        size_t minSegmentSingles = 65536;
        // 硬下界：段的时间跨度至少覆盖 factor 个重叠窗（重叠窗 = timeWindow + delayTime）。
        // 段跨度小于重叠窗时 carry 占比会趋近 100%，同一批数据被反复重算，且会破坏
        // 抽取边界的单调性。压力触发与延迟兜底都不能突破它，唯一例外是停机 flush。
        // 取 4 时 carry 相对开销约 25%；调大可进一步摊薄。0 会被夹到 1。
        uint32_t minSegmentOverlapFactor = 4;

        // ---- 触发策略 ----
        // 任一节点 chunk 占用率或内存池占用率超过此比例时，立即触发处理（不再等攒批）。
        double bufferHighWaterRatio = 0.80;
        // 距上次处理超过此时长则立即触发，为实时性提供上界。0 = 关闭该兜底。
        uint32_t maxProcessLatencyMs = 50;
        // “缓冲高水位但水位线无法前进”时的告警节流间隔。
        uint32_t nodeStallWarnMs = 1000;
        // 默认关闭：某节点长期无数据时死等并对上游背压，保证跨节点对齐绝对精确。
        // 开启后，静默超过 nodeStallTimeoutMs 的节点会被剔出水位线计算，
        // 该段标记为降级（degradedSegments），以牺牲对齐精度换取实时性。
        bool allowStalledNodeBypass = false;
        uint32_t nodeStallTimeoutMs = 5000;

        size_t maxTotalMemoryBytes = 2ULL * 1024 * 1024 * 1024; // 内存池最大容量，单位字节（默认 2 GB）
        bool useMemoryPool = true;

        // 默认使用多 GPU 任务并行引擎（gpuIds 为空时自动检测全部 GPU）。
        // 设 enableMultiGpu=false 可临时回退单 GPU Coincidence 路径。
        bool enableMultiGpu = true;
        std::vector<uint32_t> gpuIds;
        uint32_t instancePerGpu = 1;
        // 多 GPU 在飞段数。0 = 按 GPU 数 × instancePerGpu 推导（与 R2S computePipelineDepth 同一思路）。
        size_t coinPipelineDepth = 0;

        uint64_t getTotalSafetyMargin() const;
        // 重叠窗长度（100fs）：coinWindow + delayTime，即 carry 需要覆盖的回溯长度。
        uint64_t overlapLength_100fs() const;
        // 硬下界对应的最小段时间跨度（100fs）。
        uint64_t minSegmentSpan_100fs() const;
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
        std::atomic<uint64_t> coinKernelNs{0};
        std::atomic<uint64_t> extractNs{0};
        std::atomic<uint64_t> sinkNs{0};
        std::atomic<uint64_t> drainWaitNs{0};
        std::atomic<uint64_t> writeQueueWaitNs{0};
        std::atomic<uint64_t> coinKernelBatches{0};

        // 触发来源分解
        std::atomic<uint64_t> triggerByWatermark{0}; // 攒够 minSegmentSingles
        std::atomic<uint64_t> triggerByPressure{0};  // 缓冲高水位
        std::atomic<uint64_t> triggerByDeadline{0};  // 延迟兜底
        // 缓冲高水位但水位线无法前进（某节点滞后），已对上游背压
        std::atomic<uint64_t> watermarkStallEvents{0};
        // allowStalledNodeBypass 生效、对齐精度降级的段数
        std::atomic<uint64_t> degradedSegments{0};
        // 被段时长硬下界挡住而推迟的轮数
        std::atomic<uint64_t> heldByMinDuration{0};
        // 硬下界与显存预算冲突、不得不放宽预算的段数
        std::atomic<uint64_t> oversizedSegments{0};
        // 累计随批重算的 carry 条数；除以 totalSinglesProcessed 即 carry 重算开销比
        std::atomic<uint64_t> carrySinglesTotal{0};
        std::atomic<uint64_t> maxNodeBacklogChunks{0};

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
        bool hadError() const { return m_processFailed.load(); }
        size_t getNodeCount() const { return m_nodeCount; }
        bool usingMultiGpu() const noexcept { return m_useMultiGpu; }
        size_t gpuCount() const noexcept;

        SharedMemoryPool::MemoryStatus getMemoryStatus() const;

    private:
        // 处理循环的一轮决策结果。
        struct SegmentDecision
        {
            bool process = false;    // 本轮是否处理
            uint64_t boundary = 0;   // 抽取边界（恒 <= watermark 且 > m_lastWatermark）
            size_t pendingCount = 0; // boundary 之下待抽取的 singles 条数
        };

        void initializeOutput();
        void finalizeOutput();
        // 非 const：allowStalledNodeBypass 生效时会累加 degradedSegments。
        uint64_t calculateWatermark();
        // 在 [m_lastWatermark, watermark] 内解出满足显存预算与段时长硬下界的抽取边界。
        uint64_t clampSegmentBoundary(uint64_t watermark, size_t pending, size_t *outCount);
        size_t countPendingBefore(uint64_t boundary) const;
        bool anyNodeAboveHighWater() const;
        void processingLoop();
        void drainLoop();
        void writerLoop();
        void processCoincidence(std::span<const Single> singles, uint64_t carryCutoffTime_100fs);
        // 抽取 -> 归并 -> 分批送内核 -> 更新 carry。
        bool processSegment(uint64_t boundary, size_t expectedCount);
        size_t extractAndMergeInto(Single *dest, size_t destCap, uint64_t boundary, bool *truncated);
        size_t acquireInputSlot();
        void releaseInputSlot(size_t idx);
        bool submitOwnedSlot(size_t slotIdx, size_t count, uint64_t carryCutoff,
                             uint64_t carryBound);
        bool submitCopiedSpan(std::span<const Single> sorted, uint64_t carryCutoff,
                              uint64_t carryBound);
        void enqueueWrite(std::vector<Listmode> &&prompt, std::vector<Listmode> &&delay);
        void initInputSlots();
        void stopWriterAndJoin();
        // enforceMinDuration 为 true 时子批切点还要守段时长硬下界（压力/超时都不能突破）。
        // 停机 flush 传 false，否则尾部不足一个重叠窗的数据永远处理不掉。
        bool processSinglesRange(std::span<Single> sorted, uint64_t finalCarryBound,
                                 bool enforceMinDuration = true);
        void updateCarrySingles(std::span<const Single> sortedBatch, uint64_t watermark);
        uint64_t overlapLength_100fs() const;
        void ensureStagingCapacity(size_t elements);
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
        // 单 GPU 回退路径的 D2H 暂存。刻意不用 UniPtr：它按 host/cuda 脏标记做隐式同步，
        // 而 prompt / delay 两次回拷的长度不同，第二次会触发一次 host->device 反向同步并
        // 因元素数不匹配抛异常。这里只需要一次纯粹的 D2H。
        std::vector<Listmode> m_coinHostBuffer;

        std::unique_ptr<multi_gpu::CoincidenceMultiGpuEngine> m_multiGpuEngine;
        bool m_useMultiGpu = false;

        struct CoinInputSlot
        {
            openpni::tools::HostUniquePtr<Single> buffer{
                std::make_unique<openpni::detail::VAllocatorCUDAHost>()};
        };
        std::vector<CoinInputSlot> m_inputSlots;
        std::deque<size_t> m_freeSlots;
        std::mutex m_slotMutex;
        std::condition_variable m_slotCv;
        size_t m_inputSlotCapacity = 0;

        std::deque<size_t> m_submittedSlots;
        std::mutex m_submittedMutex;
        std::condition_variable m_submittedCv;

        struct ListmodeWriteItem
        {
            std::vector<Listmode> prompt;
            std::vector<Listmode> delay;
        };
        std::deque<ListmodeWriteItem> m_writeQueue;
        size_t m_writeQueueCap = 0;
        std::mutex m_writeMutex;
        std::condition_variable m_writeCv;
        std::atomic<bool> m_writeStop{false};

        // 上一段尾部保留（长度见 overlapLength_100fs），供下一段隔段匹配。
        std::vector<Single> m_carrySingles;
        uint64_t m_lastWatermark = 0;

        // 抽取暂存：m_stageRaw 按 [carry][node0][node1][...] 布局收集各节点有序段，
        // 归并后落到 m_stageMerged，再整体（或分批）送内核。两块都是 pinned 内存，
        // 使 H2D 走真异步路径。
        openpni::tools::HostUniquePtr<Single> m_stageRaw{
            std::make_unique<openpni::detail::VAllocatorCUDAHost>()};
        openpni::tools::HostUniquePtr<Single> m_stageMerged{
            std::make_unique<openpni::detail::VAllocatorCUDAHost>()};
        size_t m_stageCapacity = 0;
        std::vector<std::pair<size_t, size_t>> m_mergeRuns;
        // calculateWatermark 是否因 allowStalledNodeBypass 剔除了停滞节点
        bool m_watermarkDegraded = false;

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
        std::thread m_drainThread;
        std::thread m_writerThread;
        std::atomic<bool> m_running{false};
        std::atomic<bool> m_processFailed{false};

        // 事件驱动唤醒：任一节点 push 成功后 notify，处理线程据此提前结束等待。
        std::mutex m_wakeMutex;
        std::condition_variable m_wakeCv;
        std::atomic<uint64_t> m_wakeSeq{0};

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
