#pragma once

#include <pni/PnI-Config.hpp>

#include <cstdint>
#include <pni/io/IO.hpp>
#include "core/io/IOAdapter.hpp"
// #include <pni/node/BDMBiDR2S.hpp>

#include <pni/node/raw2singles/BDM2R2S.hpp>
#include <pni/node/raw2singles/BDM50100/BDM50100R2S.hpp>
#include <pni/node/raw2singles/ConvergedR2S.hpp>
#include <pni/node/Coincidence.hpp>
#include "tools/SinglesProcess.hpp"
#include <algorithm>
#include <cstring>
#include <limits>
#include <iostream>
#include <fstream>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <utility>
#include <vector>
#include <filesystem>
#include <unordered_set>
#include <optional>

namespace openpni::distributed::r2s::multi_gpu
{
    class R2S50100MultiGpuEngine;
    struct PinnedRawSlot;
}

namespace openpni::distributed::r2s
{
    using Single = openpni::Single;

    /**
     * @brief 单事件数据就绪回调函数类型
     *
     * @param singles 转换后的全局单事件数据（移动语义）
     * @param clock_ms 计算机时钟时间戳（毫秒）
     * @param duration_ms 数据段持续时间（毫秒）
     * @return bool 返回true表示处理成功，false表示需要停止处理
     */
    using SinglesReadyCallback = std::function<bool(
        std::vector<Single> &&singles,
        uint64_t clock_ms,
        uint32_t duration_ms)>;

    /**
    * @brief 原始 Single 视图回调（零额外 host 物化）
     *
     * 说明：
     * - span 仅在回调函数返回前有效，回调内若异步使用需自行拷贝。
    * - 当该回调已设置时，processR2S 会优先调用它，以避免额外的
    *   device -> host 拷贝（若上游在 GPU 上生成 Single）。
     */
    using SinglesSpanReadyCallback = std::function<bool(
        std::span<Single const> singles,
        uint64_t clock_ms,
        uint32_t duration_ms)>;
    // 判断 ptr 是否为 GPU Device 内存
    bool isDevicePointer(const void *ptr);

    /**
     * @brief 将 R2S 输出 singles 物化到 host 侧内存
     *
     * 该函数用于异步处理链路：回调返回后原始 span 不再保证有效，
     * 因此需要在回调内完成 host 拷贝并持有数据所有权。
     */
    std::vector<Single> materializeSinglesOnHost(std::span<Single const> singles);

    /**
     * @brief 读取目录下的校正文件路径列表
     *
     * @param directory 目录路径
     * @param extensions 过滤后缀列表（为空表示不过滤，例如 {".data", ".bin"}）
     * @param sortByName 是否按文件名排序
     * @param namePrefix 文件名前缀（用于按数字排序）
     * @param nameSuffix 文件名后缀（用于按数字排序）
     * @return std::vector<std::string> 目录下的校正文件完整路径
     */
    std::vector<std::string> collectCalibrationFiles(
        const std::string &directory,
        const std::vector<std::string> &extensions = {},
        bool sortByName = true,
        const std::string &namePrefix = {},
        const std::string &nameSuffix = {});

    auto timer(auto func, auto time)
    {
        auto now = std::chrono::steady_clock::now();
        for (int i = 0; i < time; i++)
            func();
        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(end - now).count();
    }

    /**
     * @brief 探测器类型枚举
     */
    enum class DetectorType
    {
        BDM2,
        BDMBiD, // 已在pni-core中移除
        BDM50100,
        BDM100100,
        Unknown
    };

    /**
     * @brief R2S 处理配置结构体
     */
    struct R2SProcessConfig
    {
        //分布式处理配置
        std::string rawdataPath;                   // 原始数据文件路径（单文件；目录批处理请用 processR2SDirectory）
        std::string resultPath;                    // 结果输出目录
        std::vector<std::string> calibrationFiles; // 校准文件列表（按下标 = 全局通道号；未分配通道可为占位空串）
        DetectorType detectorType;                 // 探测器类型
        uint32_t crystalsPerChannel;               // 每个通道的晶体数
        uint32_t r2sResultIndex;                   // R2S结果数组中的目标索引（由 ConvergedR2S 注册顺序决定）
        std::string outputFileName;                // 输出文件名前缀（实际文件名可由批处理追加 inputClock 等后缀）
        u_int16_t channelNums;                     // 整机通道总数上界（9120=576）；不表示本节点必处理 0..N-1
        std::vector<uint16_t> channelIndices;      // 本节点分配处理的通道集合（空则处理 [0, channelNums)）
        bool sortDataByTime = false;               // 50100 段内排序由 libpni 完成；外部再排是可选后处理
        bool saveData2SingleFile = true;           // 是否保存为 Single 文件格式
        bool asyncFileWrite = false;               // 是否异步写入文件（提高处理吞吐量）
        size_t asyncWriteQueueSize = 200;          // 异步写入队列大小
        uint32_t progressLogInterval = 50;         // 处理进度日志间隔，0 表示关闭
        bool forceFullCalibrationLoad = false;     // 历史标志：50100 默认打开；generator 实际只为 channelIndices（或全部）创建

        // Singles 输出写盘策略：maxFileSizeBytes 为 0 表示不分卷（单文件，默认行为，
        // 与既有 930 R2S 验证行为保持一致）
        uint64_t singlesMaxFileSizeBytes = 0;    // 单个 singles 文件的最大大小（字节），超过后自动分卷
        bool singlesOverwriteExisting = true;    // 是否允许覆盖已存在的输出文件

        // R2S算法参数配置
        bool matchXTalkEnabled = false;   // 是否启用串扰匹配
        float timeWindow = 25.0f;         // 时间窗口
        float timeShift = 25.0f;          // 时间偏移
        bool crossTalkEnabled = false;    // 是否启用串扰校正
        float crossTalkTimeWindow = 2.0f; // 串扰时间窗口 (ns)
        bool useEnergyCut = false;                   // 是否启用能量窗过滤
        float energyCutLow = 0.0f;                    // 能量窗下限 eV
        float energyCutHigh = 0.0f;                   // 能量窗上限 eV
        uint32_t __deviceId = 0; // CUDA设备ID（后处理/回退路径主 GPU）

        // BDM50100 默认使用多 GPU 任务并行引擎（gpuIds 为空时自动检测全部 GPU）。
        // 设 enableMultiGpu=false 可临时回退 ConvergedR2S 单 GPU 路径（待真实数据验收后移除）。
        bool enableMultiGpu = true;
        std::vector<uint32_t> gpuIds;            // 空 = 使用 [0, device_count)
        uint32_t instancePerGpu = 1;             // 每张 GPU 上的 compute 实例数
        long double maxInputGibits = 0.0L;       // 0 = 不预分配 singles 缓冲
        float inputBurstToleranceCoef = 1.2f;    // ring slot 预分配系数
        uint32_t computePipelineDepth = 1;       // 多 GPU 在飞段数；1 = submit 后立刻 next。Bridge 默认用 leaseQueueCapacity

        openpni::device::bdm50100_v2::caliCoef::EnergyThresholds_t energyThresholds = {60, 80, 100, 120, 140, 160, 180, 200, 0,  0.0454, 0.1111, 1.964, -0.0014}; // 能量阈值数组 for 50100

        // 分布式处理回调，使用时需设置（可与 saveData2SingleFile 同时使用，支持同时保存文件和流式传输）
        SinglesReadyCallback onSinglesReady = nullptr;         // 传输 host 侧 Single
        SinglesSpanReadyCallback onSinglesSpanReady = nullptr; // 50100 多 GPU 热路径可为 device span，回调内须 D2H 或 materialize

        R2SProcessConfig()
            : detectorType(DetectorType::Unknown), crystalsPerChannel(0), r2sResultIndex(0), outputFileName("singles")
        {
        }
    };

    /**
     * @brief 异步文件写入任务数据
     */
    struct AsyncWriteTask
    {
        std::vector<Single> singles;
        uint64_t clock_ms;
        uint32_t duration_ms;
    };

    /**
     * @brief 异步 Single 文件写入器
     *
     * 使用独立线程异步写入文件，避免阻塞主处理线程
     */
    class AsyncSingleFileWriter
    {
    public:
        explicit AsyncSingleFileWriter(size_t maxQueueSize = 100);

        ~AsyncSingleFileWriter();

        /**
         * @brief 打开文件并启动写入线程
         *
         * @param sessionDir 输出目录
         * @param filePrefix 文件名前缀（不含扩展名）；当 options.io.maxFileSizeBytes == 0 时，
         *                   最终文件名为 "{sessionDir}/{filePrefix}.lsingle"（与历史行为一致）；
         *                   > 0 时自动按 "{filePrefix}_{seq:04d}.lsingle" 分卷。
         */
        bool open(const std::string &sessionDir, const std::string &filePrefix, uint32_t totalCrystals,
                  openpni::distributed::coreio::SingleWriterOptions options = {});

        /**
         * @brief 异步提交写入任务
         *
         * @return true 成功提交，false 队列已满或已停止
         */
        bool submit(std::vector<Single> &&singles, uint64_t clock_ms, uint32_t duration_ms);

        /**
         * @brief 停止写入器，等待所有任务完成
         */
        void stop();

        /**
         * @brief 等待所有待处理任务完成
         */
        void flush();

        uint64_t getTotalWritten() const { return m_totalWritten.load(); }
        size_t getPendingCount() const { return m_pendingCount.load(); }
        bool isRunning() const { return m_running.load(); }

        /**
         * @brief 获取底层输出文件写入器（用于同步写入模式）
         */
        openpni::distributed::coreio::RollingFileWriter<
            openpni::distributed::coreio::SinglesFileWriter,
            openpni::distributed::coreio::SingleWriterOptions> &
        getOutput()
        {
            return m_output;
        }

    private:
        void writerLoop();

        openpni::distributed::coreio::RollingFileWriter<
            openpni::distributed::coreio::SinglesFileWriter,
            openpni::distributed::coreio::SingleWriterOptions>
            m_output;
        std::thread m_writerThread;
        std::queue<AsyncWriteTask> m_queue;
        std::mutex m_mutex;
        std::condition_variable m_cvNotEmpty;
        std::condition_variable m_cvNotFull;
        std::condition_variable m_cvFlushed;
        std::atomic<bool> m_running{false};
        std::atomic<uint64_t> m_totalWritten{0};
        std::atomic<size_t> m_pendingCount{0};
        size_t m_maxQueueSize;
    };

    /**
     * @brief 追加单事件数据到 Single 文件（新 listmode 格式）
     *
     * @param outputFile 支持自动分卷的 Singles 写入器（maxFileSizeBytes == 0 时等价于单文件写入）
     */
    bool appendSinglesToSingleFile(
        openpni::distributed::coreio::RollingFileWriter<
            openpni::distributed::coreio::SinglesFileWriter,
            openpni::distributed::coreio::SingleWriterOptions> &outputFile,
        std::span<Single const> singles,
        uint64_t clock_ms,
        uint32_t duration_ms);

    /**
     * @brief 创建指定类型的 SingleGenerator
     *
     * 通道无关的算法参数（如 BDM50100 的 matchXTalkEnabled/timeWindow/timeShift/
     * crossTalkEnabled/crossTalkTimeWindow）统一从传入的完整 R2SProcessConfig 读取，
     * 而不是在函数内部硬编码，以便不同的 create*Config 工厂函数（含未来针对
     * 9120 各环的定制配置）可以各自控制这些参数。
     *
     * @param config 完整的 R2S 处理配置（提供 detectorType 及对应的算法参数）
     * @param channelIndex 该 generator 对应的通道索引（本地/全局编号均可，取决于调用方约定）
     * @param calibrationFile 该通道对应的校准文件路径
     */
    openpni::interface::ISingleGenerator *createSingleGenerator(
        const R2SProcessConfig &config,
        uint16_t localIndex,
        uint16_t globalChannelIndex);

    /**
     * @brief 零拷贝租约：持有 Acq RawDataView，析构/release 时归还包槽。
     */
    struct RawDataLease
    {
        openpni::RawDataView view{};
        std::function<void()> releaseFn;
        bool released = true;

        RawDataLease() = default;

        RawDataLease(openpni::RawDataView v, std::function<void()> release)
            : view(v), releaseFn(std::move(release)), released(false)
        {
        }

        RawDataLease(const RawDataLease &) = delete;
        RawDataLease &operator=(const RawDataLease &) = delete;

        RawDataLease(RawDataLease &&other) noexcept
        {
            *this = std::move(other);
        }

        RawDataLease &operator=(RawDataLease &&other) noexcept
        {
            if (this != &other)
            {
                release();
                view = other.view;
                releaseFn = std::move(other.releaseFn);
                released = other.released;
                other.released = true;
                other.releaseFn = nullptr;
                other.view = {};
            }
            return *this;
        }

        ~RawDataLease()
        {
            release();
        }

        void release()
        {
            if (released)
            {
                return;
            }
            released = true;
            if (releaseFn)
            {
                auto fn = std::move(releaseFn);
                releaseFn = nullptr;
                fn();
            }
        }

        /** @brief 取消归还回调（用于入队失败，避免析构时误 Release）。 */
        void disarm()
        {
            released = true;
            releaseFn = nullptr;
        }
    };

    /**
     * @brief 单生产者单消费者无锁环形队列（零拷贝租约）
     *
     * 约束：
     * - 仅允许一个生产者线程调用 tryPushLease。
     * - 仅允许一个消费者线程调用 tryConsumeOne。
     */
    class RawDataLeaseSpscRingQueue
    {
    public:
        explicit RawDataLeaseSpscRingQueue(size_t capacity = 2)
        {
            if (capacity == 0)
            {
                throw std::invalid_argument("RawDataLeaseSpscRingQueue capacity must be > 0");
            }
            m_slots.resize(capacity);
        }

        size_t capacity() const
        {
            return m_slots.size();
        }

        size_t size() const
        {
            const uint64_t head = m_head.load(std::memory_order_acquire);
            const uint64_t tail = m_tail.load(std::memory_order_acquire);
            return static_cast<size_t>(head - tail);
        }

        bool empty() const
        {
            return size() == 0;
        }

        size_t peakSize() const
        {
            return static_cast<size_t>(m_peakDepth.load(std::memory_order_relaxed));
        }

        bool tryPushLease(RawDataLease &&lease)
        {
            const uint64_t head = m_head.load(std::memory_order_relaxed);
            const uint64_t tail = m_tail.load(std::memory_order_acquire);

            if (head - tail >= static_cast<uint64_t>(m_slots.size()))
            {
                // Caller still owns the Acq view; must not Release on failed push.
                lease.disarm();
                return false;
            }

            m_slots[head % m_slots.size()] = std::make_unique<RawDataLease>(std::move(lease));
            m_head.store(head + 1, std::memory_order_release);
            updatePeakDepth(head + 1 - tail);
            return true;
        }

        template <typename Consumer>
        bool tryConsumeOne(Consumer &&consumer)
        {
            const uint64_t tail = m_tail.load(std::memory_order_relaxed);
            const uint64_t head = m_head.load(std::memory_order_acquire);

            if (tail >= head)
            {
                return false;
            }

            auto &slot = m_slots[tail % m_slots.size()];
            std::unique_ptr<RawDataLease> lease = std::move(slot);
            m_tail.store(tail + 1, std::memory_order_release);
            if (lease)
            {
                std::forward<Consumer>(consumer)(*lease);
            }
            return true;
        }

    private:
        void updatePeakDepth(uint64_t depth)
        {
            uint64_t currentPeak = m_peakDepth.load(std::memory_order_relaxed);
            while (depth > currentPeak &&
                   !m_peakDepth.compare_exchange_weak(
                       currentPeak,
                       depth,
                       std::memory_order_relaxed,
                       std::memory_order_relaxed))
            {
            }
        }

        std::vector<std::unique_ptr<RawDataLease>> m_slots;
        alignas(64) std::atomic<uint64_t> m_head{0};
        alignas(64) std::atomic<uint64_t> m_tail{0};
        std::atomic<uint64_t> m_peakDepth{0};
    };

    /**
     * @brief 可复用的 R2S 实时处理器（支持直接处理内存中的 RawDataView）
     *
     * 使用方式：
     * 1. initialize(inputChannelNum)
     * 2. 对每个采集段调用 processSegment(view)
     * 3. finalize()
     */
    class R2SStreamProcessor
    {
    public:
        explicit R2SStreamProcessor(const R2SProcessConfig &config);

        ~R2SStreamProcessor();

        bool initialize(uint16_t inputChannelNum = 0);

        // inputKeepAlive 在未 bounce 时持有到本段 H2D 完成（采集桥传入 RawDataLease）。
        bool processSegment(
            const openpni::RawDataView &view,
            std::shared_ptr<void> inputKeepAlive = {});

        /**
         * @brief 关闭当前 singles 输出并按新前缀重新打开（用于目录批处理多文件切换输出名）
         */
        bool reopenOutput(const std::string &filePrefix);

        bool finalize();

    private:
        bool prepareChannelsToProcess();

        bool buildLocalChannelMaps();

        bool prepareGenerators();

        bool prepareOutput();

        bool openOutputWithPrefix(const std::string &filePrefix);

        bool dispatchSinglesToCallback(std::span<Single const> singles, uint64_t clockMs, uint32_t durationMs);

        bool dispatchSinglesToFile(std::span<Single const> singles, uint64_t clockMs, uint32_t durationMs);

        bool completeOldestMultiGpuSegment();

        void cleanupGenerators();

        R2SProcessConfig m_config;
        uint16_t m_inputChannelNum = 0;
        bool m_hasStreamingCallback = false;
        bool m_initialized = false;
        bool m_finalized = false;
        bool m_hadError = false;

        std::vector<uint16_t> m_channelsToProcess;
        std::unordered_set<uint16_t> m_assignedChannelSet;
        bool m_filterUnassignedChannels = false;
        std::vector<uint16_t> m_globalToLocalChannel;  // indexed by global channel, value = local index (or UINT16_MAX)
        std::vector<uint16_t> m_localToGlobalChannel;  // indexed by local index, value = global channel
        std::vector<openpni::interface::ISingleGenerator *> m_generatorsVector;
        openpni::ConvergedR2S m_r2s;
        std::unique_ptr<multi_gpu::R2S50100MultiGpuEngine> m_multiGpuEngine;
        bool m_useMultiGpu50100 = false;
        std::vector<Single> m_multiGpuHostSingles;

        struct PendingMultiGpuSubmit
        {
            uint64_t clockMs = 0;
            uint32_t durationMs = 0;
            std::unique_ptr<openpni::RawDataView> submittedView;
            std::unique_ptr<multi_gpu::PinnedRawSlot> bounce;
            std::vector<uint64_t> filteredOffset;
            std::vector<uint16_t> filteredLength;
            std::vector<uint16_t> filteredChannel;
            std::vector<uint16_t> remappedChannel;
            std::shared_ptr<void> inputKeepAlive;
        };
        std::queue<PendingMultiGpuSubmit> m_pendingMultiGpu;
        std::queue<std::unique_ptr<multi_gpu::PinnedRawSlot>> m_freeBounceSlots;

        openpni::distributed::coreio::RollingFileWriter<
            openpni::distributed::coreio::SinglesFileWriter,
            openpni::distributed::coreio::SingleWriterOptions>
            m_singleOutput;
        std::unique_ptr<AsyncSingleFileWriter> m_asyncWriter;
        std::string m_outputFilePath;

        uint64_t m_totalSegmentsProcessed = 0;
        uint64_t m_totalRawPackets = 0;
        uint64_t m_totalSingles = 0;
    };

    /**
     * @brief 采集 RawData -> 零拷贝租约队列 -> R2S 异步桥接器
     *
     * 设计目标：
     * - 采集线程只做入队（传递 RawDataView 租约），不复制 packet bytes。
     * - R2S 在独立线程消费；处理后调用 Release 归还 Acq 包槽。
     * - 支持反压或可选丢弃策略（丢弃时仍归还包槽）。
     */
    class AsyncRawDataToR2SBridge
    {
    public:
        using ReleaseFn = std::function<void(uint64_t packetCount)>;

        struct Config
        {
            size_t leaseQueueCapacity = 2;  // 在途段数（建议 1～2）
            bool blockWhenQueueFull = true; // true: 反压等待空槽；false: 立即返回
            bool dropWhenQueueFull = false; // 仅在 !blockWhenQueueFull 时生效
            uint32_t queueFullBackoffUs = 50;
            uint32_t consumerIdleBackoffUs = 50;
            uint64_t queueFullWarnEvery = 0; // 0 = 关闭告警
        };

        struct Stats
        {
            uint64_t enqueuedSegments = 0;  // 成功入队的采集段数
            uint64_t processedSegments = 0; // 成功处理的采集段数
            uint64_t droppedSegments = 0;   // 因队列满而丢弃的采集段数
            uint64_t enqueueFullHits = 0;   // 采集段入队时队列已满的次数
            uint64_t queuePeakDepth = 0;    // 队列深度峰值
            bool healthy = true;            // 是否健康（处理线程未发生错误）
        };

        explicit AsyncRawDataToR2SBridge(const R2SProcessConfig &r2sConfig);

        AsyncRawDataToR2SBridge(
            const R2SProcessConfig &r2sConfig,
            const Config &config);

        ~AsyncRawDataToR2SBridge();

        bool start(uint16_t inputChannelNum = 0);

        bool stop();

        /**
         * @brief 设置包槽归还回调（通常绑定 Acq::Release）。必须在 start 前设置。
         */
        void setReleaseFn(ReleaseFn releaseFn);

        bool enqueueRawData(const openpni::RawDataView &view);

        std::function<bool(const openpni::RawDataView &)> makeRawDataCallback();

        Stats stats() const;

        bool healthy() const;

    private:
        void consumerLoop();
        bool enqueueZeroCopyLease(const openpni::RawDataView &view);

        R2SStreamProcessor m_r2sProcessor;
        Config m_config;
        ReleaseFn m_releaseFn;
        std::unique_ptr<RawDataLeaseSpscRingQueue> m_leaseQueue;

        std::thread m_consumerThread;
        std::atomic<bool> m_started{false};
        std::atomic<bool> m_running{false};
        std::atomic<bool> m_failed{false};

        std::atomic<uint64_t> m_enqueuedSegments{0};
        std::atomic<uint64_t> m_processedSegments{0};
        std::atomic<uint64_t> m_droppedSegments{0};
        std::atomic<uint64_t> m_enqueueFullHits{0};
    };

    /**
     * @brief 通用的 R2S 处理函数
     *
     * @param config R2S处理配置
     * @return bool 成功返回true，失败返回false
     */
    bool processR2S(const R2SProcessConfig &config);

    /**
     * @brief 目录中的 raw 文件条目（按 pniRaw-<clock>.bin 解析）
     */
    struct RawDataFileEntry
    {
        uint64_t startClock = 0;
        std::string path;
    };

    /**
     * @brief 扫描目录下的 raw 文件并按 startClock 排序
     */
    std::vector<RawDataFileEntry> collectRawDataFiles(
        const std::string &directory,
        const std::string &namePrefix = "pniRaw-",
        const std::string &nameSuffix = ".bin");

    /**
     * @brief 构造 singles 输出文件名前缀：{base}_{inputClock}[_{runMs}]
     */
    std::string makeSinglesOutputPrefix(
        const std::string &basePrefix,
        uint64_t inputClock,
        bool appendRunTimestamp = false);

    /**
     * @brief 目录批处理：一次 initialize，多文件复用 processor；每文件按 inputClock 切换输出前缀
     *
     * @param config 模板配置（rawdataPath 会被逐文件覆盖；outputFileName 作为前缀）
     * @param rawdataDir raw 目录
     * @param appendRunTimestamp 是否在输出名中追加系统时钟毫秒，避免覆盖
     */
    bool processR2SDirectory(
        R2SProcessConfig config,
        const std::string &rawdataDir,
        bool appendRunTimestamp = false,
        const std::string &rawNamePrefix = "pniRaw-",
        const std::string &rawNameSuffix = ".bin");

    /**
     * @brief 创建 BDM2 处理配置
     */
    R2SProcessConfig createBDM2Config(
        const std::string &rawdataPath,
        const std::string &resultPath,
        const std::vector<std::string> &calibrationFiles,
        std::string outputFileName = "singles",
        const std::vector<uint16_t> &channelIndices = {});

    /**
     * @brief 创建 BDM50100 处理配置
     */
    R2SProcessConfig createBDM50100Config(
        const std::string &rawdataPath,
        const std::string &resultPath,
        const std::vector<std::string> &calibrationFiles,
        std::string outputFileName = "singles",
        const std::vector<uint16_t> &channelIndices = {});

    /**
     * @brief 创建 9120 处理配置
     *
     * channelNums 固定为整机通道数：48*3*instrumentRingCount（默认 576）。
     * 本节点职责由 channelIndices 表达（例如 Node0: 0..287，Node1: 288..575）。
     *
     * 校正目录 calibrationDirs：
     * - 若 size == instrumentRingCount：第 k 个目录对应环 k，空串表示该环不加载；
     * - 若 channelIndices 非空且 size == 覆盖环数：按 channelIndices 推导环号升序，与目录一一对应；
     * - 否则按环 0..dirs.size()-1 顺序落位。
     * 文件按全局通道下标写入 calibrationFiles（环 k → [k*144, (k+1)*144)）。
     */
    R2SProcessConfig createBDM50100_9120Config(
        const std::string &rawdataPath,
        const std::string &resultPath,
        const std::vector<std::string> &calibrationDirs,
        std::string outputFileName = "singles",
        const std::vector<uint16_t> &channelIndices = {},
        uint16_t instrumentRingCount = 4);

} // namespace openpni::distributed::r2s
