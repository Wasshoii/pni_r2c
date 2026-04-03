#pragma once

#include <pni/PnI-Config.hpp>

#include <cstdint>
#include <pni/io/IO.hpp>
#include <pni/io/v1/V1.hpp>
// #include <pni/node/BDMBiDR2S.hpp>

#include <pni/node/BDM2R2S.hpp>
#include <pni/node/ConvergedR2S.hpp>
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

namespace openpni::distributed::r2s
{
    using GlobalSingle = openpni::v1::basic::GlobalSingle_t;
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
        std::vector<GlobalSingle> &&singles,
        uint64_t clock_ms,
        uint32_t duration_ms)>;

    /**
     * @brief 原始 Single 视图回调（零额外 GlobalSingle 中间转换）
     *
     * 说明：
     * - span 仅在回调函数返回前有效，回调内若异步使用需自行拷贝。
     * - 当该回调已设置时，processR2S 会优先调用它，以避免额外的
     *   LocalSingle -> GlobalSingle 转换。
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
        Unknown
    };

    /**
     * @brief R2S 处理配置结构体
     */
    struct R2SProcessConfig
    {
        std::string rawdataPath;                   // 原始数据文件路径
        std::string resultPath;                    // 结果输出路径
        std::vector<std::string> calibrationFiles; // 校准文件列表
        DetectorType detectorType;                 // 探测器类型
        uint32_t crystalsPerChannel;               // 每个通道的晶体数
        uint32_t r2sResultIndex;                   // R2S结果数组中的目标索引 (BDM2=0, BDMBiD=1)
        std::string outputFileName;                // 输出文件名
        u_int16_t channelNums;                     // 通道总数
        std::vector<uint16_t> channelIndices;      // 要处理的通道索引列表（空则处理所有通道）
        bool sortDataByTime = true;                // 是否按时间排序输出数据
        bool saveData2SingleFile = true;           // 是否保存为 Single 文件格式
        bool asyncFileWrite = false;               // 是否异步写入文件（提高处理吞吐量）
        size_t asyncWriteQueueSize = 200;          // 异步写入队列大小
        uint32_t progressLogInterval = 50;         // 处理进度日志间隔，0 表示关闭

        // 分布式处理回调，使用时需设置（可与 saveData2SingleFile 同时使用，支持同时保存文件和流式传输）
        SinglesReadyCallback onSinglesReady = nullptr;         // 传输转换后的 GlobalSingle
        SinglesSpanReadyCallback onSinglesSpanReady = nullptr; // 直接传输原始 Single 数据，避免转换开销

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
        std::vector<GlobalSingle> singles;
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
         */
        bool open(const std::string &filePath, uint32_t totalCrystals);

        /**
         * @brief 异步提交写入任务
         *
         * @return true 成功提交，false 队列已满或已停止
         */
        bool submit(std::vector<GlobalSingle> &&singles, uint64_t clock_ms, uint32_t duration_ms);

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
         * @brief 获取底层输出文件对象（用于同步写入模式）
         */
        openpni::io::v1::single::SingleFileOutput *getOutput() { return m_output.get(); }

    private:
        void writerLoop();

        std::unique_ptr<openpni::io::v1::single::SingleFileOutput> m_output;
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
     * @brief 将 LocalSingle 转换为 GlobalSingle_t
     */
    std::vector<GlobalSingle> convertLocalToGlobalSingles(
        std::span<Single const> singles,
        uint32_t crystalsPerChannel);

    /**
     * @brief 追加单事件数据到 Single 文件（标准格式）
     */
    bool appendSinglesToSingleFile(
        openpni::io::v1::single::SingleFileOutput &outputFile,
        std::span<Single const> singles,
        uint32_t crystalsPerChannel,
        uint64_t clock_ms,
        uint32_t duration_ms);

    /**
     * @brief 创建指定类型的 SingleGenerator
     */
    openpni::interface::SingleGenerator *createSingleGenerator(
        DetectorType type,
        uint16_t channelIndex,
        const std::string &calibrationFile);

    /**
     * @brief RawData 段缓存（拥有数据所有权）
     *
     * 用于把采集线程中的 RawDataView 深拷贝到可跨线程持有的内存。
     */
    struct RawDataSegmentBuffer
    {
        std::vector<uint8_t> data;
        std::vector<uint16_t> length;
        std::vector<uint64_t> offset;
        std::vector<uint16_t> channel;
        uint64_t clock_ms = 0;
        uint64_t duration_ms = 0;

        void reserve(size_t packetCapacity, size_t byteCapacity)
        {
            length.reserve(packetCapacity);
            offset.reserve(packetCapacity);
            channel.reserve(packetCapacity);
            data.reserve(byteCapacity);
        }

        bool copyFrom(const openpni::RawDataView &view)
        {
            if (view.count == 0)
            {
                data.clear();
                length.clear();
                offset.clear();
                channel.clear();
                clock_ms = view.clock_ms;
                duration_ms = view.duration_ms;
                return true;
            }

            if (!view.length || !view.offset || !view.channel)
            {
                return false;
            }

            uint64_t maxEnd = 0;
            for (uint64_t i = 0; i < view.count; ++i)
            {
                const uint64_t end = view.offset[i] + static_cast<uint64_t>(view.length[i]);
                if (end > maxEnd)
                {
                    maxEnd = end;
                }
            }

            if (maxEnd > static_cast<uint64_t>(std::numeric_limits<size_t>::max()))
            {
                return false;
            }

            if (maxEnd > 0 && !view.data)
            {
                return false;
            }

            const size_t dataBytes = static_cast<size_t>(maxEnd);
            data.resize(dataBytes);
            if (dataBytes > 0)
            {
                std::memcpy(data.data(), view.data, dataBytes);
            }

            length.assign(view.length, view.length + view.count);
            offset.assign(view.offset, view.offset + view.count);
            channel.assign(view.channel, view.channel + view.count);
            clock_ms = view.clock_ms;
            duration_ms = view.duration_ms;
            return true;
        }

        openpni::RawDataView toRawView()
        {
            openpni::RawDataView view;
            view.data = data.empty() ? nullptr : data.data();
            view.length = length.empty() ? nullptr : length.data();
            view.offset = offset.empty() ? nullptr : offset.data();
            view.channel = channel.empty() ? nullptr : channel.data();
            view.count = length.size();
            view.clock_ms = clock_ms;
            view.duration_ms = duration_ms;
            return view;
        }
    };

    /**
     * @brief 单生产者单消费者无锁环形队列（RawData 段）
     *
     * 约束：
     * - 仅允许一个生产者线程调用 tryPushCopy。
     * - 仅允许一个消费者线程调用 tryConsumeOne。
     */
    class RawDataSpscRingQueue
    {
    public:
        struct Config
        {
            size_t capacity = 128;               // 槽位数量（segment 级别）
            size_t reservePacketsPerSlot = 4096; // 每个槽位预留包数
            size_t reserveBytesPerSlot = 4 * 1024 * 1024;
        };

        RawDataSpscRingQueue()
            : RawDataSpscRingQueue(Config())
        {
        }

        explicit RawDataSpscRingQueue(const Config &config)
            : m_config(config)
        {
            if (m_config.capacity == 0)
            {
                throw std::invalid_argument("RawDataSpscRingQueue capacity must be > 0");
            }

            m_slots.resize(m_config.capacity);
            for (auto &slot : m_slots)
            {
                slot.reserve(m_config.reservePacketsPerSlot, m_config.reserveBytesPerSlot);
            }
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
            const uint64_t head = m_head.load(std::memory_order_acquire);
            const uint64_t tail = m_tail.load(std::memory_order_acquire);
            return head == tail;
        }

        size_t peakSize() const
        {
            return static_cast<size_t>(m_peakDepth.load(std::memory_order_relaxed));
        }

        bool tryPushCopy(const openpni::RawDataView &view)
        {
            const uint64_t head = m_head.load(std::memory_order_relaxed);
            const uint64_t tail = m_tail.load(std::memory_order_acquire);

            if (head - tail >= static_cast<uint64_t>(m_slots.size()))
            {
                return false;
            }

            auto &slot = m_slots[head % m_slots.size()];
            if (!slot.copyFrom(view))
            {
                return false;
            }

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
            std::forward<Consumer>(consumer)(slot);
            m_tail.store(tail + 1, std::memory_order_release);
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

        Config m_config;
        std::vector<RawDataSegmentBuffer> m_slots;

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

        bool processSegment(const openpni::RawDataView &view);

        bool finalize();

    private:
        bool prepareChannelsToProcess();

        bool prepareGenerators();

        bool prepareOutput();

        bool dispatchSinglesToCallback(std::span<Single const> singles, uint64_t clockMs, uint32_t durationMs);

        bool dispatchSinglesToFile(std::span<Single const> singles, uint64_t clockMs, uint32_t durationMs);

        void cleanupGenerators();

        R2SProcessConfig m_config;
        uint16_t m_inputChannelNum = 0;
        bool m_hasStreamingCallback = false;
        bool m_initialized = false;
        bool m_finalized = false;
        bool m_hadError = false;

        std::vector<uint16_t> m_channelsToProcess;
        std::vector<openpni::interface::SingleGenerator *> m_generatorsVector;
        openpni::ConvergedR2S m_r2s;

        std::unique_ptr<openpni::io::v1::single::SingleFileOutput> m_singleOutput;
        std::unique_ptr<AsyncSingleFileWriter> m_asyncWriter;
        std::string m_outputFilePath;

        uint64_t m_totalSegmentsProcessed = 0;
        uint64_t m_totalRawPackets = 0;
        uint64_t m_totalSingles = 0;
    };

    /**
     * @brief 采集 RawData -> 无锁队列 -> R2S 异步桥接器
     *
     * 设计目标：
     * - 采集线程只做入队，减少在采集热路径上的 R2S 计算阻塞。
     * - R2S 在独立线程消费队列，支持反压或可选丢弃策略。
     */
    class AsyncRawDataToR2SBridge
    {
    public:
        struct Config
        {
            RawDataSpscRingQueue::Config queue;
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

        bool enqueueRawData(const openpni::RawDataView &view);

        std::function<bool(const openpni::RawDataView &)> makeRawDataCallback();

        Stats stats() const;

        bool healthy() const;

    private:
        void consumerLoop();

        R2SStreamProcessor m_r2sProcessor;
        RawDataSpscRingQueue m_queue;
        Config m_config;

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
     * @brief 创建 BDM2 处理配置
     */
    R2SProcessConfig createBDM2Config(
        const std::string &rawdataPath,
        const std::string &resultPath,
        const std::vector<std::string> &calibrationFiles,
        std::string outputFileName = "singles",
        const std::vector<uint16_t> &channelIndices = {});

    // /**
    //  * @brief 创建 BDMBiD 处理配置
    //  */
    // R2SProcessConfig createBDMBiDConfig(
    //     const std::string &rawdataPath,
    //     const std::string &resultPath,
    //     const std::vector<std::string> &calibrationFiles,
    //     std::string outputFileName = "singles",
    //     const std::vector<uint16_t> &channelIndices = {})
    // {
    //     R2SProcessConfig config;
    //     config.rawdataPath = rawdataPath;
    //     config.resultPath = resultPath;
    //     config.calibrationFiles = calibrationFiles;
    //     config.detectorType = DetectorType::BDMBiD;
    //     config.crystalsPerChannel = 400 * 8; // BDMBiD: 20x20 晶体阵列, 8个阵列
    //     config.r2sResultIndex = 1;           // BDMBiD 数据在位置 1
    //     config.outputFileName = outputFileName;
    //     config.channelNums = 4;                 // BDMBiD 通道总数4
    //     config.channelIndices = channelIndices; // 要处理的通道列表（空则处理所有）
    //     return config;
    // }
} // namespace openpni::distributed::r2s