#pragma once

#include <pni/io/IO.hpp>
#include <pni/io/ListmodeIO.hpp>
#include <pni/experimental/node/Coincidence.hpp>
#include <pni/experimental/tools/Parallel.hpp>
#include <pni/CudaPtr.hpp>

#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <memory>
#include <span>
#include <functional>
#include <chrono>
#include <algorithm>
#include <execution>
#include <optional>
#include <unordered_map>
#include <filesystem>
#include <iostream>

namespace openpni::distributed::streaming
{

    namespace fs = std::filesystem;

    // ==================== 数据结构定义 ====================

    /**
     * @brief 带时间戳的单事件数据块
     * 从分布式节点接收的数据单元
     * 
     * 对应 R2S 节点的 SingleSegmentHeader：
     * - computerClock_ms ← clock
     * - duration_ms ← duration
     * - singles.size() ← count
     */
    struct TimestampedSingleChunk
    {
        uint16_t nodeId;                                     // 来源节点ID
        uint64_t chunkId;                                    // 块序号（用于顺序保证）
        uint64_t computerClock_ms;                           // 计算机时钟（对应 SingleSegmentHeader.clock）
        uint32_t duration_ms;                                // 持续时间（对应 SingleSegmentHeader.duration）
        std::vector<openpni::basic::GlobalSingle_t> singles; // 单事件数据（count = singles.size()）

        // 块内时间范围（来自 PET 时钟板，绝对精确）
        uint64_t minTime_pico = UINT64_MAX;
        uint64_t maxTime_pico = 0;

        /**
         * @brief 更新块内时间范围
         * 优化：假设块内数据已按 timeValue_pico 排序，直接取首尾 O(1)
         */
        void updateTimeRange()
        {
            if (singles.empty())
            {
                minTime_pico = UINT64_MAX;
                maxTime_pico = 0;
                return;
            }
            // 块内数据已排序，直接取首尾
            minTime_pico = singles.front().timeValue_pico;
            maxTime_pico = singles.back().timeValue_pico;
        }

        /**
         * @brief 块级别比较：按计算机时钟排序
         */
        bool operator<(const TimestampedSingleChunk &other) const
        {
            // 首先按计算机时钟排序
            if (computerClock_ms != other.computerClock_ms)
                return computerClock_ms < other.computerClock_ms;
            // 相同时钟则按 chunkId 排序
            return chunkId < other.chunkId;
        }
    };

    // ==================== 节点环形缓冲区 ====================

    /**
     * @brief 节点数据接收缓冲区（支持按时间排序的优先队列模式）
     *
     * 两种模式：
     * 1. FIFO 模式（默认）：保持接收顺序，假设网络顺序正确
     * 2. BY_CLOCK 模式：按 computerClock_ms 排序，处理网络乱序
     *
     * 线程安全：支持单生产者（gRPC接收线程）多消费者（处理线程）模式
     */
    class NodeRingBuffer
    {
    public:
        /**
         * @brief 缓冲区排序模式
         */
        enum class SortMode
        {
            FIFO,    // 先进先出（默认，依赖网络顺序）
            BY_CLOCK // 按计算机时钟排序（处理乱序）
        };

        explicit NodeRingBuffer(uint16_t nodeId, size_t maxChunks = 100,
                                SortMode mode = SortMode::FIFO)
            : m_nodeId(nodeId), m_maxChunks(maxChunks), m_sortMode(mode) {}

        /**
         * @brief 生产者：接收来自节点的数据块
         * @param chunk 数据块（移动语义）
         * @return 成功返回true，缓冲区关闭返回false
         */
        bool push(TimestampedSingleChunk &&chunk)
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            // 等待空间可用
            m_cvNotFull.wait(lock, [this]
                             { return currentSize() < m_maxChunks || m_closed; });

            if (m_closed)
                return false;

            // 检查块序号连续性（警告但不阻止）
            if (m_expectedChunkId > 0 && chunk.chunkId != m_expectedChunkId)
            {
                m_outOfOrderCount++;
                std::cerr << "[NodeRingBuffer] Warning: Node " << m_nodeId
                          << " received chunk " << chunk.chunkId
                          << ", expected " << m_expectedChunkId
                          << " (out of order or gap)" << std::endl;
            }
            m_expectedChunkId = chunk.chunkId + 1;

            chunk.updateTimeRange();

            if (m_sortMode == SortMode::FIFO)
            {
                m_fifoQueue.push(std::move(chunk));
            }
            else
            {
                m_priorityQueue.push(std::move(chunk));
            }

            m_cvNotEmpty.notify_one();
            return true;
        }

        /**
         * @brief 消费者：获取最早的数据块（不移除）
         */
        const TimestampedSingleChunk *peek() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return peekFront_unlocked();
        }

        /**
         * @brief 消费者：阻塞式移除最早的数据块
         * @return 数据块，缓冲区关闭且为空时返回nullopt
         */
        std::optional<TimestampedSingleChunk> pop()
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            m_cvNotEmpty.wait(lock, [this]
                              { return currentSize() > 0 || m_closed; });

            if (currentSize() == 0)
                return std::nullopt;

            auto chunk = popFront_unlocked();
            m_cvNotFull.notify_one();
            return chunk;
        }

        /**
         * @brief 非阻塞尝试获取数据块
         * @return 数据块，无数据时返回nullopt
         */
        std::optional<TimestampedSingleChunk> tryPop()
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (currentSize() == 0)
                return std::nullopt;

            auto chunk = popFront_unlocked();
            m_cvNotFull.notify_one();
            return chunk;
        }

        /**
         * @brief 获取当前最小待处理时间（PET 时钟，pico）
         * @return 最小时间（pico），无数据返回UINT64_MAX
         */
        uint64_t getMinPendingTime() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            auto *front = peekFront_unlocked();
            return front ? front->minTime_pico : UINT64_MAX;
        }

        /**
         * @brief 获取当前最大待处理时间（PET 时钟，pico）
         */
        uint64_t getMaxPendingTime() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (currentSize() == 0)
                return 0;

            // 遍历所有块找最大值
            uint64_t maxTime = 0;
            if (m_sortMode == SortMode::FIFO)
            {
                std::queue<TimestampedSingleChunk> tempQueue = m_fifoQueue;
                while (!tempQueue.empty())
                {
                    maxTime = std::max(maxTime, tempQueue.front().maxTime_pico);
                    tempQueue.pop();
                }
            }
            else
            {
                auto tempPQ = m_priorityQueue;
                while (!tempPQ.empty())
                {
                    maxTime = std::max(maxTime, tempPQ.top().maxTime_pico);
                    tempPQ.pop();
                }
            }
            return maxTime;
        }

        /**
         * @brief 获取当前最小计算机时钟（ms）- 用于安全边界计算
         */
        uint64_t getMinComputerClock() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            auto *front = peekFront_unlocked();
            return front ? front->computerClock_ms : UINT64_MAX;
        }

        /**
         * @brief 获取乱序统计信息
         */
        size_t getOutOfOrderCount() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_outOfOrderCount;
        }

        bool empty() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return currentSize() == 0;
        }

        size_t size() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return currentSize();
        }

        uint16_t nodeId() const { return m_nodeId; }

        SortMode sortMode() const { return m_sortMode; }

        /**
         * @brief 关闭缓冲区，唤醒所有等待线程
         */
        void close()
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_closed = true;
            m_cvNotEmpty.notify_all();
            m_cvNotFull.notify_all();
        }

        bool isClosed() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_closed;
        }

    private:
        uint16_t m_nodeId;
        size_t m_maxChunks;
        SortMode m_sortMode;
        mutable std::mutex m_mutex;
        std::condition_variable m_cvNotEmpty;
        std::condition_variable m_cvNotFull;

        // FIFO 模式的队列
        std::queue<TimestampedSingleChunk> m_fifoQueue;

        // BY_CLOCK 模式的优先队列（最小堆，按 computerClock_ms 排序）
        struct ChunkComparator
        {
            bool operator()(const TimestampedSingleChunk &a, const TimestampedSingleChunk &b) const
            {
                // priority_queue 默认是最大堆，返回 true 时 a 优先级低于 b
                // 我们需要最小堆（最小 clock 优先），所以返回 a > b
                return b < a;
            }
        };
        std::priority_queue<TimestampedSingleChunk, std::vector<TimestampedSingleChunk>,
                            ChunkComparator>
            m_priorityQueue;

        bool m_closed = false;
        uint64_t m_expectedChunkId = 0; // 用于检测乱序
        size_t m_outOfOrderCount = 0;   // 乱序计数

        // 内部辅助方法（必须持有锁）
        size_t currentSize() const
        {
            return (m_sortMode == SortMode::FIFO) ? m_fifoQueue.size() : m_priorityQueue.size();
        }

        const TimestampedSingleChunk *peekFront_unlocked() const
        {
            if (m_sortMode == SortMode::FIFO)
            {
                return m_fifoQueue.empty() ? nullptr : &m_fifoQueue.front();
            }
            else
            {
                return m_priorityQueue.empty() ? nullptr : &m_priorityQueue.top();
            }
        }

        TimestampedSingleChunk popFront_unlocked()
        {
            if (m_sortMode == SortMode::FIFO)
            {
                auto chunk = std::move(m_fifoQueue.front());
                m_fifoQueue.pop();
                return chunk;
            }
            else
            {
                // priority_queue 的 top() 返回 const ref，需要拷贝后再 pop
                auto chunk = m_priorityQueue.top();
                m_priorityQueue.pop();
                return chunk;
            }
        }
    };

    // ==================== 时间对齐器配置 ====================

    /**
     * @brief 时间对齐器配置
     */
    struct TimeAlignerConfig
    {
        // 时间窗口配置
        uint64_t alignmentWindow_pico = 200'000'000'000; // 200ms 对齐窗口
        uint64_t safetyMargin_pico = 10'000'000'000;     // 10ms 安全边距

        // 符合处理配置
        openpni::experimental::node::CoincidenceProtocol coinProtocol;
        uint16_t channelNum = 0;
        uint32_t crystalsPerChannel = 0;

        // 输出配置
        std::string outputDir;
        bool savePrompt = true;
        bool saveDelay = true;

        // 性能配置
        size_t maxChunksPerNode = 100;     // 每节点最大缓冲块数
        uint32_t processingIntervalMs = 5; // 处理循环间隔

        // ==================== 新增：两级时间排序配置 ====================

        /**
         * @brief 缓冲区排序模式
         * FIFO: 按接收顺序（默认，假设网络顺序正确）
         * BY_CLOCK: 按 computerClock_ms 排序（处理网络乱序）
         */
        NodeRingBuffer::SortMode bufferSortMode = NodeRingBuffer::SortMode::FIFO;

        /**
         * @brief 最大允许的计算机时钟偏差（ms）
         * 用于检测异常和计算安全边界
         */
        uint64_t maxClockSkew_ms = 100;

        /**
         * @brief 是否使用计算机时钟辅助计算安全边界
         * 如果 true，结合 computerClock_ms 和 timeValue_pico 计算更精确的安全边界
         */
        bool useClockBasedSafeBoundary = true;
    };

    // ==================== 统计信息 ====================

    /**
     * @brief 处理统计信息
     */
    struct ProcessingStatistics
    {
        std::atomic<uint64_t> totalSinglesReceived{0};
        std::atomic<uint64_t> totalSinglesProcessed{0};
        std::atomic<uint64_t> totalPromptPairs{0};
        std::atomic<uint64_t> totalDelayPairs{0};
        std::atomic<uint64_t> alignmentWindowsProcessed{0};
        std::atomic<double> avgProcessingTime_ms{0};
        std::atomic<uint64_t> currentTimeBoundary_pico{0};

        void reset()
        {
            totalSinglesReceived = 0;
            totalSinglesProcessed = 0;
            totalPromptPairs = 0;
            totalDelayPairs = 0;
            alignmentWindowsProcessed = 0;
            avgProcessingTime_ms = 0;
            currentTimeBoundary_pico = 0;
        }
    };

    // ==================== 流式时间对齐器 ====================

    /**
     * @brief 流式时间对齐器
     *
     * 核心组件：收集各节点数据，按 PET 时钟进行精确时间对齐
     *
     * 工作流程：
     * 1. 各节点通过 getNodeBuffer() 获取缓冲区并推送数据
     * 2. 处理线程计算全局安全时间边界
     * 3. 从各节点提取安全边界内的数据
     * 4. 局部排序后进行符合计算
     * 5. 更新时间边界，重复
     */
    class StreamingTimeAligner
    {
    public:
        StreamingTimeAligner(const TimeAlignerConfig &config, size_t nodeCount)
            : m_config(config), m_nodeCount(nodeCount)
        {
            // 为每个节点创建缓冲区（使用配置的排序模式）
            for (size_t i = 0; i < nodeCount; ++i)
            {
                m_nodeBuffers.push_back(
                    std::make_unique<NodeRingBuffer>(i, config.maxChunksPerNode,
                                                     config.bufferSortMode));
                m_partialChunks[i] = std::nullopt;
            }

            // 初始化符合处理器
            std::vector<uint32_t> crystalNumOfEachChannel(
                config.channelNum, config.crystalsPerChannel);
            m_coinNode.setTotalCrystalNumOfEachChannel(crystalNumOfEachChannel);

            std::cout << "[StreamingTimeAligner] Initialized with " << nodeCount
                      << " nodes, " << config.channelNum << " channels"
                      << ", bufferMode="
                      << (config.bufferSortMode == NodeRingBuffer::SortMode::FIFO ? "FIFO" : "BY_CLOCK")
                      << std::endl;
        }

        ~StreamingTimeAligner()
        {
            stop();
        }

        /**
         * @brief 获取节点缓冲区（用于数据接收）
         * @param nodeId 节点ID
         * @return 缓冲区指针，无效ID返回nullptr
         */
        NodeRingBuffer *getNodeBuffer(uint16_t nodeId)
        {
            if (nodeId >= m_nodeBuffers.size())
                return nullptr;
            return m_nodeBuffers[nodeId].get();
        }

        /**
         * @brief 启动处理流水线
         */
        void start()
        {
            if (m_running.exchange(true))
            {
                std::cerr << "[StreamingTimeAligner] Already running" << std::endl;
                return;
            }

            // 初始化输出文件
            initializeOutput();

            // 启动处理线程
            m_processorThread = std::thread([this]
                                            { processingLoop(); });

            std::cout << "[StreamingTimeAligner] Started" << std::endl;
        }

        /**
         * @brief 停止处理
         * @param waitForCompletion 是否等待处理完所有剩余数据
         */
        void stop(bool waitForCompletion = true)
        {
            if (!m_running.exchange(false))
            {
                return; // 已经停止
            }

            // 关闭所有缓冲区
            for (auto &buf : m_nodeBuffers)
            {
                buf->close();
            }

            // 等待处理线程结束
            if (m_processorThread.joinable())
            {
                m_processorThread.join();
            }

            // 关闭输出文件
            finalizeOutput();

            std::cout << "[StreamingTimeAligner] Stopped" << std::endl;
        }

        /**
         * @brief 获取统计信息
         */
        const ProcessingStatistics &getStatistics() const
        {
            return m_stats;
        }

        /**
         * @brief 检查是否正在运行
         */
        bool isRunning() const
        {
            return m_running.load();
        }

        /**
         * @brief 获取节点数量
         */
        size_t getNodeCount() const
        {
            return m_nodeCount;
        }

    private:
        /**
         * @brief 初始化输出文件
         */
        void initializeOutput()
        {
            fs::create_directories(m_config.outputDir);

            uint32_t totalCrystals = m_config.channelNum * m_config.crystalsPerChannel;

            if (m_config.savePrompt)
            {
                m_promptWriter = std::make_unique<openpni::io::listmode::ListmodeFileOutput>();
                m_promptWriter->setBytes4CrystalIndex(openpni::io::single::CrystalIndexType::UINT32);
                m_promptWriter->setBytes4TimeValue1_2(openpni::io::listmode::TimeValue1_2Type::INT16);
                m_promptWriter->setTotalCrystalNum(totalCrystals);
                m_promptWriter->open(m_config.outputDir + "/prompt.lmf");
                std::cout << "[StreamingTimeAligner] Prompt output: "
                          << m_config.outputDir << "/prompt.lmf" << std::endl;
            }

            if (m_config.saveDelay)
            {
                m_delayWriter = std::make_unique<openpni::io::listmode::ListmodeFileOutput>();
                m_delayWriter->setBytes4CrystalIndex(openpni::io::single::CrystalIndexType::UINT32);
                m_delayWriter->setBytes4TimeValue1_2(openpni::io::listmode::TimeValue1_2Type::INT16);
                m_delayWriter->setTotalCrystalNum(totalCrystals);
                m_delayWriter->open(m_config.outputDir + "/delay.lmf");
                std::cout << "[StreamingTimeAligner] Delay output: "
                          << m_config.outputDir << "/delay.lmf" << std::endl;
            }
        }

        /**
         * @brief 关闭输出文件
         * @note ListmodeFileOutput 在析构时自动关闭文件，无需显式 close
         */
        void finalizeOutput()
        {
            std::lock_guard<std::mutex> lock(m_outputMutex);
            // 通过 reset() 触发析构函数，自动关闭文件
            m_promptWriter.reset();
            m_delayWriter.reset();
        }

        /**
         * @brief 计算安全时间边界的结果结构
         */
        struct SafeTimeBoundaryResult
        {
            uint64_t safeTimeBoundary_pico = 0; // 安全时间边界（PET 时钟）
            uint64_t globalMinTime_pico = 0;    // 全局最小 PET 时间
            uint64_t globalMinClock_ms = 0;     // 全局最小计算机时钟
            size_t nodesWithData = 0;           // 有数据的节点数
            bool allNodesHaveData = false;      // 是否所有节点都有数据
            bool isValid = false;               // 结果是否有效
        };

        /**
         * @brief 计算全局安全时间边界（两级时间方案）
         *
         * 结合 computerClock_ms（块级别）和 timeValue_pico（事件级别）计算安全边界
         *
         * @return 安全时间边界计算结果
         */
        SafeTimeBoundaryResult calculateSafeTimeBoundary() const
        {
            SafeTimeBoundaryResult result;
            result.globalMinTime_pico = UINT64_MAX;
            result.globalMinClock_ms = UINT64_MAX;
            result.allNodesHaveData = true;
            result.nodesWithData = 0;

            // 1. 收集各节点的时间信息
            for (const auto &buf : m_nodeBuffers)
            {
                uint64_t nodeMinTime_pico = buf->getMinPendingTime();
                uint64_t nodeMinClock_ms = buf->getMinComputerClock();

                if (nodeMinTime_pico == UINT64_MAX)
                {
                    result.allNodesHaveData = false;
                }
                else
                {
                    result.globalMinTime_pico = std::min(result.globalMinTime_pico, nodeMinTime_pico);
                    result.globalMinClock_ms = std::min(result.globalMinClock_ms, nodeMinClock_ms);
                    result.nodesWithData++;
                }
            }

            // 2. 检查部分块中的数据
            for (const auto &[nodeId, partial] : m_partialChunks)
            {
                if (partial.has_value() && !partial->singles.empty())
                {
                    result.globalMinTime_pico = std::min(result.globalMinTime_pico, partial->minTime_pico);
                    result.globalMinClock_ms = std::min(result.globalMinClock_ms, partial->computerClock_ms);
                    result.nodesWithData++;
                }
            }

            // 3. 如果没有数据，返回无效结果
            if (result.nodesWithData == 0 || result.globalMinTime_pico == UINT64_MAX)
            {
                result.isValid = false;
                return result;
            }

            result.isValid = true;

            // 4. 计算安全边界
            if (m_config.useClockBasedSafeBoundary)
            {
                // 两级时间方案：利用 computerClock_ms 进行更精确的边界计算
                // 假设：如果 computerClock 已经过了 X ms，则对应的 PET 时间至少也过了 X ms
                // 安全边界 = globalMinTime_pico - (maxClockSkew_ms * 1e9)

                uint64_t clockBasedMargin_pico = m_config.maxClockSkew_ms * 1'000'000'000ULL;
                uint64_t effectiveMargin = std::max(m_config.safetyMargin_pico, clockBasedMargin_pico);

                if (result.allNodesHaveData)
                {
                    result.safeTimeBoundary_pico = result.globalMinTime_pico > effectiveMargin
                                                       ? result.globalMinTime_pico - effectiveMargin
                                                       : 0;
                }
                else
                {
                    // 不是所有节点都有数据时，使用更大的安全边距
                    uint64_t largerMargin = effectiveMargin * 2;
                    result.safeTimeBoundary_pico = result.globalMinTime_pico > largerMargin
                                                       ? result.globalMinTime_pico - largerMargin
                                                       : 0;
                }
            }
            else
            {
                // 原始方案：仅使用 safetyMargin_pico
                if (result.allNodesHaveData)
                {
                    result.safeTimeBoundary_pico = result.globalMinTime_pico > m_config.safetyMargin_pico
                                                       ? result.globalMinTime_pico - m_config.safetyMargin_pico
                                                       : 0;
                }
                else
                {
                    uint64_t largerMargin = m_config.safetyMargin_pico * 2;
                    result.safeTimeBoundary_pico = result.globalMinTime_pico > largerMargin
                                                       ? result.globalMinTime_pico - largerMargin
                                                       : 0;
                }
            }

            return result;
        }

        /**
         * @brief 核心处理循环
         *
         * 策略：
         * 1. 使用 calculateSafeTimeBoundary() 计算全局安全时间边界（两级时间方案）
         * 2. 从各节点缓冲区提取 [上次边界, 当前安全边界] 范围内的数据
         * 3. k-way 归并后进行符合计算
         * 4. 更新时间边界，重复
         */
        void processingLoop()
        {
            uint64_t lastProcessedTime_pico = 0;
            size_t consecutiveEmptyRounds = 0;
            const size_t maxEmptyRounds = 100; // 连续空轮次后处理剩余数据

            while (m_running.load())
            {
                auto startTime = std::chrono::high_resolution_clock::now();

                // 1. 使用两级时间方案计算安全边界
                auto boundaryResult = calculateSafeTimeBoundary();

                // 如果没有足够数据，短暂等待
                if (!boundaryResult.isValid || boundaryResult.nodesWithData == 0)
                {
                    consecutiveEmptyRounds++;
                    if (consecutiveEmptyRounds > maxEmptyRounds && !m_running.load())
                    {
                        break; // 停止且无数据，退出
                    }
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(m_config.processingIntervalMs));
                    continue;
                }

                consecutiveEmptyRounds = 0;

                uint64_t safeTimeBoundary = boundaryResult.safeTimeBoundary_pico;

                // 确保时间向前推进（至少推进一个符合窗口）
                if (safeTimeBoundary <= lastProcessedTime_pico + m_config.coinProtocol.timeWindow_ps)
                {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(m_config.processingIntervalMs));
                    continue;
                }

                // 2. 从各节点提取时间范围内的数据
                std::vector<openpni::basic::GlobalSingle_t> windowSingles;
                extractSinglesInTimeRange(lastProcessedTime_pico, safeTimeBoundary, windowSingles);

                if (!windowSingles.empty())
                {
                    // 数据已通过 k-way 归并排序，无需重复排序
                    // 3. 符合计算
                    processCoincidence(windowSingles);

                    // 更新统计
                    m_stats.totalSinglesProcessed += windowSingles.size();
                    m_stats.alignmentWindowsProcessed++;
                }

                // 5. 更新时间边界
                lastProcessedTime_pico = safeTimeBoundary;
                m_stats.currentTimeBoundary_pico = safeTimeBoundary;

                // 计算处理时间
                auto endTime = std::chrono::high_resolution_clock::now();
                double elapsed_ms = std::chrono::duration<double, std::milli>(
                                        endTime - startTime)
                                        .count();

                // 更新平均处理时间（指数移动平均）
                double currentAvg = m_stats.avgProcessingTime_ms.load();
                m_stats.avgProcessingTime_ms = currentAvg * 0.9 + elapsed_ms * 0.1;
            }

            // 处理剩余数据
            flushRemaining();
        }

        /**
         * @brief 从各节点缓冲区提取指定时间范围内的数据
         *
         * 优化：每个节点的数据块已按时间排序，使用 k-way 归并而非全排序
         */
        void extractSinglesInTimeRange(
            uint64_t startTime_pico,
            uint64_t endTime_pico,
            std::vector<openpni::basic::GlobalSingle_t> &output)
        {
            // 收集各节点符合时间范围的数据（每个 vector 内部保持有序）
            std::vector<std::vector<openpni::basic::GlobalSingle_t>> nodeDataList;
            nodeDataList.reserve(m_nodeBuffers.size());

            for (size_t nodeId = 0; nodeId < m_nodeBuffers.size(); ++nodeId)
            {
                auto &buf = m_nodeBuffers[nodeId];
                auto &partial = m_partialChunks[nodeId];
                std::vector<openpni::basic::GlobalSingle_t> nodeSingles;

                // 首先处理上次遗留的部分块
                if (partial.has_value() && !partial->singles.empty())
                {
                    extractFromChunkSorted(partial.value(), startTime_pico, endTime_pico, nodeSingles);

                    // 如果块已完全消费，清除
                    if (partial->singles.empty())
                    {
                        partial.reset();
                    }
                }

                // 继续从缓冲区获取新块
                while (true)
                {
                    const auto *peek = buf->peek();
                    if (!peek)
                        break;

                    // 如果块的最小时间已超过窗口，停止
                    if (peek->minTime_pico > endTime_pico)
                        break;

                    // 获取块
                    auto chunkOpt = buf->tryPop();
                    if (!chunkOpt)
                        break;

                    auto &chunk = chunkOpt.value();
                    extractFromChunkSorted(chunk, startTime_pico, endTime_pico, nodeSingles);

                    // 如果块未完全消费，保存为部分块
                    if (!chunk.singles.empty())
                    {
                        partial = std::move(chunk);
                        break;
                    }
                }

                if (!nodeSingles.empty())
                {
                    nodeDataList.push_back(std::move(nodeSingles));
                }
            }

            // k-way 归并：将多个已排序的 vector 合并为一个有序 vector
            kWayMergeSorted(nodeDataList, output);
        }

        /**
         * @brief k-way 归并多个已排序序列
         *
         * 使用优先队列实现，时间复杂度 O(N log K)，其中 N 是总元素数，K 是节点数
         */
        void kWayMergeSorted(
            std::vector<std::vector<openpni::basic::GlobalSingle_t>> &sortedLists,
            std::vector<openpni::basic::GlobalSingle_t> &output)
        {
            if (sortedLists.empty())
                return;

            // 特殊情况：只有一个列表，直接移动
            if (sortedLists.size() == 1)
            {
                output = std::move(sortedLists[0]);
                return;
            }

            // 计算总大小并预分配
            size_t totalSize = 0;
            for (const auto &list : sortedLists)
            {
                totalSize += list.size();
            }
            output.reserve(output.size() + totalSize);

            // 使用迭代器和索引的结构
            struct MergeEntry
            {
                size_t listIdx;
                size_t elemIdx;
                uint64_t time;

                bool operator>(const MergeEntry &other) const
                {
                    return time > other.time; // 最小堆
                }
            };

            // 初始化优先队列（最小堆）
            std::priority_queue<MergeEntry, std::vector<MergeEntry>, std::greater<MergeEntry>> minHeap;

            for (size_t i = 0; i < sortedLists.size(); ++i)
            {
                if (!sortedLists[i].empty())
                {
                    minHeap.push({i, 0, sortedLists[i][0].timeValue_pico});
                }
            }

            // 归并
            while (!minHeap.empty())
            {
                auto entry = minHeap.top();
                minHeap.pop();

                output.push_back(std::move(sortedLists[entry.listIdx][entry.elemIdx]));

                // 如果该列表还有更多元素，加入堆
                if (entry.elemIdx + 1 < sortedLists[entry.listIdx].size())
                {
                    size_t nextIdx = entry.elemIdx + 1;
                    minHeap.push({entry.listIdx, nextIdx, sortedLists[entry.listIdx][nextIdx].timeValue_pico});
                }
            }
        }

        /**
         * @brief 从单个块中提取指定时间范围的数据（保持排序）
         *
         * 优化：假设块内数据已按 timeValue_pico 排序，使用二分查找 + 范围移动
         * 时间复杂度从 O(n) 的 stable_partition 优化为 O(log n) 查找 + O(k) 移动
         */
        void extractFromChunkSorted(
            TimestampedSingleChunk &chunk,
            uint64_t startTime_pico,
            uint64_t endTime_pico,
            std::vector<openpni::basic::GlobalSingle_t> &output)
        {
            if (chunk.singles.empty())
                return;

            // 使用二分查找定位时间范围
            // lower_bound: 第一个 >= startTime 的位置
            auto rangeBegin = std::lower_bound(
                chunk.singles.begin(), chunk.singles.end(), startTime_pico,
                [](const openpni::basic::GlobalSingle_t &s, uint64_t t)
                {
                    return s.timeValue_pico < t;
                });

            // upper_bound: 第一个 > endTime 的位置
            auto rangeEnd = std::upper_bound(
                rangeBegin, chunk.singles.end(), endTime_pico,
                [](uint64_t t, const openpni::basic::GlobalSingle_t &s)
                {
                    return t < s.timeValue_pico;
                });

            // 移动范围内的数据到输出
            if (rangeBegin != rangeEnd)
            {
                output.insert(output.end(),
                              std::make_move_iterator(rangeBegin),
                              std::make_move_iterator(rangeEnd));
            }

            // 从块中移除已提取的数据
            // 保留 rangeEnd 之后的数据（时间 > endTime，留待下次处理）
            // 移除 rangeBegin 之前和 [rangeBegin, rangeEnd) 的数据
            chunk.singles.erase(chunk.singles.begin(), rangeEnd);
            chunk.updateTimeRange();
        }

        /**
         * @brief 执行符合计算
         */
        void processCoincidence(const std::vector<openpni::basic::GlobalSingle_t> &singles)
        {
            if (singles.empty())
                return;

            // 转换为 LocalSingle 格式
            std::vector<openpni::experimental::interface::LocalSingle> localSingles(singles.size());
            const uint32_t cpc = m_config.crystalsPerChannel;

            openpni::experimental::tools::parallel_for_each(
                singles.size(),
                [&](size_t i)
                {
                    const auto &g = singles[i];
                    auto &l = localSingles[i];
                    l.channelIndex = g.globalCrystalIndex / cpc;
                    l.crystalIndex = g.globalCrystalIndex % cpc;
                    l.energy = g.energy;
                    l.timevalue_pico = g.timeValue_pico;
                });

            try
            {
                // 使用 cuda_sync_ptr 管理设备内存，自动处理分配和释放
                auto d_singles = openpni::make_cuda_sync_ptr_from_hcopy(
                    std::span<const openpni::experimental::interface::LocalSingle>(localSingles),
                    "StreamingTimeAligner_singles");

                std::vector<std::span<openpni::experimental::interface::LocalSingle const>> inputList;
                inputList.push_back(d_singles.cspan());

                auto [prompt, delay] = m_coinNode.getDListmode(inputList, m_config.coinProtocol);

                // 保存结果
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
                std::cerr << "[StreamingTimeAligner] Coincidence error: "
                          << e.what() << std::endl;
            }
            // cuda_sync_ptr 在离开作用域时自动释放设备内存
        }

        /**
         * @brief 保存符合结果
         */
        void saveCoincidenceResult(
            openpni::io::listmode::ListmodeFileOutput &output,
            std::span<openpni::experimental::node::LocalListmode const> coins)
        {
            if (coins.empty())
                return;

            // 使用 cuda_sync_ptr 的 allocator 进行 GPU -> Host 拷贝
            std::vector<openpni::experimental::node::LocalListmode> hostBuf(coins.size());
            openpni::basic::cuda_ptr::cuda_ptr_allocator<openpni::basic::cuda_ptr::CudaPtrType::sync> allocator;
            allocator.copy_from_device_to_host(hostBuf.data(), coins);

            // 转换格式
            std::vector<openpni::basic::Listmode_t> listmodeData(coins.size());
            const uint32_t cpc = m_config.crystalsPerChannel;

            for (size_t i = 0; i < hostBuf.size(); ++i)
            {
                const auto &loc = hostBuf[i];
                auto &glob = listmodeData[i];
                glob.globalCrystalIndex1 = (uint32_t)loc.channelIndex1 * cpc + loc.crystalIndex1;
                glob.globalCrystalIndex2 = (uint32_t)loc.channelIndex2 * cpc + loc.crystalIndex2;
                glob.time1_2pico = static_cast<int16_t>(loc.time1_2pico);
            }

            std::lock_guard<std::mutex> lock(m_outputMutex);
            output.appendSegment(listmodeData.data(), listmodeData.size(), 0, 0);
        }

        /**
         * @brief 处理所有剩余数据
         */
        void flushRemaining()
        {
            std::cout << "[StreamingTimeAligner] Flushing remaining data..." << std::endl;

            // 收集各节点剩余数据（保持各自有序）
            std::vector<std::vector<openpni::basic::GlobalSingle_t>> nodeDataList;
            nodeDataList.reserve(m_nodeBuffers.size());

            for (size_t nodeId = 0; nodeId < m_nodeBuffers.size(); ++nodeId)
            {
                auto &buf = m_nodeBuffers[nodeId];
                auto &partial = m_partialChunks[nodeId];
                std::vector<openpni::basic::GlobalSingle_t> nodeSingles;

                // 部分块中的数据
                if (partial.has_value() && !partial->singles.empty())
                {
                    nodeSingles.insert(nodeSingles.end(),
                                       std::make_move_iterator(partial->singles.begin()),
                                       std::make_move_iterator(partial->singles.end()));
                    partial.reset();
                }

                // 缓冲区中的数据
                while (auto chunk = buf->tryPop())
                {
                    nodeSingles.insert(nodeSingles.end(),
                                       std::make_move_iterator(chunk->singles.begin()),
                                       std::make_move_iterator(chunk->singles.end()));
                }

                if (!nodeSingles.empty())
                {
                    nodeDataList.push_back(std::move(nodeSingles));
                }
            }

            if (!nodeDataList.empty())
            {
                // k-way 归并
                std::vector<openpni::basic::GlobalSingle_t> remaining;
                kWayMergeSorted(nodeDataList, remaining);

                std::cout << "[StreamingTimeAligner] Processing " << remaining.size()
                          << " remaining singles..." << std::endl;

                // 符合计算（数据已有序）
                processCoincidence(remaining);

                m_stats.totalSinglesProcessed += remaining.size();
            }

            std::cout << "[StreamingTimeAligner] Flush complete" << std::endl;
        }

        // 配置
        TimeAlignerConfig m_config;
        size_t m_nodeCount;

        // 节点缓冲区
        std::vector<std::unique_ptr<NodeRingBuffer>> m_nodeBuffers;
        std::unordered_map<size_t, std::optional<TimestampedSingleChunk>> m_partialChunks;

        // 符合处理
        openpni::experimental::node::Coincidence m_coinNode;

        // 输出
        std::unique_ptr<openpni::io::listmode::ListmodeFileOutput> m_promptWriter;
        std::unique_ptr<openpni::io::listmode::ListmodeFileOutput> m_delayWriter;
        std::mutex m_outputMutex;

        // 处理线程
        std::thread m_processorThread;
        std::atomic<bool> m_running{false};

        // 统计
        ProcessingStatistics m_stats;
    };

    // ==================== 工厂函数 ====================

    /**
     * @brief 创建 BDM2 探测器的时间对齐器配置
     */
    inline TimeAlignerConfig createBDM2AlignerConfig(
        const std::string &outputDir,
        const openpni::experimental::node::CoincidenceProtocol &coinProtocol = {})
    {
        TimeAlignerConfig config;
        config.outputDir = outputDir;
        config.channelNum = 48;
        config.crystalsPerChannel = 169 * 4; // 13x13 * 4 arrays
        config.coinProtocol = coinProtocol;
        return config;
    }

    /**
     * @brief 创建 BDMBiD 探测器的时间对齐器配置
     */
    inline TimeAlignerConfig createBDMBiDAlignerConfig(
        const std::string &outputDir,
        const openpni::experimental::node::CoincidenceProtocol &coinProtocol = {})
    {
        TimeAlignerConfig config;
        config.outputDir = outputDir;
        config.channelNum = 4;
        config.crystalsPerChannel = 400 * 8; // 20x20 * 8 arrays
        config.coinProtocol = coinProtocol;
        return config;
    }

} // namespace openpni::distributed::streaming
