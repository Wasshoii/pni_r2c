#pragma once

#include <pni/io/IO.hpp>
#include <pni/io/ListmodeIO.hpp>
#include <pni/experimental/node/Coincidence.hpp>
#include <pni/experimental/tools/Parallel.hpp>
#include <pni/CudaPtr.hpp>

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

namespace openpni::distributed::streaming
{

    namespace fs = std::filesystem;

    // ==================== 数据结构 ====================

    /**
     * @brief 带时间戳的单事件数据块
     *
     * 来自 R2S 节点的 SingleSegmentHeader 包含: {count, clock, duration}
     * - count: 段内事件数
     * - clock: 段开始时的计算机时钟（ms）
     * - duration: 段持续时间（ms）
     */
    struct TimestampedSingleChunk
    {
        uint16_t nodeId = 0;                                 // 来源节点ID
        uint64_t chunkId = 0;                                // 块序号（用于检测丢失/乱序）
        uint64_t computerClock_ms = 0;                       // 计算机时钟（毫秒）
        uint32_t duration_ms = 0;                            // 数据块持续时间（毫秒）
        std::vector<openpni::basic::GlobalSingle_t> singles; // 单事件数据

        // 缓存的 PET 时间范围（从 singles 中提取）
        uint64_t minTime_pico = UINT64_MAX; // 块内最小 PET 时间（pico）
        uint64_t maxTime_pico = 0;          // 块内最大 PET 时间（pico）

        /**
         * @brief 更新时间范围缓存
         */
        void updateTimeRange()
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
                minTime_pico = std::min(minTime_pico, s.timeValue_pico);
                maxTime_pico = std::max(maxTime_pico, s.timeValue_pico);
            }
        }

        // 用于按 PET 时间排序（处理网络乱序）
        bool operator<(const TimestampedSingleChunk &other) const
        {
            return minTime_pico < other.minTime_pico;
        }
    };

    // ==================== 节点缓冲区 ====================

    /**
     * @brief 单节点数据缓冲区
     *
     * 特性：
     * - 线程安全的生产者-消费者模式
     * - 按 minTime_pico 排序插入（处理网络乱序）
     * - 同一节点的数据段不重叠，按时间顺序排列
     */
    class NodeRingBuffer
    {
    public:
        explicit NodeRingBuffer(uint16_t nodeId, size_t maxChunks = 100)
            : m_nodeId(nodeId), m_maxChunks(maxChunks) {}

        /**
         * @brief 生产者：接收来自节点的数据块
         *
         * 处理网络乱序：按 minTime_pico 插入到正确位置
         *
         * @param chunk 数据块（移动语义）
         * @return 成功返回true，缓冲区关闭返回false
         */
        bool push(TimestampedSingleChunk &&chunk)
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            // 等待空间可用
            m_cvNotFull.wait(lock, [this]
                             { return m_buffer.size() < m_maxChunks || m_closed; });

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

            // 乱序处理：按 minTime_pico 插入到正确位置
            // 由于同节点数据段不重叠，通常只需要检查末尾几个元素
            if (m_buffer.empty() || chunk.minTime_pico >= m_buffer.back().minTime_pico)
            {
                // 最常见情况：按顺序到达，直接追加
                m_buffer.push_back(std::move(chunk));
            }
            else
            {
                // 乱序到达：二分查找正确位置并插入
                auto it = std::lower_bound(m_buffer.begin(), m_buffer.end(), chunk);
                m_buffer.insert(it, std::move(chunk));
                m_reorderedCount++;
            }

            m_cvNotEmpty.notify_one();
            return true;
        }

        /**
         * @brief 消费者：获取最早的数据块（不移除）
         */
        const TimestampedSingleChunk *front() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_buffer.empty() ? nullptr : &m_buffer.front();
        }

        /**
         * @brief 消费者：弹出最早的数据块
         */
        std::optional<TimestampedSingleChunk> pop()
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            m_cvNotEmpty.wait(lock, [this]
                              { return !m_buffer.empty() || m_closed; });

            if (m_buffer.empty())
                return std::nullopt;

            auto chunk = std::move(m_buffer.front());
            m_buffer.pop_front();
            m_cvNotFull.notify_one();
            return chunk;
        }

        /**
         * @brief 非阻塞尝试获取数据块
         */
        std::optional<TimestampedSingleChunk> tryPop()
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_buffer.empty())
                return std::nullopt;

            auto chunk = std::move(m_buffer.front());
            m_buffer.pop_front();
            m_cvNotFull.notify_one();
            return chunk;
        }

        /**
         * @brief 提取所有 maxTime_pico <= boundary 的完整段
         */
        std::vector<TimestampedSingleChunk> extractCompleteBefore(uint64_t boundary)
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            std::vector<TimestampedSingleChunk> result;

            while (!m_buffer.empty() && m_buffer.front().maxTime_pico <= boundary)
            {
                result.push_back(std::move(m_buffer.front()));
                m_buffer.pop_front();
            }

            if (!result.empty())
            {
                m_cvNotFull.notify_one();
            }
            return result;
        }

        /**
         * @brief 获取队首段的 minTime_pico
         */
        uint64_t getFrontMinTime() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_buffer.empty() ? UINT64_MAX : m_buffer.front().minTime_pico;
        }

        /**
         * @brief 获取统计信息
         */
        size_t getOutOfOrderCount() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_outOfOrderCount;
        }

        size_t getReorderedCount() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_reorderedCount;
        }

        bool empty() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_buffer.empty();
        }

        size_t size() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_buffer.size();
        }

        uint16_t nodeId() const { return m_nodeId; }

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
        mutable std::mutex m_mutex;
        std::condition_variable m_cvNotEmpty;
        std::condition_variable m_cvNotFull;

        // 使用 deque 支持按时间顺序插入（处理乱序）
        std::deque<TimestampedSingleChunk> m_buffer;

        bool m_closed = false;
        uint64_t m_expectedChunkId = 0;
        size_t m_outOfOrderCount = 0; // chunkId 乱序计数
        size_t m_reorderedCount = 0;  // 实际重排序次数
    };

    /**
     * @brief 时间对齐器配置
     */
    struct TimeAlignerConfig
    {
        // 安全边距配置
        uint64_t safetyMargin_pico = 10'000'000'000; // 10ms 安全边距

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
        std::atomic<uint64_t> chunksProcessed{0};
        std::atomic<double> avgProcessingTime_ms{0};
        std::atomic<uint64_t> currentTimeBoundary_pico{0};

        void reset()
        {
            totalSinglesReceived = 0;
            totalSinglesProcessed = 0;
            totalPromptPairs = 0;
            totalDelayPairs = 0;
            chunksProcessed = 0;
            avgProcessingTime_ms = 0;
            currentTimeBoundary_pico = 0;
        }
    };

    // ==================== 流式时间对齐器 ====================

    /**
     * @brief 流式时间对齐器
     *
     * 核心组件：收集各节点数据，按 PET 时钟进行时间对齐
     *
     * 简化工作流程：
     * 1. 各节点通过 getNodeBuffer() 获取缓冲区并推送数据
     * 2. 计算全局安全时间边界 = min(各节点队首段的 minTime_pico) - safetyMargin
     * 3. 从各节点提取 maxTime_pico <= 安全边界 的完整段（不拆分）
     * 4. 直接拼接数据（不预排序，符合计算内部已包含排序）
     * 5. 执行符合计算
     */
    class StreamingTimeAligner
    {
    public:
        StreamingTimeAligner(const TimeAlignerConfig &config, size_t nodeCount)
            : m_config(config), m_nodeCount(nodeCount)
        {
            // 为每个节点创建缓冲区
            for (size_t i = 0; i < nodeCount; ++i)
            {
                m_nodeBuffers.push_back(
                    std::make_unique<NodeRingBuffer>(i, config.maxChunksPerNode));
            }

            // 初始化符合处理器
            std::vector<uint32_t> crystalNumOfEachChannel(
                config.channelNum, config.crystalsPerChannel);
            m_coinNode.setTotalCrystalNumOfEachChannel(crystalNumOfEachChannel);

            std::cout << "[StreamingTimeAligner] Initialized with " << nodeCount
                      << " nodes, " << config.channelNum << " channels"
                      << std::endl;
        }

        ~StreamingTimeAligner()
        {
            stop();
        }

        /**
         * @brief 获取节点缓冲区（用于数据接收）
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

            initializeOutput();
            m_processorThread = std::thread([this]
                                            { processingLoop(); });
            std::cout << "[StreamingTimeAligner] Started" << std::endl;
        }

        /**
         * @brief 停止处理
         */
        void stop(bool waitForCompletion = true)
        {
            if (!m_running.exchange(false))
                return;

            for (auto &buf : m_nodeBuffers)
            {
                buf->close();
            }

            if (m_processorThread.joinable())
            {
                m_processorThread.join();
            }

            finalizeOutput();
            std::cout << "[StreamingTimeAligner] Stopped" << std::endl;
        }

        const ProcessingStatistics &getStatistics() const { return m_stats; }
        bool isRunning() const { return m_running.load(); }
        size_t getNodeCount() const { return m_nodeCount; }

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
            }

            if (m_config.saveDelay)
            {
                m_delayWriter = std::make_unique<openpni::io::listmode::ListmodeFileOutput>();
                m_delayWriter->setBytes4CrystalIndex(openpni::io::single::CrystalIndexType::UINT32);
                m_delayWriter->setBytes4TimeValue1_2(openpni::io::listmode::TimeValue1_2Type::INT16);
                m_delayWriter->setTotalCrystalNum(totalCrystals);
                m_delayWriter->open(m_config.outputDir + "/delay.lmf");
            }
        }

        /**
         * @brief 关闭输出文件
         */
        void finalizeOutput()
        {
            std::lock_guard<std::mutex> lock(m_outputMutex);
            m_promptWriter.reset();
            m_delayWriter.reset();
        }

        /**
         * @brief 计算全局安全时间边界
         *
         * 安全边界 = min(各节点队首段的 minTime_pico) - safetyMargin
         * 只提取 maxTime_pico <= 安全边界 的完整段
         */
        uint64_t calculateSafeTimeBoundary() const
        {
            uint64_t globalMinTime = UINT64_MAX;
            size_t nodesWithData = 0;

            for (const auto &buf : m_nodeBuffers)
            {
                uint64_t frontMinTime = buf->getFrontMinTime();
                if (frontMinTime != UINT64_MAX)
                {
                    globalMinTime = std::min(globalMinTime, frontMinTime);
                    nodesWithData++;
                }
            }

            // 如果没有数据或不是所有节点都有数据，返回0
            if (nodesWithData == 0 || nodesWithData < m_nodeCount)
            {
                return 0;
            }

            // 安全边界 = 全局最小时间 - 安全边距
            if (globalMinTime > m_config.safetyMargin_pico)
            {
                return globalMinTime - m_config.safetyMargin_pico;
            }
            return 0;
        }

        /**
         * @brief 核心处理循环（简化版）
         *
         * 简化策略：
         * 1. 计算安全边界 = min(各节点队首段的 minTime_pico) - safetyMargin
         * 2. 提取所有 maxTime_pico <= 安全边界 的完整段
         * 3. 直接拼接（不预排序），符合计算内部处理排序
         */
        void processingLoop()
        {
            size_t consecutiveEmptyRounds = 0;
            const size_t maxEmptyRounds = 100;

            while (m_running.load())
            {
                auto startTime = std::chrono::high_resolution_clock::now();

                // 1. 计算安全边界
                uint64_t safeTimeBoundary = calculateSafeTimeBoundary();

                if (safeTimeBoundary == 0)
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

                // 2. 从各节点提取完整段并直接拼接
                std::vector<openpni::basic::GlobalSingle_t> allSingles;
                size_t chunksExtracted = 0;

                for (auto &buf : m_nodeBuffers)
                {
                    auto chunks = buf->extractCompleteBefore(safeTimeBoundary);
                    for (auto &chunk : chunks)
                    {
                        // 直接追加，无需排序（符合计算内部处理）
                        allSingles.insert(allSingles.end(),
                                          std::make_move_iterator(chunk.singles.begin()),
                                          std::make_move_iterator(chunk.singles.end()));
                        chunksExtracted++;
                    }
                }

                // 3. 执行符合计算
                if (!allSingles.empty())
                {
                    processCoincidence(allSingles);

                    m_stats.totalSinglesProcessed += allSingles.size();
                    m_stats.chunksProcessed += chunksExtracted;
                }

                m_stats.currentTimeBoundary_pico = safeTimeBoundary;

                // 计算处理时间
                auto endTime = std::chrono::high_resolution_clock::now();
                double elapsed_ms = std::chrono::duration<double, std::milli>(
                                        endTime - startTime)
                                        .count();
                double currentAvg = m_stats.avgProcessingTime_ms.load();
                m_stats.avgProcessingTime_ms = currentAvg * 0.9 + elapsed_ms * 0.1;
            }

            // 处理剩余数据
            flushRemaining();
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
                // 使用 cuda_sync_ptr 管理设备内存
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

            // GPU -> Host 拷贝
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

            std::vector<openpni::basic::GlobalSingle_t> remaining;

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
                std::cout << "[StreamingTimeAligner] Processing " << remaining.size()
                          << " remaining singles..." << std::endl;
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
        config.crystalsPerChannel = 169 * 4;
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
        config.crystalsPerChannel = 400 * 8;
        config.coinProtocol = coinProtocol;
        return config;
    }

} // namespace openpni::distributed::streaming
