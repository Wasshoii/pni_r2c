#pragma once

#include <pni/io/IO.hpp>
#include <pni/node/Coincidence.hpp>
#include <pni/tools/Parallel.hpp>
#include <pni/CudaPtr.hpp>
#include <pni/tools/UniPtr.hpp>
#include <pni/io/v1/PetDataType_v1.h>

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
    using GlobalSingle = openpni::v1::basic::GlobalSingle_t;
    using Single = openpni::Single;
    namespace fs = std::filesystem;

    // ==================== 共享内存池 ====================

    /**
     * @brief 共享内存池
     *
     * 用于限制所有节点缓冲区的总内存使用量。
     * 当内存超限时，生产者会被阻塞直到有足够空间。
     */
    class SharedMemoryPool
    {
    public:
        /**
         * @brief 内存状态信息
         */
        struct MemoryStatus
        {
            size_t usedBytes;
            size_t maxBytes;
            double usageRatio;
        };

        /**
         * @brief 构造函数
         * @param maxMemoryBytes 最大内存限制（字节），默认 1GB
         */
        explicit SharedMemoryPool(size_t maxMemoryBytes = 1ULL * 1024 * 1024 * 1024)
            : m_maxMemoryBytes(maxMemoryBytes), m_usedMemoryBytes(0) {}

        /**
         * @brief 尝试分配内存
         * @param bytes 请求的字节数
         * @param timeoutMs 超时时间（毫秒），0 表示无限等待
         * @return 分配成功返回 true
         */
        bool tryAllocate(size_t bytes, uint32_t timeoutMs = 0)
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
                    return false; // 超时
                }
            }

            if (m_closed)
                return false;

            m_usedMemoryBytes += bytes;
            m_peakMemoryBytes = std::max(m_peakMemoryBytes, m_usedMemoryBytes);
            m_totalAllocations++;
            return true;
        }

        /**
         * @brief 释放内存
         * @param bytes 释放的字节数
         */
        void release(size_t bytes)
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (bytes > m_usedMemoryBytes)
            {
                std::cerr << "[SharedMemoryPool] Warning: releasing more than allocated ("
                          << bytes << " > " << m_usedMemoryBytes << ")" << std::endl;
                m_usedMemoryBytes = 0;
            }
            else
            {
                m_usedMemoryBytes -= bytes;
            }
            m_cvAvailable.notify_all();
        }

        /**
         * @brief 获取当前使用的内存
         */
        size_t getUsedMemory() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_usedMemoryBytes;
        }

        /**
         * @brief 获取峰值内存使用量
         */
        size_t getPeakMemory() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_peakMemoryBytes;
        }

        /**
         * @brief 获取最大内存限制
         */
        size_t getMaxMemory() const { return m_maxMemoryBytes; }

        /**
         * @brief 获取内存使用率 (0.0 - 1.0)
         */
        double getUsageRatio() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return static_cast<double>(m_usedMemoryBytes) / m_maxMemoryBytes;
        }

        /**
         * @brief 获取总分配次数
         */
        size_t getTotalAllocations() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_totalAllocations;
        }

        /**
         * @brief 关闭内存池，唤醒所有等待线程
         */
        void close()
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_closed = true;
            m_cvAvailable.notify_all();
        }

        /**
         * @brief 获取内存状态信息
         */
        MemoryStatus getStatus() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return MemoryStatus{
                m_usedMemoryBytes,
                m_maxMemoryBytes,
                static_cast<double>(m_usedMemoryBytes) / m_maxMemoryBytes};
        }

        /**
         * @brief 打印内存池状态
         */
        void printStatus() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            std::cout << "[SharedMemoryPool] Status:\n"
                      << "  Max memory:   " << (m_maxMemoryBytes / 1024.0 / 1024.0) << " MB\n"
                      << "  Used memory:  " << (m_usedMemoryBytes / 1024.0 / 1024.0) << " MB ("
                      << (100.0 * m_usedMemoryBytes / m_maxMemoryBytes) << "%)\n"
                      << "  Peak memory:  " << (m_peakMemoryBytes / 1024.0 / 1024.0) << " MB\n"
                      << "  Allocations:  " << m_totalAllocations << "\n";
        }

    private:
        size_t m_maxMemoryBytes;
        size_t m_usedMemoryBytes;
        size_t m_peakMemoryBytes = 0;
        size_t m_totalAllocations = 0;
        bool m_closed = false;
        mutable std::mutex m_mutex;
        std::condition_variable m_cvAvailable;
    };

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
        uint16_t nodeId = 0;               // 来源节点ID
        uint64_t chunkId = 0;              // 块序号（用于检测丢失/乱序）
        uint64_t computerClock_ms = 0;     // 计算机时钟（毫秒）
        uint32_t duration_ms = 0;          // 数据块持续时间（毫秒）
        std::vector<GlobalSingle> singles; // 单事件数据

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

        /**
         * @brief 计算此 chunk 占用的内存大小（字节）
         */
        size_t memorySize() const
        {
            // 固定字段 + vector 容量 * 元素大小
            return sizeof(TimestampedSingleChunk) +
                   singles.capacity() * sizeof(GlobalSingle);
        }

        /**
         * @brief 计算 singles 数据占用的内存大小（字节）
         */
        size_t singlesMemorySize() const
        {
            return singles.capacity() * sizeof(GlobalSingle);
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
     * - 支持共享内存池进行全局内存限制
     */
    class NodeRingBuffer
    {
    public:
        /**
         * @brief 构造函数
         * @param nodeId 节点ID
         * @param maxChunks 最大 chunk 数量限制
         * @param memoryPool 共享内存池（可选，nullptr 表示不限制内存）
         */
        explicit NodeRingBuffer(uint16_t nodeId, size_t maxChunks = 100,
                                SharedMemoryPool *memoryPool = nullptr)
            : m_nodeId(nodeId), m_maxChunks(maxChunks), m_memoryPool(memoryPool) {}

        /**
         * @brief 生产者：接收来自节点的数据块
         *
         * 处理网络乱序：按 minTime_pico 插入到正确位置
         *
         * @param chunk 数据块（移动语义）
         * @param timeoutMs 超时时间（毫秒），0 表示无限等待
         * @return 成功返回true，缓冲区关闭或内存不足返回false
         */
        bool push(TimestampedSingleChunk &&chunk, uint32_t timeoutMs = 0)
        {
            // 计算此 chunk 占用的内存
            size_t chunkMemory = chunk.memorySize();

            // 如果有内存池，先申请内存配额
            if (m_memoryPool)
            {
                if (!m_memoryPool->tryAllocate(chunkMemory, timeoutMs))
                {
                    std::cerr << "[NodeRingBuffer] Node " << m_nodeId
                              << " failed to allocate memory for chunk "
                              << chunk.chunkId << std::endl;
                    return false; // 内存池关闭或分配失败
                }
            }

            std::unique_lock<std::mutex> lock(m_mutex);

            // 等待空间可用（chunk 数量限制）
            auto canPush = [this]
            { return m_buffer.size() < m_maxChunks || m_closed; };

            if (timeoutMs == 0)
            {
                m_cvNotFull.wait(lock, canPush);
            }
            else
            {
                if (!m_cvNotFull.wait_for(lock, std::chrono::milliseconds(timeoutMs), canPush))
                {
                    // 超时，归还内存配额
                    if (m_memoryPool)
                    {
                        m_memoryPool->release(chunkMemory);
                    }
                    std::cerr << "[NodeRingBuffer] Node " << m_nodeId
                              << " push timeout for chunk " << chunk.chunkId << std::endl;
                    return false;
                }
            }

            if (m_closed)
            {
                // 归还内存配额
                if (m_memoryPool)
                {
                    m_memoryPool->release(chunkMemory);
                }
                return false;
            }

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

            // 水位线追踪：记录已收到数据的最大事件时间
            m_maxEventTimeReceived = std::max(m_maxEventTimeReceived, chunk.maxTime_pico);

            // 记录此 chunk 的内存占用（用于释放时计算）
            m_bufferMemoryBytes += chunkMemory;

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
            size_t chunkMemory = chunk.memorySize();
            m_buffer.pop_front();
            m_bufferMemoryBytes -= chunkMemory;

            // 释放内存池配额
            if (m_memoryPool)
            {
                lock.unlock(); // 释放锁后再操作内存池，避免死锁
                m_memoryPool->release(chunkMemory);
            }

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
            size_t chunkMemory = chunk.memorySize();
            m_buffer.pop_front();
            m_bufferMemoryBytes -= chunkMemory;

            // 释放内存池配额（注意：这里持有锁，但 release 不会死锁）
            if (m_memoryPool)
            {
                m_memoryPool->release(chunkMemory);
            }

            m_cvNotFull.notify_one();
            return chunk;
        }

        /**
         * @brief 提取所有 timeValue_pico <= boundary 的事件（精确时间边界）
         *
         * 与完整段提取不同，此方法会拆分跨越边界的段：
         * - 完全在边界内的段（maxTime <= boundary）：整个提取
         * - 跨越边界的段（minTime <= boundary < maxTime）：只提取边界内的事件
         * - 完全在边界外的段（minTime > boundary）：不提取
         *
         * @param boundary 时间边界（pico）
         * @return 提取的单事件数据
         */
        std::vector<GlobalSingle> extractSinglesBefore(uint64_t boundary)
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            std::vector<GlobalSingle> result;
            size_t releasedMemory = 0;

            while (!m_buffer.empty())
            {
                auto &frontChunk = m_buffer.front();

                if (frontChunk.maxTime_pico <= boundary)
                {
                    // 情况1：整个段都在边界内，全部提取
                    size_t chunkMemory = frontChunk.memorySize();
                    result.insert(result.end(),
                                  std::make_move_iterator(frontChunk.singles.begin()),
                                  std::make_move_iterator(frontChunk.singles.end()));
                    m_buffer.pop_front();

                    // 安全地减少内存计数
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
                    // 情况2：段跨越边界，需要拆分
                    // 段内数据已按 timeValue_pico 排序，使用二分查找 O(log n)
                    // upper_bound 找到第一个 > boundary 的位置
                    auto splitPoint = std::upper_bound(
                        frontChunk.singles.begin(),
                        frontChunk.singles.end(),
                        boundary,
                        [](uint64_t bound, const GlobalSingle &s)
                        {
                            return bound < s.timeValue_pico;
                        });

                    // 提取边界内的事件 [begin, splitPoint)
                    if (splitPoint != frontChunk.singles.begin())
                    {
                        result.insert(result.end(),
                                      std::make_move_iterator(frontChunk.singles.begin()),
                                      std::make_move_iterator(splitPoint));

                        // 移除已提取的事件
                        frontChunk.singles.erase(frontChunk.singles.begin(), splitPoint);

                        // 更新时间范围
                        frontChunk.updateTimeRange();

                        // 注意：部分提取时，vector 内存并未真正释放
                        // 不在这里调整 m_bufferMemoryBytes，等段完全移除时再处理
                    }

                    // 如果段为空，移除
                    if (frontChunk.singles.empty())
                    {
                        size_t remainingMemory = frontChunk.memorySize();
                        m_buffer.pop_front();

                        // 安全地减少内存计数
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

                    // 跨越边界的段处理完后停止（后续段必然在边界外）
                    break;
                }
                else
                {
                    // 情况3：整个段都在边界外（minTime > boundary），停止
                    break;
                }
            }

            // 释放内存池配额
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

        /**
         * @brief 提取所有 maxTime_pico <= boundary 的完整段
         */
        std::vector<TimestampedSingleChunk> extractCompleteBefore(uint64_t boundary)
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

            // 释放内存池配额
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

        /**
         * @brief 获取队首段的 minTime_pico
         */
        uint64_t getFrontMinTime() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_buffer.empty() ? UINT64_MAX : m_buffer.front().minTime_pico;
        }

        /**
         * @brief 获取已收到数据的最大事件时间（水位线追踪）
         *
         * 这代表该节点"已报告到的时间点"，用于计算全局水位线
         */
        uint64_t getMaxEventTime() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_maxEventTimeReceived;
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

        /**
         * @brief 获取此缓冲区当前占用的内存（字节）
         */
        size_t getBufferMemoryBytes() const
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_bufferMemoryBytes;
        }

    private:
        uint16_t m_nodeId;
        size_t m_maxChunks;
        SharedMemoryPool *m_memoryPool; // 共享内存池（可选）
        mutable std::mutex m_mutex;
        std::condition_variable m_cvNotEmpty;
        std::condition_variable m_cvNotFull;

        // 使用 deque 支持按时间顺序插入（处理乱序）
        std::deque<TimestampedSingleChunk> m_buffer;

        bool m_closed = false;
        uint64_t m_expectedChunkId = 0;
        size_t m_outOfOrderCount = 0;        // chunkId 乱序计数
        size_t m_reorderedCount = 0;         // 实际重排序次数
        uint64_t m_maxEventTimeReceived = 0; // 已收到数据的最大事件时间（水位线追踪）
        size_t m_bufferMemoryBytes = 0;      // 此缓冲区当前占用的内存
    };

    /**
     * @brief 时间对齐器配置
     */
    struct TimeAlignerConfig
    {
        // ==================== 水位线与安全裕量配置 ====================
        /**
         * @brief 网络延迟安全裕量（pico）
         *
         * 考虑因素：
         * - 网络传输延迟
         * - 各节点处理延迟不一致
         * - 数据块不是严格按时间顺序到达
         *
         * 例：节点A在 T=100μs 发送时间戳为 T=95μs 的事件，
         * 由于网络延迟，可能在其他节点已报告 T=98μs 后才到达。
         */
        uint64_t networkLatencyMargin_pico = 5'000'000'000; // 5ms 网络延迟裕量

        // 符合处理配置
        openpni::CoincidenceProtocol coinProtocol;
        uint16_t channelNum = 0;
        uint32_t crystalsPerChannel = 0;

        // 输出配置
        std::string outputDir;
        bool savePrompt = true;
        bool saveDelay = true;

        // 性能配置
        size_t maxChunksPerNode = 100;       // 每节点最大缓冲块数
        uint32_t processingIntervalMs = 200; // 处理循环间隔，pni采集设置中每次读出数据大约为100ms，这里设置为200ms以平衡延迟和效率

        // 内存池配置
        size_t maxTotalMemoryBytes = 2ULL * 1024 * 1024 * 1024; // 2GB 默认最大内存
        bool useMemoryPool = true;                              // 是否启用内存池限制

        /**
         * @brief 计算总安全裕量
         *
         * 总安全裕量 = 网络延迟裕量 + 符合时间窗口 + 延迟符合窗口
         * 确保在水位线之前的所有可能形成符合对的事件都已到达
         */
        uint64_t getTotalSafetyMargin() const
        {
            // 符合时间窗口（ps -> pico）
            uint64_t coinWindow_pico = static_cast<uint64_t>(coinProtocol.timeWindow_ps);
            // 延迟符合窗口（ps -> pico）
            uint64_t delayWindow_pico = static_cast<uint64_t>(coinProtocol.delayTime_ps);

            // 总裕量 = 网络延迟 + max(符合窗口, 延迟窗口)
            // 延迟窗口通常更大，是主要考虑因素
            return networkLatencyMargin_pico + std::max(coinWindow_pico, delayWindow_pico);
        }
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
            // 创建共享内存池（如果启用）
            if (config.useMemoryPool)
            {
                m_memoryPool = std::make_unique<SharedMemoryPool>(config.maxTotalMemoryBytes);
                std::cout << "[StreamingTimeAligner] Memory pool enabled: "
                          << (config.maxTotalMemoryBytes / (1024 * 1024)) << " MB limit"
                          << std::endl;
            }

            // 为每个节点创建缓冲区
            SharedMemoryPool *poolPtr = m_memoryPool.get();
            for (size_t i = 0; i < nodeCount; ++i)
            {
                m_nodeBuffers.push_back(
                    std::make_unique<NodeRingBuffer>(i, config.maxChunksPerNode, poolPtr));
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

            // 关闭内存池（唤醒所有等待的生产者）
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
            std::cout << "[StreamingTimeAligner] Stopped" << std::endl;
        }

        const ProcessingStatistics &getStatistics() const { return m_stats; }
        bool isRunning() const { return m_running.load(); }
        size_t getNodeCount() const { return m_nodeCount; }

        /**
         * @brief 获取内存池使用状态
         */
        SharedMemoryPool::MemoryStatus getMemoryStatus() const
        {
            if (m_memoryPool)
            {
                return m_memoryPool->getStatus();
            }
            return SharedMemoryPool::MemoryStatus{0, 0, 0.0};
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
                m_promptWriter = std::make_unique<openpni::io::v1::listmode::ListmodeFileOutput>();
                m_promptWriter->setBytes4CrystalIndex(openpni::io::v1::single::CrystalIndexType::UINT32);
                m_promptWriter->setBytes4TimeValue1_2(openpni::io::v1::listmode::TimeValue1_2Type::INT16);
                m_promptWriter->setTotalCrystalNum(totalCrystals);
                m_promptWriter->open(m_config.outputDir + "/prompt.lmf");
            }

            if (m_config.saveDelay)
            {
                m_delayWriter = std::make_unique<openpni::io::v1::listmode::ListmodeFileOutput>();
                m_delayWriter->setBytes4CrystalIndex(openpni::io::v1::single::CrystalIndexType::UINT32);
                m_delayWriter->setBytes4TimeValue1_2(openpni::io::v1::listmode::TimeValue1_2Type::INT16);
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
         * @brief 计算全局水位线（Watermark）
         *
         * 水位线设计原理：
         * ```
         * 节点A的数据流:  ──●──●────●──●──────●───────→ 时间
         *                            ↑
         *                     A的最大事件时间 = 50μs
         *
         * 节点B的数据流: ──●────●──●──────●───────────→ 时间
         *                               ↑
         *                        B的最大事件时间 = 45μs
         *
         * 节点C的数据流: ──●──●──●────●────────────────→ 时间
         *                           ↑
         *                    C的最大事件时间 = 40μs
         *
         * 全局水位线 = min(50, 45, 40) - 安全裕量 = 40 - 5 = 35μs
         * 在35μs之前的所有数据都可以安全处理！
         * ```
         *
         * @return 水位线时间（pico），0 表示数据不足
         */
        uint64_t calculateWatermark() const
        {
            uint64_t globalMinMaxTime = UINT64_MAX;
            size_t nodesWithData = 0;

            for (const auto &buf : m_nodeBuffers)
            {
                uint64_t nodeMaxTime = buf->getMaxEventTime();
                if (nodeMaxTime > 0)
                {
                    // 取各节点"最大事件时间"的最小值
                    globalMinMaxTime = std::min(globalMinMaxTime, nodeMaxTime);
                    nodesWithData++;
                }
            }

            // 要求所有节点都有数据才能计算有效水位线
            // 否则可能遗漏某个节点的早期数据
            if (nodesWithData < m_nodeCount || globalMinMaxTime == UINT64_MAX)
            {
                return 0;
            }

            // 水位线 = 全局最小的"最大事件时间" - 总安全裕量
            uint64_t safetyMargin = m_config.getTotalSafetyMargin();

            if (globalMinMaxTime > safetyMargin)
            {
                return globalMinMaxTime - safetyMargin;
            }
            return 0;
        }

        /**
         * @brief 核心处理循环（水位线版）
         *
         * 水位线策略：
         * 1. 计算全局水位线 = min(各节点最大事件时间) - 安全裕量
         * 2. 提取所有 maxTime_pico <= 水位线 的完整段
         * 3. 直接拼接（不预排序），符合计算内部处理排序
         *
         */
        void processingLoop()
        {
            size_t consecutiveEmptyRounds = 0;
            const size_t maxEmptyRounds = 100;
            uint64_t lastWatermark = 0;

            while (m_running.load())
            {
                auto startTime = std::chrono::high_resolution_clock::now();

                // 1. 计算全局水位线
                uint64_t watermark = calculateWatermark();

                if (watermark == 0 || watermark <= lastWatermark)
                {
                    // 水位线未前进，等待更多数据
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

                // 2. 从各节点精确提取水位线之前的事件
                std::vector<GlobalSingle> allSingles;

                for (auto &buf : m_nodeBuffers)
                {
                    // 使用精确时间边界提取，会拆分跨越边界的段
                    auto singles = buf->extractSinglesBefore(watermark);
                    if (!singles.empty())
                    {
                        allSingles.insert(allSingles.end(),
                                          std::make_move_iterator(singles.begin()),
                                          std::make_move_iterator(singles.end()));
                    }
                }

                // 3. 执行符合计算
                if (!allSingles.empty())
                {
                    processCoincidence(allSingles);

                    m_stats.totalSinglesProcessed += allSingles.size();
                    m_stats.chunksProcessed++;
                }

                // 更新水位线记录
                lastWatermark = watermark;
                m_stats.currentTimeBoundary_pico = watermark;

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
        void processCoincidence(const std::vector<GlobalSingle> &singles)
        {
            if (singles.empty())
                return;

            // 转换为 LocalSingle 格式
            std::vector<Single> localSingles(singles.size());
            const uint32_t cpc = m_config.crystalsPerChannel;

            openpni::tools::parallel_for_each_CPU(
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
                // 统一使用 UniPtr 管理 host/device 双端数据
                m_singleBuffer.CopyFromHost(std::span<const Single>(localSingles));

                std::vector<std::span<Single const>> inputList;
                inputList.push_back(m_singleBuffer.CudaRSpan());

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
            openpni::io::v1::listmode::ListmodeFileOutput &output,
            std::span<Listmode const> coins)
        {
            if (coins.empty())
                return;

            // 统一使用 UniPtr 做 device -> host 同步
            m_coinBuffer.CopyFromCuda(coins);
            auto hostBuf = m_coinBuffer.HostRSpan();

            // 转换格式
            std::vector<openpni::v1::basic::Listmode_t> listmodeData(coins.size());
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

            std::vector<GlobalSingle> remaining;

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
        openpni::Coincidence m_coinNode;
        openpni::tools::UniPtr<Single> m_singleBuffer{"StreamingTimeAligner_singles"};
        openpni::tools::UniPtr<Listmode> m_coinBuffer{"StreamingTimeAligner_coins"};

        // 输出
        std::unique_ptr<openpni::io::v1::listmode::ListmodeFileOutput> m_promptWriter;
        std::unique_ptr<openpni::io::v1::listmode::ListmodeFileOutput> m_delayWriter;
        std::mutex m_outputMutex;

        // 处理线程
        std::thread m_processorThread;
        std::atomic<bool> m_running{false};

        // 内存池（所有节点共享）
        std::unique_ptr<SharedMemoryPool> m_memoryPool;

        // 统计
        ProcessingStatistics m_stats;
    };

    // ==================== 工厂函数 ====================

    /**
     * @brief 创建 BDM2 探测器的时间对齐器配置
     */
    inline TimeAlignerConfig createBDM2AlignerConfig(
        const std::string &outputDir,
        const openpni::CoincidenceProtocol &coinProtocol = {})
    {
        TimeAlignerConfig config;
        config.outputDir = outputDir;
        config.channelNum = 48;
        config.crystalsPerChannel = 169 * 4;
        config.coinProtocol = coinProtocol;
        return config;
    }

    // /**
    //  * @brief 创建 BDMBiD 探测器的时间对齐器配置
    //  */
    // inline TimeAlignerConfig createBDMBiDAlignerConfig(
    //     const std::string &outputDir,
    //     const openpni::CoincidenceProtocol &coinProtocol = {})
    // {
    //     TimeAlignerConfig config;
    //     config.outputDir = outputDir;
    //     config.channelNum = 4;
    //     config.crystalsPerChannel = 400 * 8;
    //     config.coinProtocol = coinProtocol;
    //     return config;
    // }

} // namespace openpni::distributed::streaming
