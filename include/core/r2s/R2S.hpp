#pragma once

#include <pni/PnI-Config.hpp>

#include <cstdint>
#include <pni/io/IO.hpp>
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
    inline bool isDevicePointer(const void *ptr)
    {
        cudaPointerAttributes attr;
        auto status = cudaPointerGetAttributes(&attr, ptr);
#if CUDART_VERSION >= 10000
        if (status == cudaSuccess && attr.type == cudaMemoryTypeDevice)
            return true;
#else
        if (status == cudaSuccess && attr.memoryType == cudaMemoryTypeDevice)
            return true;
#endif
        return false;
    }

    /**
     * @brief 将 R2S 输出 singles 物化到 host 侧内存
     *
     * 该函数用于异步处理链路：回调返回后原始 span 不再保证有效，
     * 因此需要在回调内完成 host 拷贝并持有数据所有权。
     */
    inline std::vector<Single> materializeSinglesOnHost(std::span<Single const> singles)
    {
        std::vector<Single> hostSingles;
        if (singles.empty())
        {
            return hostSingles;
        }

        hostSingles.resize(singles.size());
        const Single *dataPtr = singles.data();
        if (isDevicePointer(dataPtr))
        {
            cudaError_t err = cudaMemcpy(hostSingles.data(), dataPtr,
                                         singles.size() * sizeof(Single),
                                         cudaMemcpyDeviceToHost);
            if (err != cudaSuccess)
            {
                throw std::runtime_error("cudaMemcpyDeviceToHost failed: " +
                                         std::string(cudaGetErrorString(err)));
            }
            return hostSingles;
        }

        std::copy(singles.begin(), singles.end(), hostSingles.begin());
        return hostSingles;
    }

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
        explicit AsyncSingleFileWriter(size_t maxQueueSize = 100)
            : m_maxQueueSize(maxQueueSize)
        {
        }

        ~AsyncSingleFileWriter()
        {
            stop();
        }

        /**
         * @brief 打开文件并启动写入线程
         */
        bool open(const std::string &filePath, uint32_t totalCrystals)
        {
            m_output = std::make_unique<openpni::io::v1::single::SingleFileOutput>();
            m_output->setBytes4CrystalIndex(openpni::io::v1::single::CrystalIndexType::UINT32);
            m_output->setBytes4TimeValue(openpni::io::v1::single::TimeValueType::UINT64);
            m_output->setBytes4Energy(openpni::io::v1::single::EnergyType::FLT32);
            m_output->setTotalCrystalNum(totalCrystals);
            m_output->open(filePath);

            m_running = true;
            m_writerThread = std::thread([this]
                                         { writerLoop(); });

            std::cout << "[AsyncWriter] Started, queue size: " << m_maxQueueSize << std::endl;
            return true;
        }

        /**
         * @brief 异步提交写入任务
         *
         * @return true 成功提交，false 队列已满或已停止
         */
        bool submit(std::vector<GlobalSingle> &&singles, uint64_t clock_ms, uint32_t duration_ms)
        {
            if (!m_running.load())
            {
                return false;
            }

            std::unique_lock<std::mutex> lock(m_mutex);

            // 等待队列有空间
            m_cvNotFull.wait(lock, [this]
                             { return m_queue.size() < m_maxQueueSize || !m_running.load(); });

            if (!m_running.load())
            {
                return false;
            }

            m_queue.push({std::move(singles), clock_ms, duration_ms});
            m_pendingCount++;
            lock.unlock();
            m_cvNotEmpty.notify_one();

            return true;
        }

        /**
         * @brief 停止写入器，等待所有任务完成
         */
        void stop()
        {
            if (!m_running.exchange(false))
            {
                return;
            }

            m_cvNotEmpty.notify_all();
            m_cvNotFull.notify_all();

            if (m_writerThread.joinable())
            {
                m_writerThread.join();
            }

            std::cout << "[AsyncWriter] Stopped, total written: " << m_totalWritten.load()
                      << " singles" << std::endl;
        }

        /**
         * @brief 等待所有待处理任务完成
         */
        void flush()
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cvFlushed.wait(lock, [this]
                             { return m_queue.empty() || !m_running.load(); });
        }

        uint64_t getTotalWritten() const { return m_totalWritten.load(); }
        size_t getPendingCount() const { return m_pendingCount.load(); }
        bool isRunning() const { return m_running.load(); }

        /**
         * @brief 获取底层输出文件对象（用于同步写入模式）
         */
        openpni::io::v1::single::SingleFileOutput *getOutput() { return m_output.get(); }

    private:
        void writerLoop()
        {
            while (m_running.load() || !m_queue.empty())
            {
                AsyncWriteTask task;
                {
                    std::unique_lock<std::mutex> lock(m_mutex);
                    m_cvNotEmpty.wait(lock, [this]
                                      { return !m_queue.empty() || !m_running.load(); });

                    if (m_queue.empty())
                    {
                        continue;
                    }

                    task = std::move(m_queue.front());
                    m_queue.pop();
                    m_pendingCount--;
                }

                m_cvNotFull.notify_one();

                // 写入文件
                if (!task.singles.empty())
                {
                    bool success = m_output->appendSegment(
                        task.singles.data(),
                        task.singles.size(),
                        task.clock_ms,
                        task.duration_ms);

                    if (success)
                    {
                        m_totalWritten += task.singles.size();
                    }
                    else
                    {
                        std::cerr << "[AsyncWriter] Failed to write segment" << std::endl;
                    }
                }

                // 通知 flush 等待者
                if (m_queue.empty())
                {
                    m_cvFlushed.notify_all();
                }
            }
        }

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
        uint32_t crystalsPerChannel)
    {
        std::vector<GlobalSingle> globalSingles;
        globalSingles.reserve(singles.size());

        // 从 GPU 内存拷贝到 Host（如果需要）
        std::vector<Single> hostBuf;
        const Single *dataPtr = singles.data();

        if (isDevicePointer(dataPtr))
        {
            hostBuf.resize(singles.size());
            cudaError_t err = cudaMemcpy(hostBuf.data(), dataPtr,
                                         singles.size() * sizeof(Single),
                                         cudaMemcpyDeviceToHost);
            if (err != cudaSuccess)
            {
                throw std::runtime_error("cudaMemcpyDeviceToHost failed: " +
                                         std::string(cudaGetErrorString(err)));
            }
            dataPtr = hostBuf.data();
        }

        // 转换为 GlobalSingle_t
        for (size_t i = 0; i < singles.size(); i++)
        {
            GlobalSingle gs;
            gs.globalCrystalIndex = dataPtr[i].channelIndex * crystalsPerChannel + dataPtr[i].crystalIndex;
            gs.energy = dataPtr[i].energy;
            gs.timeValue_pico = dataPtr[i].timevalue_pico;
            globalSingles.push_back(gs);
        }

        return globalSingles;
    }

    /**
     * @brief 追加单事件数据到 Single 文件（标准格式）
     */
    bool appendSinglesToSingleFile(
        openpni::io::v1::single::SingleFileOutput &outputFile,
        std::span<Single const> singles,
        uint32_t crystalsPerChannel,
        uint64_t clock_ms,
        uint32_t duration_ms)
    {
        if (singles.empty())
        {
            return true;
        }

        try
        {
            // 转换 LocalSingle 到 GlobalSingle_t
            auto globalSingles = convertLocalToGlobalSingles(singles, crystalsPerChannel);

            // 追加到文件
            return outputFile.appendSegment(globalSingles.data(), globalSingles.size(),
                                            clock_ms, duration_ms);
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error appending singles to file: " << e.what() << std::endl;
            return false;
        }
    }

    /**
     * @brief 创建指定类型的 SingleGenerator
     */
    openpni::interface::SingleGenerator *createSingleGenerator(
        DetectorType type,
        uint16_t channelIndex,
        const std::string &calibrationFile)
    {
        openpni::interface::SingleGenerator *generator = nullptr;

        switch (type)
        {
        case DetectorType::BDM2:
            generator = new openpni::BDM2R2S();
            break;
        // remove BID
        // case DetectorType::BDMBiD:
        //     generator = new openpni::experimental::node::BDMBiDR2S();
        //     break;
        default:
            throw std::runtime_error("Unknown detector type");
        }

        generator->setChannelIndex(channelIndex);
        generator->loadCalibration(calibrationFile);

        return generator;
    }

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
        explicit R2SStreamProcessor(const R2SProcessConfig &config)
            : m_config(config)
        {
        }

        ~R2SStreamProcessor()
        {
            finalize();
        }

        bool initialize(uint16_t inputChannelNum = 0)
        {
            if (m_initialized)
            {
                return true;
            }

            m_inputChannelNum = inputChannelNum;

            try
            {
                std::cout << "Starting R2S processing..." << std::endl;
                std::cout << "Detector: " << (m_config.detectorType == DetectorType::BDM2 ? "BDM2" : "BDMBiD") << std::endl;
                if (m_inputChannelNum > 0)
                {
                    std::cout << "Input channels: " << m_inputChannelNum << std::endl;
                }

                if (!prepareChannelsToProcess())
                {
                    m_hadError = true;
                    return false;
                }

                if (!prepareGenerators())
                {
                    m_hadError = true;
                    return false;
                }

                std::cout << "Setting up ConvergedR2S with generators..." << std::endl;
                m_r2s.SetChannels(m_generatorsVector);
                std::cout << "Setup complete." << std::endl;

                if (!prepareOutput())
                {
                    m_hadError = true;
                    return false;
                }

                m_initialized = true;
                m_finalized = false;
                return true;
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error initializing R2S stream processor: " << e.what() << std::endl;
                m_hadError = true;
                cleanupGenerators();
                return false;
            }
        }

        bool processSegment(const openpni::RawDataView &view)
        {
            if (!m_initialized)
            {
                std::cerr << "R2S stream processor is not initialized" << std::endl;
                m_hadError = true;
                return false;
            }

            const uint64_t segmentId = m_totalSegmentsProcessed++;

            const bool needPerfLog =
                m_config.progressLogInterval > 0 &&
                (segmentId == 0 || segmentId % m_config.progressLogInterval == 0);

            if (!view.count || !view.data)
            {
                if (needPerfLog)
                {
                    std::cout << "Segment " << segmentId << ": No data, skipping" << std::endl;
                }
                return true;
            }

            m_totalRawPackets += view.count;

            const uint64_t clockMs = view.clock_ms;
            const uint32_t durationMs =
                view.duration_ms > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())
                    ? std::numeric_limits<uint32_t>::max()
                    : static_cast<uint32_t>(view.duration_ms);

            const auto perfStart = needPerfLog ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};

            try
            {
                auto d_data = openpni::DPackets::FromHost(
                    view.data,
                    view.offset,
                    view.length,
                    view.channel,
                    view.count);

                auto r2sResults = m_r2s.R2S_CUDA(
                    d_data.raw,
                    d_data.offset,
                    d_data.length,
                    d_data.channel,
                    d_data.count);

                if (m_config.r2sResultIndex >= r2sResults.size())
                {
                    std::cerr << "Error: r2sResultIndex " << m_config.r2sResultIndex
                              << " is out of range, result size=" << r2sResults.size() << std::endl;
                    m_hadError = true;
                    return false;
                }

                auto &singlesSpan = r2sResults[m_config.r2sResultIndex];

                if (m_config.sortDataByTime)
                {
                    if (!singlesSpan.empty() && isDevicePointer(singlesSpan.data()))
                    {
                        openpni::distributed::r2s::d_sortSinglesByTime_R2S(
                            const_cast<Single *>(singlesSpan.data()),
                            singlesSpan.size());
                    }
                }

                const bool callbackSuccess = dispatchSinglesToCallback(singlesSpan, clockMs, durationMs);
                const bool fileSuccess = dispatchSinglesToFile(singlesSpan, clockMs, durationMs);

                m_totalSingles += singlesSpan.size();

                if (needPerfLog)
                {
                    const auto perfEnd = std::chrono::steady_clock::now();
                    const auto timeMs = std::chrono::duration_cast<std::chrono::milliseconds>(perfEnd - perfStart).count();

                    std::cout << "Segment " << segmentId
                              << ": Processed " << view.count << " packets, generated "
                              << singlesSpan.size() << " singles";

                    if (timeMs > 0)
                    {
                        const double speedMbPerSec =
                            static_cast<double>(view.count * 1024ULL) / 1024.0 / 1024.0 /
                            (static_cast<double>(timeMs) / 1000.0);
                        std::cout << ", speed=" << speedMbPerSec << " MB/s";
                    }

                    std::cout << std::endl;
                }

                if (!callbackSuccess || !fileSuccess)
                {
                    m_hadError = true;
                }

                return callbackSuccess && fileSuccess;
            }
            catch (const std::exception &e)
            {
                std::cerr << "Exception at segment " << segmentId << ": " << e.what() << std::endl;
                m_hadError = true;
                return false;
            }
        }

        bool finalize()
        {
            if (m_finalized)
            {
                return !m_hadError;
            }

            m_finalized = true;

            if (m_asyncWriter)
            {
                std::cout << "Waiting for async writer to complete..." << std::endl;
                m_asyncWriter->flush();
                m_asyncWriter->stop();
                std::cout << "Async writer completed, written: " << m_asyncWriter->getTotalWritten()
                          << " singles" << std::endl;
            }

            std::cout << "\n=== Processing Complete ===" << std::endl;
            std::cout << "Total raw packets: " << m_totalRawPackets << std::endl;
            std::cout << "Total singles: " << m_totalSingles << std::endl;
            std::cout << "Singles/Packet ratio: "
                      << (m_totalRawPackets > 0 ? static_cast<double>(m_totalSingles) / m_totalRawPackets : 0.0)
                      << std::endl;

            if (m_config.saveData2SingleFile && m_hasStreamingCallback)
            {
                std::cout << "Output file: " << m_outputFilePath
                          << (m_config.asyncFileWrite ? " (async)" : " (sync)") << std::endl;
                std::cout << "Data also streamed via callback" << std::endl;
            }
            else if (m_config.saveData2SingleFile)
            {
                std::cout << "Output file: " << m_outputFilePath
                          << (m_config.asyncFileWrite ? " (async)" : " (sync)") << std::endl;
            }
            else
            {
                std::cout << "Data streamed via callback" << std::endl;
            }

            std::cout << "===========================\n"
                      << std::endl;

            cleanupGenerators();
            m_initialized = false;
            return !m_hadError;
        }

    private:
        bool prepareChannelsToProcess()
        {
            m_channelsToProcess.clear();

            if (m_config.channelIndices.empty())
            {
                for (uint16_t i = 0; i < m_config.channelNums; i++)
                {
                    m_channelsToProcess.push_back(i);
                }
                std::cout << "Processing all channels" << std::endl;
            }
            else
            {
                m_channelsToProcess = m_config.channelIndices;
                std::cout << "Processing selected channels: ";
                for (auto ch : m_channelsToProcess)
                {
                    std::cout << ch << " ";
                }
                std::cout << std::endl;

                const uint16_t validateRange = std::max<uint16_t>(m_config.channelNums, m_inputChannelNum);
                for (auto ch : m_channelsToProcess)
                {
                    if (ch >= validateRange)
                    {
                        std::cerr << "Error: Channel index " << ch
                                  << " is out of range (0-" << (validateRange - 1) << ")" << std::endl;
                        return false;
                    }
                }
            }

            return true;
        }

        bool prepareGenerators()
        {
            size_t requiredCalibrationCount = 0;
            if (m_config.channelIndices.empty())
            {
                requiredCalibrationCount = m_config.channelNums;
            }
            else
            {
                uint16_t maxChannelIndex = 0;
                for (auto channelIndex : m_config.channelIndices)
                {
                    maxChannelIndex = std::max<uint16_t>(maxChannelIndex, channelIndex);
                }
                requiredCalibrationCount = static_cast<size_t>(maxChannelIndex) + 1;
            }

            if (m_config.calibrationFiles.size() < requiredCalibrationCount)
            {
                std::cerr << "Error: Not enough calibration files. Need at least " << requiredCalibrationCount
                          << ", got " << m_config.calibrationFiles.size() << std::endl;
                return false;
            }

            std::cout << "Loading " << m_channelsToProcess.size() << " channels' calibration data..." << std::endl;

            if (m_config.channelIndices.empty())
            {
                for (size_t i = 0; i < m_config.channelNums; i++)
                {
                    try
                    {
                        auto generator = createSingleGenerator(
                            m_config.detectorType,
                            static_cast<uint16_t>(i),
                            m_config.calibrationFiles[i]);
                        m_generatorsVector.push_back(generator);
                    }
                    catch (const std::exception &e)
                    {
                        std::cerr << "Error creating generator for channel " << i << ": " << e.what() << std::endl;
                        cleanupGenerators();
                        return false;
                    }
                }
            }
            else
            {
                for (size_t i = 0; i < m_config.channelIndices.size(); i++)
                {
                    try
                    {
                        auto channelIndex = m_config.channelIndices[i];
                        auto generator = createSingleGenerator(
                            m_config.detectorType,
                            channelIndex,
                            m_config.calibrationFiles[channelIndex]);
                        m_generatorsVector.push_back(generator);
                    }
                    catch (const std::exception &e)
                    {
                        std::cerr << "Error creating generator for channel " << m_config.channelIndices[i]
                                  << ": " << e.what() << std::endl;
                        cleanupGenerators();
                        return false;
                    }
                }
            }

            return true;
        }

        bool prepareOutput()
        {
            m_hasStreamingCallback =
                static_cast<bool>(m_config.onSinglesSpanReady) ||
                static_cast<bool>(m_config.onSinglesReady);

            if (!m_config.saveData2SingleFile && !m_hasStreamingCallback)
            {
                std::cerr << "Error: saveData2SingleFile is false and no callback is set" << std::endl;
                return false;
            }

            if (m_config.saveData2SingleFile)
            {
                const uint32_t totalCrystals = m_config.channelNums * m_config.crystalsPerChannel;
                m_outputFilePath = m_config.resultPath + "/" + m_config.outputFileName + ".single";

                if (m_config.asyncFileWrite)
                {
                    m_asyncWriter = std::make_unique<AsyncSingleFileWriter>(m_config.asyncWriteQueueSize);
                    m_asyncWriter->open(m_outputFilePath, totalCrystals);
                    std::cout << "Output file (async): " << m_outputFilePath << std::endl;
                }
                else
                {
                    m_singleOutput = std::make_unique<openpni::io::v1::single::SingleFileOutput>();
                    m_singleOutput->setBytes4CrystalIndex(openpni::io::v1::single::CrystalIndexType::UINT32);
                    m_singleOutput->setBytes4TimeValue(openpni::io::v1::single::TimeValueType::UINT64);
                    m_singleOutput->setBytes4Energy(openpni::io::v1::single::EnergyType::FLT32);
                    m_singleOutput->setTotalCrystalNum(totalCrystals);
                    m_singleOutput->open(m_outputFilePath);
                    std::cout << "Output file (sync): " << m_outputFilePath << std::endl;
                }

                std::cout << "Total crystals: " << totalCrystals << std::endl;
            }

            if (m_config.saveData2SingleFile && m_hasStreamingCallback)
            {
                std::cout << "Dual mode: data will be saved to file AND sent via callback" << std::endl;
            }
            else if (m_hasStreamingCallback)
            {
                std::cout << "Streaming mode: data will be sent via callback only" << std::endl;
            }

            return true;
        }

        bool dispatchSinglesToCallback(std::span<Single const> singles, uint64_t clockMs, uint32_t durationMs)
        {
            if (m_config.onSinglesSpanReady)
            {
                const bool callbackSuccess = m_config.onSinglesSpanReady(singles, clockMs, durationMs);
                if (!callbackSuccess)
                {
                    std::cerr << "Callback onSinglesSpanReady returned false, stopping" << std::endl;
                }
                return callbackSuccess;
            }

            if (m_config.onSinglesReady)
            {
                auto globalSingles = convertLocalToGlobalSingles(singles, m_config.crystalsPerChannel);
                const bool callbackSuccess = m_config.onSinglesReady(std::move(globalSingles), clockMs, durationMs);
                if (!callbackSuccess)
                {
                    std::cerr << "Callback onSinglesReady returned false, stopping" << std::endl;
                }
                return callbackSuccess;
            }

            return true;
        }

        bool dispatchSinglesToFile(std::span<Single const> singles, uint64_t clockMs, uint32_t durationMs)
        {
            if (!m_config.saveData2SingleFile)
            {
                return true;
            }

            if (m_config.asyncFileWrite)
            {
                auto globalSinglesForFile = convertLocalToGlobalSingles(singles, m_config.crystalsPerChannel);
                const bool fileSuccess = m_asyncWriter->submit(
                    std::move(globalSinglesForFile),
                    clockMs,
                    durationMs);
                if (!fileSuccess)
                {
                    std::cerr << "Failed to submit segment to async writer" << std::endl;
                }
                return fileSuccess;
            }

            const bool fileSuccess = appendSinglesToSingleFile(
                *m_singleOutput,
                singles,
                m_config.crystalsPerChannel,
                clockMs,
                durationMs);
            if (!fileSuccess)
            {
                std::cerr << "Failed to append segment to single file" << std::endl;
            }
            return fileSuccess;
        }

        void cleanupGenerators()
        {
            for (auto *generator : m_generatorsVector)
            {
                delete generator;
            }
            m_generatorsVector.clear();
        }

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
            uint64_t enqueuedSegments = 0;
            uint64_t processedSegments = 0;
            uint64_t droppedSegments = 0;
            uint64_t enqueueFullHits = 0;
            uint64_t queuePeakDepth = 0;
            bool healthy = true;
        };

        explicit AsyncRawDataToR2SBridge(const R2SProcessConfig &r2sConfig)
            : AsyncRawDataToR2SBridge(r2sConfig, Config())
        {
        }

        AsyncRawDataToR2SBridge(
            const R2SProcessConfig &r2sConfig,
            const Config &config)
            : m_r2sProcessor(r2sConfig),
              m_queue(config.queue),
              m_config(config)
        {
        }

        ~AsyncRawDataToR2SBridge()
        {
            stop();
        }

        bool start(uint16_t inputChannelNum = 0)
        {
            if (m_started.exchange(true, std::memory_order_acq_rel))
            {
                std::cerr << "[RawDataR2SBridge] already started" << std::endl;
                return false;
            }

            m_failed.store(false, std::memory_order_release);
            m_running.store(true, std::memory_order_release);

            if (!m_r2sProcessor.initialize(inputChannelNum))
            {
                m_running.store(false, std::memory_order_release);
                m_started.store(false, std::memory_order_release);
                return false;
            }

            m_consumerThread = std::thread([this]
                                           { consumerLoop(); });
            return true;
        }

        bool stop()
        {
            if (!m_started.exchange(false, std::memory_order_acq_rel))
            {
                return !m_failed.load(std::memory_order_acquire);
            }

            m_running.store(false, std::memory_order_release);
            if (m_consumerThread.joinable())
            {
                m_consumerThread.join();
            }

            const bool finalizeOk = m_r2sProcessor.finalize();
            return finalizeOk && !m_failed.load(std::memory_order_acquire);
        }

        bool enqueueRawData(const openpni::RawDataView &view)
        {
            if (!m_running.load(std::memory_order_acquire) || m_failed.load(std::memory_order_acquire))
            {
                return false;
            }

            while (m_running.load(std::memory_order_acquire) && !m_failed.load(std::memory_order_acquire))
            {
                if (m_queue.tryPushCopy(view))
                {
                    m_enqueuedSegments.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }

                const uint64_t fullHits = m_enqueueFullHits.fetch_add(1, std::memory_order_relaxed) + 1;
                if (m_config.queueFullWarnEvery > 0 && fullHits % m_config.queueFullWarnEvery == 0)
                {
                    std::cerr << "[RawDataR2SBridge] queue is full, depth="
                              << m_queue.size() << "/" << m_queue.capacity() << std::endl;
                }

                if (!m_config.blockWhenQueueFull)
                {
                    if (m_config.dropWhenQueueFull)
                    {
                        m_droppedSegments.fetch_add(1, std::memory_order_relaxed);
                        return true;
                    }

                    return false;
                }

                if (m_config.queueFullBackoffUs > 0)
                {
                    std::this_thread::sleep_for(std::chrono::microseconds(m_config.queueFullBackoffUs));
                }
            }

            return false;
        }

        std::function<bool(const openpni::RawDataView &)> makeRawDataCallback()
        {
            return [this](const openpni::RawDataView &view)
            {
                return enqueueRawData(view);
            };
        }

        Stats stats() const
        {
            Stats s;
            s.enqueuedSegments = m_enqueuedSegments.load(std::memory_order_relaxed);
            s.processedSegments = m_processedSegments.load(std::memory_order_relaxed);
            s.droppedSegments = m_droppedSegments.load(std::memory_order_relaxed);
            s.enqueueFullHits = m_enqueueFullHits.load(std::memory_order_relaxed);
            s.queuePeakDepth = m_queue.peakSize();
            s.healthy = !m_failed.load(std::memory_order_acquire);
            return s;
        }

        bool healthy() const
        {
            return !m_failed.load(std::memory_order_acquire);
        }

    private:
        void consumerLoop()
        {
            while ((m_running.load(std::memory_order_acquire) || !m_queue.empty()) &&
                   !m_failed.load(std::memory_order_acquire))
            {
                const bool consumed = m_queue.tryConsumeOne(
                    [this](RawDataSegmentBuffer &segment)
                    {
                        auto view = segment.toRawView();
                        if (!m_r2sProcessor.processSegment(view))
                        {
                            m_failed.store(true, std::memory_order_release);
                            m_running.store(false, std::memory_order_release);
                            return;
                        }

                        m_processedSegments.fetch_add(1, std::memory_order_relaxed);
                    });

                if (!consumed)
                {
                    if (m_config.consumerIdleBackoffUs > 0)
                    {
                        std::this_thread::sleep_for(std::chrono::microseconds(m_config.consumerIdleBackoffUs));
                    }
                }
            }
        }

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
    bool processR2S(const R2SProcessConfig &config)
    {
        try
        {
            auto mRawFileInput = std::make_unique<openpni::io::v1::RawFileInput>();
            mRawFileInput->open(config.rawdataPath);

            auto header = mRawFileInput->header();
            auto channelNum = header.channelNum;
            auto segmentNum = header.segmentNum;

            std::cout << "Channels: " << channelNum << std::endl;
            std::cout << "Segments: " << segmentNum << std::endl;

            R2SStreamProcessor processor(config);
            if (!processor.initialize(channelNum))
            {
                return false;
            }

            std::cout << "Processing " << segmentNum << " segments..." << std::endl;
            for (uint64_t i = 0; i < segmentNum; i++)
            {
                auto segment = mRawFileInput->readSegment(i, i + 1);
                auto segHeader = mRawFileInput->segmentHeader(i);
                auto view = segment.view(header, segHeader);
                view.clock_ms = segHeader.clock;
                view.duration_ms = segHeader.duration;

                if (!processor.processSegment(view))
                {
                    std::cerr << "R2S processing failed at segment " << i << std::endl;
                    return false;
                }
            }

            return processor.finalize();
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error in processR2S: " << e.what() << std::endl;
            return false;
        }
    }

    /**
     * @brief 创建 BDM2 处理配置
     */
    R2SProcessConfig createBDM2Config(
        const std::string &rawdataPath,
        const std::string &resultPath,
        const std::vector<std::string> &calibrationFiles,
        std::string outputFileName = "singles",
        const std::vector<uint16_t> &channelIndices = {})
    {
        R2SProcessConfig config;
        config.rawdataPath = rawdataPath;
        config.resultPath = resultPath;
        config.calibrationFiles = calibrationFiles;
        config.detectorType = DetectorType::BDM2;
        config.crystalsPerChannel = 169 * 4; // BDM2: 13x13 晶体阵列, 4个阵列
        config.r2sResultIndex = 0;           // BDM2 数据在位置 0
        config.outputFileName = outputFileName;
        config.channelNums = 48;                // BDM2 通道总数48
        config.channelIndices = channelIndices; // 要处理的通道列表（空则处理所有）
        return config;
    }

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