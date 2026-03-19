#pragma once
#include <pni/io/IO.hpp>
#include <filesystem>
#include <iostream>
#include <vector>
#include <string>
#include <pni/tools/Parallel.hpp>
#include <pni/node/Coincidence.hpp>
#include "core/r2s/R2S.hpp"
#include <execution>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <span>

namespace openpni::distributed::coin
{
    namespace fs = std::filesystem;
    using ListmodeFileOutput = openpni::io::v1::listmode::ListmodeFileOutput;
    using GlobalSingle = openpni::v1::basic::GlobalSingle;

    /**
     * @brief 符合 处理配置结构体
     */
    struct CoincidenceProcessConfig
    {
        bool enable = false;
        openpni::CoincidenceProtocol protocol;
        uint16_t channelNum = 0;
        uint32_t crystalsPerChannel = 0;
        std::string outputDir;
    };

    /**
     * @brief IO Context to keep files open across chunks (Modified for Listmode Output)
     */
    struct CoincidenceIOContext
    {
        std::unique_ptr<ListmodeFileOutput> promptWriter;
        std::unique_ptr<ListmodeFileOutput> delayWriter;
        std::string outputDir;

        CoincidenceIOContext(const std::string &dir) : outputDir(dir)
        {
            fs::create_directories(dir);
        }

        ListmodeFileOutput &getStream(const std::string &type, uint32_t totalCrystals)
        {
            if (type == "prompt")
            {
                if (!promptWriter)
                {
                    std::string path = outputDir + "/prompt.lmf";
                    promptWriter = std::make_unique<ListmodeFileOutput>();
                    promptWriter->setBytes4CrystalIndex(openpni::io::v1::single::CrystalIndexType::UINT32);
                    promptWriter->setBytes4TimeValue1_2(openpni::io::v1::listmode::TimeValue1_2Type::INT16);
                    promptWriter->setTotalCrystalNum(totalCrystals);
                    promptWriter->open(path);
                }
                return *promptWriter;
            }
            else // delay
            {
                if (!delayWriter)
                {
                    std::string path = outputDir + "/delay.lmf";
                    delayWriter = std::make_unique<ListmodeFileOutput>();
                    delayWriter->setBytes4CrystalIndex(openpni::io::v1::single::CrystalIndexType::UINT32);
                    delayWriter->setBytes4TimeValue1_2(openpni::io::v1::listmode::TimeValue1_2Type::INT16);
                    delayWriter->setTotalCrystalNum(totalCrystals);
                    delayWriter->open(path);
                }
                return *delayWriter;
            }
        }
    };

    /**
     * @brief 保存符合结果到 Listmode 文件
     */
    void saveCoincidenceEvents(
        ListmodeFileOutput &output,
        std::span<Listmode const> coins,
        uint32_t crystalsPerChannel)
    {
        if (coins.empty())
            return;

        std::vector<Listmode> hostBuf;
        const Listmode *srcPtr = coins.data();

        // GPU -> Host copy if needed
        if (r2s::isDevicePointer(srcPtr))
        {
            hostBuf.resize(coins.size());
            cudaError_t err = cudaMemcpy(hostBuf.data(), srcPtr, coins.size() * sizeof(Listmode), cudaMemcpyDeviceToHost);
            if (err != cudaSuccess)
            {
                throw std::runtime_error("cudaMemcpyDeviceToHost failed: " +
                                         std::string(cudaGetErrorString(err)));
            }
            srcPtr = hostBuf.data();
        }

        // Convert LocalListmode (Host) to Standard Listmode_t
        std::vector<openpni::v1::basic::Listmode_t> listmodeData(coins.size());

        // Parallel conversion
        openpni::tools::parallel_for_each_CPU(
            coins.size(),
            [&](size_t i)
            {
                const auto &loc = srcPtr[i];
                auto &glob = listmodeData[i];

                // Calculate Global Indices
                glob.globalCrystalIndex1 = (uint32_t)loc.channelIndex1 * crystalsPerChannel + loc.crystalIndex1;
                glob.globalCrystalIndex2 = (uint32_t)loc.channelIndex2 * crystalsPerChannel + loc.crystalIndex2;

                // Copy time difference directly (LocalListmode has 'time1_2pico' member)
                glob.time1_2pico = static_cast<int16_t>(loc.time1_2pico);
            });

        // Write segment (using 0 for clock/duration as they are stream segments)
        output.appendSegment(listmodeData.data(), listmodeData.size(), 0, 0);
    }

    /**
     * @brief 处理符合计算
     */
    void processCoincidenceForChunk(
        const std::span<const GlobalSingle> &singles,
        const CoincidenceProcessConfig &config,
        const openpni::Coincidence &coinNode,
        CoincidenceIOContext *ioCtx)
    {
        if (singles.empty() || config.crystalsPerChannel == 0 || !ioCtx)
            return;

        // 1. Convert GlobalSingle to LocalSingle (Host)
        std::vector<Single> localSingles(singles.size());
        const uint32_t cpc = config.crystalsPerChannel;

        // 并行转换
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

        // 2. Upload to GPU
        Single *d_singles_ptr = nullptr;
        size_t bytes = localSingles.size() * sizeof(Single);
        cudaError_t err = cudaMalloc(&d_singles_ptr, bytes);
        if (err != cudaSuccess)
        {
            std::cerr << "cudaMalloc failed: " << cudaGetErrorString(err) << std::endl;
            return;
        }

        err = cudaMemcpy(d_singles_ptr, localSingles.data(), bytes, cudaMemcpyHostToDevice);
        if (err != cudaSuccess)
        {
            std::cerr << "cudaMemcpy failed: " << cudaGetErrorString(err) << std::endl;
            cudaFree(d_singles_ptr);
            return;
        }

        // 3. Perform Coincidence & Save
        try
        {
            std::span<Single const> d_span(d_singles_ptr, localSingles.size());

            std::vector<std::span<Single const>> inputList;
            inputList.push_back(d_span);

            auto [prompt, delay] = coinNode.getDListmode(inputList, config.protocol);

            // Process Prompt
            if (!prompt.empty())
            {
                saveCoincidenceEvents(
                    ioCtx->getStream("prompt", config.channelNum * config.crystalsPerChannel),
                    prompt,
                    config.crystalsPerChannel);
            }

            // Process Delay
            if (!delay.empty())
            {
                saveCoincidenceEvents(
                    ioCtx->getStream("delay", config.channelNum * config.crystalsPerChannel),
                    delay,
                    config.crystalsPerChannel);
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << "Coincidence error: " << e.what() << std::endl;
        }

        // Cleanup
        cudaFree(d_singles_ptr);
    }

    /**
     * @brief 直接解析到缓冲区，避免返回 vector 导致的分配和拷贝
     */
    void parseSingleSegmentBytesToBuffer(
        const openpni::io::v1::single::SingleSegmentBytes &segBytes,
        const openpni::io::v1::single::SingleFileHeader &fileHeader,
        uint64_t count,
        GlobalSingle *destBuffer)
    {
        // Lambda Selection for Crystal Index
        std::function<uint32_t(uint64_t)> getCrystalIndex;
        if (fileHeader.bytes4CrystalIndex == 2)
        {
            auto ptr = reinterpret_cast<const uint16_t *>(segBytes.crystalIndexBytes.get());
            getCrystalIndex = [ptr](uint64_t i)
            { return ptr[i]; };
        }
        else if (fileHeader.bytes4CrystalIndex == 4)
        {
            auto ptr = reinterpret_cast<const uint32_t *>(segBytes.crystalIndexBytes.get());
            getCrystalIndex = [ptr](uint64_t i)
            { return ptr[i]; };
        }
        else // 3 bytes
        {
            auto ptr = reinterpret_cast<const uint8_t *>(segBytes.crystalIndexBytes.get());
            getCrystalIndex = [ptr](uint64_t i)
            {
                const uint8_t *p = ptr + i * 3;
                return p[0] | (p[1] << 8) | (p[2] << 16);
            };
        }

        // Lambda Selection for Time Value
        std::function<uint64_t(uint64_t)> getTimeValue;
        if (fileHeader.bytes4TimeValue == 8)
        {
            auto ptr = reinterpret_cast<const uint64_t *>(segBytes.timeValueBytes.get());
            getTimeValue = [ptr](uint64_t i)
            { return ptr[i]; };
        }
        else if (fileHeader.bytes4TimeValue == 4)
        {
            auto ptr = reinterpret_cast<const uint32_t *>(segBytes.timeValueBytes.get());
            getTimeValue = [ptr](uint64_t i)
            { return ptr[i]; };
        }
        else
        {
            int bytes = fileHeader.bytes4TimeValue;
            auto ptr = reinterpret_cast<const uint8_t *>(segBytes.timeValueBytes.get());
            getTimeValue = [ptr, bytes](uint64_t i)
            {
                const uint8_t *p = ptr + i * bytes;
                uint64_t val = 0;
                for (int k = 0; k < bytes; k++)
                    val |= (static_cast<uint64_t>(p[k]) << (k * 8));
                return val;
            };
        }

        // Lambda Selection for Energy
        std::function<float(uint64_t)> getEnergy;
        if (fileHeader.bytes4Energy == 4)
        {
            auto ptr = reinterpret_cast<const float *>(segBytes.energyBytes.get());
            getEnergy = [ptr](uint64_t i)
            { return ptr[i]; };
        }
        else if (fileHeader.bytes4Energy == 1)
        {
            auto ptr = reinterpret_cast<const uint8_t *>(segBytes.energyBytes.get());
            getEnergy = [ptr](uint64_t i)
            { return static_cast<float>(ptr[i]) * 4.0f; };
        }
        else if (fileHeader.bytes4Energy == 2)
        {
            auto ptr = reinterpret_cast<const uint16_t *>(segBytes.energyBytes.get());
            getEnergy = [ptr](uint64_t i)
            { return static_cast<float>(ptr[i]) * 0.01f; };
        }
        else
        {
            getEnergy = [](uint64_t)
            { return 511.0f; };
        }

        // Loop填充到目标 Buffer
        for (uint64_t i = 0; i < count; i++)
        {
            destBuffer[i].globalCrystalIndex = getCrystalIndex(i);
            destBuffer[i].timeValue_pico = getTimeValue(i);
            destBuffer[i].energy = getEnergy(i);
        }
    }

    /**
     * @brief 从字节数据解析 GlobalSingle 数组 (Optimized)
     */
    std::vector<GlobalSingle> parseSingleSegmentBytes(
        const openpni::io::v1::single::SingleSegmentBytes &segBytes,
        const openpni::io::v1::single::SingleFileHeader &fileHeader,
        uint64_t count)
    {
        std::vector<GlobalSingle> singles(count);
        parseSingleSegmentBytesToBuffer(segBytes, fileHeader, count, singles.data());
        return singles;
    }

    /**
     * @brief 异步文件写入器
     * 将数据写入任务放入队列，后台单线程负责实际 fwrite
     */
    class AsyncSingleWriter
    {
    public:
        AsyncSingleWriter(const std::string &path,
                          openpni::io::v1::single::SingleFileOutput &outputHelper,
                          size_t maxMemoryBytes = 16ULL * 1024 * 1024 * 1024)
            : m_outputHelper(outputHelper), m_maxMemoryBytes(maxMemoryBytes), m_currentMemoryBytes(0), m_running(true)
        {
            // 提前打开文件，确保清空旧内容
            m_outputHelper.open(path);

            m_worker = std::thread([this]()
                                   { workerLoop(); });
        }

        ~AsyncSingleWriter()
        {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_running = false;
            }
            m_cv_data.notify_one();
            if (m_worker.joinable())
                m_worker.join();
        }

        void submit(std::vector<GlobalSingle> &&data, uint64_t clock, uint32_t duration)
        {
            size_t dataSize = data.capacity() * sizeof(GlobalSingle);

            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv_capacity.wait(lock, [this, dataSize]
                               { return m_currentMemoryBytes + dataSize <= m_maxMemoryBytes; });

            m_currentMemoryBytes += dataSize;
            m_queue.push({std::move(data), clock, duration});

            lock.unlock();
            m_cv_data.notify_one();
        }

    private:
        void workerLoop()
        {
            while (true)
            {
                WriteTask task;

                {
                    std::unique_lock<std::mutex> lock(m_mutex);
                    m_cv_data.wait(lock, [this]
                                   { return !m_queue.empty() || !m_running; });

                    if (!m_running && m_queue.empty())
                        return; // 退出条件

                    if (m_queue.empty())
                        continue;

                    task = std::move(m_queue.front());
                    m_queue.pop();
                }

                // 执行实际写入
                if (!task.data.empty())
                {
                    size_t taskSize = task.data.capacity() * sizeof(GlobalSingle);
                    m_outputHelper.appendSegment(task.data.data(), task.data.size(), task.clock, task.duration);

                    // 显式释放内存
                    task.data.clear();
                    task.data.shrink_to_fit();

                    {
                        std::lock_guard<std::mutex> lock(m_mutex);
                        m_currentMemoryBytes -= taskSize;
                    }
                    m_cv_capacity.notify_one();
                }
            }
        }

        struct WriteTask
        {
            std::vector<GlobalSingle> data;
            uint64_t clock;
            uint32_t duration;
        };

        openpni::io::v1::single::SingleFileOutput &m_outputHelper;
        size_t m_maxMemoryBytes;
        size_t m_currentMemoryBytes;
        std::thread m_worker;
        std::mutex m_mutex;
        std::condition_variable m_cv_data;
        std::condition_variable m_cv_capacity;
        std::queue<WriteTask> m_queue;
        bool m_running;
    };

    /**
     * @brief 合并多个 Single 文件为一个文件（支持分布式采集的时间偏差处理）
     *
     *
     * @param inputFiles 输入的 Single 文件路径列表
     * @param outputFile 输出的合并后的 Single 文件路径
     * @param coinConfig 符合处理配置
     * @param saveMergedSingles 是否保存合并后的 Singles 文件（如果只做符合处理可设为 false）
     * @return bool 成功返回true，失败返回false
     */
    bool merge_single_files(
        const std::vector<std::string> &inputFiles,
        const std::string &outputFile,
        const CoincidenceProcessConfig &coinConfig = {},
        bool saveMergedSingles = false)
    {
        if (inputFiles.empty())
        {
            std::cerr << "Error: No input files specified" << std::endl;
            return false;
        }

        // 设置符合参数
        openpni::Coincidence coinNode;
        std::vector<uint32_t> crystalNumOfEachChannel(coinConfig.channelNum, coinConfig.crystalsPerChannel);
        coinNode.setTotalCrystalNumOfEachChannel(crystalNumOfEachChannel);

        std::unique_ptr<CoincidenceIOContext> ioCtx;

        try
        {
            std::cout << "Merging " << inputFiles.size() << " Single files..." << std::endl;
            if (coinConfig.enable)
            {
                std::cout << "Coincidence processing enabled. Output to: " << coinConfig.outputDir << std::endl;
                ioCtx = std::make_unique<CoincidenceIOContext>(coinConfig.outputDir);
            }

            // 1. 打开所有输入文件并验证兼容性
            std::vector<std::unique_ptr<openpni::io::v1::single::SingleFileInput>> inputs;
            inputs.reserve(inputFiles.size());

            openpni::io::v1::single::SingleFileHeader firstHeader;
            uint32_t maxCrystalNum = 0;
            uint64_t totalSegments = 0;
            uint64_t totalSingles = 0;

            for (size_t i = 0; i < inputFiles.size(); i++)
            {
                auto input = std::make_unique<openpni::io::v1::single::SingleFileInput>();
                input->open(inputFiles[i]);

                auto header = input->header();

                if (i == 0)
                {
                    firstHeader = header;
                    std::cout << "  File format: bytes4CrystalIndex=" << static_cast<int>(header.bytes4CrystalIndex)
                              << ", bytes4TimeValue=" << static_cast<int>(header.bytes4TimeValue)
                              << ", bytes4Energy=" << static_cast<int>(header.bytes4Energy) << std::endl;
                }
                else
                {
                    // 验证文件格式兼容性
                    if (header.bytes4CrystalIndex != firstHeader.bytes4CrystalIndex ||
                        header.bytes4TimeValue != firstHeader.bytes4TimeValue ||
                        header.bytes4Energy != firstHeader.bytes4Energy)
                    {
                        std::cerr << "Error: Incompatible file format at file " << i << ": " << inputFiles[i] << std::endl;
                        return false;
                    }
                }

                maxCrystalNum = std::max(maxCrystalNum, header.cystalNum);
                totalSegments += header.segmentNum;

                // 统计总事件数
                for (uint32_t j = 0; j < header.segmentNum; j++)
                {
                    totalSingles += input->segmentHeader(j).count;
                }

                inputs.push_back(std::move(input));
                std::cout << "  Opened: " << inputFiles[i] << " (" << header.segmentNum << " segments)" << std::endl;
            }

            std::cout << "Total segments to merge: " << totalSegments << std::endl;
            std::cout << "Total singles to merge: " << totalSingles << std::endl;

            // 2. 创建输出文件
            openpni::io::v1::single::SingleFileOutput output;

            // 设置输出文件参数
            output.setBytes4CrystalIndex(static_cast<openpni::io::v1::single::CrystalIndexType>(firstHeader.bytes4CrystalIndex));
            output.setBytes4TimeValue(static_cast<openpni::io::v1::single::TimeValueType>(firstHeader.bytes4TimeValue));
            output.setBytes4Energy(static_cast<openpni::io::v1::single::EnergyType>(firstHeader.bytes4Energy));
            output.setTotalCrystalNum(maxCrystalNum);

            std::unique_ptr<AsyncSingleWriter> asyncWriter;
            if (saveMergedSingles)
            {
                asyncWriter = std::make_unique<AsyncSingleWriter>(outputFile, output);
                std::cout << "Output file (Async): " << outputFile << std::endl;
            }
            else
            {
                std::cout << "Merged singles saving DISABLED." << std::endl;
            }

            // 3. 收集所有段的时间信息
            struct SegmentTimeInfo
            {
                size_t fileIndex;
                uint32_t segmentIndex;
                uint64_t startTime_ms; // clock
                uint64_t endTime_ms;   // clock + duration
                uint32_t duration_ms;
                uint64_t count;
            };
            std::vector<SegmentTimeInfo> allSegments;

            for (size_t i = 0; i < inputs.size(); i++)
            {
                auto header = inputs[i]->header();
                for (uint32_t j = 0; j < header.segmentNum; j++)
                {
                    auto segHeader = inputs[i]->segmentHeader(j);
                    SegmentTimeInfo info;
                    info.fileIndex = i;
                    info.segmentIndex = j;
                    info.startTime_ms = segHeader.clock;
                    info.duration_ms = segHeader.duration;
                    info.endTime_ms = segHeader.clock + segHeader.duration;
                    info.count = segHeader.count;
                    allSegments.push_back(info);
                }
            }

            // 4. 分析时间分布
            if (!allSegments.empty())
            {
                auto minStart = std::min_element(allSegments.begin(), allSegments.end(),
                                                 [](const auto &a, const auto &b)
                                                 { return a.startTime_ms < b.startTime_ms; });
                auto maxEnd = std::max_element(allSegments.begin(), allSegments.end(),
                                               [](const auto &a, const auto &b)
                                               { return a.endTime_ms < b.endTime_ms; });

                std::cout << "\nTime range analysis:" << std::endl;
                std::cout << "  First segment starts at: " << minStart->startTime_ms << " ms" << std::endl;
                std::cout << "  Last segment ends at: " << maxEnd->endTime_ms << " ms" << std::endl;
                std::cout << "  Total time span: " << (maxEnd->endTime_ms - minStart->startTime_ms) << " ms" << std::endl;
            }

            // 5. 基于段起始时间的归并

            {
                std::cout << "\nMerging with segment-based merge strategy..." << std::endl;

                // 按起始时间排序段
                std::sort(allSegments.begin(), allSegments.end(),
                          [](const SegmentTimeInfo &a, const SegmentTimeInfo &b)
                          {
                              return a.startTime_ms < b.startTime_ms;
                          });

                // 分析时间重叠情况
                std::cout << "Analyzing segment overlap patterns..." << std::endl;
                std::vector<size_t> overlapGroups; // 记录需要归并的段组边界
                overlapGroups.push_back(0);

                // 只要 startTime_ms 在 3ms 误差范围内，就视为同一组
                // 假设段是按 startTime_ms 排序的
                uint64_t currentGroupStartTime = allSegments[0].startTime_ms;

                for (size_t i = 1; i < allSegments.size(); i++)
                {
                    // 如果当前段的开始时间与组的开始时间差距很大（> 5ms），则开启新组
                    // 这里的 5ms 是一个容差，适应分布式时钟同步的情况 (预期是同start，误差<3ms)
                    if (allSegments[i].startTime_ms > currentGroupStartTime + 5)
                    {
                        overlapGroups.push_back(i);
                        currentGroupStartTime = allSegments[i].startTime_ms;
                    }
                    // 否则，该段属于当前组，继续
                }
                overlapGroups.push_back(allSegments.size()); // 最后一个边界

                std::cout << "Segments grouped into " << (overlapGroups.size() - 1) << " time-aligned groups" << std::endl;

                // Performance Timers
                double t_total = 0.0;
                double t_read_parse = 0.0;
                double t_sort = 0.0;
                double t_coin = 0.0;
                double t_write = 0.0;

                auto mergeProcessStart = std::chrono::high_resolution_clock::now();

                std::vector<std::unique_ptr<std::mutex>> fileMutexes;
                for (size_t i = 0; i < inputs.size(); ++i)
                {
                    fileMutexes.push_back(std::make_unique<std::mutex>());
                }

                // 处理每个组
                uint64_t totalWritten = 0;
                uint32_t segmentsWritten = 0;

                for (size_t groupIdx = 0; groupIdx < overlapGroups.size() - 1; groupIdx++)
                {
                    size_t groupStart = overlapGroups[groupIdx];
                    size_t groupEnd = overlapGroups[groupIdx + 1];
                    size_t groupSize = groupEnd - groupStart;

                    if (groupSize == 1)
                    {
                        // 单个段优化：直接解析并处理
                        const auto &segInfo = allSegments[groupStart];
                        auto &input = *inputs[segInfo.fileIndex];

                        auto t1 = std::chrono::high_resolution_clock::now();
                        auto segBytes = input.readSegment(segInfo.segmentIndex);
                        auto segHeader = input.segmentHeader(segInfo.segmentIndex);
                        auto fileHeader = input.header();

                        // 直接分配一次 vector
                        std::vector<GlobalSingle> singles(segHeader.count);
                        parseSingleSegmentBytesToBuffer(segBytes, fileHeader, segHeader.count, singles.data());
                        auto t2 = std::chrono::high_resolution_clock::now();
                        t_read_parse += std::chrono::duration<double, std::milli>(t2 - t1).count();

                        if (coinConfig.enable)
                        {
                            auto tc1 = std::chrono::high_resolution_clock::now();
                            processCoincidenceForChunk(singles, coinConfig, coinNode, ioCtx.get());
                            auto tc2 = std::chrono::high_resolution_clock::now();
                            t_coin += std::chrono::duration<double, std::milli>(tc2 - tc1).count();
                        }

                        auto tw1 = std::chrono::high_resolution_clock::now();
                        if (saveMergedSingles)
                        {
                            asyncWriter->submit(std::move(singles), segInfo.startTime_ms, segInfo.duration_ms);
                            totalWritten += segHeader.count;
                        }
                        auto tw2 = std::chrono::high_resolution_clock::now();
                        t_write += std::chrono::duration<double, std::milli>(tw2 - tw1).count();

                        segmentsWritten++;
                    }
                    else
                    {
                        // 多个段：并行读取 + 零拷贝合并
                        uint64_t totalCount = 0;

                        // 计算总数量
                        for (size_t i = groupStart; i < groupEnd; i++)
                        {
                            totalCount += allSegments[i].count;
                        }

                        auto t1 = std::chrono::high_resolution_clock::now();
                        // 一次性分配大内存
                        std::vector<GlobalSingle> mergedSingles(totalCount);
                        std::vector<uint64_t> segmentOffsets; // 每个段的起始位置
                        segmentOffsets.reserve(groupEnd - groupStart);

                        uint64_t currentOffset = 0;
                        for (size_t i = groupStart; i < groupEnd; i++)
                        {
                            segmentOffsets.push_back(currentOffset);
                            currentOffset += allSegments[i].count;
                        }

                        // 并行加载并直接写入 mergedSingles 的对应位置
                        openpni::tools::parallel_for_each_CPU(
                            groupEnd - groupStart,
                            [&](std::size_t idx)
                            {
                                size_t globalIdx = groupStart + idx;
                                const auto &segInfo = allSegments[globalIdx];
                                auto &input = *inputs[segInfo.fileIndex];

                                auto segBytes = [&]()
                                {
                                    std::lock_guard<std::mutex> lock(*fileMutexes[segInfo.fileIndex]);
                                    return input.readSegment(segInfo.segmentIndex);
                                }();
                                auto fileHeader = input.header();

                                // 直接写入大数组的特定偏移位置
                                GlobalSingle *destPtr = mergedSingles.data() + segmentOffsets[idx];
                                parseSingleSegmentBytesToBuffer(segBytes, fileHeader, segInfo.count, destPtr);
                            });
                        auto t2 = std::chrono::high_resolution_clock::now();
                        t_read_parse += std::chrono::duration<double, std::milli>(t2 - t1).count();

                        // 使用并行排序 (C++17)
                        // 如果编译器不支持 std::execution::par_unseq，可回退到 std::sort
                        auto ts1 = std::chrono::high_resolution_clock::now();
                        std::sort(std::execution::par_unseq,
                                  mergedSingles.begin(), mergedSingles.end(),
                                  [](const GlobalSingle &a, const GlobalSingle &b)
                                  {
                                      return a.timeValue_pico < b.timeValue_pico;
                                  });
                        auto ts2 = std::chrono::high_resolution_clock::now();
                        t_sort += std::chrono::duration<double, std::milli>(ts2 - ts1).count();

                        if (coinConfig.enable)
                        {
                            auto tc1 = std::chrono::high_resolution_clock::now();
                            processCoincidenceForChunk(mergedSingles, coinConfig, coinNode, ioCtx.get());
                            auto tc2 = std::chrono::high_resolution_clock::now();
                            t_coin += std::chrono::duration<double, std::milli>(tc2 - tc1).count();
                        }

                        // 写入一个大段
                        // 使用组内最早的开始时间和最晚的结束时间
                        // 但通常应该保持对齐
                        uint64_t groupStartTime_ms = allSegments[groupStart].startTime_ms;

                        // 寻找最大结束时间
                        uint64_t groupEndTime_ms = 0;
                        for (size_t i = groupStart; i < groupEnd; ++i)
                        {
                            groupEndTime_ms = std::max(groupEndTime_ms, allSegments[i].endTime_ms);
                        }

                        uint32_t groupDuration_ms = static_cast<uint32_t>(groupEndTime_ms - groupStartTime_ms);

                        auto tw1 = std::chrono::high_resolution_clock::now();
                        if (saveMergedSingles)
                        {
                            asyncWriter->submit(std::move(mergedSingles), groupStartTime_ms, groupDuration_ms);
                            totalWritten += totalCount;
                        }
                        auto tw2 = std::chrono::high_resolution_clock::now();
                        t_write += std::chrono::duration<double, std::milli>(tw2 - tw1).count();

                        segmentsWritten++;
                    }

                    if ((groupIdx + 1) % 50 == 0 || groupIdx == overlapGroups.size() - 2)
                    {
                        std::cout << "  Progress: " << (groupIdx + 1) << "/" << (overlapGroups.size() - 1)
                                  << " groups, " << segmentsWritten << " segments, "
                                  << totalWritten << " singles written" << std::endl;
                    }
                }

                auto mergeProcessEnd = std::chrono::high_resolution_clock::now();
                t_total = std::chrono::duration<double, std::milli>(mergeProcessEnd - mergeProcessStart).count();

                std::cout << "\n=== Performance Analysis ===" << std::endl;
                std::cout << "Total Merge Time : " << t_total << " ms" << std::endl;
                std::cout << "  Read & Parse   : " << t_read_parse << " ms (" << (t_read_parse / t_total * 100) << "%)" << std::endl;
                std::cout << "  Sorting        : " << t_sort << " ms (" << (t_sort / t_total * 100) << "%)" << std::endl;
                std::cout << "  Coincidence    : " << t_coin << " ms (" << (t_coin / t_total * 100) << "%)" << std::endl;
                std::cout << "  Writing        : " << t_write << " ms (" << (t_write / t_total * 100) << "%)" << std::endl;
                std::cout << "============================" << std::endl;
            }

            std::cout << "\n=== Merge Complete ===" << std::endl;
            std::cout << "Merged " << totalSegments << " segments from " << inputs.size() << " files" << std::endl;
            std::cout << "Total singles: " << totalSingles << std::endl;
            std::cout << "Output: " << outputFile << std::endl;
            std::cout << "=======================\n"
                      << std::endl;

            return true;
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error merging Single files: " << e.what() << std::endl;
            return false;
        }
    }
}