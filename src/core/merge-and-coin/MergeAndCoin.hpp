#pragma once
#include <pni/io/IO.hpp>
#include <pni/io/ListmodeIO.hpp>
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
#include <stdexcept>

namespace openpni::distributed::coin
{
    namespace fs = std::filesystem;
    using ListmodeFileOutput = openpni::io::listmode::ListmodeFileOutput;
    using ListmodeFileInput = openpni::io::listmode::ListmodeFileInput;
    using ListmodeFileHeader = openpni::io::listmode::ListmodeFileHeader;
    using ListmodeFileSegment = openpni::io::listmode::ListmodeFileSegment;
    using SupportedFields = openpni::io::listmode::SupportedFields;

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

    inline ListmodeFileHeader createCoinListmodeHeader()
    {
        ListmodeFileHeader header;
        auto fieldsInUse = static_cast<SupportedFields>(
            SupportedFields::local_crystal_index1 |
            SupportedFields::local_crystal_index2 |
            SupportedFields::channel_index1 |
            SupportedFields::channel_index2 |
            SupportedFields::time_of_flight);
        header.SetFieldsInUse(fieldsInUse);
        header.SetBitsForStorage(SupportedFields::local_crystal_index1, 16);
        header.SetBitsForStorage(SupportedFields::local_crystal_index2, 16);
        header.SetBitsForStorage(SupportedFields::channel_index1, 16);
        header.SetBitsForStorage(SupportedFields::channel_index2, 16);
        header.SetBitsForStorage(SupportedFields::time_of_flight, 16);
        header.SetFileTypeName(openpni::io::listmode::fields::file_type_coin_listmode);
        return header;
    }

    inline ListmodeFileHeader createSingleListmodeHeader()
    {
        ListmodeFileHeader header;
        auto fieldsInUse = static_cast<SupportedFields>(
            SupportedFields::local_crystal_index1 |
            SupportedFields::channel_index1 |
            SupportedFields::energy1 |
            SupportedFields::absolute_timestamp1);
        header.SetFieldsInUse(fieldsInUse);
        header.SetBitsForStorage(SupportedFields::local_crystal_index1, 16);
        header.SetBitsForStorage(SupportedFields::channel_index1, 16);
        header.SetBitsForStorage(SupportedFields::energy1, 32);
        header.SetBitsForStorage(SupportedFields::absolute_timestamp1, 64);
        header.SetFileTypeName(openpni::io::listmode::fields::file_type_single_listmode);
        return header;
    }

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
            (void)totalCrystals;
            if (type == "prompt")
            {
                if (!promptWriter)
                {
                    std::string path = outputDir + "/prompt.lmf";
                    promptWriter = std::make_unique<ListmodeFileOutput>(createCoinListmodeHeader());
                    promptWriter->Open(path);
                }
                return *promptWriter;
            }
            else // delay
            {
                if (!delayWriter)
                {
                    std::string path = outputDir + "/delay.lmf";
                    delayWriter = std::make_unique<ListmodeFileOutput>(createCoinListmodeHeader());
                    delayWriter->Open(path);
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
        (void)crystalsPerChannel;
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

        const std::span<Listmode const> listmodes(srcPtr, coins.size());
        ListmodeFileSegment segment;
        segment.SetListmodes(listmodes);
        segment.SetClockMs(0);
        segment.SetDurationMs(0);
        output.AppendSegment(std::move(segment));
    }

    /**
     * @brief 处理符合计算
     */
    void processCoincidenceForChunk(
        const std::span<const Single> &singles,
        const CoincidenceProcessConfig &config,
        const openpni::Coincidence &coinNode,
        CoincidenceIOContext *ioCtx)
    {
        if (singles.empty() || config.crystalsPerChannel == 0 || !ioCtx)
            return;

        // Upload to GPU
        Single *d_singles_ptr = nullptr;
        size_t bytes = singles.size() * sizeof(Single);
        cudaError_t err = cudaMalloc(&d_singles_ptr, bytes);
        if (err != cudaSuccess)
        {
            std::cerr << "cudaMalloc failed: " << cudaGetErrorString(err) << std::endl;
            return;
        }

        err = cudaMemcpy(d_singles_ptr, singles.data(), bytes, cudaMemcpyHostToDevice);
        if (err != cudaSuccess)
        {
            std::cerr << "cudaMemcpy failed: " << cudaGetErrorString(err) << std::endl;
            cudaFree(d_singles_ptr);
            return;
        }

        // 3. Perform Coincidence & Save
        try
        {
            std::span<Single const> d_span(d_singles_ptr, singles.size());

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

    inline std::vector<Single> readSinglesFromSegment(ListmodeFileSegment &segment)
    {
        const auto data = segment.GetHAnyData();
        if (!data.local_crystal_index1 || !data.channel_index1 || !data.absolute_timestamp1)
        {
            throw std::runtime_error("Single segment missing required fields");
        }

        std::vector<Single> singles(data.count);
        for (std::size_t i = 0; i < data.count; ++i)
        {
            singles[i].channelIndex = data.channel_index1[i];
            singles[i].crystalIndex = data.local_crystal_index1[i];
            singles[i].timevalue_pico = data.absolute_timestamp1[i];
            singles[i].energy = data.energy1 ? data.energy1[i] : 0.0f;
        }
        return singles;
    }

    inline void fillSinglesFromSegment(ListmodeFileSegment &segment, Single *dest)
    {
        const auto data = segment.GetHAnyData();
        if (!data.local_crystal_index1 || !data.channel_index1 || !data.absolute_timestamp1)
        {
            throw std::runtime_error("Single segment missing required fields");
        }

        for (std::size_t i = 0; i < data.count; ++i)
        {
            dest[i].channelIndex = data.channel_index1[i];
            dest[i].crystalIndex = data.local_crystal_index1[i];
            dest[i].timevalue_pico = data.absolute_timestamp1[i];
            dest[i].energy = data.energy1 ? data.energy1[i] : 0.0f;
        }
    }

    class AsyncSingleWriter
    {
    public:
        AsyncSingleWriter(const std::string &path,
                          size_t maxMemoryBytes = 16ULL * 1024 * 1024 * 1024)
            : m_output(createSingleListmodeHeader())
            , m_maxMemoryBytes(maxMemoryBytes)
            , m_currentMemoryBytes(0)
            , m_running(true)
        {
            m_output.Open(path);
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

        void submit(std::vector<Single> &&data, uint64_t clock, uint32_t duration)
        {
            size_t dataSize = data.capacity() * sizeof(Single);

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
                        return;

                    if (m_queue.empty())
                        continue;

                    task = std::move(m_queue.front());
                    m_queue.pop();
                }

                if (!task.data.empty())
                {
                    size_t taskSize = task.data.capacity() * sizeof(Single);
                    ListmodeFileSegment segment;
                    segment.SetSingles(std::span<const Single>(task.data.data(), task.data.size()));
                    segment.SetClockMs(task.clock);
                    segment.SetDurationMs(task.duration);
                    m_output.AppendSegment(std::move(segment));

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
            std::vector<Single> data;
            uint64_t clock;
            uint32_t duration;
        };

        ListmodeFileOutput m_output;
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
            std::vector<std::unique_ptr<ListmodeFileInput>> inputs;
            inputs.reserve(inputFiles.size());

            uint64_t totalSegments = 0;
            uint64_t totalSingles = 0;

            for (size_t i = 0; i < inputFiles.size(); i++)
            {
                auto input = std::make_unique<ListmodeFileInput>();
                input->Open(inputFiles[i]);

                const auto &header = input->Header();
                if (header.FileTypeName() != openpni::io::listmode::fields::file_type_single_listmode)
                {
                    std::cerr << "Error: Not a single listmode file: " << inputFiles[i] << std::endl;
                    return false;
                }

                const int fields = static_cast<int>(header.FieldsInUse());
                const int required = static_cast<int>(SupportedFields::local_crystal_index1 |
                                                     SupportedFields::channel_index1 |
                                                     SupportedFields::absolute_timestamp1);
                if ((fields & required) != required)
                {
                    std::cerr << "Error: Missing required fields in file: " << inputFiles[i] << std::endl;
                    return false;
                }

                const auto segmentNum = input->SegmentNum();
                totalSegments += segmentNum;

                inputs.push_back(std::move(input));
                std::cout << "  Opened: " << inputFiles[i] << " (" << segmentNum << " segments)" << std::endl;
            }

            std::cout << "Total segments to merge: " << totalSegments << std::endl;

            // 2. 创建输出文件

            std::unique_ptr<AsyncSingleWriter> asyncWriter;
            if (saveMergedSingles)
            {
                asyncWriter = std::make_unique<AsyncSingleWriter>(outputFile);
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
                const auto segmentNum = inputs[i]->SegmentNum();
                for (uint32_t j = 0; j < segmentNum; j++)
                {
                    auto segment = inputs[i]->ReadSegment(j);
                    auto data = segment.GetHAnyData();
                    SegmentTimeInfo info;
                    info.fileIndex = i;
                    info.segmentIndex = j;
                    info.startTime_ms = segment.GetClockMs();
                    info.duration_ms = segment.GetDurationMs();
                    info.endTime_ms = info.startTime_ms + info.duration_ms;
                    info.count = data.count;
                    allSegments.push_back(info);
                    totalSingles += data.count;
                }
            }

            std::cout << "Total singles to merge: " << totalSingles << std::endl;

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
                        auto segment = input.ReadSegment(segInfo.segmentIndex);
                        std::vector<Single> singles = readSinglesFromSegment(segment);
                        auto t2 = std::chrono::high_resolution_clock::now();
                        t_read_parse += std::chrono::duration<double, std::milli>(t2 - t1).count();

                        if (coinConfig.enable)
                        {
                            auto tc1 = std::chrono::high_resolution_clock::now();
                            processCoincidenceForChunk(
                                std::span<const Single>(singles.data(), singles.size()),
                                coinConfig,
                                coinNode,
                                ioCtx.get());
                            auto tc2 = std::chrono::high_resolution_clock::now();
                            t_coin += std::chrono::duration<double, std::milli>(tc2 - tc1).count();
                        }

                        auto tw1 = std::chrono::high_resolution_clock::now();
                        if (saveMergedSingles)
                        {
                            asyncWriter->submit(std::move(singles), segInfo.startTime_ms, segInfo.duration_ms);
                            totalWritten += segInfo.count;
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
                        std::vector<Single> mergedSingles(totalCount);
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

                                auto segment = [&]()
                                {
                                    std::lock_guard<std::mutex> lock(*fileMutexes[segInfo.fileIndex]);
                                    return input.ReadSegment(segInfo.segmentIndex);
                                }();

                                // 直接写入大数组的特定偏移位置
                                Single *destPtr = mergedSingles.data() + segmentOffsets[idx];
                                fillSinglesFromSegment(segment, destPtr);
                            });
                        auto t2 = std::chrono::high_resolution_clock::now();
                        t_read_parse += std::chrono::duration<double, std::milli>(t2 - t1).count();

                        // 使用并行排序 (C++17)
                        // 如果编译器不支持 std::execution::par_unseq，可回退到 std::sort
                        auto ts1 = std::chrono::high_resolution_clock::now();
                        std::sort(std::execution::par_unseq,
                                  mergedSingles.begin(), mergedSingles.end(),
                                  [](const Single &a, const Single &b)
                                  {
                                      return a.timevalue_pico < b.timevalue_pico;
                                  });
                        auto ts2 = std::chrono::high_resolution_clock::now();
                        t_sort += std::chrono::duration<double, std::milli>(ts2 - ts1).count();

                        if (coinConfig.enable)
                        {
                            auto tc1 = std::chrono::high_resolution_clock::now();
                            processCoincidenceForChunk(
                                std::span<const Single>(mergedSingles.data(), mergedSingles.size()),
                                coinConfig,
                                coinNode,
                                ioCtx.get());
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