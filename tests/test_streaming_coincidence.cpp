/**
 * @file test_streaming_coincidence.cpp
 * @brief 流式符合计算系统测试
 *
 * 测试 StreamingTimeAligner 的核心功能：
 * 1. 多节点数据接收
 * 2. 时间对齐
 * 3. 符合计算
 */

#include "../src/core/streaming-coin/StreamingCoincidence.hpp"
#include <pni/io/IO.hpp>
#include <iostream>
#include <random>
#include <chrono>
#include <iomanip>
#include <functional>
#include <filesystem>

using namespace openpni::distributed::streaming;
namespace fs = std::filesystem;

// ==================== 文件读取工具函数 ====================

/**
 * @brief 从 SingleSegmentBytes 解析数据到 GlobalSingle_t 数组
 *
 * @param segBytes 段数据字节
 * @param fileHeader 文件头信息
 * @param count 事件数量
 * @param destBuffer 目标缓冲区
 */
void parseSingleSegmentBytesToBuffer(
    const openpni::io::single::SingleSegmentBytes &segBytes,
    const openpni::io::single::SingleFileHeader &fileHeader,
    uint64_t count,
    openpni::basic::GlobalSingle_t *destBuffer)
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

    // 并行解析
    for (uint64_t i = 0; i < count; i++)
    {
        destBuffer[i].globalCrystalIndex = getCrystalIndex(i);
        destBuffer[i].timeValue_pico = getTimeValue(i);
        destBuffer[i].energy = getEnergy(i);
    }
}

/**
 * @brief 从 Single 文件读取数据并写入 NodeRingBuffer
 *
 * 此函数模拟分布式节点的数据生产过程：
 * 1. 打开 Single 文件
 * 2. 逐段读取数据
 * 3. 转换为 TimestampedSingleChunk 格式
 * 4. 推送到节点缓冲区
 *
 * @param buffer 目标缓冲区
 * @param nodeId 节点ID
 * @param filePath Single 文件路径
 * @param simulateDelay 是否模拟网络延迟（毫秒），0表示不模拟
 * @return 成功返回 true
 */
bool loadSingleFileToBuffer(
    NodeRingBuffer *buffer,
    uint16_t nodeId,
    const std::string &filePath,
    uint32_t simulateDelay = 0)
{
    if (!buffer)
    {
        std::cerr << "[loadSingleFileToBuffer] Error: buffer is null" << std::endl;
        return false;
    }

    if (!fs::exists(filePath))
    {
        std::cerr << "[loadSingleFileToBuffer] Error: file not found: " << filePath << std::endl;
        return false;
    }

    try
    {
        // 打开 Single 文件
        openpni::io::single::SingleFileInput inputFile;
        inputFile.open(filePath);

        auto fileHeader = inputFile.header();
        uint32_t segmentNum = fileHeader.segmentNum;

        std::cout << "[loadSingleFileToBuffer] Node " << nodeId
                  << " loading file: " << filePath << std::endl;
        std::cout << "  Segments: " << segmentNum << std::endl;
        std::cout << "  Format: crystalIndex=" << (int)fileHeader.bytes4CrystalIndex
                  << "B, timeValue=" << (int)fileHeader.bytes4TimeValue
                  << "B, energy=" << (int)fileHeader.bytes4Energy << "B" << std::endl;

        uint64_t totalSinglesLoaded = 0;

        // 逐段读取并推送到缓冲区
        for (uint32_t segIdx = 0; segIdx < segmentNum; ++segIdx)
        {
            // 读取段数据
            auto segBytes = inputFile.readSegment(segIdx);
            auto segHeader = inputFile.segmentHeader(segIdx);

            if (segHeader.count == 0)
            {
                std::cout << "  Segment " << segIdx << ": empty, skipping" << std::endl;
                continue;
            }

            // 创建 TimestampedSingleChunk
            TimestampedSingleChunk chunk;
            chunk.nodeId = nodeId;
            chunk.chunkId = segIdx;
            chunk.computerClock_ms = segHeader.clock; // 使用段的 clock 作为计算机时钟
            chunk.duration_ms = segHeader.duration;

            // 分配空间并解析数据
            chunk.singles.resize(segHeader.count);
            parseSingleSegmentBytesToBuffer(segBytes, fileHeader, segHeader.count, chunk.singles.data());

            // 推送到缓冲区
            if (!buffer->push(std::move(chunk), 5000)) // 5秒超时
            {
                std::cerr << "[loadSingleFileToBuffer] Node " << nodeId
                          << " failed to push segment " << segIdx << std::endl;
                return false;
            }

            totalSinglesLoaded += segHeader.count;

            // 模拟网络延迟
            if (simulateDelay > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(simulateDelay));
            }
        }

        std::cout << "[loadSingleFileToBuffer] Node " << nodeId
                  << " loaded " << totalSinglesLoaded << " singles from "
                  << segmentNum << " segments" << std::endl;

        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "[loadSingleFileToBuffer] Error: " << e.what() << std::endl;
        return false;
    }
}

/**
 * @brief 文件加载线程函数
 *
 * 用于多线程并行加载多个节点的数据
 */
void fileLoaderThread(
    NodeRingBuffer *buffer,
    uint16_t nodeId,
    const std::string &filePath,
    uint32_t simulateDelay,
    std::atomic<bool> *success)
{
    bool result = loadSingleFileToBuffer(buffer, nodeId, filePath, simulateDelay);
    if (success)
    {
        success->store(result);
    }
}
namespace fs = std::filesystem;

// ==================== 测试工具函数 ====================

/**
 * @brief 生成模拟的单事件数据
 */
std::vector<openpni::basic::GlobalSingle_t> generateMockSingles(
    size_t count,
    uint64_t baseTime_pico,
    uint64_t timeRange_pico,
    uint32_t maxCrystalIndex,
    std::mt19937 &rng)
{
    std::vector<openpni::basic::GlobalSingle_t> singles;
    singles.reserve(count);

    std::uniform_int_distribution<uint64_t> timeDist(0, timeRange_pico);
    std::uniform_int_distribution<uint32_t> crystalDist(0, maxCrystalIndex - 1);
    std::uniform_real_distribution<float> energyDist(350000, 650000); // 350-650 keV

    for (size_t i = 0; i < count; ++i)
    {
        openpni::basic::GlobalSingle_t s;
        s.globalCrystalIndex = crystalDist(rng);
        s.energy = energyDist(rng);
        s.timeValue_pico = baseTime_pico + timeDist(rng);
        singles.push_back(s);
    }

    return singles;
}

/**
 * @brief 数据生产者线程函数
 * 模拟分布式节点向缓冲区推送数据
 */
void dataProducerThread(
    NodeRingBuffer *buffer,
    uint16_t nodeId,
    size_t numChunks,
    size_t singlesPerChunk,
    uint64_t chunkDuration_pico,
    uint32_t maxCrystalIndex)
{
    std::mt19937 rng(nodeId * 12345 + std::random_device{}());

    uint64_t currentTime_pico = 0;

    for (size_t chunkIdx = 0; chunkIdx < numChunks; ++chunkIdx)
    {
        // 生成数据块
        TimestampedSingleChunk chunk;
        chunk.nodeId = nodeId;
        chunk.chunkId = chunkIdx;
        chunk.computerClock_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();
        chunk.duration_ms = chunkDuration_pico / 1'000'000'000; // pico -> ms

        // 生成单事件数据
        chunk.singles = generateMockSingles(
            singlesPerChunk,
            currentTime_pico,
            chunkDuration_pico,
            maxCrystalIndex,
            rng);

        // 推送到缓冲区
        if (!buffer->push(std::move(chunk)))
        {
            std::cerr << "[Producer " << nodeId << "] Failed to push chunk " << chunkIdx << std::endl;
            break;
        }

        // 更新时间
        currentTime_pico += chunkDuration_pico;

        // 模拟数据产生间隔
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::cout << "[Producer " << nodeId << "] Finished producing " << numChunks << " chunks" << std::endl;
}

// ==================== 测试用例 ====================

/**
 * @brief 测试1：NodeRingBuffer 基本功能
 */
bool testNodeRingBuffer()
{
    std::cout << "\n=== Test 1: NodeRingBuffer Basic Functionality ===" << std::endl;

    NodeRingBuffer buffer(0, 10);

    // 测试空缓冲区
    if (!buffer.empty())
    {
        std::cerr << "FAIL: New buffer should be empty" << std::endl;
        return false;
    }

    // 测试 push
    TimestampedSingleChunk chunk;
    chunk.nodeId = 0;
    chunk.chunkId = 0;
    chunk.computerClock_ms = 1000;
    chunk.duration_ms = 100;
    // GlobalSingle_t: {globalCrystalIndex, timeValue_pico, energy}
    chunk.singles.push_back({0, 1000000, 500000.0f});
    chunk.singles.push_back({1, 2000000, 500000.0f});

    if (!buffer.push(std::move(chunk)))
    {
        std::cerr << "FAIL: Push to empty buffer should succeed" << std::endl;
        return false;
    }

    if (buffer.empty())
    {
        std::cerr << "FAIL: Buffer should not be empty after push" << std::endl;
        return false;
    }

    // 测试 front
    const auto *peeked = buffer.front();
    if (!peeked || peeked->chunkId != 0)
    {
        std::cerr << "FAIL: front() should return the pushed chunk" << std::endl;
        return false;
    }

    // 测试 tryPop
    auto popped = buffer.tryPop();
    if (!popped.has_value())
    {
        std::cerr << "FAIL: TryPop should return the chunk" << std::endl;
        return false;
    }

    if (!buffer.empty())
    {
        std::cerr << "FAIL: Buffer should be empty after pop" << std::endl;
        return false;
    }

    std::cout << "PASS: NodeRingBuffer basic functionality" << std::endl;
    return true;
}

/**
 * @brief 测试2：多线程生产者
 */
bool testMultiProducerBuffer()
{
    std::cout << "\n=== Test 2: Multi-Producer Buffer ===" << std::endl;

    const size_t numNodes = 4;
    const size_t chunksPerNode = 10;
    const size_t singlesPerChunk = 100;
    const uint64_t chunkDuration_pico = 100'000'000'000; // 100ms
    const uint32_t maxCrystalIndex = 1000;

    std::vector<std::unique_ptr<NodeRingBuffer>> buffers;
    for (size_t i = 0; i < numNodes; ++i)
    {
        buffers.push_back(std::make_unique<NodeRingBuffer>(i, 50));
    }

    // 启动生产者线程
    std::vector<std::thread> producers;
    for (size_t i = 0; i < numNodes; ++i)
    {
        producers.emplace_back(dataProducerThread,
                               buffers[i].get(), i, chunksPerNode, singlesPerChunk,
                               chunkDuration_pico, maxCrystalIndex);
    }

    // 等待生产者完成
    for (auto &t : producers)
    {
        t.join();
    }

    // 验证数据
    size_t totalChunks = 0;
    size_t totalSingles = 0;

    for (size_t i = 0; i < numNodes; ++i)
    {
        while (auto chunk = buffers[i]->tryPop())
        {
            totalChunks++;
            totalSingles += chunk->singles.size();
        }
    }

    std::cout << "Total chunks: " << totalChunks
              << " (expected: " << numNodes * chunksPerNode << ")" << std::endl;
    std::cout << "Total singles: " << totalSingles
              << " (expected: " << numNodes * chunksPerNode * singlesPerChunk << ")" << std::endl;

    if (totalChunks == numNodes * chunksPerNode &&
        totalSingles == numNodes * chunksPerNode * singlesPerChunk)
    {
        std::cout << "PASS: Multi-producer buffer" << std::endl;
        return true;
    }
    else
    {
        std::cerr << "FAIL: Data count mismatch" << std::endl;
        return false;
    }
}

/**
 * @brief 测试3：TimestampedSingleChunk 时间范围计算
 */
bool testTimeRangeCalculation()
{
    std::cout << "\n=== Test 3: Time Range Calculation ===" << std::endl;

    TimestampedSingleChunk chunk;
    // GlobalSingle_t: {globalCrystalIndex, timeValue_pico, energy}
    chunk.singles.push_back({0, 1000, 500000.0f});
    chunk.singles.push_back({1, 5000, 500000.0f});
    chunk.singles.push_back({2, 3000, 500000.0f});
    chunk.singles.push_back({3, 2000, 500000.0f});

    chunk.updateTimeRange();

    if (chunk.minTime_pico != 1000 || chunk.maxTime_pico != 5000)
    {
        std::cerr << "FAIL: Time range calculation incorrect" << std::endl;
        std::cerr << "  Expected: [1000, 5000], Got: ["
                  << chunk.minTime_pico << ", " << chunk.maxTime_pico << "]" << std::endl;
        return false;
    }

    std::cout << "PASS: Time range calculation" << std::endl;
    return true;
}

/**
 * @brief 测试4：配置创建
 */
bool testConfigCreation()
{
    std::cout << "\n=== Test 4: Config Creation ===" << std::endl;

    // BDM2 配置
    auto bdm2Config = createBDM2AlignerConfig("/tmp/bdm2_output");
    if (bdm2Config.channelNum != 48 || bdm2Config.crystalsPerChannel != 169 * 4)
    {
        std::cerr << "FAIL: BDM2 config incorrect" << std::endl;
        return false;
    }
    std::cout << "  BDM2: " << bdm2Config.channelNum << " channels, "
              << bdm2Config.crystalsPerChannel << " crystals/channel" << std::endl;

    // BDMBiD 配置
    auto bdmbidConfig = createBDMBiDAlignerConfig("/tmp/bdmbid_output");
    if (bdmbidConfig.channelNum != 4 || bdmbidConfig.crystalsPerChannel != 400 * 8)
    {
        std::cerr << "FAIL: BDMBiD config incorrect" << std::endl;
        return false;
    }
    std::cout << "  BDMBiD: " << bdmbidConfig.channelNum << " channels, "
              << bdmbidConfig.crystalsPerChannel << " crystals/channel" << std::endl;

    std::cout << "PASS: Config creation" << std::endl;
    return true;
}

/**
 * @brief 测试5：共享内存池功能
 */
bool testSharedMemoryPool()
{
    std::cout << "\n=== Test 5: SharedMemoryPool Functionality ===" << std::endl;

    // 创建 100KB 限制的内存池
    const size_t maxMemory = 100 * 1024; // 100 KB
    SharedMemoryPool pool(maxMemory);

    // 测试基本分配
    if (!pool.tryAllocate(10 * 1024, 1000))
    {
        std::cerr << "FAIL: First allocation should succeed" << std::endl;
        return false;
    }

    auto status = pool.getStatus();
    if (status.usedBytes != 10 * 1024)
    {
        std::cerr << "FAIL: Used memory incorrect after allocation" << std::endl;
        return false;
    }

    std::cout << "  After 10KB allocation: " << status.usedBytes / 1024 << " KB used, "
              << (status.usageRatio * 100) << "% usage" << std::endl;

    // 测试多次分配
    for (int i = 0; i < 8; ++i)
    {
        if (!pool.tryAllocate(10 * 1024, 1000))
        {
            std::cerr << "FAIL: Allocation " << i << " should succeed" << std::endl;
            return false;
        }
    }

    status = pool.getStatus();
    std::cout << "  After 90KB total allocation: " << status.usedBytes / 1024 << " KB used, "
              << (status.usageRatio * 100) << "% usage" << std::endl;

    // 测试超限分配（应该超时失败，因为只剩10KB）
    auto start = std::chrono::steady_clock::now();
    bool allocated = pool.tryAllocate(20 * 1024, 100); // 尝试分配20KB，只等100ms
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();

    if (allocated)
    {
        std::cerr << "FAIL: Over-limit allocation should timeout" << std::endl;
        return false;
    }
    std::cout << "  Over-limit allocation timed out after " << elapsed << " ms (expected ~100ms)" << std::endl;

    // 测试释放后分配
    pool.release(50 * 1024); // 释放 50KB
    status = pool.getStatus();
    std::cout << "  After releasing 50KB: " << status.usedBytes / 1024 << " KB used" << std::endl;

    if (!pool.tryAllocate(20 * 1024, 1000))
    {
        std::cerr << "FAIL: Allocation after release should succeed" << std::endl;
        return false;
    }

    status = pool.getStatus();
    std::cout << "  After allocating 20KB more: " << status.usedBytes / 1024 << " KB used" << std::endl;

    pool.printStatus();

    std::cout << "PASS: SharedMemoryPool functionality" << std::endl;
    return true;
}

/**
 * @brief 测试6：带内存池的 NodeRingBuffer
 */
bool testBufferWithMemoryPool()
{
    std::cout << "\n=== Test 6: NodeRingBuffer with Memory Pool ===" << std::endl;

    // 创建 50KB 限制的内存池
    const size_t maxMemory = 50 * 1024;
    SharedMemoryPool pool(maxMemory);

    // 创建带内存池的缓冲区
    NodeRingBuffer buffer(0, 100, &pool);

    // 每个 single 约 16 字节 (4 + 8 + 4)，100 个 = 1600 字节
    const size_t singlesPerChunk = 100;
    const size_t expectedChunkSize = singlesPerChunk * sizeof(openpni::basic::GlobalSingle_t);
    std::cout << "  Expected chunk memory: ~" << expectedChunkSize << " bytes" << std::endl;

    // 推送多个数据块，直到内存池接近满
    int pushedCount = 0;
    for (int i = 0; i < 50; ++i)
    {
        TimestampedSingleChunk chunk;
        chunk.nodeId = 0;
        chunk.chunkId = i;
        chunk.computerClock_ms = i * 100;
        chunk.duration_ms = 100;

        // 生成单事件数据
        for (size_t j = 0; j < singlesPerChunk; ++j)
        {
            // GlobalSingle_t: {globalCrystalIndex, timeValue_pico, energy}
            chunk.singles.push_back({static_cast<unsigned>(j), static_cast<uint64_t>(i * 1000000 + j * 1000), 500000.0f});
        }

        if (!buffer.push(std::move(chunk), 100))
        { // 100ms 超时，缓冲区或内存池满时返回 false
            std::cout << "  Push blocked after " << pushedCount << " chunks (memory pool full)" << std::endl;
            break;
        }
        pushedCount++;
    }

    auto status = pool.getStatus();
    std::cout << "  Memory pool: " << status.usedBytes / 1024 << " KB / "
              << status.maxBytes / 1024 << " KB (" << (status.usageRatio * 100) << "%)" << std::endl;
    std::cout << "  Buffer memory: " << buffer.getBufferMemoryBytes() / 1024 << " KB" << std::endl;

    if (pushedCount < 1)
    {
        std::cerr << "FAIL: Should have pushed at least 1 chunk" << std::endl;
        return false;
    }

    // 弹出一些数据块
    int poppedCount = 0;
    while (auto chunk = buffer.tryPop())
    {
        poppedCount++;
        if (poppedCount >= 5)
            break;
    }
    std::cout << "  Popped " << poppedCount << " chunks" << std::endl;

    status = pool.getStatus();
    std::cout << "  Memory pool after pop: " << status.usedBytes / 1024 << " KB ("
              << (status.usageRatio * 100) << "%)" << std::endl;

    // 验证内存已释放
    if (status.usedBytes >= maxMemory * 0.9)
    {
        std::cerr << "FAIL: Memory should have been released after pop" << std::endl;
        return false;
    }

    pool.printStatus();

    std::cout << "PASS: NodeRingBuffer with memory pool" << std::endl;
    return true;
}

/**
 * @brief 测试7：使用处理好的数据测试coincidence计算
 *
 * 此测试从磁盘读取 Single 文件，写入 NodeRingBuffer，
 * 然后通过 StreamingTimeAligner 进行符合计算
 */
bool testStreamingCoincidenceComputation()
{
    std::cout << "\n=== Test 7: Streaming Coincidence Computation ===" << std::endl;

    // 配置文件路径
    std::vector<std::string> files = {
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/split/singles_dist0.single",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/split/singles_dist1.single",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/split/singles_dist2.single"};

    // 检查文件是否存在
    std::vector<std::string> validFiles;
    for (const auto &f : files)
    {
        if (fs::exists(f))
        {
            validFiles.push_back(f);
            std::cout << "  Found file: " << f << std::endl;
        }
        else
        {
            std::cout << "  File not found (skipped): " << f << std::endl;
        }
    }

    if (validFiles.empty())
    {
        std::cout << "  No valid input files found. Test skipped." << std::endl;
        return true; // 没有文件不算失败
    }

    size_t nodeCount = validFiles.size();
    std::cout << "  Using " << nodeCount << " nodes/files" << std::endl;

    // 创建输出目录
    std::string outputDir = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/coin/streaming_coin_test_output";
    fs::create_directories(outputDir);

    // 创建 TimeAligner 配置
    TimeAlignerConfig alignerConfig = createBDM2AlignerConfig(outputDir);
    alignerConfig.networkLatencyMargin_pico = 10'000'000'000;      // 10ms 网络延迟裕量
    alignerConfig.processingIntervalMs = 100;                      // 100ms 处理间隔
    alignerConfig.maxChunksPerNode = 200;                          // 每节点最大200个chunk
    alignerConfig.maxTotalMemoryBytes = 1ULL * 1024 * 1024 * 1024; // 1GB 内存限制

    std::cout << "  Config:" << std::endl;
    std::cout << "    Channel num: " << alignerConfig.channelNum << std::endl;
    std::cout << "    Crystals per channel: " << alignerConfig.crystalsPerChannel << std::endl;
    // std::cout << "    Time window: " << coinProtocol.timeWindow_ps << " ps" << std::endl;
    // std::cout << "    Delay time: " << coinProtocol.delayTime_ps << " ps" << std::endl;
    std::cout << "    Output dir: " << outputDir << std::endl;

    // 创建 StreamingTimeAligner
    StreamingTimeAligner aligner(alignerConfig, nodeCount);

    // 启动处理线程
    aligner.start();
    std::cout << "  StreamingTimeAligner started" << std::endl;

    // 创建多个线程并行加载数据文件
    std::vector<std::thread> loaderThreads;
    std::vector<std::atomic<bool>> loadResults(nodeCount);

    for (size_t i = 0; i < nodeCount; ++i)
    {
        loadResults[i].store(false);
        loaderThreads.emplace_back(
            fileLoaderThread,
            aligner.getNodeBuffer(i),
            static_cast<uint16_t>(i),
            validFiles[i],
            10, // 10ms 模拟延迟，测试乱序处理
            &loadResults[i]);
    }

    // 等待所有加载线程完成
    std::cout << "  Waiting for file loaders to complete..." << std::endl;
    for (auto &t : loaderThreads)
    {
        t.join();
    }

    // 检查加载结果
    bool allLoaded = true;
    for (size_t i = 0; i < nodeCount; ++i)
    {
        if (!loadResults[i].load())
        {
            std::cerr << "  Node " << i << " failed to load data" << std::endl;
            allLoaded = false;
        }
    }

    if (!allLoaded)
    {
        aligner.stop(false);
        std::cerr << "FAIL: Some files failed to load" << std::endl;
        return false;
    }

    std::cout << "  All files loaded successfully" << std::endl;

    // 等待处理完成（给一些时间让数据被处理）
    std::cout << "  Waiting for processing to complete..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 停止并等待所有数据处理完毕
    aligner.stop(true);

    // 获取统计信息
    const auto &stats = aligner.getStatistics();
    std::cout << "\n  Processing Statistics:" << std::endl;
    std::cout << "    Singles processed: " << stats.totalSinglesProcessed.load() << std::endl;
    std::cout << "    Prompt pairs: " << stats.totalPromptPairs.load() << std::endl;
    std::cout << "    Delay pairs: " << stats.totalDelayPairs.load() << std::endl;
    std::cout << "    Chunks processed: " << stats.chunksProcessed.load() << std::endl;
    std::cout << "    Avg processing time: " << stats.avgProcessingTime_ms.load() << " ms" << std::endl;

    // 检查输出文件
    std::string promptFile = outputDir + "/prompt.lmf";
    std::string delayFile = outputDir + "/delay.lmf";

    bool promptExists = fs::exists(promptFile);
    bool delayExists = fs::exists(delayFile);

    std::cout << "\n  Output Files:" << std::endl;
    if (promptExists)
    {
        std::cout << "    Prompt: " << promptFile << " ("
                  << fs::file_size(promptFile) << " bytes)" << std::endl;
    }
    if (delayExists)
    {
        std::cout << "    Delay: " << delayFile << " ("
                  << fs::file_size(delayFile) << " bytes)" << std::endl;
    }

    // 验证处理结果
    if (stats.totalSinglesProcessed.load() > 0)
    {
        std::cout << "PASS: Streaming Coincidence Computation" << std::endl;
        return true;
    }
    else
    {
        std::cerr << "FAIL: No singles were processed" << std::endl;
        return false;
    }
}

/**
 * @brief 测试8：单独测试文件加载到缓冲区功能
 */
bool testFileToBufferLoading()
{
    std::cout << "\n=== Test 8: File to Buffer Loading ===" << std::endl;

    // 使用一个测试文件
    std::string testFile = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/split/singles_dist0.single";

    if (!fs::exists(testFile))
    {
        std::cout << "  Test file not found: " << testFile << std::endl;
        std::cout << "  Test skipped." << std::endl;
        return true;
    }

    // 创建共享内存池和缓冲区
    // 测试文件有 456 段，每段约 135K singles，每个 single 16 字节
    // 总数据量约 456 * 135000 * 16 = ~1GB，需要足够大的内存池
    const size_t maxMemory = 2ULL * 1024 * 1024 * 1024; // 2GB
    SharedMemoryPool memPool(maxMemory);
    NodeRingBuffer buffer(0, 500, &memPool); // 允许 500 个 chunks

    std::cout << "  Loading file: " << testFile << std::endl;

    // 加载文件
    bool loadResult = loadSingleFileToBuffer(&buffer, 0, testFile, 0);

    if (!loadResult)
    {
        std::cerr << "FAIL: Failed to load file to buffer" << std::endl;
        return false;
    }

    // 统计缓冲区内容
    size_t totalChunks = buffer.size();
    size_t totalSingles = 0;
    uint64_t minTime = UINT64_MAX;
    uint64_t maxTime = 0;

    std::cout << "  Buffer status:" << std::endl;
    std::cout << "    Chunks in buffer: " << totalChunks << std::endl;
    std::cout << "    Buffer memory: " << buffer.getBufferMemoryBytes() / (1024.0 * 1024.0) << " MB" << std::endl;

    memPool.printStatus();

    // 读取并验证数据
    while (auto chunk = buffer.tryPop())
    {
        totalSingles += chunk->singles.size();
        if (!chunk->singles.empty())
        {
            minTime = std::min(minTime, chunk->minTime_pico);
            maxTime = std::max(maxTime, chunk->maxTime_pico);
        }
    }

    std::cout << "  Data summary:" << std::endl;
    std::cout << "    Total singles: " << totalSingles << std::endl;
    std::cout << "    Time range: [" << minTime << ", " << maxTime << "] pico" << std::endl;
    std::cout << "    Time span: " << (maxTime - minTime) / 1e12 << " seconds" << std::endl;

    if (totalSingles > 0)
    {
        std::cout << "PASS: File to Buffer Loading" << std::endl;
        return true;
    }
    else
    {
        std::cerr << "FAIL: No singles loaded" << std::endl;
        return false;
    }
}
// ==================== 主函数 ====================

int main()
{
    std::cout << "=======================================" << std::endl;
    std::cout << " Streaming Coincidence System Tests" << std::endl;
    std::cout << "=======================================" << std::endl;

    int passed = 0;
    int failed = 0;

    // 运行基础测试
    if (testNodeRingBuffer())
        passed++;
    else
        failed++;
    if (testMultiProducerBuffer())
        passed++;
    else
        failed++;
    if (testTimeRangeCalculation())
        passed++;
    else
        failed++;
    if (testConfigCreation())
        passed++;
    else
        failed++;
    if (testSharedMemoryPool())
        passed++;
    else
        failed++;
    if (testBufferWithMemoryPool())
        passed++;
    else
        failed++;
    // 运行文件加载测试
    if (testFileToBufferLoading())
        passed++;
    else
        failed++;

    // 运行完整的流式符合计算测试
    if (testStreamingCoincidenceComputation())
        passed++;
    else
        failed++;

    // 总结
    std::cout << "\n=======================================" << std::endl;
    std::cout << " Test Summary: " << passed << " passed, " << failed << " failed" << std::endl;
    std::cout << "=======================================" << std::endl;

    return failed > 0 ? 1 : 0;
}
