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
#include <iostream>
#include <random>
#include <chrono>
#include <iomanip>

using namespace openpni::distributed::streaming;

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
    chunk.singles.push_back({0, 500000, 1000000});
    chunk.singles.push_back({1, 500000, 2000000});

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

// ==================== 主函数 ====================

int main()
{
    std::cout << "=======================================" << std::endl;
    std::cout << " Streaming Coincidence System Tests" << std::endl;
    std::cout << "=======================================" << std::endl;

    int passed = 0;
    int failed = 0;

    // 运行测试
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

    // 总结
    std::cout << "\n=======================================" << std::endl;
    std::cout << " Test Summary: " << passed << " passed, " << failed << " failed" << std::endl;
    std::cout << "=======================================" << std::endl;

    return failed > 0 ? 1 : 0;
}
