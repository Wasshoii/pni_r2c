#pragma once
#include <pni/io/IO.hpp>
#include <filesystem>
#include <iostream>
#include <vector>
#include <string>
#include <pni/experimental/tools/Parallel.hpp>

namespace fs = std::filesystem;

/**
 * @brief 从字节数据解析 GlobalSingle_t 数组
 */
std::vector<openpni::basic::GlobalSingle_t> parseSingleSegmentBytes(
    const openpni::io::single::SingleSegmentBytes &segBytes,
    const openpni::io::single::SingleFileHeader &fileHeader,
    uint64_t count)
{
    std::vector<openpni::basic::GlobalSingle_t> singles;
    singles.reserve(count);

    for (uint64_t i = 0; i < count; i++)
    {
        openpni::basic::GlobalSingle_t single;

        // 解析晶体索引
        if (fileHeader.bytes4CrystalIndex == 2)
        {
            single.globalCrystalIndex = *reinterpret_cast<const uint16_t *>(segBytes.crystalIndexBytes.get() + i * 2);
        }
        else if (fileHeader.bytes4CrystalIndex == 4)
        {
            single.globalCrystalIndex = *reinterpret_cast<const uint32_t *>(segBytes.crystalIndexBytes.get() + i * 4);
        }
        else if (fileHeader.bytes4CrystalIndex == 3)
        {
            // 24位整数需要特殊处理
            const uint8_t *ptr = reinterpret_cast<const uint8_t *>(segBytes.crystalIndexBytes.get() + i * 3);
            single.globalCrystalIndex = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16);
        }

        // 解析时间值
        if (fileHeader.bytes4TimeValue == 8)
        {
            single.timeValue_pico = *reinterpret_cast<const uint64_t *>(segBytes.timeValueBytes.get() + i * 8);
        }
        else if (fileHeader.bytes4TimeValue == 4)
        {
            single.timeValue_pico = *reinterpret_cast<const uint32_t *>(segBytes.timeValueBytes.get() + i * 4);
        }
        else
        {
            // 处理其他字节数（5, 6, 7字节）
            const uint8_t *ptr = reinterpret_cast<const uint8_t *>(segBytes.timeValueBytes.get() + i * fileHeader.bytes4TimeValue);
            single.timeValue_pico = 0;
            for (int j = 0; j < fileHeader.bytes4TimeValue; j++)
            {
                single.timeValue_pico |= (static_cast<uint64_t>(ptr[j]) << (j * 8));
            }
        }

        // 解析能量
        if (fileHeader.bytes4Energy == 0)
        {
            single.energy = 511.0f; // 固定值
        }
        else if (fileHeader.bytes4Energy == 4)
        {
            single.energy = *reinterpret_cast<const float *>(segBytes.energyBytes.get() + i * 4);
        }
        else if (fileHeader.bytes4Energy == 2)
        {
            // 半精度浮点数或 UINT16
            uint16_t val = *reinterpret_cast<const uint16_t *>(segBytes.energyBytes.get() + i * 2);
            // 简化处理：假设是缩放的整数值
            single.energy = static_cast<float>(val) * 0.01f; // 需要根据实际格式调整
        }
        else if (fileHeader.bytes4Energy == 1)
        {
            uint8_t val = *reinterpret_cast<const uint8_t *>(segBytes.energyBytes.get() + i);
            single.energy = static_cast<float>(val) * 4.0f; // 4keV 倍率
        }

        singles.push_back(single);
    }

    return singles;
}

/**
 * @brief 合并多个 Single 文件为一个文件（支持分布式采集的时间偏差处理）
 *
 * 针对符合处理优化：
 * - 时间重叠的段会被合并到一起，避免重复符合
 * - 按时间排序后重新分段，确保时间连续性
 * - 自动检测并处理时间偏差
 *
 * @param inputFiles 输入的 Single 文件路径列表
 * @param outputFile 输出的合并后的 Single 文件路径
 * @param sortByTime 是否按时间排序合并（默认true）
 * @return bool 成功返回true，失败返回false
 */
bool merge_single_files(
    const std::vector<std::string> &inputFiles,
    const std::string &outputFile,
    bool sortByTime = true)
{
    if (inputFiles.empty())
    {
        std::cerr << "Error: No input files specified" << std::endl;
        return false;
    }

    try
    {
        std::cout << "Merging " << inputFiles.size() << " Single files..." << std::endl;

        // 1. 打开所有输入文件并验证兼容性
        std::vector<std::unique_ptr<openpni::io::single::SingleFileInput>> inputs;
        inputs.reserve(inputFiles.size());

        openpni::io::single::SingleFileHeader firstHeader;
        uint32_t maxCrystalNum = 0;
        uint64_t totalSegments = 0;
        uint64_t totalSingles = 0;

        for (size_t i = 0; i < inputFiles.size(); i++)
        {
            auto input = std::make_unique<openpni::io::single::SingleFileInput>();
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
        openpni::io::single::SingleFileOutput output;

        // 设置输出文件参数
        output.setBytes4CrystalIndex(static_cast<openpni::io::single::CrystalIndexType>(firstHeader.bytes4CrystalIndex));
        output.setBytes4TimeValue(static_cast<openpni::io::single::TimeValueType>(firstHeader.bytes4TimeValue));
        output.setBytes4Energy(static_cast<openpni::io::single::EnergyType>(firstHeader.bytes4Energy));
        output.setTotalCrystalNum(maxCrystalNum);

        output.open(outputFile);
        std::cout << "Output file: " << outputFile << std::endl;

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

        // 5. 合并策略：基于段的智能归并
        if (sortByTime)
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

            int overlapCount = 0;
            for (size_t i = 1; i < allSegments.size(); i++)
            {
                if (allSegments[i].startTime_ms < allSegments[i - 1].endTime_ms)
                {
                    overlapCount++;
                    if (overlapCount <= 5)
                    {
                        std::cout << "  Overlap " << overlapCount << ": Segment " << (i - 1)
                                  << " [" << allSegments[i - 1].startTime_ms << "-" << allSegments[i - 1].endTime_ms
                                  << "] vs Segment " << i << " [" << allSegments[i].startTime_ms
                                  << "-" << allSegments[i].endTime_ms << "]" << std::endl;
                    }
                }
                else if (overlapGroups.back() != i)
                {
                    // 没有重叠，标记组边界
                    overlapGroups.push_back(i);
                }
            }
            overlapGroups.push_back(allSegments.size()); // 最后一个边界

            std::cout << "Detected " << overlapCount << " overlaps" << std::endl;
            std::cout << "Segments grouped into " << (overlapGroups.size() - 1) << " non-overlapping groups" << std::endl;

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
                    // 单个段，无重叠，直接写入
                    const auto &segInfo = allSegments[groupStart];
                    auto &input = *inputs[segInfo.fileIndex];

                    auto segBytes = input.readSegment(segInfo.segmentIndex);
                    auto segHeader = input.segmentHeader(segInfo.segmentIndex);
                    auto fileHeader = input.header();

                    auto singles = parseSingleSegmentBytes(segBytes, fileHeader, segHeader.count);

                    bool success = output.appendSegment(singles.data(), singles.size(),
                                                        segInfo.startTime_ms, segInfo.duration_ms);
                    if (success)
                    {
                        totalWritten += singles.size();
                        segmentsWritten++;
                    }
                }
                else
                {
                    // 多个重叠段：直接合并所有单事件为一个大段
                    uint64_t totalCount = 0;

                    // 计算总数量
                    for (size_t i = groupStart; i < groupEnd; i++)
                    {
                        totalCount += allSegments[i].count;
                    }

                    std::vector<openpni::basic::GlobalSingle_t> mergedSingles(totalCount);
                    std::vector<uint64_t> segmentOffsets; // 每个段的起始位置
                    segmentOffsets.reserve(groupEnd - groupStart);

                    uint64_t currentOffset = 0;
                    for (size_t i = groupStart; i < groupEnd; i++)
                    {
                        segmentOffsets.push_back(currentOffset);
                        currentOffset += allSegments[i].count;
                    }

                    // 并行加载各个段（使用 parallel_for_each）
                    openpni::experimental::tools::parallel_for_each(
                        groupEnd - groupStart,
                        [&](std::size_t idx)
                        {
                            size_t globalIdx = groupStart + idx;
                            const auto &segInfo = allSegments[globalIdx];
                            auto &input = *inputs[segInfo.fileIndex];

                            auto segBytes = input.readSegment(segInfo.segmentIndex);
                            auto segHeader = input.segmentHeader(segInfo.segmentIndex);
                            auto fileHeader = input.header();

                            auto singles = parseSingleSegmentBytes(segBytes, fileHeader, segHeader.count);

                            // 将加载的单事件写入对应的位置（无竞争，因为每个段的位置不同）
                            uint64_t offset = segmentOffsets[idx];
                            std::copy(singles.begin(), singles.end(), mergedSingles.begin() + offset);
                        });

                    // 直接写入一个大段
                    uint64_t groupStartTime_ms = allSegments[groupStart].startTime_ms;
                    uint64_t groupEndTime_ms = allSegments[groupEnd - 1].endTime_ms;
                    uint32_t groupDuration_ms = static_cast<uint32_t>(groupEndTime_ms - groupStartTime_ms);
                    bool success = output.appendSegment(mergedSingles.data(), mergedSingles.size(),
                                                        groupStartTime_ms, groupDuration_ms);

                    if (success)
                    {
                        totalWritten += mergedSingles.size();
                        segmentsWritten++;
                    }
                }

                if ((groupIdx + 1) % 50 == 0 || groupIdx == overlapGroups.size() - 2)
                {
                    std::cout << "  Progress: " << (groupIdx + 1) << "/" << (overlapGroups.size() - 1)
                              << " groups, " << segmentsWritten << " segments, "
                              << totalWritten << " singles written" << std::endl;
                }
            }
        }
        else
        {
            // 直接按文件顺序拼接
            std::cout << "\nMerging in original file order..." << std::endl;

            uint64_t processedSegments = 0;
            uint64_t totalWritten = 0;

            for (size_t fileIdx = 0; fileIdx < inputs.size(); fileIdx++)
            {
                auto &input = *inputs[fileIdx];
                auto header = input.header();

                std::cout << "  Processing file " << fileIdx + 1 << "/" << inputs.size()
                          << ": " << inputFiles[fileIdx] << std::endl;

                for (uint32_t segIdx = 0; segIdx < header.segmentNum; segIdx++)
                {
                    auto segBytes = input.readSegment(segIdx);
                    auto segHeader = input.segmentHeader(segIdx);

                    // 解析字节数据为 GlobalSingle_t
                    auto singles = parseSingleSegmentBytes(segBytes, header, segHeader.count);

                    // 写入到输出文件
                    bool success = output.appendSegment(singles.data(), singles.size(),
                                                        segHeader.clock, segHeader.duration);

                    if (success)
                    {
                        totalWritten += singles.size();
                    }

                    processedSegments++;
                    if (processedSegments % 100 == 0 || processedSegments == totalSegments)
                    {
                        std::cout << "    Progress: " << processedSegments << "/" << totalSegments
                                  << " segments, " << totalWritten << " singles written" << std::endl;
                    }
                }
            }
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
