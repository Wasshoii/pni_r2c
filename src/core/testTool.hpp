#pragma once
#include <pni/io/IO.hpp>
#include <filesystem>
#include <iostream>
#include <vector>
#include <string>
#include <pni/experimental/tools/Parallel.hpp>

namespace fs = std::filesystem;

/**
 * @brief 从原始数据文件中提取指定通道的数据并保存到新文件
 *
 * @param inputRawDataPath 输入的原始数据文件路径
 * @param channelIndexToExtract 需要提取的通道号
 * @param outputFolderName 输出文件夹名称（可选，默认为 "channel_X_extracted"）
 * @return bool 成功返回true，失败返回false
 */
bool extract_channel_from_rawdata(
    const std::string &inputRawDataPath,
    uint16_t channelIndexToExtract,
    const std::string &outputFolderName = "")
{
    try
    {
        // 1. 打开输入文件
        openpni::io::RawFileInput inputFile;
        inputFile.open(inputRawDataPath);

        auto header = inputFile.header();
        auto segmentNum = header.segmentNum;
        auto channelNum = header.channelNum;

        std::cout << "Input file info:" << std::endl;
        std::cout << "  Total channels: " << channelNum << std::endl;
        std::cout << "  Total segments: " << segmentNum << std::endl;
        std::cout << "  Extracting channel: " << channelIndexToExtract << std::endl;

        // 检查通道号是否有效
        if (channelIndexToExtract >= channelNum)
        {
            std::cerr << "Error: Channel index " << channelIndexToExtract
                      << " is out of range (0-" << (channelNum - 1) << ")" << std::endl;
            return false;
        }

        // 2. 创建输出目录
        fs::path inputPath(inputRawDataPath);
        fs::path parentDir = inputPath.parent_path();

        std::string folderName = outputFolderName.empty()
                                     ? "channel_" + std::to_string(channelIndexToExtract) + "_extracted"
                                     : outputFolderName;

        fs::path outputDir = parentDir / folderName;

        if (!fs::exists(outputDir))
        {
            fs::create_directories(outputDir);
            std::cout << "Created output directory: " << outputDir << std::endl;
        }
        else
        {
            std::cout << "Output directory already exists: " << outputDir << std::endl;
        }

        // 3. 创建输出文件
        std::string outputFileName = inputPath.stem().string() + "_channel_" + std::to_string(channelIndexToExtract) + ".raw";
        fs::path outputPath = outputDir / outputFileName;

        openpni::io::RawFileOutput outputFile;
        outputFile.setChannelNum(1); // 只有一个通道

        // 获取原通道的类型名称
        std::string channelTypeName = inputFile.typeNameOfChannel(channelIndexToExtract);
        outputFile.setTypeNameOfChannel(0, channelTypeName); // 输出文件中该通道索引为0

        outputFile.open(outputPath.string());

        std::cout << "Output file: " << outputPath << std::endl;
        std::cout << "Channel type: " << channelTypeName << std::endl;

        // 4. 逐段处理数据
        uint64_t totalPacketsExtracted = 0;
        uint64_t totalPacketsProcessed = 0;

        for (uint32_t segIdx = 0; segIdx < segmentNum; segIdx++)
        {
            // 读取原始段
            auto segment = inputFile.readSegment(segIdx, segIdx + 1);
            auto segHeader = inputFile.segmentHeader(segIdx);
            auto view = segment.view(header, segHeader);

            totalPacketsProcessed += view.count;

            if (!view.count || !view.data)
            {
                std::cout << "Segment " << segIdx << ": No data, skipping" << std::endl;
                continue;
            }

            // 统计该通道的数据包数量
            uint64_t channelPacketCount = 0;
            for (uint64_t i = 0; i < view.count; i++)
            {
                if (view.channel[i] == channelIndexToExtract)
                {
                    channelPacketCount++;
                }
            }

            if (channelPacketCount == 0)
            {
                std::cout << "Segment " << segIdx << ": No data for channel "
                          << channelIndexToExtract << ", skipping" << std::endl;
                continue;
            }

            // 分配新的数组来存储过滤后的数据
            std::vector<uint8_t> filteredData;
            std::vector<uint16_t> filteredLength;
            std::vector<uint64_t> filteredOffset;
            std::vector<uint16_t> filteredChannel;

            filteredLength.reserve(channelPacketCount);
            filteredOffset.reserve(channelPacketCount);
            filteredChannel.reserve(channelPacketCount);

            uint64_t currentOffset = 0;

            for (uint64_t i = 0; i < view.count; i++)
            {
                if (view.channel[i] == channelIndexToExtract)
                {
                    // 复制数据
                    uint64_t packetStart = view.offset[i];
                    uint16_t packetLength = view.length[i];

                    filteredData.insert(filteredData.end(),
                                        view.data + packetStart,
                                        view.data + packetStart + packetLength);

                    filteredLength.push_back(packetLength);
                    filteredOffset.push_back(currentOffset);
                    filteredChannel.push_back(0); // 输出文件中通道索引为0

                    currentOffset += packetLength;
                }
            }

            // 创建新的 RawDataView 并写入
            openpni::process::RawDataView filteredView;
            filteredView.data = filteredData.data();
            filteredView.length = filteredLength.data();
            filteredView.offset = filteredOffset.data();
            filteredView.channel = filteredChannel.data();
            filteredView.count = channelPacketCount;
            filteredView.clock_ms = segHeader.clock;
            filteredView.duration_ms = segHeader.duration;

            bool writeSuccess = outputFile.appendSegment(filteredView);

            if (writeSuccess)
            {
                totalPacketsExtracted += channelPacketCount;
                std::cout << "Segment " << segIdx << "/" << segmentNum
                          << ": Extracted " << channelPacketCount << " packets" << std::endl;
            }
            else
            {
                std::cerr << "Failed to write segment " << segIdx << std::endl;
            }
        }

        // 5. 输出统计信息
        std::cout << "\n=== Extraction Complete ===" << std::endl;
        std::cout << "Total packets processed: " << totalPacketsProcessed << std::endl;
        std::cout << "Total packets extracted: " << totalPacketsExtracted << std::endl;
        std::cout << "Extraction ratio: "
                  << (totalPacketsProcessed > 0 ? (100.0 * totalPacketsExtracted / totalPacketsProcessed) : 0.0)
                  << "%" << std::endl;
        std::cout << "Output file: " << outputPath << std::endl;

        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in extractChannelFromRawData: " << e.what() << std::endl;
        return false;
    }
}

/**
 * @brief 从原始数据文件中提取多个通道的数据并保存到新文件
 *
 * @param inputRawDataPath 输入的原始数据文件路径
 * @param channelIndices 需要提取的通道号列表
 * @param outputFolderName 输出文件夹名称（可选）
 * @return bool 成功返回true，失败返回false
 */
bool extract_multiple_channels_from_rawdata(
    const std::string &inputRawDataPath,
    const std::vector<uint16_t> &channelIndices,
    const std::string &outputFolderName = "")
{
    if (channelIndices.empty())
    {
        std::cerr << "Error: No channels specified for extraction" << std::endl;
        return false;
    }

    try
    {
        // 1. 打开输入文件
        openpni::io::RawFileInput inputFile;
        inputFile.open(inputRawDataPath);

        auto header = inputFile.header();
        auto segmentNum = header.segmentNum;
        auto channelNum = header.channelNum;

        std::cout << "Input file info:" << std::endl;
        std::cout << "  Total channels: " << channelNum << std::endl;
        std::cout << "  Total segments: " << segmentNum << std::endl;
        std::cout << "  Extracting channels: ";
        for (auto ch : channelIndices)
        {
            std::cout << ch << " ";
        }
        std::cout << std::endl;

        // 检查通道号是否有效
        for (auto ch : channelIndices)
        {
            if (ch >= channelNum)
            {
                std::cerr << "Error: Channel index " << ch
                          << " is out of range (0-" << (channelNum - 1) << ")" << std::endl;
                return false;
            }
        }

        // 2. 创建输出目录
        fs::path inputPath(inputRawDataPath);
        fs::path parentDir = inputPath.parent_path();

        std::string folderName = outputFolderName.empty()
                                     ? "channels_extracted"
                                     : outputFolderName;

        fs::path outputDir = parentDir / folderName;

        if (!fs::exists(outputDir))
        {
            fs::create_directories(outputDir);
            std::cout << "Created output directory: " << outputDir << std::endl;
        }

        // 3. 创建输出文件
        std::string outputFileName = inputPath.stem().string() + "_extracted.raw";
        fs::path outputPath = outputDir / outputFileName;

        openpni::io::RawFileOutput outputFile;
        outputFile.setChannelNum(channelIndices.size());

        // 设置每个通道的类型
        for (size_t i = 0; i < channelIndices.size(); i++)
        {
            std::string channelTypeName = inputFile.typeNameOfChannel(channelIndices[i]);
            outputFile.setTypeNameOfChannel(i, channelTypeName);
            std::cout << "  Channel " << channelIndices[i] << " -> Output index "
                      << i << " (type: " << channelTypeName << ")" << std::endl;
        }

        outputFile.open(outputPath.string());
        std::cout << "Output file: " << outputPath << std::endl;

        // 4. 逐段处理数据
        uint64_t totalPacketsExtracted = 0;
        uint64_t totalPacketsProcessed = 0;

        for (uint32_t segIdx = 0; segIdx < segmentNum; segIdx++)
        {
            auto segment = inputFile.readSegment(segIdx, segIdx + 1);
            auto segHeader = inputFile.segmentHeader(segIdx);
            auto view = segment.view(header, segHeader);

            totalPacketsProcessed += view.count;

            if (!view.count || !view.data)
            {
                continue;
            }

            // 统计要提取的数据包数量
            uint64_t channelPacketCount = 0;
            for (uint64_t i = 0; i < view.count; i++)
            {
                for (auto ch : channelIndices)
                {
                    if (view.channel[i] == ch)
                    {
                        channelPacketCount++;
                        break;
                    }
                }
            }

            if (channelPacketCount == 0)
            {
                continue;
            }

            // 分配新的数组
            std::vector<uint8_t> filteredData;
            std::vector<uint16_t> filteredLength;
            std::vector<uint64_t> filteredOffset;
            std::vector<uint16_t> filteredChannel;

            filteredLength.reserve(channelPacketCount);
            filteredOffset.reserve(channelPacketCount);
            filteredChannel.reserve(channelPacketCount);

            uint64_t currentOffset = 0;

            for (uint64_t i = 0; i < view.count; i++)
            {
                // 检查该包是否属于要提取的通道
                auto it = std::find(channelIndices.begin(), channelIndices.end(), view.channel[i]);
                if (it != channelIndices.end())
                {
                    // 找到该通道在输出文件中的索引
                    uint16_t outputChannelIndex = std::distance(channelIndices.begin(), it);

                    // 复制数据
                    uint64_t packetStart = view.offset[i];
                    uint16_t packetLength = view.length[i];

                    filteredData.insert(filteredData.end(),
                                        view.data + packetStart,
                                        view.data + packetStart + packetLength);

                    filteredLength.push_back(packetLength);
                    filteredOffset.push_back(currentOffset);
                    filteredChannel.push_back(outputChannelIndex);

                    currentOffset += packetLength;
                }
            }

            // 创建新的 RawDataView 并写入
            openpni::process::RawDataView filteredView;
            filteredView.data = filteredData.data();
            filteredView.length = filteredLength.data();
            filteredView.offset = filteredOffset.data();
            filteredView.channel = filteredChannel.data();
            filteredView.count = channelPacketCount;
            filteredView.clock_ms = segHeader.clock;
            filteredView.duration_ms = segHeader.duration;

            if (outputFile.appendSegment(filteredView))
            {
                totalPacketsExtracted += channelPacketCount;
                if (segIdx % 10 == 0 || segIdx == segmentNum - 1)
                {
                    std::cout << "Segment " << segIdx << "/" << segmentNum
                              << ": Extracted " << channelPacketCount << " packets" << std::endl;
                }
            }
        }

        // 5. 输出统计信息
        std::cout << "\n=== Extraction Complete ===" << std::endl;
        std::cout << "Total packets processed: " << totalPacketsProcessed << std::endl;
        std::cout << "Total packets extracted: " << totalPacketsExtracted << std::endl;
        std::cout << "Extraction ratio: "
                  << (totalPacketsProcessed > 0 ? (100.0 * totalPacketsExtracted / totalPacketsProcessed) : 0.0)
                  << "%" << std::endl;
        std::cout << "Output file: " << outputPath << std::endl;

        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in extractMultipleChannelsFromRawData: " << e.what() << std::endl;
        return false;
    }
}

/**
 * @brief RawData 文件信息结构体
 */
struct RawDataFileInfo
{
    uint16_t channelNum{0};       // 通道数量
    uint32_t segmentNum{0};       // 段数量
    uint64_t totalCount{0};       // 总数据包数量
    uint64_t totalClock_ms{0};    // 总时钟时间（毫秒）- 所有段的clock累加
    uint64_t totalDuration_ms{0}; // 总持续时间（毫秒）- 所有段的duration累加
    uint64_t firstClock_ms{0};    // 第一段的起始时钟（毫秒）
    uint64_t lastClock_ms{0};     // 最后一段的起始时钟（毫秒）

    void print() const
    {
        std::cout << "\n=== RawData File Information ===" << std::endl;
        std::cout << "Channel Number: " << channelNum << std::endl;
        std::cout << "Segment Number: " << segmentNum << std::endl;
        std::cout << "Total Packet Count: " << totalCount << std::endl;
        std::cout << "Total Duration (ms): " << totalDuration_ms << std::endl;
        std::cout << "First Segment Clock (ms): " << firstClock_ms << std::endl;
        std::cout << "Last Segment Clock (ms): " << lastClock_ms << std::endl;
        std::cout << "================================\n"
                  << std::endl;
    }
};

/**
 * @brief 获取 RawData 文件的详细信息
 *
 * @param rawDataPath RawData 文件路径
 * @return RawDataFileInfo 文件信息结构体，如果读取失败则所有字段为0
 */
RawDataFileInfo getRawDataFileInfo(const std::string &rawDataPath)
{
    RawDataFileInfo info;

    try
    {
        openpni::io::RawFileInput inputFile;
        inputFile.open(rawDataPath);

        auto header = inputFile.header();
        info.channelNum = header.channelNum;
        info.segmentNum = header.segmentNum;

        // 遍历所有段获取详细信息
        for (uint32_t i = 0; i < header.segmentNum; i++)
        {
            auto segHeader = inputFile.segmentHeader(i);

            info.totalCount += segHeader.count;
            info.totalDuration_ms += segHeader.duration;

            if (i == 0)
            {
                info.firstClock_ms = segHeader.clock;
            }
            if (i == header.segmentNum - 1)
            {
                info.lastClock_ms = segHeader.clock;
            }
        }

        info.totalClock_ms = info.lastClock_ms - info.firstClock_ms +
                             (header.segmentNum > 0 ? inputFile.segmentHeader(header.segmentNum - 1).duration : 0);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error reading RawData file info: " << e.what() << std::endl;
        return RawDataFileInfo{}; // 返回空信息
    }

    return info;
}

/**
 * @brief 获取 RawData 文件信息并打印
 *
 * @param rawDataPath RawData 文件路径
 * @return bool 成功返回true，失败返回false
 */
bool print_rawdata_file_info(const std::string &rawDataPath)
{
    auto info = getRawDataFileInfo(rawDataPath);

    if (info.segmentNum == 0)
    {
        std::cerr << "Failed to read RawData file: " << rawDataPath << std::endl;
        return false;
    }

    info.print();
    return true;
}

/**
 * @brief Single 文件信息结构体
 */
struct SingleFileInfo
{
    uint16_t version{0};           // 文件版本
    uint32_t segmentNum{0};        // 段数量
    uint32_t crystalNum{0};        // 晶体数量
    uint8_t bytes4CrystalIndex{0}; // 每个晶体索引占用的字节数
    uint8_t bytes4TimeValue{0};    // 每个时间值占用的字节数
    uint8_t bytes4Energy{0};       // 每个能量值占用的字节数
    uint64_t totalCount{0};        // 总单事件数量
    uint64_t totalDuration_ms{0};  // 总持续时间（毫秒）- 所有段的duration累加
    uint64_t firstClock_ms{0};     // 第一段的起始时钟（毫秒）
    uint64_t lastClock_ms{0};      // 最后一段的起始时钟（毫秒）

    void print() const
    {
        std::cout << "\n=== Single File Information ===" << std::endl;
        std::cout << "Version: " << version << std::endl;
        std::cout << "Segment Number: " << segmentNum << std::endl;
        std::cout << "Crystal Number: " << crystalNum << std::endl;
        std::cout << "Bytes for Crystal Index: " << static_cast<int>(bytes4CrystalIndex) << std::endl;
        std::cout << "Bytes for Time Value: " << static_cast<int>(bytes4TimeValue) << std::endl;
        std::cout << "Bytes for Energy: " << static_cast<int>(bytes4Energy) << std::endl;
        std::cout << "Total Single Event Count: " << totalCount << std::endl;
        std::cout << "Total Duration (ms): " << totalDuration_ms << std::endl;
        std::cout << "First Segment Clock (ms): " << firstClock_ms << std::endl;
        std::cout << "Last Segment Clock (ms): " << lastClock_ms << std::endl;
        std::cout << "================================\n"
                  << std::endl;
    }
};

/**
 * @brief 获取 Single 文件的详细信息
 *
 * @param singlePath Single 文件路径
 * @return SingleFileInfo 文件信息结构体，如果读取失败则所有字段为0
 */
SingleFileInfo getSingleFileInfo(const std::string &singlePath)
{
    SingleFileInfo info;

    try
    {
        openpni::io::single::SingleFileInput inputFile;
        inputFile.open(singlePath);

        auto header = inputFile.header();
        info.version = header.version;
        info.segmentNum = header.segmentNum;
        info.crystalNum = header.cystalNum;
        info.bytes4CrystalIndex = header.bytes4CrystalIndex;
        info.bytes4TimeValue = header.bytes4TimeValue;
        info.bytes4Energy = header.bytes4Energy;

        // 遍历所有段获取详细信息
        for (uint32_t i = 0; i < header.segmentNum; i++)
        {
            auto segHeader = inputFile.segmentHeader(i);

            info.totalCount += segHeader.count;
            info.totalDuration_ms += segHeader.duration;

            if (i == 0)
            {
                info.firstClock_ms = segHeader.clock;
            }
            if (i == header.segmentNum - 1)
            {
                info.lastClock_ms = segHeader.clock;
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error reading Single file info: " << e.what() << std::endl;
        return SingleFileInfo{}; // 返回空信息
    }

    return info;
}

/**
 * @brief 获取 Single 文件信息并打印
 *
 * @param singlePath Single 文件路径
 * @return bool 成功返回true，失败返回false
 */
bool print_single_file_info(const std::string &singlePath)
{
    auto info = getSingleFileInfo(singlePath);

    if (info.segmentNum == 0)
    {
        std::cerr << "Failed to read Single file: " << singlePath << std::endl;
        return false;
    }

    info.print();
    return true;
}
