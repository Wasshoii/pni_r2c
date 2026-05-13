#pragma once
#include <pni/io/IO.hpp>
#include <filesystem>
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include <pni/tools/Parallel.hpp>
#include <pni/core/CommonDataType.hpp>
#include <pni/process/Acquisition.hpp>
#include <pni/detector/BDM50100.hpp>
#include "core/io/IOAdapter.hpp"
//#include <pni/bdm_system/BDM50100Array.hpp>

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
        openpni::distributed::coreio::RawDataFileReader inputFile(
            openpni::distributed::coreio::IOBackend::Latest);
        inputFile.Open(inputRawDataPath);

        const auto &info = inputFile.Info();
        auto segmentNum = info.segmentNum;
        auto channelNum = info.channelNum;

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

        openpni::distributed::coreio::RawDataWriterOptions options;
        options.backend = openpni::distributed::coreio::IOBackend::Latest;
        options.channelNum = channelNum;
        options.channelTypeNames = info.channelTypeNames;

        openpni::distributed::coreio::RawDataFileWriter outputFile(std::move(options));
        outputFile.Open(outputPath.string());

        std::cout << "Output file: " << outputPath << std::endl;
        std::cout << "Channel header count: " << channelNum << " (Preserved to maintain ID: " << channelIndexToExtract << ")" << std::endl;

        // 4. 逐段处理数据
        uint64_t totalPacketsExtracted = 0;
        uint64_t totalPacketsProcessed = 0;

        for (uint32_t segIdx = 0; segIdx < segmentNum; segIdx++)
        {
            // 读取原始段
            auto segment = inputFile.ReadSegment(segIdx, segIdx + 1);
            auto view = segment.View();

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
                    filteredChannel.push_back(channelIndexToExtract); // 保持原始通道ID

                    currentOffset += packetLength;
                }
            }

            // 创建新的 RawDataView 并写入
            openpni::RawDataView filteredView;
            filteredView.data = filteredData.data();
            filteredView.length = filteredLength.data();
            filteredView.offset = filteredOffset.data();
            filteredView.channel = filteredChannel.data();
            filteredView.count = channelPacketCount;
            filteredView.clock_ms = view.clock_ms;
            filteredView.duration_ms = view.duration_ms;
            filteredView.channelNum = channelNum;

            bool writeSuccess = outputFile.AppendSegment(filteredView);

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
        openpni::distributed::coreio::RawDataFileReader inputFile(
            openpni::distributed::coreio::IOBackend::Latest);
        inputFile.Open(inputRawDataPath);

        const auto &info = inputFile.Info();
        auto segmentNum = info.segmentNum;
        auto channelNum = info.channelNum;

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
        const auto minIt = std::min_element(channelIndices.begin(), channelIndices.end());
        const auto maxIt = std::max_element(channelIndices.begin(), channelIndices.end());
        const uint16_t minChannel = *minIt;
        const uint16_t maxChannel = *maxIt;
        const auto channelCount = channelIndices.size();

        std::string outputFileName = inputPath.stem().string();
        outputFileName += "_ch" + std::to_string(minChannel) + "-" + std::to_string(maxChannel);
        outputFileName += "_n" + std::to_string(channelCount);
        outputFileName += ".raw";
        fs::path outputPath = outputDir / outputFileName;

        openpni::distributed::coreio::RawDataWriterOptions options;
        options.backend = openpni::distributed::coreio::IOBackend::Latest;
        options.channelNum = channelNum;
        options.channelTypeNames = info.channelTypeNames;

        openpni::distributed::coreio::RawDataFileWriter outputFile(std::move(options));
        outputFile.Open(outputPath.string());
        std::cout << "Output file: " << outputPath << std::endl;

        // 4. 逐段处理数据
        uint64_t totalPacketsExtracted = 0;
        uint64_t totalPacketsProcessed = 0;

        for (uint32_t segIdx = 0; segIdx < segmentNum; segIdx++)
        {
            auto segment = inputFile.ReadSegment(segIdx, segIdx + 1);
            auto view = segment.View();

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
                    // 复制数据
                    uint64_t packetStart = view.offset[i];
                    uint16_t packetLength = view.length[i];

                    filteredData.insert(filteredData.end(),
                                        view.data + packetStart,
                                        view.data + packetStart + packetLength);

                    filteredLength.push_back(packetLength);
                    filteredOffset.push_back(currentOffset);
                    filteredChannel.push_back(view.channel[i]); // 保持原始通道ID

                    currentOffset += packetLength;
                }
            }

            // 创建新的 RawDataView 并写入
            openpni::RawDataView filteredView;
            filteredView.data = filteredData.data();
            filteredView.length = filteredLength.data();
            filteredView.offset = filteredOffset.data();
            filteredView.channel = filteredChannel.data();
            filteredView.count = channelPacketCount;
            filteredView.clock_ms = view.clock_ms;
            filteredView.duration_ms = view.duration_ms;
            filteredView.channelNum = channelNum;

            if (outputFile.AppendSegment(filteredView))
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

struct PacketPositionInfo {
  uint64_t offset;
  uint16_t length;
  uint16_t channel;
};
/**
 * @brief 将 50100 原始 UDP 包文件 + pos.bin 转换为标准 RawData 文件
 *
 * @param rawDataPath 输入 rawData.bin 路径（连续 1286 字节 UDP 包）
 * @param posPath 输入 pos.bin 路径（PacketPositionInfo 数组）
 * @param outputRawDataPath 输出 rawdata 文件路径
 * @param packetsPerSegment 每段写入的包数量
 * @param channelNumOverride 可选的通道数覆盖值（0 表示自动推断）
 * @param channelTypeName 通道类型名称（写入文件头）
 * @param forceReplace 输出文件存在时是否覆盖
 * @param reservedBytes 预留磁盘空间（必须大于0，否则不会写入）
 * @return bool 成功返回 true，失败返回 false
 */
bool convert_50100_rawdata_with_pos_to_standard(const std::string &rawDataPath,
                                                const std::string &posPath,
                                                const std::string &outputRawDataPath,
                                                uint32_t packetsPerSegment = 10000,
                                                uint16_t channelNumOverride = 0,
                                                const std::string &channelTypeName = "BDM50100",
                                                bool forceReplace = false,
                                                uint64_t reservedBytes = 1ull * 1024 * 1024)
{
    using openpni::device::bdm50100::UDP_PACKET_SIZE;

    try
    {
        if (packetsPerSegment == 0)
        {
            std::cerr << "Invalid packetsPerSegment: 0" << std::endl;
            return false;
        }

        // 1) 读取 pos.bin
        std::ifstream posFile(posPath, std::ios::binary);
        if (!posFile)
        {
            std::cerr << "Failed to open pos file: " << posPath << std::endl;
            return false;
        }
        posFile.seekg(0, std::ios::end);
        const std::streamsize posBytes = posFile.tellg();
        posFile.seekg(0, std::ios::beg);

        if (posBytes <= 0 || posBytes % sizeof(PacketPositionInfo) != 0)
        {
            std::cerr << "Invalid pos file size: " << posBytes << std::endl;
            return false;
        }

        const uint64_t posCount = static_cast<uint64_t>(posBytes / sizeof(PacketPositionInfo));
        std::vector<PacketPositionInfo> positions(posCount);
        posFile.read(reinterpret_cast<char *>(positions.data()), posBytes);
        if (!posFile)
        {
            std::cerr << "Failed to read pos file: " << posPath << std::endl;
            return false;
        }

        // 2) 打开 rawdata.bin
        std::ifstream rawFile(rawDataPath, std::ios::binary);
        if (!rawFile)
        {
            std::cerr << "Failed to open raw data file: " << rawDataPath << std::endl;
            return false;
        }
        rawFile.seekg(0, std::ios::end);
        const uint64_t rawBytes = static_cast<uint64_t>(rawFile.tellg());
        rawFile.seekg(0, std::ios::beg);

        // 3) 推断通道数
        uint16_t maxChannel = 0;
        bool hasValidChannel = false;
        for (const auto &pos : positions)
        {
            if (pos.channel == UINT16_MAX)
            {
                continue;
            }
            hasValidChannel = true;
            if (pos.channel > maxChannel)
            {
                maxChannel = pos.channel;
            }
        }
        const uint16_t channelNum = channelNumOverride > 0
                                        ? channelNumOverride
                                        : static_cast<uint16_t>(hasValidChannel ? (maxChannel + 1) : 0);

        if (channelNum == 0)
        {
            std::cerr << "Cannot infer channelNum from pos file." << std::endl;
            return false;
        }

        // 4) 初始化输出文件
        fs::path outputPath(outputRawDataPath);
        if (outputPath.has_parent_path() && !fs::exists(outputPath.parent_path()))
        {
            fs::create_directories(outputPath.parent_path());
        }
        if (fs::exists(outputPath) && !forceReplace)
        {
            std::cerr << "Output file already exists: " << outputRawDataPath << std::endl;
            return false;
        }

        openpni::io::rawdata::RawDataFileHeader header;
        header.SetChannelNum(channelNum);
        for (uint16_t i = 0; i < channelNum; ++i)
        {
            header.SetNameOfChannel(i, channelTypeName);
        }

        openpni::io::IOOptions options;
        if (reservedBytes == 0)
        {
            reservedBytes = 1;
        }
        options.SetReservedBytes(static_cast<std::size_t>(reservedBytes));
        options.SetCreatePathIfNotExist(true);
        options.SetEnableOverrideExistingFile(forceReplace);
        options.SetIOQueueSize(2);

        openpni::io::RawFileOutput output(std::move(header), std::move(options));
        output.Open(outputRawDataPath);

        // 5) 自动判断 offset 单位（字节偏移 vs 包序号）
        auto estimate_mode = [&](bool offsetIsIndex) {
            uint64_t invalid = 0;
            const uint64_t sampleCount = std::min<uint64_t>(positions.size(), 1000);
            for (uint64_t i = 0; i < sampleCount; ++i)
            {
                const auto &pos = positions[i];
                if (pos.channel == UINT16_MAX)
                {
                    continue;
                }
                const uint64_t baseOffset = offsetIsIndex ? pos.offset * UDP_PACKET_SIZE : pos.offset;
                if (baseOffset + UDP_PACKET_SIZE > rawBytes)
                {
                    invalid++;
                }
            }
            return invalid;
        };

        bool offsetIsIndex = false;
        const uint64_t invalidDirect = estimate_mode(false);
        const uint64_t invalidIndex = estimate_mode(true);
        if (invalidIndex < invalidDirect)
        {
            offsetIsIndex = true;
        }

        // 6) 分段写入
        uint64_t totalWritten = 0;
        uint64_t skippedPackets = 0;
        uint64_t skippedInvalidChannel = 0;
        uint64_t skippedOutOfRange = 0;
        for (uint64_t base = 0; base < posCount; base += packetsPerSegment)
        {
            const uint64_t end = std::min<uint64_t>(posCount, base + packetsPerSegment);
            std::vector<uint8_t> data;
            std::vector<uint16_t> length;
            std::vector<uint64_t> offset;
            std::vector<uint16_t> channel;

            data.reserve((end - base) * UDP_PACKET_SIZE);
            length.reserve(end - base);
            offset.reserve(end - base);
            channel.reserve(end - base);

            uint64_t currentOffset = 0;
            for (uint64_t i = base; i < end; ++i)
            {
                const auto &pos = positions[i];
                if (pos.channel == UINT16_MAX)
                {
                    skippedPackets++;
                    skippedInvalidChannel++;
                    continue;
                }
                const uint64_t packetOffset = offsetIsIndex ? pos.offset * UDP_PACKET_SIZE : pos.offset;
                if (packetOffset + UDP_PACKET_SIZE > rawBytes)
                {
                    skippedPackets++;
                    skippedOutOfRange++;
                    continue;
                }

                data.resize(currentOffset + UDP_PACKET_SIZE);
                rawFile.seekg(static_cast<std::streamoff>(packetOffset), std::ios::beg);
                rawFile.read(reinterpret_cast<char *>(data.data() + currentOffset), UDP_PACKET_SIZE);
                if (!rawFile)
                {
                    std::cerr << "Failed to read raw data at offset " << pos.offset << std::endl;
                    return false;
                }

                length.push_back(static_cast<uint16_t>(UDP_PACKET_SIZE));
                offset.push_back(currentOffset);
                channel.push_back(pos.channel);
                currentOffset += UDP_PACKET_SIZE;
            }

            if (channel.empty())
            {
                continue;
            }

            openpni::RawDataView view;
            view.data = data.data();
            view.length = length.data();
            view.offset = offset.data();
            view.channel = channel.data();
            view.count = channel.size();
            view.clock_ms = 0;
            view.duration_ms = 0;
            view.channelNum = channelNum;

            output.AppendSegment(view);
            totalWritten += view.count;
        }

        std::cout << "Conversion done. packets=" << posCount
              << ", written=" << totalWritten
              << ", skipped=" << skippedPackets
              << ", skippedInvalidChannel=" << skippedInvalidChannel
              << ", skippedOutOfRange=" << skippedOutOfRange
              << ", channelNum=" << channelNum
              << ", offsetMode=" << (offsetIsIndex ? "index" : "bytes")
              << std::endl;

        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in convert_50100_rawdata_with_pos_to_standard: " << e.what() << std::endl;
        return false;
    }
}

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
        openpni::io::v1::RawFileInput inputFile;
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
        openpni::io::v1::single::SingleFileInput inputFile;
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
