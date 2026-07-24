#pragma once
#include <pni/io/IO.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <array>
#include <limits>
#include <cmath>
#include <vector>
#include <string>
#include <algorithm>
#include <cstring>
#include <iomanip>
#include <map>
#include <optional>
#include <sstream>
#include <set>
#include <pni/tools/Parallel.hpp>
#include <pni/core/CommonDataType.hpp>
#include <pni/detector/BDM50100.hpp>
#include <pni/io/ListmodeIO.hpp>
// 注意：getSingleFileInfo() 读取的是一种独立的旧版 "Single" 二进制格式（与 R2S/Coincidence
// 主流程使用的 Listmode 格式无关），当前 pni 库只在 v1 命名空间下导出了该格式的实现，
// 因此这里仍需直接使用 v1 头文件，与本文件其余部分已收敛到 Latest 的 IO 无关。
#include <pni/io/v1/SingleIO_v1.hpp>
#include "core/io/IOAdapter.hpp"
#include "core/merge-and-coin/MergeAndCoin.hpp"
#include "core/r2s/R2S.hpp"
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
        openpni::distributed::coreio::RawDataFileReader inputFile;
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
        openpni::distributed::coreio::RawDataFileReader inputFile;
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
 * @brief 直接将 50100 原始 rawdata 转为标准 RawData 文件
 *
 * @param inputRawPath 输入 done_YYYYMMDD-<clock>.bin 路径（含 510 字节头）
 * @param outputRawPath 输出标准 rawdata 路径
 * @param clock_ms 写入段头的时钟（毫秒）
 * @param packetsPerSegment 每段写入的包数量，0 表示一个文件一段
 * @param channelNum 通道数量（930 单环为 144；9120 四环合并输出为 576）
 * @param channelTypeName 通道类型名称
 * @param forceReplace 输出文件存在时是否覆盖
 * @param reservedBytes 预留磁盘空间（必须大于0，否则不会写入）
 * @param ringOffset BDM 硬件环号偏移量（默认为 0，表示不偏移），写入 UDP 包 srcChannel 高 8 位。
 *                   930 单环数据内部占用硬件环 2/3/4（共 144 通道）；模拟 9120 第 k 个物理环时
 *                   可设 ringOffset = k （k=0,1,2,3），使全局 bdmId 落在 [k*144, k*144+143]。
 *                   合并四环数据时 channelNum 应设为 576。
 * @return bool 成功返回 true，失败返回 false
 */
bool convert_50100_original_rawdata_to_standard(const std::string &inputRawPath,
                                                const std::string &outputRawPath,
                                                uint64_t clock_ms,
                                                uint32_t packetsPerSegment = 0,
                                                uint16_t channelNum = 144,
                                                const std::string &channelTypeName = "BDM50100",
                                                bool forceReplace = true,
                                                uint64_t reservedBytes = 1ull * 1024 * 1024,
                                                uint16_t ringOffset = 0)
{
    using openpni::device::bdm50100::UDP_PACKET_SIZE;
    using openpni::device::bdm50100::UDP_RAWDATA_SIZE;
    using openpni::device::bdm50100::UDP_UNUSED_SIZE;

    auto dump_bytes = [](const uint8_t *data, std::size_t count) {
        std::ios::fmtflags f(std::cout.flags());
        std::cout << std::hex << std::setfill('0');
        for (std::size_t i = 0; i < count; ++i)
        {
            std::cout << std::setw(2) << static_cast<int>(data[i]);
            if (i + 1 < count)
            {
                std::cout << ' ';
            }
        }
        std::cout.flags(f);
    };

    struct DataFrame50100Old
    {
        uint8_t data[UDP_RAWDATA_SIZE];
        uint16_t srcChannel;
    };

    constexpr int kBdmPerHardwareRing = 48;

    auto mapSrcChannelToBdmId = [](uint16_t srcChannel) -> int {
        const int ring = static_cast<int>(srcChannel >> 8);
        const int ip = static_cast<int>(srcChannel & 0xFF);
        return (ring - 2) * kBdmPerHardwareRing + (ip - 1);
    };

    auto applyRingOffset = [](uint16_t srcChannel, uint16_t ringOffset, uint16_t *outSrcChannel) -> bool {
        const int ring = static_cast<int>(srcChannel >> 8);
        const int ip = static_cast<int>(srcChannel & 0xFF);
        if (ringOffset == 0)
        {
            *outSrcChannel = srcChannel;
            return true;
        }

        const int newRing = ring + static_cast<int>(ringOffset) * 3;
        if (newRing < 2 || newRing > 255)
        {
            std::cerr << "Ring offset overflow: ring=" << ring
                      << ", ringOffset=" << ringOffset
                      << ", newRing=" << newRing
                      << ", srcChannel=" << srcChannel << std::endl;
            return false;
        }

        *outSrcChannel = static_cast<uint16_t>((static_cast<uint16_t>(newRing) << 8) |
                                               static_cast<uint16_t>(ip & 0xFF));
        return true;
    };

    try
    {
        std::ifstream input(inputRawPath, std::ios::binary);
        if (!input)
        {
            std::cerr << "Failed to open raw data file: " << inputRawPath << std::endl;
            return false;
        }

        std::array<char, 510> header{};
        input.read(header.data(), static_cast<std::streamsize>(header.size()));
        if (!input)
        {
            std::cerr << "Failed to read raw data header: " << inputRawPath << std::endl;
            return false;
        }

        input.seekg(0, std::ios::end);
        const std::streamsize fileBytes = input.tellg();
        const std::streamsize dataBytes = fileBytes - static_cast<std::streamsize>(header.size());
        if (dataBytes <= 0 || dataBytes % static_cast<std::streamsize>(sizeof(DataFrame50100Old)) != 0)
        {
            std::cerr << "Invalid raw data size: " << fileBytes << std::endl;
            return false;
        }
        const uint64_t frameCount = static_cast<uint64_t>(dataBytes / sizeof(DataFrame50100Old));
        if (frameCount == 0)
        {
            std::cerr << "Empty raw data file: " << inputRawPath << std::endl;
            return false;
        }

        if (packetsPerSegment == 0)
        {
            packetsPerSegment = static_cast<uint32_t>(std::min<uint64_t>(frameCount, UINT32_MAX));
        }

        fs::path outputPath(outputRawPath);
        if (outputPath.has_parent_path() && !fs::exists(outputPath.parent_path()))
        {
            fs::create_directories(outputPath.parent_path());
        }
        if (fs::exists(outputPath) && !forceReplace)
        {
            std::cerr << "Output file already exists: " << outputRawPath << std::endl;
            return false;
        }

        openpni::io::rawdata::RawDataFileHeader headerOut;
        headerOut.SetChannelNum(channelNum);
        for (uint16_t i = 0; i < channelNum; ++i)
        {
            headerOut.SetNameOfChannel(i, channelTypeName);
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

        openpni::io::RawFileOutput output(std::move(headerOut), std::move(options));
        output.Open(outputRawPath);

        const uint64_t totalFrames = frameCount;
        const uint64_t totalSegments = (totalFrames + packetsPerSegment - 1) / packetsPerSegment;
        input.seekg(static_cast<std::streamoff>(header.size()), std::ios::beg);

        uint64_t processed = 0;
        uint64_t invalidChannel = 0;
        bool dumpedSample = false;
        for (uint64_t seg = 0; seg < totalSegments; ++seg)
        {
            const uint64_t remaining = totalFrames - processed;
            const uint64_t frameThisSeg = std::min<uint64_t>(remaining, packetsPerSegment);

            std::vector<DataFrame50100Old> frames(frameThisSeg);
            input.read(reinterpret_cast<char *>(frames.data()),
                       static_cast<std::streamsize>(frameThisSeg * sizeof(DataFrame50100Old)));
            if (!input)
            {
                std::cerr << "Failed to read raw frames at segment " << seg << std::endl;
                return false;
            }

            std::vector<uint8_t> data(frameThisSeg * UDP_PACKET_SIZE);
            std::vector<uint16_t> length(frameThisSeg, static_cast<uint16_t>(UDP_PACKET_SIZE));
            std::vector<uint64_t> offset(frameThisSeg, 0);
            std::vector<uint16_t> channel(frameThisSeg, 0);

            for (uint64_t i = 0; i < frameThisSeg; ++i)
            {
                const auto &frame = frames[i];
                const uint64_t packetOffset = i * UDP_PACKET_SIZE;
                offset[i] = packetOffset;

                uint16_t outSrcChannel = 0;
                if (!applyRingOffset(frame.srcChannel, ringOffset, &outSrcChannel))
                {
                    std::cerr << "Failed to apply ringOffset at frame " << (processed + i) << std::endl;
                    return false;
                }

                uint8_t *dst = data.data() + packetOffset;
                std::memset(dst, 0, UDP_UNUSED_SIZE);
                std::memcpy(dst + UDP_UNUSED_SIZE, frame.data, UDP_RAWDATA_SIZE);
                std::memcpy(dst + UDP_UNUSED_SIZE + UDP_RAWDATA_SIZE, &outSrcChannel, sizeof(uint16_t));

                const int bdmId = mapSrcChannelToBdmId(outSrcChannel);

                if (bdmId < 0 + ringOffset * channelNum || bdmId >= static_cast<int>(channelNum) + ringOffset * channelNum)
                {
                    invalidChannel++;
                    std::cerr << "Invalid BDM ID: " << bdmId
                              << " (srcChannel=" << frame.srcChannel
                              << ", outSrcChannel=" << outSrcChannel
                              << ", ringOffset=" << ringOffset
                              << ") at frame " << (processed + i) << std::endl;
                    return false;
                }

                channel[i] = static_cast<uint16_t>(bdmId);
            }

            if (!dumpedSample)
            {
                const std::size_t sampleCount = std::min<std::size_t>(3, channel.size());
                std::cout << "Sample packets (input vs output)" << std::endl;
                if (ringOffset != 0)
                {
                    std::cout << "  ringOffset=" << ringOffset << std::endl;
                }
                for (std::size_t i = 0; i < sampleCount; ++i)
                {
                    const auto &frame = frames[i];
                    uint16_t outSrcChannel = 0;
                    applyRingOffset(frame.srcChannel, ringOffset, &outSrcChannel);
                    const int bdmId = mapSrcChannelToBdmId(outSrcChannel);
                    const uint8_t *outPacket = data.data() + i * UDP_PACKET_SIZE;

                    std::cout << "  idx=" << (processed + i)
                              << ", srcChannel=" << frame.srcChannel
                              << ", outSrcChannel=" << outSrcChannel
                              << ", bdmId=" << bdmId
                              << ", in[0:15]=";
                    dump_bytes(frame.data, 16);
                    std::cout << ", out[0:15]=";
                    dump_bytes(outPacket, 16);
                    std::cout << std::endl;
                }
                dumpedSample = true;
            }

            openpni::RawDataView view;
            view.data = data.data();
            view.length = length.data();
            view.offset = offset.data();
            view.channel = channel.data();
            view.count = channel.size();
            view.clock_ms = clock_ms;
            view.duration_ms = 0;
            view.channelNum = channelNum;

            output.AppendSegment(view);
            processed += frameThisSeg;
        }

        if (invalidChannel > 0)
        {
            std::cerr << "Invalid channel count: " << invalidChannel << std::endl;
            return false;
        }

        std::cout << "Conversion done. frames=" << frameCount
                  << ", segments=" << totalSegments
                  << ", output=" << outputRawPath << std::endl;
        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in convert_50100_original_rawdata_to_standard: " << e.what() << std::endl;
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
        openpni::distributed::coreio::RawDataFileReader inputFile;
        inputFile.Open(rawDataPath);

        const auto &header = inputFile.Info();
        info.channelNum = header.channelNum;
        info.segmentNum = header.segmentNum;

        uint32_t lastDuration_ms = 0;
        // 遍历所有段获取详细信息
        for (uint32_t i = 0; i < header.segmentNum; i++)
        {
            auto segment = inputFile.ReadSegment(i, i + 1);
            auto view = segment.View();

            info.totalCount += view.count;
            info.totalDuration_ms += view.duration_ms;

            if (i == 0)
            {
                info.firstClock_ms = view.clock_ms;
            }
            if (i == header.segmentNum - 1)
            {
                info.lastClock_ms = view.clock_ms;
                lastDuration_ms = view.duration_ms;
            }
        }

        info.totalClock_ms = info.lastClock_ms - info.firstClock_ms +
                             (header.segmentNum > 0 ? lastDuration_ms : 0);
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

/**
 * @brief 读取 singles 文件并导出纯 Single 数据（不包含任何头部或段头）
 *
 * @param singlePath 输入的 singles 文件路径
 * @param outputSuffix 输出文件后缀（默认 "_payload.single"）
 * @return bool 成功返回 true，失败返回 false
 */
bool export_singles_payload_only(
    const std::string &singlePath,
    const std::string &outputSuffix = "_payload.nlm")
{
    try
    {
        openpni::io::listmode::ListmodeFileInput inputFile;
        inputFile.Open(singlePath);

        const auto &header = inputFile.Header();
        if (header.FileTypeName() != openpni::io::listmode::fields::file_type_single_listmode)
        {
            std::cerr << "Error: Not a single listmode file: " << singlePath << std::endl;
            return false;
        }

        const auto segmentNum = inputFile.SegmentNum();
        if (segmentNum == 0)
        {
            std::cerr << "Error: Single file has no segments: " << singlePath << std::endl;
            return false;
        }

        fs::path inputPath(singlePath);
        fs::path outputPath = inputPath.parent_path() /
                              (inputPath.stem().string() + outputSuffix);

        std::ofstream output(outputPath, std::ios::binary | std::ios::trunc);
        if (!output.is_open())
        {
            std::cerr << "Error: Failed to open output file: " << outputPath << std::endl;
            return false;
        }

        uint64_t totalSingles = 0;

        std::vector<openpni::Single> buffer;
        for (uint32_t segIdx = 0; segIdx < segmentNum; ++segIdx)
        {
            auto segment = inputFile.ReadSegment(segIdx, segIdx + 1);
            const auto data = segment.GetHAnyData();
            if (!data.channel_index1 || !data.local_crystal_index1 || !data.absolute_timestamp1_100fs)
            {
                std::cerr << "Error: Segment " << segIdx << " missing required single fields" << std::endl;
                return false;
            }

            const uint64_t count = data.count;
            if (count == 0)
            {
                continue;
            }

            buffer.resize(count);
            for (uint64_t i = 0; i < count; ++i)
            {
                auto &single = buffer[i];
                single.channelIndex = data.channel_index1[i];
                single.crystalIndex = data.local_crystal_index1[i];
                single.timevalue_100fs = data.absolute_timestamp1_100fs[i];
                single.energy = data.energy1[i];
            }

            output.write(reinterpret_cast<const char *>(buffer.data()),
                         static_cast<std::streamsize>(buffer.size() * sizeof(openpni::Single)));

            if (!output)
            {
                std::cerr << "Error: Failed to write segment " << segIdx << " to " << outputPath << std::endl;
                return false;
            }

            totalSingles += buffer.size();
        }

        std::cout << "Exported " << totalSingles << " singles to " << outputPath << std::endl;
        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in export_singles_payload_only: " << e.what() << std::endl;
        return false;
    }
}

/**
 * @brief 目标 Listmode 头部参数（与 ReadFileHeadFn.m 的 Type=1 对齐）
 */
struct TargetListmodeHeader
{
    std::string magic{"RRRRAAAAYYYYSSSS"};
    uint16_t headCrc{0};
    uint32_t commonInfoLength{44};
    uint16_t type{1};
    std::string softVer{"CS001.7.0.260211"};
    uint32_t headLength{510};

    uint32_t deviceInfoLength{96};
    std::string device{"DigitMI 930 "};
    std::string serial{};
    uint16_t axisDetectors{3};
    uint16_t transDetectors{48};
    uint16_t detectorRings{72};
    uint16_t detectorChannels{12};
    uint16_t ipCount{48};
    uint16_t ipStart{1};
    uint16_t chCount{288};
    uint16_t chStart{1};
    std::array<float, 8> mvtThreds{{0}};
    std::array<float, 3> mvtParams{{0}};

    uint32_t studyInfoLength{360};
    uint16_t isotope{0};
    float activity{0.0f};
    std::string injectTime{};
    std::string time{};
    uint16_t duration{0};
    float timeWindow{2.0f};
    float delayWindow{100.0f};
    float xTalkWindow{1002.0f};
    std::array<uint32_t, 2> energyWindow{{0, 0}};
    uint16_t positionWindow{13};
    uint16_t corrected{3};
    float tablePosition{0.0f};
    float tableHeight{0.0f};
    float petCtSpacing{0.0f};
    uint16_t tableCount{0};
    uint16_t tableIndex{0};
    float scanLengthPerTable{0.0f};
    std::string patientId{};
    std::string studyId{};
    std::string patientName{};
    std::string patientSex{};
    float patientHeight{0.0f};
    float patientWeight{0.0f};

    uint32_t dataInfoLength{10};
    uint32_t dataLength{0};
    uint16_t dataCrc{0};
};

static void write_padded_string(std::ofstream &out, const std::string &value, std::size_t size)
{
    std::string clipped = value;
    if (clipped.size() > size)
    {
        clipped.resize(size);
    }
    out.write(clipped.data(), static_cast<std::streamsize>(clipped.size()));
    if (clipped.size() < size)
    {
        const std::string padding(size - clipped.size(), '\0');
        out.write(padding.data(), static_cast<std::streamsize>(padding.size()));
    }
}

template <typename T>
static void write_le(std::ofstream &out, const T &value)
{
    out.write(reinterpret_cast<const char *>(&value), sizeof(T));
}

static std::streampos write_target_listmode_header(std::ofstream &out, const TargetListmodeHeader &h)
{
    write_padded_string(out, h.magic, 16);
    write_le(out, h.headCrc);
    write_le(out, h.commonInfoLength);
    write_le(out, h.type);
    write_padded_string(out, h.softVer, 16);
    write_le(out, h.headLength);

    write_le(out, h.deviceInfoLength);
    write_padded_string(out, h.device, 16);
    write_padded_string(out, h.serial, 16);
    write_le(out, h.axisDetectors);
    write_le(out, h.transDetectors);
    write_le(out, h.detectorRings);
    write_le(out, h.detectorChannels);
    write_le(out, h.ipCount);
    write_le(out, h.ipStart);
    write_le(out, h.chCount);
    write_le(out, h.chStart);
    for (const auto &v : h.mvtThreds)
    {
        write_le(out, v);
    }
    for (const auto &v : h.mvtParams)
    {
        write_le(out, v);
    }

    write_le(out, h.studyInfoLength);
    write_le(out, h.isotope);
    write_le(out, h.activity);
    write_padded_string(out, h.injectTime, 16);
    write_padded_string(out, h.time, 16);
    write_le(out, h.duration);
    write_le(out, h.timeWindow);
    write_le(out, h.delayWindow);
    write_le(out, h.xTalkWindow);
    write_le(out, h.energyWindow[0]);
    write_le(out, h.energyWindow[1]);
    write_le(out, h.positionWindow);
    write_le(out, h.corrected);
    write_le(out, h.tablePosition);
    write_le(out, h.tableHeight);
    write_le(out, h.petCtSpacing);
    write_le(out, h.tableCount);
    write_le(out, h.tableIndex);
    write_le(out, h.scanLengthPerTable);
    write_padded_string(out, h.patientId, 64);
    write_padded_string(out, h.studyId, 64);
    write_padded_string(out, h.patientName, 128);
    write_padded_string(out, h.patientSex, 8);
    write_le(out, h.patientHeight);
    write_le(out, h.patientWeight);

    write_le(out, h.dataInfoLength);
    const std::streampos dataLengthPos = out.tellp();
    write_le(out, h.dataLength);
    write_le(out, h.dataCrc);

    const std::streampos currentPos = out.tellp();
    const std::streampos targetPos = static_cast<std::streampos>(h.headLength);
    if (currentPos < targetPos)
    {
        const std::size_t padSize = static_cast<std::size_t>(targetPos - currentPos);
        const std::string padding(padSize, '\0');
        out.write(padding.data(), static_cast<std::streamsize>(padding.size()));
    }
    else if (currentPos > targetPos)
    {
        std::cerr << "Warning: Listmode header size (" << currentPos
                  << ") exceeds HeadLength (" << h.headLength << ")" << std::endl;
    }

    return dataLengthPos;
}

/**
 * @brief 将 Single 文件转换为目标 Listmode 格式（16 字节/条：IP、CH、Energy、Time）
 *        Energy 从 eV 转换为 keV
 *
 * @param singlePath 输入 Single 文件路径
 * @param outputPath 输出目标 Listmode 文件路径
 * @param header 目标文件头参数（可按需修改）
 * @param ipBase IP 起始值（默认 1）
 * @param chBase CH 起始值（默认 1）
 * @param energyScale 能量缩放系数（默认 0.001，把 eV 转为 keV）
 * @param timeScale 时间缩放系数（默认 0.001，把 ps 转为 ns）
 * @return bool 成功返回 true，失败返回 false
 */
bool convert_single_to_RS_listmode(const std::string &singlePath,
                                       const std::string &outputPath,
                                       TargetListmodeHeader header = TargetListmodeHeader{},
                                       uint16_t ipBase = 1,
                                       uint16_t chBase = 1,
                                       double energyScale = 1,
                                       double timeScale = 0.0001)
{
    try
    {
        openpni::io::listmode::ListmodeFileInput inputFile;
        inputFile.Open(singlePath);

        const auto &srcHeader = inputFile.Header();
        if (srcHeader.FileTypeName() != openpni::io::listmode::fields::file_type_single_listmode)
        {
            std::cerr << "Error: Not a single listmode file: " << singlePath << std::endl;
            return false;
        }

        const auto segmentNum = inputFile.SegmentNum();
        if (segmentNum == 0)
        {
            std::cerr << "Error: Single file has no segments: " << singlePath << std::endl;
            return false;
        }

        const auto fieldsInUse = srcHeader.FieldsInUse();
        if ((fieldsInUse & openpni::io::listmode::SupportedFields::energy1) == 0)
        {
            std::cerr << "Warning: source single file has no energy1 field enabled." << std::endl;
        }

        std::ofstream output(outputPath, std::ios::binary | std::ios::trunc);
        if (!output.is_open())
        {
            std::cerr << "Error: Failed to open output file: " << outputPath << std::endl;
            return false;
        }

        const auto dataLengthPos = write_target_listmode_header(output, header);

        struct TargetListmodeEvent
        {
            uint16_t ip;
            uint16_t ch;
            float energy;
            double time;
        };

        uint64_t totalSingles = 0;
        uint64_t skippedInvalid = 0;
        uint64_t missingEnergy = 0;
        uint64_t nonFiniteEnergy = 0;
        uint64_t finiteRawCount = 0;
        long double rawEnergySum = 0.0;
        double rawEnergyMin = std::numeric_limits<double>::infinity();
        double rawEnergyMax = -std::numeric_limits<double>::infinity();
        double minEnergy = std::numeric_limits<double>::infinity();
        double maxEnergy = -std::numeric_limits<double>::infinity();
        std::vector<TargetListmodeEvent> buffer;

        for (uint32_t segIdx = 0; segIdx < segmentNum; ++segIdx)
        {
            auto segment = inputFile.ReadSegment(segIdx, segIdx + 1);
            const auto singles = openpni::distributed::coin::readSinglesFromSegment(segment);
            if (singles.empty())
            {
                continue;
            }

            buffer.clear();
            buffer.reserve(singles.size());
            for (const auto &single : singles)
            {
                const uint16_t chIndex = single.channelIndex;
                const uint16_t crystalIndex = single.crystalIndex;
                if ((chIndex & 0x8000u) != 0 || (crystalIndex & 0x8000u) != 0)
                {
                    skippedInvalid++;
                    continue;
                }

                TargetListmodeEvent evt{};
                // PNI BDM ID 映射到 RS IP：BDM ID 从 0 开始，每 48 个 BDM ID 映射到一个环，IP 从 2.1(ipv4 低16位，其中高8位为环数，低8为通道号) 开始
                const uint16_t bdmId = single.channelIndex;
                constexpr uint16_t kBdmPerRing = 48;
                const uint16_t ring = static_cast<uint16_t>(bdmId / kBdmPerRing + 2);
                const uint16_t ip = static_cast<uint16_t>(bdmId % kBdmPerRing + 1);
                const uint16_t ipAddr = static_cast<uint16_t>(((ring & 0xFFu) << 8) | (ip & 0xFFu));
                evt.ip = static_cast<uint16_t>(ipAddr + ipBase - 1);
                
                // PNI CH 映射到 RS CH：每 36 个 PNI CH 映射到 37 个 RS CH，分为 8 组，RS CH每一组最后一个为虚拟通道，不使用
                const uint16_t pniCh = single.crystalIndex;
                constexpr uint16_t kPniChannelsPerGroup = 36;
                constexpr uint16_t kRsChannelsPerGroup = 37;
                constexpr uint16_t kGroupCount = 8;
                if (pniCh >= kPniChannelsPerGroup * kGroupCount)
                {
                    skippedInvalid++;
                    continue;
                }
                const uint16_t group = static_cast<uint16_t>(pniCh / kPniChannelsPerGroup);
                const uint16_t offset = static_cast<uint16_t>(pniCh % kPniChannelsPerGroup);
                const uint16_t mappedCh = static_cast<uint16_t>(group * kRsChannelsPerGroup + offset);
                evt.ch = static_cast<uint16_t>(mappedCh + chBase);

                float energyKev = 0.0f;
                const double energyEv = static_cast<double>(single.energy);
                if (std::isfinite(energyEv))
                {
                    rawEnergyMin = std::min(rawEnergyMin, energyEv);
                    rawEnergyMax = std::max(rawEnergyMax, energyEv);
                    rawEnergySum += energyEv;
                    finiteRawCount++;
                }
                energyKev = static_cast<float>(energyEv * energyScale);
                if (!std::isfinite(energyKev) || energyKev < 0.0f)
                {
                    energyKev = 0.0f;
                    nonFiniteEnergy++;
                }
                else
                {
                    minEnergy = std::min(minEnergy, static_cast<double>(energyKev));
                    maxEnergy = std::max(maxEnergy, static_cast<double>(energyKev));
                }
                evt.energy = energyKev;
                evt.time = static_cast<double>(single.timevalue_100fs * timeScale);
                buffer.push_back(evt);
            }

            if (!buffer.empty())
            {
                output.write(reinterpret_cast<const char *>(buffer.data()),
                             static_cast<std::streamsize>(buffer.size() * sizeof(TargetListmodeEvent)));
            }

            if (!output)
            {
                std::cerr << "Error: Failed to write segment " << segIdx << " to " << outputPath << std::endl;
                return false;
            }

            totalSingles += buffer.size();
        }

        const uint64_t dataBytes = totalSingles * sizeof(TargetListmodeEvent);
        if (dataBytes > std::numeric_limits<uint32_t>::max())
        {
            std::cerr << "Error: DataLength overflow: " << dataBytes << std::endl;
            return false;
        }

        output.seekp(dataLengthPos, std::ios::beg);
        const uint32_t dataLength32 = static_cast<uint32_t>(dataBytes);
        write_le(output, dataLength32);

        std::cout << "Converted " << totalSingles << " singles to " << outputPath << std::endl;
        std::cout << "[ConvertStats] skippedInvalid=" << skippedInvalid
              << ", missingEnergy=" << missingEnergy
              << ", nonFiniteEnergy=" << nonFiniteEnergy
              << ", rawEnergyMin=" << (std::isfinite(rawEnergyMin) ? rawEnergyMin : 0.0)
              << ", rawEnergyMax=" << (std::isfinite(rawEnergyMax) ? rawEnergyMax : 0.0)
              << ", rawEnergyMean=" << (finiteRawCount > 0 ? static_cast<double>(rawEnergySum / static_cast<long double>(finiteRawCount)) : 0.0)
              << ", energyScale=" << energyScale
              << ", energyMinKeV=" << (std::isfinite(minEnergy) ? minEnergy : 0.0)
              << ", energyMaxKeV=" << (std::isfinite(maxEnergy) ? maxEnergy : 0.0)
              << std::endl;
        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in convert_single_to_target_listmode: " << e.what() << std::endl;
        return false;
    }
}

/**
 * @brief 将 RS Listmode 单事件文件转换为 PNI singles (.lsingle)
 *        Energy 从 keV 转换为 eV
 *
 * @param rsPath 输入 RS listmode 文件路径（16B/条：IP、CH、Energy、Time）
 * @param outputPath 输出 PNI singles 文件路径
 * @param totalCrystals PNI singles 总晶体数
 * @param ipBase IP 起始值（默认 1）
 * @param chBase CH 起始值（默认 1）
 * @param energyScale 能量缩放系数（默认 1000，把 keV 转为 eV）
 * @param timeScale 时间缩放系数（默认 1000，把 ns 转为 ps）
 * @return bool 成功返回 true，失败返回 false
 */
bool convert_RS_listmode_to_single(const std::string &rsPath,
                                   const std::string &outputPath,
                                   uint32_t totalCrystals,
                                   uint16_t ipBase = 1,
                                   uint16_t chBase = 1,
                                   double energyScale = 1.0,
                                   double timeScale = 1000.0)
{
    struct RsListmodeEvent
    {
        uint16_t ip;
        uint16_t ch;
        float energy;
        double time;
    };

    try
    {
        std::ifstream input(rsPath, std::ios::binary);
        if (!input.is_open())
        {
            std::cerr << "Error: Failed to open RS listmode file: " << rsPath << std::endl;
            return false;
        }

        auto read_exact = [&](char *dst, std::size_t size) -> bool {
            input.read(dst, static_cast<std::streamsize>(size));
            return static_cast<std::size_t>(input.gcount()) == size;
        };

        auto read_le = [&](auto &value) -> bool {
            return read_exact(reinterpret_cast<char *>(&value), sizeof(value));
        };

        char magicBuf[16] = {};
        if (!read_exact(magicBuf, sizeof(magicBuf)))
        {
            std::cerr << "Error: Failed to read RS magic: " << rsPath << std::endl;
            return false;
        }

        uint16_t headCrc = 0;
        uint32_t commonInfoLength = 0;
        uint16_t type = 0;
        char softVerBuf[16] = {};
        uint32_t headLength = 0;

        if (!read_le(headCrc) || !read_le(commonInfoLength) || !read_le(type))
        {
            std::cerr << "Error: Failed to read RS header fields: " << rsPath << std::endl;
            return false;
        }
        if (!read_exact(softVerBuf, sizeof(softVerBuf)) || !read_le(headLength))
        {
            std::cerr << "Error: Failed to read RS header length: " << rsPath << std::endl;
            return false;
        }

        const std::string magic(magicBuf, magicBuf + 16);
        if (magic != "RRRRAAAAYYYYSSSS")
        {
            std::cerr << "Error: Invalid RS listmode magic: " << rsPath << std::endl;
            return false;
        }

        constexpr uint32_t kHeaderPrefixBytes = 16 + 2 + 4 + 2 + 16 + 4;
        if (headLength < kHeaderPrefixBytes)
        {
            std::cerr << "Error: Invalid RS header length: " << headLength << std::endl;
            return false;
        }

        const auto fileSize = std::filesystem::file_size(rsPath);
        if (fileSize < headLength)
        {
            std::cerr << "Error: RS file smaller than header: " << rsPath << std::endl;
            return false;
        }

        const uint64_t dataBytes = static_cast<uint64_t>(fileSize) - static_cast<uint64_t>(headLength);
        const uint64_t recordBytes = sizeof(RsListmodeEvent);
        const uint64_t blockBytes = recordBytes * 2; // MATLAB: fix(len/size/2)*size*2
        const uint64_t usableBytes = (dataBytes / blockBytes) * blockBytes;
        if (usableBytes == 0)
        {
            std::cerr << "Error: RS listmode data is empty after alignment: " << rsPath << std::endl;
            return false;
        }

        input.seekg(static_cast<std::streamoff>(headLength), std::ios::beg);
        if (!input)
        {
            std::cerr << "Error: Failed to seek RS listmode data: " << rsPath << std::endl;
            return false;
        }

        openpni::distributed::coreio::SingleWriterOptions opts;
        openpni::distributed::coreio::RollingFileWriter<
            openpni::distributed::coreio::SinglesFileWriter,
            openpni::distributed::coreio::SingleWriterOptions>
            writer;
        {
            const fs::path outPath(outputPath);
            const bool opened = writer.Open(
                outPath.parent_path().string(), outPath.stem().string(),
                outPath.extension().empty() ? "lsingle" : outPath.extension().string().substr(1),
                opts,
                [totalCrystals](openpni::distributed::coreio::SinglesFileWriter &w, const std::string &path)
                {
                    w.Open(path, totalCrystals);
                });
            if (!opened)
            {
                std::cerr << "Error: Failed to open output file: " << outputPath << std::endl;
                return false;
            }
        }

        constexpr std::size_t kSegmentEvents = 1024 * 1024;
        std::vector<RsListmodeEvent> rsBuffer(kSegmentEvents);
        std::vector<openpni::Single> pniBuffer;
        pniBuffer.reserve(kSegmentEvents);

        uint64_t totalEvents = 0;
        uint64_t skippedInvalid = 0;
        uint64_t nonFiniteEnergy = 0;
        uint64_t nonFiniteTime = 0;
        double minEnergyEv = std::numeric_limits<double>::infinity();
        double maxEnergyEv = -std::numeric_limits<double>::infinity();

        uint64_t remainingBytes = usableBytes;
        while (input && remainingBytes > 0)
        {
            const uint64_t bytesToRead = std::min<uint64_t>(
                remainingBytes,
                static_cast<uint64_t>(kSegmentEvents) * sizeof(RsListmodeEvent));
            input.read(reinterpret_cast<char *>(rsBuffer.data()),
                       static_cast<std::streamsize>(bytesToRead));
            const std::streamsize bytesRead = input.gcount();
            if (bytesRead <= 0)
            {
                break;
            }

            const std::size_t count = static_cast<std::size_t>(bytesRead / static_cast<std::streamsize>(recordBytes));
            if (count == 0)
            {
                continue;
            }

            pniBuffer.clear();
            pniBuffer.reserve(count);

            for (std::size_t i = 0; i < count; ++i)
            {
                const auto &evt = rsBuffer[i];
                if (evt.ip < ipBase || evt.ch < chBase)
                {
                    skippedInvalid++;
                    continue;
                }

                openpni::Single s{};

                const uint16_t ipAddr = static_cast<uint16_t>(evt.ip - ipBase + 1);
                const uint16_t ring = static_cast<uint16_t>((ipAddr >> 8) & 0xFFu);
                const uint16_t ip = static_cast<uint16_t>(ipAddr & 0xFFu);
                constexpr uint16_t kBdmPerRing = 48;
                if (ring < 2 || ip < 1 || ip > kBdmPerRing)
                {
                    skippedInvalid++;
                    continue;
                }
                const uint16_t bdmId = static_cast<uint16_t>((ring - 2) * kBdmPerRing + (ip - 1));
                s.channelIndex = bdmId;

                const uint16_t rsCh = static_cast<uint16_t>(evt.ch - chBase);
                constexpr uint16_t kPniChannelsPerGroup = 36;
                constexpr uint16_t kRsChannelsPerGroup = 37;
                constexpr uint16_t kGroupCount = 8;
                if (rsCh >= kRsChannelsPerGroup * kGroupCount)
                {
                    skippedInvalid++;
                    continue;
                }
                const uint16_t group = static_cast<uint16_t>(rsCh / kRsChannelsPerGroup);
                const uint16_t offset = static_cast<uint16_t>(rsCh % kRsChannelsPerGroup);
                if (offset >= kPniChannelsPerGroup)
                {
                    skippedInvalid++;
                    continue;
                }
                const uint16_t pniCh = static_cast<uint16_t>(group * kPniChannelsPerGroup + offset);
                s.crystalIndex = pniCh;

                const double energyKev = static_cast<double>(evt.energy);
                double energyEv = energyKev * energyScale;
                if (!std::isfinite(energyEv) || energyEv < 0.0)
                {
                    energyEv = 0.0;
                    nonFiniteEnergy++;
                }
                else
                {
                    minEnergyEv = std::min(minEnergyEv, energyEv);
                    maxEnergyEv = std::max(maxEnergyEv, energyEv);
                }
                s.energy = static_cast<float>(energyEv);

                const double timeVal = static_cast<double>(evt.time) * timeScale;
                if (!std::isfinite(timeVal) || timeVal < 0.0)
                {
                    s.timevalue_100fs = 0;
                    nonFiniteTime++;
                }
                else
                {
                    s.timevalue_100fs = static_cast<uint64_t>(std::llround(timeVal));
                }

                pniBuffer.push_back(s);
            }

            if (!openpni::distributed::r2s::appendSinglesToSingleFile(writer,
                                                std::span<const openpni::Single>(pniBuffer.data(), pniBuffer.size()),
                                                0,
                                                0))
            {
                std::cerr << "Error: Failed to append singles: " << outputPath << std::endl;
                return false;
            }

            totalEvents += pniBuffer.size();
            remainingBytes -= static_cast<uint64_t>(count) * recordBytes;
        }

        std::cout << "Converted " << totalEvents << " RS events to PNI singles: " << outputPath << std::endl;
        std::cout << "[RS->PNI] skippedInvalid=" << skippedInvalid
                  << ", nonFiniteEnergy=" << nonFiniteEnergy
                  << ", nonFiniteTime=" << nonFiniteTime
                  << ", energyScale=" << energyScale
                  << ", energyMinEv=" << (std::isfinite(minEnergyEv) ? minEnergyEv : 0.0)
                  << ", energyMaxEv=" << (std::isfinite(maxEnergyEv) ? maxEnergyEv : 0.0)
                  << std::endl;
        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in convert_RS_listmode_to_single: " << e.what() << std::endl;
        return false;
    }
}

/**
 * @brief 将多个环目录中同 clock 的 pniRaw-<clock>.bin 合并为整机通道头的单文件
 *
 * 对每个共同的 startClock：按 segment 下标对齐，把各输入文件同一段的包拼接成一个输出段。
 * 输出文件头 channelNum 使用 outputChannelNum（9120 推荐 576），包内全局通道号保持不变。
 *
 * @param inputDirs 输入目录列表（例如 ring0、ring1）
 * @param outputDir 输出目录（例如 pni_raw_node0）
 * @param outputChannelNum 输出文件头通道数（默认 576）
 * @param namePrefix 文件名前缀（默认 "pniRaw-"）
 * @param nameSuffix 文件名后缀（默认 ".bin"）
 * @param forceReplace 输出已存在时是否覆盖
 */
bool merge_rawdata_dirs_by_clock(
    const std::vector<std::string> &inputDirs,
    const std::string &outputDir,
    uint16_t outputChannelNum = 576,
    const std::string &namePrefix = "pniRaw-",
    const std::string &nameSuffix = ".bin",
    bool forceReplace = true)
{
    auto parse_clock = [&](const std::string &name) -> std::optional<uint64_t> {
        if (name.size() < namePrefix.size() + nameSuffix.size())
        {
            return std::nullopt;
        }
        if (name.compare(0, namePrefix.size(), namePrefix) != 0)
        {
            return std::nullopt;
        }
        if (name.compare(name.size() - nameSuffix.size(), nameSuffix.size(), nameSuffix) != 0)
        {
            return std::nullopt;
        }
        const size_t start = namePrefix.size();
        const size_t end = name.size() - nameSuffix.size();
        if (end <= start)
        {
            return std::nullopt;
        }
        try
        {
            return static_cast<uint64_t>(std::stoull(name.substr(start, end - start)));
        }
        catch (...)
        {
            return std::nullopt;
        }
    };

    auto append_view_packets = [](const openpni::RawDataView &view,
                                  std::vector<uint8_t> &data,
                                  std::vector<uint16_t> &length,
                                  std::vector<uint64_t> &offset,
                                  std::vector<uint16_t> &channel) {
        if (!view.count || !view.data || !view.offset || !view.length || !view.channel)
        {
            return;
        }
        for (uint64_t i = 0; i < view.count; ++i)
        {
            const uint64_t packetStart = view.offset[i];
            const uint16_t packetLength = view.length[i];
            offset.push_back(data.size());
            length.push_back(packetLength);
            channel.push_back(view.channel[i]);
            data.insert(data.end(),
                        view.data + packetStart,
                        view.data + packetStart + packetLength);
        }
    };

    try
    {
        if (inputDirs.size() < 2)
        {
            std::cerr << "merge_rawdata_dirs_by_clock: need at least 2 input dirs" << std::endl;
            return false;
        }
        if (outputChannelNum == 0)
        {
            std::cerr << "merge_rawdata_dirs_by_clock: outputChannelNum must be > 0" << std::endl;
            return false;
        }

        // clock -> 各目录下的文件路径（与 inputDirs 对齐，缺失则为空）
        std::map<uint64_t, std::vector<std::string>> clockToPaths;
        for (size_t dirIdx = 0; dirIdx < inputDirs.size(); ++dirIdx)
        {
            const fs::path dirPath(inputDirs[dirIdx]);
            if (!fs::exists(dirPath) || !fs::is_directory(dirPath))
            {
                std::cerr << "Input directory not found: " << inputDirs[dirIdx] << std::endl;
                return false;
            }

            for (const auto &entry : fs::directory_iterator(dirPath))
            {
                if (!entry.is_regular_file())
                {
                    continue;
                }
                const std::string name = entry.path().filename().string();
                auto clockOpt = parse_clock(name);
                if (!clockOpt)
                {
                    continue;
                }
                auto &paths = clockToPaths[*clockOpt];
                if (paths.size() < inputDirs.size())
                {
                    paths.resize(inputDirs.size());
                }
                paths[dirIdx] = entry.path().string();
            }
        }

        if (clockToPaths.empty())
        {
            std::cerr << "No matching " << namePrefix << "*" << nameSuffix
                      << " files found in input dirs" << std::endl;
            return false;
        }

        fs::create_directories(outputDir);

        size_t mergedFiles = 0;
        size_t skippedIncomplete = 0;
        for (const auto &[clock, paths] : clockToPaths)
        {
            bool complete = true;
            for (const auto &p : paths)
            {
                if (p.empty())
                {
                    complete = false;
                    break;
                }
            }
            if (!complete)
            {
                ++skippedIncomplete;
                std::cerr << "Skip clock=" << clock << ": missing file in some input dir" << std::endl;
                continue;
            }

            const fs::path outputPath = fs::path(outputDir) / (namePrefix + std::to_string(clock) + nameSuffix);
            if (fs::exists(outputPath) && !forceReplace)
            {
                std::cerr << "Output exists, skip: " << outputPath << std::endl;
                continue;
            }

            std::vector<openpni::distributed::coreio::RawDataFileReader> readers(paths.size());
            uint32_t maxSegments = 0;
            for (size_t i = 0; i < paths.size(); ++i)
            {
                readers[i].Open(paths[i]);
                maxSegments = std::max(maxSegments, readers[i].Info().segmentNum);
            }

            openpni::distributed::coreio::RawDataWriterOptions options;
            options.channelNum = outputChannelNum;
            options.channelTypeNames.assign(outputChannelNum, "BDM50100");
            options.io.enableOverrideExistingFile = forceReplace;

            openpni::distributed::coreio::RawDataFileWriter writer(std::move(options));
            writer.Open(outputPath.string());

            uint64_t totalPackets = 0;
            for (uint32_t segIdx = 0; segIdx < maxSegments; ++segIdx)
            {
                std::vector<uint8_t> data;
                std::vector<uint16_t> length;
                std::vector<uint64_t> offset;
                std::vector<uint16_t> channel;
                uint64_t clockMs = clock;
                uint64_t durationMs = 0;
                bool any = false;

                for (size_t i = 0; i < readers.size(); ++i)
                {
                    if (segIdx >= readers[i].Info().segmentNum)
                    {
                        continue;
                    }
                    auto segment = readers[i].ReadSegment(segIdx, segIdx + 1);
                    auto view = segment.View();
                    if (!view.count || !view.data)
                    {
                        continue;
                    }
                    if (!any)
                    {
                        clockMs = view.clock_ms != 0 ? view.clock_ms : clock;
                        durationMs = view.duration_ms;
                        any = true;
                    }
                    append_view_packets(view, data, length, offset, channel);
                }

                if (!any || channel.empty())
                {
                    continue;
                }

                openpni::RawDataView mergedView;
                mergedView.data = data.data();
                mergedView.length = length.data();
                mergedView.offset = offset.data();
                mergedView.channel = channel.data();
                mergedView.count = channel.size();
                mergedView.clock_ms = clockMs;
                mergedView.duration_ms = durationMs;
                mergedView.channelNum = outputChannelNum;

                if (!writer.AppendSegment(mergedView))
                {
                    std::cerr << "Failed to write merged segment " << segIdx
                              << " for clock=" << clock << std::endl;
                    return false;
                }
                totalPackets += channel.size();
            }

            std::cout << "Merged clock=" << clock
                      << " segments<=" << maxSegments
                      << " packets=" << totalPackets
                      << " -> " << outputPath << std::endl;
            ++mergedFiles;
        }

        std::cout << "merge_rawdata_dirs_by_clock done: merged=" << mergedFiles
                  << ", skippedIncomplete=" << skippedIncomplete
                  << ", outputDir=" << outputDir << std::endl;
        return mergedFiles > 0;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in merge_rawdata_dirs_by_clock: " << e.what() << std::endl;
        return false;
    }
}
