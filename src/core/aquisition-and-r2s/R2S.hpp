#pragma once
#include <pni/io/IO.hpp>
// #include <pni/node/BDMBiDR2S.hpp>

#include <pni/node/BDM2R2S.hpp>
#include <pni/node/ConvergedR2S.hpp>
#include <pni/node/Coincidence.hpp>
#include "../tools/SinglesProcess.hpp"
#include <iostream>
#include <fstream>

namespace openpni::distributed::r2s
{
    using GlobalSingle = openpni::v1::basic::GlobalSingle_t;
    using Single = openpni::Single;
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
        BDMBiD,
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
        bool sortDataByTime = false;               // 是否按时间排序输出数据

        R2SProcessConfig()
            : detectorType(DetectorType::Unknown), crystalsPerChannel(0), r2sResultIndex(0), outputFileName("singles")
        {
        }
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
                                         singles.size() * sizeof(openpni::experimental::interface::LocalSingle),
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
     * @brief 通用的 R2S 处理函数
     *
     * @param config R2S处理配置
     * @return bool 成功返回true，失败返回false
     */
    bool processR2S(const R2SProcessConfig &config)
    {
        try
        {
            std::cout << "Starting R2S processing..." << std::endl;

            // 1. 打开原始数据文件
            auto mRawFileInput = std::make_unique<openpni::io::v1::RawFileInput>();
            mRawFileInput->open(config.rawdataPath);

            auto header = mRawFileInput->header();
            auto channelNum = header.channelNum;
            auto segmentNum = header.segmentNum;

            std::cout << "Detector: " << (config.detectorType == DetectorType::BDM2 ? "BDM2" : "BDMBiD") << std::endl;
            std::cout << "Channels: " << channelNum << std::endl;
            std::cout << "Segments: " << segmentNum << std::endl;

            // 2. 确定要处理的通道
            std::vector<uint16_t> channelsToProcess;
            if (config.channelIndices.empty())
            {
                // 如果未指定通道，则处理所有通道
                for (uint16_t i = 0; i < config.channelNums; i++)
                {
                    channelsToProcess.push_back(i);
                }
                std::cout << "Processing all channels" << std::endl;
            }
            else
            {
                channelsToProcess = config.channelIndices;
                std::cout << "Processing selected channels: ";
                for (auto ch : channelsToProcess)
                {
                    std::cout << ch << " ";
                }
                std::cout << std::endl;

                // 验证通道索引有效性
                for (auto ch : channelsToProcess)
                {
                    if (ch >= config.channelNums)
                    {
                        std::cerr << "Error: Channel index " << ch
                                  << " is out of range (0-" << (channelNum - 1) << ")" << std::endl;
                        return false;
                    }
                }
            }

            // 3. 检查校准文件数量
            if (config.calibrationFiles.size() < channelsToProcess.size())
            {
                std::cerr << "Error: Not enough calibration files. Need " << channelsToProcess.size()
                          << ", got " << config.calibrationFiles.size() << std::endl;
                return false;
            }

            // 4. 创建 SingleGenerator（只为要处理的通道创建）
            std::vector<openpni::interface::SingleGenerator *> generatorsVector;
            std::cout << "Loading " << channelsToProcess.size() << " channels' calibration data..." << std::endl;

            for (size_t i = 0; i < config.channelNums; i++)
            {
                try
                {
                    std::cout << "  Creating generator for channel " << i << "..." << std::endl;
                    auto generator = createSingleGenerator(
                        config.detectorType,
                        i,
                        config.calibrationFiles[i]);
                    std::cout << "  Generator created successfully." << std::endl;
                    generatorsVector.push_back(generator);
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Error creating generator for channel " << i << ": " << e.what() << std::endl;
                    throw;
                }
            }

            // 4. 设置 R2S
            auto R2S = openpni::ConvergedR2S();

            // std::vector<uint32_t> crystalNumOfEachChannel(channelNum, config.crystalsPerChannel);
            // Coin.setTotalCrystalNumOfEachChannel(crystalNumOfEachChannel);

            std::cout << "Setting up ConvergedR2S with generators..." << std::endl;
            R2S.SetChannels(generatorsVector);
            std::cout << "Setup complete." << std::endl;

            // 5. 创建 SingleFileOutput 用于保存结果
            openpni::io::v1::single::SingleFileOutput singleOutput;

            // 配置 Single 文件参数
            singleOutput.setBytes4CrystalIndex(openpni::io::v1::single::CrystalIndexType::UINT32);
            singleOutput.setBytes4TimeValue(openpni::io::v1::single::TimeValueType::UINT64);
            singleOutput.setBytes4Energy(openpni::io::v1::single::EnergyType::FLT32);

            // 计算总晶体数
            uint32_t totalCrystals = config.channelNums * config.crystalsPerChannel;
            singleOutput.setTotalCrystalNum(totalCrystals);

            // 打开输出文件
            std::string outputFilePath = config.resultPath + "/" + config.outputFileName + ".single";
            singleOutput.open(outputFilePath);
            std::cout << "Output file: " << outputFilePath << std::endl;
            std::cout << "Total crystals: " << totalCrystals << std::endl;

            // 6. 处理每个段
            uint64_t totalCount_raw = 0;
            uint64_t totalCount_single = 0;

            std::cout << "Processing " << segmentNum << " segments..." << std::endl;
            for (uint64_t i = 0; i < segmentNum; i++)
            {
                auto segment = mRawFileInput->readSegment(i, i + 1);
                auto segHeader = mRawFileInput->segmentHeader(i);
                auto view = segment.view(header, segHeader);
                auto count = view.count;

                totalCount_raw += count;

                if (!count || !view.data)
                {
                    std::cout << "Segment " << i << ": No data, skipping" << std::endl;
                    continue;
                }
                auto d_data = openpni::DPackets::FromHost(view.data, view.offset, view.length, view.channel, view.count);
                try
                {
                    auto time_ms = timer(
                        [&]
                        {
                            // R2S 处理
                            auto r2sResults = R2S.R2S_CUDA(d_data.raw, d_data.offset, d_data.length, d_data.channel, d_data.count);
                            // auto r2sResults = R2S.r2s_cpu(
                            //     view.data,
                            //     view.offset,
                            //     view.length,
                            //     view.channel,
                            //     view.count);

                            // 保存单事件（使用标准格式）
                            auto &singlesSpan = r2sResults[config.r2sResultIndex];

                            if (config.sortDataByTime)
                            {
                                // Sort data by time on GPU if needed
                                if (isDevicePointer(singlesSpan.data()))
                                {
                                    openpni::distributed::r2s::d_sortSinglesByTime_R2S(
                                        const_cast<Single *>(singlesSpan.data()),
                                        singlesSpan.size());
                                }
                            }

                            bool success = appendSinglesToSingleFile(
                                singleOutput,
                                singlesSpan,
                                config.crystalsPerChannel,
                                segHeader.clock,
                                segHeader.duration);

                            if (!success)
                            {
                                std::cerr << "Failed to append segment " << i << " to single file" << std::endl;
                            }

                            totalCount_single += singlesSpan.size();
                            if (i % 10 == 0 || i == segmentNum - 1)
                            {
                                std::cout << "Segment " << i << "/" << segmentNum
                                          << ": Processed " << count << " packets, generated "
                                          << singlesSpan.size() << " singles" << std::endl;
                            }
                        },
                        1);
                    if (i % 10 == 0 || i == segmentNum - 1)
                        std::cout << "Data = " << view.count * 1024 << " bytes, time = " << time_ms << "ms, Speed = " << double(view.count * 1024) / 1024 / 1024 / (time_ms / 1) << " MB/ms\n";
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Exception at segment " << i << ": " << e.what() << std::endl;
                    throw;
                }
            }

            // 7. 输出统计信息
            std::cout << "\n=== Processing Complete ===" << std::endl;
            std::cout << "Total raw packets: " << totalCount_raw << std::endl;
            std::cout << "Total singles: " << totalCount_single << std::endl;
            std::cout << "Singles/Packet ratio: "
                      << (totalCount_raw > 0 ? (double)totalCount_single / totalCount_raw : 0.0)
                      << std::endl;
            std::cout << "Output file: " << outputFilePath << std::endl;
            std::cout << "===========================\n"
                      << std::endl;

            // 8. 清理
            for (auto gen : generatorsVector)
            {
                delete gen;
            }

            return true;
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