// 在 PNI 头文件之前包含标准库头文件，避免 g++-13 命名空间冲突
#include <any>
#include <valarray>
#include <type_traits>
#include <utility>
#include <memory>
#include <string>
#include <cstdint>
#include <vector>
#include <iostream>

// PnI-Config.hpp 必须在其他 PNI 头文件之前，定义 __PNI_CUDA_MACRO__ 等宏
#include <pni/PnI-Config.hpp>
#include <pni/io/IO.hpp>
// #include <json.hpp>
#include "core/r2s/R2S.hpp"
#include "../src/tools/testTool.hpp"
#include "../src/core/merge-and-coin/MergeAndCoin.hpp"

using namespace openpni::distributed;
// void test_bdmbid()
// {
//     std::string rawdataPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/raw_data/rawdata_100.data";
//     std::string resPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid";
//     std::vector<std::string> calibrationFilePaths = {
//         "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_00.data",
//         "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_01.data",
//         "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_02.data",
//         "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_03.data"};

//     auto config = r2s::createBDMBiDConfig(rawdataPath, resPath, calibrationFilePaths);
//     r2s::processR2S(config);
// }
void test_bdm2_saveflie()
{
    // BDM2
    // for (int i = 0; i < 3; i++)
    // {
    //     std::vector<uint16_t> channels;
    //     channels.push_back(uint16_t(i * 4));
    //     channels.push_back(uint16_t(i * 4 + 1));
    //     channels.push_back(uint16_t(i * 4 + 2));
    //     channels.push_back(uint16_t(i * 4 + 3));
    //     extract_multiple_channels_from_rawdata("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/raw_data/2_PET_2Bed pet 600s-bed0.raw", channels, "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/split_Data/");
    // }
    print_rawdata_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw");
    print_rawdata_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch4_ch5_ch6_ch7.raw");
    print_rawdata_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch8_ch9_ch10_ch11.raw");

    std::string resPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/split";
    std::vector<std::string> calibrationFilePaths = {
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_00.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_01.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_02.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_03.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_04.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_05.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_06.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_07.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_08.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_09.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_10.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_11.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_12.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_13.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_14.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_15.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_16.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_17.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_18.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_19.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_20.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_21.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_22.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_23.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_24.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_25.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_26.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_27.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_28.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_29.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_30.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_31.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_32.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_33.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_34.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_35.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_36.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_37.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_38.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_39.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_40.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_41.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_42.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_43.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_44.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_45.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_46.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_47.data"};

    for (int i = 0; i < 3; i++)
    {
        std::string singleRawdataPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch" + std::to_string(i * 4) + "_ch" + std::to_string(i * 4 + 1) + "_ch" + std::to_string(i * 4 + 2) + "_ch" + std::to_string(i * 4 + 3) + ".raw";

        auto config = r2s::createBDM2Config(singleRawdataPath, resPath, calibrationFilePaths, "singles_dist" + std::to_string(i), {uint16_t(i * 4), uint16_t(i * 4 + 1), uint16_t(i * 4 + 2), uint16_t(i * 4 + 3)});
        config.asyncFileWrite = true; // 启用异步写入
        r2s::processR2S(config);
    }

    //     std::vector<std::string> files = {
    //         "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/split/singles_dist0.lsingle",
    //         "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/split/singles_dist1.lsingle",
    //         "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/split/singles_dist2.lsingle"};
    //     // // std::vector<std::string> files = {
    //     // //     "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/singles.lsingle"};
    //     auto time_ms = timer(
    //         [&]
    //         {
    //             CoincidenceProcessConfig mergeConfig;
    //             mergeConfig.enable = true;
    //             mergeConfig.channelNum = 12;
    //             mergeConfig.crystalsPerChannel = 169 * 4;
    //             mergeConfig.outputDir = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/coin";

    //             merge_single_files(files, "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/split/merged.lsingle", mergeConfig);
    //         },
    //         1);

    //     std::cout << "Merging time: " << time_ms << " ms" << std::endl;
}
void test_bdm2_callback()
{
    std::cout << "\n========== Testing R2S Callback Mode ==========\n"
              << std::endl;

    std::string resPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2/split";
    std::vector<std::string> calibrationFilePaths = {
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_00.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_01.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_02.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_03.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_04.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_05.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_06.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_07.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_08.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_09.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_10.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_11.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_12.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_13.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_14.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_15.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_16.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_17.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_18.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_19.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_20.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_21.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_22.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_23.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_24.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_25.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_26.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_27.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_28.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_29.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_30.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_31.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_32.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_33.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_34.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_35.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_36.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_37.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_38.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_39.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_40.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_41.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_42.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_43.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_44.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_45.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_46.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/calibration/channel_47.data"};

    // 统计变量
    uint64_t totalSinglesReceived = 0;
    uint64_t totalCallbacks = 0;

    // 测试单个通道组的回调模式
    int i = 0; // 只测试第一个通道组
    std::string singleRawdataPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch" + std::to_string(i * 4) + "_ch" + std::to_string(i * 4 + 1) + "_ch" + std::to_string(i * 4 + 2) + "_ch" + std::to_string(i * 4 + 3) + ".raw";

    auto config = r2s::createBDM2Config(singleRawdataPath, resPath, calibrationFilePaths, "singles_callback_test", {uint16_t(i * 4), uint16_t(i * 4 + 1), uint16_t(i * 4 + 2), uint16_t(i * 4 + 3)});

    config.saveData2SingleFile = false;
    config.asyncFileWrite = false;

    // 设置回调函数 - 模拟接收数据（实际使用时会通过 gRPC 发送）
    config.onSinglesReady = [&totalSinglesReceived, &totalCallbacks](
                                std::vector<r2s::Single> &&singles,
                                uint64_t clock_ms,
                                uint32_t duration_ms) -> bool
    {
        totalCallbacks++;
        totalSinglesReceived += singles.size();

        // 每50次回调输出一次状态
        if (totalCallbacks % 50 == 0 || totalCallbacks == 1)
        {
            std::cout << "[Callback #" << totalCallbacks << "] "
                      << "Received " << singles.size() << " singles, "
                      << "clock=" << clock_ms << "ms, "
                      << "duration=" << duration_ms << "ms" << std::endl;

            // 输出前几个单事件的详细信息
            if (!singles.empty())
            {
                std::cout << "  First single: channelIdx=" << singles[0].channelIndex
                          << ", crystalIdx=" << singles[0].crystalIndex
                          << ", energy=" << singles[0].energy
                          << ", time_pico=" << singles[0].timevalue_pico << std::endl;
            }
        }

        // 返回 true 继续处理，返回 false 停止
        return true;
    };

    std::cout << "Starting R2S processing with callback mode..." << std::endl;
    std::cout << "Raw data file: " << singleRawdataPath << std::endl;

    auto start = std::chrono::steady_clock::now();
    bool success = r2s::processR2S(config);
    auto end = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "\n========== Callback Mode Test Results ==========" << std::endl;
    std::cout << "Success: " << (success ? "Yes" : "No") << std::endl;
    std::cout << "Total callbacks: " << totalCallbacks << std::endl;
    std::cout << "Total singles received: " << totalSinglesReceived << std::endl;
    std::cout << "Processing time: " << elapsed_ms << " ms" << std::endl;
    std::cout << "Throughput: " << (elapsed_ms > 0 ? (double)totalSinglesReceived / elapsed_ms * 1000 : 0) << " singles/s" << std::endl;
    std::cout << "================================================\n"
              << std::endl;
}

void split_930_data()
{
    const std::string rawdataPath = "/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/pni_dis_r2c/data/dataAndPos3/converted_rawData.bin";
    const std::string outputFolderName = "splitdata";
    constexpr uint16_t kTotalChannels = 144;
    constexpr uint16_t kGroupCount = 4;
    constexpr uint16_t kGroupSize = kTotalChannels / kGroupCount;

    for (uint16_t groupIndex = 0; groupIndex < kGroupCount; ++groupIndex)
    {
        const uint16_t startChannel = static_cast<uint16_t>(groupIndex * kGroupSize);
        const uint16_t endChannel = static_cast<uint16_t>(startChannel + kGroupSize);
        std::vector<uint16_t> channels;
        channels.reserve(kGroupSize);
        for (uint16_t ch = startChannel; ch < endChannel; ++ch)
        {
            channels.push_back(ch);
        }

        std::cout << "Splitting channels [" << startChannel << "-" << (endChannel - 1) << "]" << std::endl;
        if (!extract_multiple_channels_from_rawdata(rawdataPath, channels, outputFolderName))
        {
            std::cerr << "Failed to split channels [" << startChannel << "-" << (endChannel - 1) << "]" << std::endl;
            break;
        }
    }

}
void test_50100_930_callback()
{
    std::cout << "\n========== Testing 50100 R2S Callback Mode ==========\n"
              << std::endl;

    std::string resPath = "/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/pni_dis_r2c/data/res";
   auto calibrationFilePaths = r2s::collectCalibrationFiles(
    "/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/pni_dis_r2c/data/cali",
    { ".bin"},
    true,
    "bdm_",
    ".bin");
    // 统计变量
    uint64_t totalSinglesReceived = 0;
    uint64_t totalCallbacks = 0;

  
    std::string singleRawdataPath =
        std::string("/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/pni_dis_r2c/data/dataAndPos3/") +
        "converted_rawData2.bin";
    // std::string singleRawdataPath =
    //     std::string("/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/pni_dis_r2c/data/dataAndPos3/splitdata/") +
    //     "converted_rawData_ch108-143_n36.raw";

    auto config = r2s::createBDM50100Config(singleRawdataPath, resPath, calibrationFilePaths, "singles_50100_test", {});

    config.saveData2SingleFile = true;
    config.asyncFileWrite = true;

    // 设置回调函数 - 模拟接收数据（实际使用时会通过 gRPC 发送）
    config.onSinglesReady = [&totalSinglesReceived, &totalCallbacks](
                                std::vector<r2s::Single> &&singles,
                                uint64_t clock_ms,
                                uint32_t duration_ms) -> bool
    {
        totalCallbacks++;
        totalSinglesReceived += singles.size();

        // 每50次回调输出一次状态
        if (totalCallbacks % 50 == 0 || totalCallbacks == 1)
        {
            std::cout << "[Callback #" << totalCallbacks << "] "
                      << "Received " << singles.size() << " singles, "
                      << "clock=" << clock_ms << "ms, "
                      << "duration=" << duration_ms << "ms" << std::endl;

            // 输出前几个单事件的详细信息
            if (!singles.empty())
            {
                std::cout << "  First single: channelIdx=" << singles[0].channelIndex
                          << ", crystalIdx=" << singles[0].crystalIndex
                          << ", energy=" << singles[0].energy
                          << ", time_pico=" << singles[0].timevalue_pico << std::endl;
            }
        }

        // 返回 true 继续处理，返回 false 停止
        return true;
    };

    std::cout << "Starting R2S processing with callback mode..." << std::endl;
    std::cout << "Raw data file: " << singleRawdataPath << std::endl;

    auto start = std::chrono::steady_clock::now();
    bool success = r2s::processR2S(config);
    auto end = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "\n========== Callback Mode Test Results ==========" << std::endl;
    std::cout << "Success: " << (success ? "Yes" : "No") << std::endl;
    std::cout << "Total callbacks: " << totalCallbacks << std::endl;
    std::cout << "Total singles received: " << totalSinglesReceived << std::endl;
    std::cout << "Processing time: " << elapsed_ms << " ms" << std::endl;
    std::cout << "Throughput: " << (elapsed_ms > 0 ? (double)totalSinglesReceived / elapsed_ms * 1000 : 0) << " singles/s" << std::endl;
    std::cout << "================================================\n"
              << std::endl;
}

int main()
{
    std::cout << "======================================" << std::endl;
    std::cout << "     PNI R2S Test Suite" << std::endl;
    std::cout << "======================================\n"
              << std::endl;

    // // 测试1: 保存文件模式
    // std::cout << "[Test 1] Testing file save mode..." << std::endl;
    // test_bdm2_saveflie();

    // // 测试2: 回调模式
    // std::cout << "[Test 2] Testing callback mode..." << std::endl;
    // test_bdm2_callback();

    convert_50100_rawdata_with_pos_to_standard(std::string("/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/pni_dis_r2c/data/dataAndPos3/rawData.bin"),
    std::string("/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/pni_dis_r2c/data/dataAndPos3/pos.bin"),
                                               std::string("/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/pni_dis_r2c/data/dataAndPos3/converted_rawData2.bin"),
                                            1024 * 1024);
    // std::cout << "[Test 3] Testing 50100 callback mode..." << std::endl;
    // test_50100_930_callback();

    // std::cout << "[Test 4] Splitting 50100 rawdata channels..." << std::endl;
    // split_930_data();
    
    std::cout << "\n======================================" << std::endl;
    std::cout << "     All tests completed!" << std::endl;
    std::cout << "======================================" << std::endl;

    return 0;
}

// g++ -fdiagnostics-color=always -g /media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/tests/test_pni_r2c.cpp /media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/build/GpuSort.o -o /media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/build/test_pni_r2c.o -std=c++20 -O3 -march=native -fopenmp $(pkg-config --cflags --libs libpni) -ltbb -L/usr/local/cuda/lib64 -lcudart && ./build/test_pni_r2c.o