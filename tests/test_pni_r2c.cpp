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
#include <algorithm>
#include <limits>
#include <cmath>
#include <filesystem>
#include <map>
#include <optional>
#include <cstddef>

// PnI-Config.hpp 必须在其他 PNI 头文件之前，定义 __PNI_CUDA_MACRO__ 等宏
#include <pni/PnI-Config.hpp>
#include <pni/io/IO.hpp>
// #include <json.hpp>
#include "core/r2s/R2S.hpp"
#include "../src/tools/testTool.hpp"
#include "../src/core/merge-and-coin/MergeAndCoin.hpp"

using namespace openpni::distributed;

std::string path_pre = "/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/NECR20260520/20260520001/PET-WB-2026_05_20_12_06_28/0";
std::string path_pre_out = "/media/lenovo/9e9a8f5e-9976-4563-bba3-f45659126f6c/50100NECR/3";
std::string data_path = path_pre + "/RawData";
std::string out_path = path_pre_out;
std::string cali_path = "/media/lenovo/1TB/50100data/pni_res/caliFile";
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

    std::string resPath = out_path + "/pni_singles";
    auto calibrationFilePaths = r2s::collectCalibrationFiles(
        cali_path,
        {".bin"},
        true,
        "bdm_",
        ".bin");
    // 统计变量
    uint64_t totalSinglesReceived = 0;
    uint64_t totalCallbacks = 0;
    uint64_t energySamples = 0;
    uint64_t energyNonFinite = 0;
    long double energySum = 0.0;
    double energyMin = std::numeric_limits<double>::infinity();
    double energyMax = -std::numeric_limits<double>::infinity();
    constexpr std::size_t kMaxEnergySamplesPerCallback = 20000;

    namespace fs = std::filesystem;
    const fs::path rawdataDir = out_path + "/pni_raw";
    const std::string rawPrefix = "pniRaw-";
    const std::string rawExt = ".bin";

    const uint64_t maxSinglesBytes = 1365463664; // 与单文件大小限制相关，单位字节（约1.23GB），可以根据需要调整
    const size_t maxRawFilesPerOutput = 0; // 0 表示不限
    const std::string outputBaseName = "singles_50100";
    constexpr uint64_t kBytesPerSingle = 16; // 16+16+32+64 bits

    auto has_prefix = [](const std::string &value, const std::string &prefix) {
        return value.rfind(prefix, 0) == 0;
    };
    auto has_suffix = [](const std::string &value, const std::string &suffix) {
        return value.size() >= suffix.size() &&
               value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
    };
    auto parse_start_clock = [&](const std::string &name) -> std::optional<uint64_t> {
        if (!has_prefix(name, rawPrefix) || !has_suffix(name, rawExt))
        {
            return std::nullopt;
        }
        const size_t start = rawPrefix.size();
        const size_t end = name.size() - rawExt.size();
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

    if (!fs::exists(rawdataDir))
    {
        std::cerr << "Rawdata directory not found: " << rawdataDir << std::endl;
        return;
    }
    fs::create_directories(resPath);

    struct RawFileEntry
    {
        uint64_t startClock;
        fs::path path;
    };

    std::vector<RawFileEntry> rawFiles;
    for (const auto &entry : fs::directory_iterator(rawdataDir))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }
        const std::string name = entry.path().filename().string();
        auto startClock = parse_start_clock(name);
        if (!startClock)
        {
            continue;
        }
        rawFiles.push_back(RawFileEntry{*startClock, entry.path()});
    }

    if (rawFiles.empty())
    {
        std::cerr << "No converted rawdata files found in: " << rawdataDir << std::endl;
        return;
    }

    std::sort(rawFiles.begin(), rawFiles.end(),
              [](const RawFileEntry &a, const RawFileEntry &b) {
                  if (a.startClock != b.startClock)
                  {
                      return a.startClock < b.startClock;
                  }
                  return a.path.filename().string() < b.path.filename().string();
              });

    struct SinglesRotator
    {
        std::string resultPath;
        std::string baseName;
        uint64_t maxBytes = 0;
        size_t maxFiles = 0;
        uint32_t totalCrystals = 0;
        size_t outputIndex = 0;
        uint64_t currentBytes = 0;
        size_t currentFiles = 0;
        bool inRawFile = false;
        std::unique_ptr<openpni::distributed::coreio::SinglesFileWriter> writer;

        bool openNew()
        {
            openpni::distributed::coreio::SingleWriterOptions opts;
            opts.backend = openpni::distributed::coreio::IOBackendContext::Get().singlesWriter;
            writer = std::make_unique<openpni::distributed::coreio::SinglesFileWriter>(std::move(opts));

            outputIndex++;
            currentBytes = 0;
            currentFiles = 0;

            const std::string outputPath = resultPath + "/" + baseName + "_part" +
                                           std::to_string(outputIndex) + ".lsingle";
            writer->Open(outputPath, totalCrystals);
            std::cout << "Output singles: " << outputPath << std::endl;
            return true;
        }

        bool startRawFile()
        {
            if (!writer || (maxFiles > 0 && currentFiles >= maxFiles))
            {
                if (!openNew())
                {
                    return false;
                }
            }

            currentFiles++;
            inRawFile = true;
            return true;
        }

        void finishRawFile()
        {
            inRawFile = false;
        }

        bool rotateForSize(uint64_t bytesNeeded)
        {
            if (maxBytes == 0)
            {
                return true;
            }
            if (!writer)
            {
                return openNew();
            }
            if (currentBytes + bytesNeeded <= maxBytes)
            {
                return true;
            }

            if (!openNew())
            {
                return false;
            }
            if (inRawFile)
            {
                currentFiles = 1;
            }
            return true;
        }
    };

    const uint32_t totalCrystals = 48 * 3 * (6 * 6 * 8);
    SinglesRotator rotator;
    rotator.resultPath = resPath;
    rotator.baseName = outputBaseName;
    rotator.maxBytes = maxSinglesBytes;
    rotator.maxFiles = maxRawFilesPerOutput;
    rotator.totalCrystals = totalCrystals;

    auto start = std::chrono::steady_clock::now();
    bool allSuccess = true;

    for (const auto &rawEntry : rawFiles)
    {
        if (!rotator.startRawFile())
        {
            allSuccess = false;
            break;
        }

        auto config = r2s::createBDM50100Config(
            rawEntry.path.string(),
            resPath,
            calibrationFilePaths,
            outputBaseName,
            {});

        config.saveData2SingleFile = false;
        config.asyncFileWrite = false;
        config.useEnergyCut = true;
        config.energyCutLow = 421.0;
        config.energyCutHigh = 1000.0;

        config.onSinglesSpanReady =
            [&](std::span<r2s::Single const> singles,
                uint64_t clock_ms,
                uint32_t duration_ms) -> bool
        {
            std::vector<r2s::Single> hostSingles;
            if (r2s::isDevicePointer(singles.data()))
            {
                hostSingles = r2s::materializeSinglesOnHost(singles);
                singles = std::span<const r2s::Single>(hostSingles.data(), hostSingles.size());
            }

            totalCallbacks++;
            totalSinglesReceived += singles.size();

            const std::size_t sampleCount = std::min<std::size_t>(singles.size(), kMaxEnergySamplesPerCallback);
            for (std::size_t i = 0; i < sampleCount; ++i)
            {
                const double energy = static_cast<double>(singles[i].energy);
                if (!std::isfinite(energy))
                {
                    energyNonFinite++;
                    continue;
                }
                energyMin = std::min(energyMin, energy);
                energyMax = std::max(energyMax, energy);
                energySum += energy;
                energySamples++;
            }

            const uint64_t bytesNeeded = static_cast<uint64_t>(singles.size()) * kBytesPerSingle;
            if (!rotator.rotateForSize(bytesNeeded))
            {
                return false;
            }

            if (!r2s::appendSinglesToSingleFile(*rotator.writer, singles, clock_ms, duration_ms))
            {
                return false;
            }

            rotator.currentBytes += bytesNeeded;

            if (totalCallbacks % 2 == 0 || totalCallbacks == 1)
            {
                std::cout << "[Callback #" << totalCallbacks << "] "
                          << "Received " << singles.size() << " singles, "
                          << "clock=" << clock_ms << "ms, "
                          << "duration=" << duration_ms << "ms" << std::endl;
                if (!singles.empty())
                {
                    std::cout << "  First single: channelIdx=" << singles[0].channelIndex
                              << ", crystalIdx=" << singles[0].crystalIndex
                              << ", energy=" << singles[0].energy
                              << ", time_pico=" << singles[0].timevalue_pico << std::endl;
                }

                if (energySamples > 0)
                {
                    const double meanEnergy = static_cast<double>(energySum / energySamples);
                    std::cout << "  Energy stats (sampled): count=" << energySamples
                              << ", nonFinite=" << energyNonFinite
                              << ", min=" << energyMin
                              << ", max=" << energyMax
                              << ", mean=" << meanEnergy
                              << std::endl;
                }
            }

            return true;
        };

        std::cout << "Starting R2S processing: " << rawEntry.path << std::endl;
        const bool success = r2s::processR2S(config);
        rotator.finishRawFile();
        if (!success)
        {
            allSuccess = false;
            break;
        }

        // //debug test
        // break;
    }

    auto end = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "\n========== Callback Mode Test Results ==========" << std::endl;
    std::cout << "Success: " << (allSuccess ? "Yes" : "No") << std::endl;
    std::cout << "Total callbacks: " << totalCallbacks << std::endl;
    std::cout << "Total singles received: " << totalSinglesReceived << std::endl;
    std::cout << "Processing time: " << elapsed_ms << " ms" << std::endl;
    std::cout << "Throughput: " << (elapsed_ms > 0 ? (double)totalSinglesReceived / elapsed_ms * 1000 : 0) << " singles/s" << std::endl;
    std::cout << "================================================\n"
              << std::endl;
}

void convert_50100_rawdata_batch_process()
{
    namespace fs = std::filesystem;

    const fs::path rawDir = data_path;
    const fs::path outDir = out_path + "/pni_raw";
    const std::string prefix = "done_";
    const std::string ext = ".bin";

    auto has_prefix = [](const std::string &value, const std::string &prefixValue) {
        return value.rfind(prefixValue, 0) == 0;
    };
    auto has_suffix = [](const std::string &value, const std::string &suffix) {
        return value.size() >= suffix.size() &&
               value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
    };
    auto parse_clock = [&](const std::string &name) -> std::optional<uint64_t> {
        if (!has_prefix(name, prefix) || !has_suffix(name, ext))
        {
            return std::nullopt;
        }
        const size_t dashPos = name.rfind('-');
        const size_t end = name.size() - ext.size();
        if (dashPos == std::string::npos || dashPos + 1 >= end)
        {
            return std::nullopt;
        }
        const std::string clockPart = name.substr(dashPos + 1, end - dashPos - 1);
        try
        {
            return static_cast<uint64_t>(std::stoull(clockPart));
        }
        catch (...)
        {
            return std::nullopt;
        }
    };

    if (!fs::exists(rawDir))
    {
        std::cerr << "Raw data directory not found: " << rawDir << std::endl;
        return;
    }
    fs::create_directories(outDir);

    struct BatchEntry
    {
        uint64_t clock;
        fs::path rawPath;
    };

    std::vector<BatchEntry> entries;
    for (const auto &entry : fs::directory_iterator(rawDir))
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
        entries.push_back(BatchEntry{*clockOpt, entry.path()});
    }

    if (entries.empty())
    {
        std::cerr << "No valid raw files found in: " << rawDir << std::endl;
        return;
    }

    std::sort(entries.begin(), entries.end(),
              [](const BatchEntry &a, const BatchEntry &b) { return a.clock < b.clock; });

    for (const auto &entry : entries)
    {
        const fs::path outputPath = outDir / ("pniRaw-" + std::to_string(entry.clock) + ".bin");
        std::cout << "Converting: " << entry.rawPath << " -> " << outputPath << std::endl;

        const bool ok = convert_50100_original_rawdata_to_standard(
            entry.rawPath.string(),
            outputPath.string(),
            entry.clock,
            0,
            144,
            "BDM50100",
            true);

        if (!ok)
        {
            std::cerr << "Failed to convert: " << entry.rawPath << std::endl;
            break;
        }
    }
}

void convert_50100_singles_batch_process()
{
    namespace fs = std::filesystem;

    const fs::path singlesDir = out_path + "/pni_singles";
    const fs::path outDir = out_path + "/rs_singles";
    const std::string prefix = "singles_50100_part";
    const std::string ext = ".lsingle";

    const double energyScale = 1.0; // eV -> keV? keep same as main()

    auto has_prefix = [](const std::string &value, const std::string &prefixValue) {
        return value.rfind(prefixValue, 0) == 0;
    };
    auto has_suffix = [](const std::string &value, const std::string &suffix) {
        return value.size() >= suffix.size() &&
               value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
    };
    auto parse_index = [&](const std::string &name) -> std::optional<uint64_t> {
        if (!has_prefix(name, prefix) || !has_suffix(name, ext))
        {
            return std::nullopt;
        }
        const size_t start = prefix.size();
        const size_t end = name.size() - ext.size();
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

    if (!fs::exists(singlesDir))
    {
        std::cerr << "Singles directory not found: " << singlesDir << std::endl;
        return;
    }
    fs::create_directories(outDir);

    struct BatchEntry
    {
        uint64_t index;
        fs::path inputPath;
    };

    std::vector<BatchEntry> entries;
    for (const auto &entry : fs::directory_iterator(singlesDir))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }
        const std::string name = entry.path().filename().string();
        auto indexOpt = parse_index(name);
        if (!indexOpt)
        {
            continue;
        }
        entries.push_back(BatchEntry{*indexOpt, entry.path()});
    }

    if (entries.empty())
    {
        std::cerr << "No singles files found in: " << singlesDir << std::endl;
        return;
    }

    std::sort(entries.begin(), entries.end(),
              [](const BatchEntry &a, const BatchEntry &b) { return a.index < b.index; });

    for (const auto &entry : entries)
    {
        const fs::path outputPath = outDir / (entry.inputPath.stem().string() + ".nlm");
        std::cout << "Converting: " << entry.inputPath << " -> " << outputPath << std::endl;

        const bool ok = convert_single_to_RS_listmode(
            entry.inputPath.string(),
            outputPath.string(),
            TargetListmodeHeader{},
            1,
            1,
            energyScale);

        if (!ok)
        {
            std::cerr << "Failed to convert: " << entry.inputPath << std::endl;
            break;
        }
    }
}

void convert_rs_singles_batch_process()
{
    namespace fs = std::filesystem;

    const fs::path rsSinglesDir = out_path + "/Singles";
    const fs::path outDir = out_path + "/pni_singles_from_rs";
    const std::string prefix = "done_";
    const std::string extNlm = ".nlm";

    const uint32_t totalCrystals = 48 * 3 * (6 * 6 * 8);
    const uint16_t ipBase = 1;
    const uint16_t chBase = 1;
    const double energyScale = 1; // keV -> eV?

    if (!fs::exists(rsSinglesDir))
    {
        std::cerr << "RS singles directory not found: " << rsSinglesDir << std::endl;
        return;
    }
    fs::create_directories(outDir);

    struct RsEntry
    {
        uint64_t clock = 0;
        fs::path path;
    };

    auto has_prefix = [](const std::string &value, const std::string &prefixValue) {
        return value.rfind(prefixValue, 0) == 0;
    };
    auto has_suffix = [](const std::string &value, const std::string &suffix) {
        return value.size() >= suffix.size() &&
               value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
    };
    auto parse_clock = [&](const std::string &name) -> std::optional<uint64_t> {
        if (!has_prefix(name, prefix))
        {
            return std::nullopt;
        }
        std::string ext;
        if (has_suffix(name, extNlm))
        {
            ext = extNlm;
        }
        else
        {
            return std::nullopt;
        }

        const size_t dashPos = name.rfind('-');
        const size_t end = name.size() - ext.size();
        if (dashPos == std::string::npos || dashPos + 1 >= end)
        {
            return std::nullopt;
        }
        try
        {
            return static_cast<uint64_t>(std::stoull(name.substr(dashPos + 1, end - dashPos - 1)));
        }
        catch (...)
        {
            return std::nullopt;
        }
    };

    std::vector<RsEntry> entries;
    for (const auto &entry : fs::directory_iterator(rsSinglesDir))
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
        entries.push_back(RsEntry{*clockOpt, entry.path()});
    }

    if (entries.empty())
    {
        std::cerr << "No RS singles files found in: " << rsSinglesDir << std::endl;
        return;
    }

    std::sort(entries.begin(), entries.end(),
              [](const RsEntry &a, const RsEntry &b) {
                  if (a.clock != b.clock)
                  {
                      return a.clock < b.clock;
                  }
                  return a.path.filename().string() < b.path.filename().string();
              });

    std::size_t outputIndex = 0;
    for (const auto &entry : entries)
    {
        outputIndex++;
        const fs::path outputPath = outDir / ("singles_50100_part" + std::to_string(outputIndex) + ".lsingle");
        std::cout << "Converting: " << entry.path << " -> " << outputPath << std::endl;

        const bool ok = convert_RS_listmode_to_single(
            entry.path.string(),
            outputPath.string(),
            totalCrystals,
            ipBase,
            chBase,
            energyScale);

        if (!ok)
        {
            std::cerr << "Failed to convert: " << entry.path << std::endl;
            break;
        }
    }
}

void analyze_first_pni_singles_in_folder(const std::string &singlesDirPath)
{
    namespace fs = std::filesystem;

    if (!fs::exists(singlesDirPath))
    {
        std::cerr << "Singles directory not found: " << singlesDirPath << std::endl;
        return;
    }

    std::vector<fs::path> files;
    for (const auto &entry : fs::directory_iterator(singlesDirPath))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }
        if (entry.path().extension() != ".lsingle")
        {
            continue;
        }
        files.push_back(entry.path());
    }

    if (files.empty())
    {
        std::cerr << "No .lsingle files found in: " << singlesDirPath << std::endl;
        return;
    }

    std::sort(files.begin(), files.end(),
              [](const fs::path &a, const fs::path &b) {
                  return a.filename().string() < b.filename().string();
              });

    const fs::path inputPath = files.front();
    std::cout << "Analyzing PNI singles: " << inputPath << std::endl;

    struct RangeStats
    {
        uint64_t count = 0;
        uint64_t nonFinite = 0;
        long double sum = 0.0;
        double min = std::numeric_limits<double>::infinity();
        double max = -std::numeric_limits<double>::infinity();

        void add(double value)
        {
            count++;
            if (!std::isfinite(value))
            {
                nonFinite++;
                return;
            }
            min = std::min(min, value);
            max = std::max(max, value);
            sum += value;
        }

        double mean() const
        {
            const uint64_t finiteCount = count - nonFinite;
            return finiteCount > 0 ? static_cast<double>(sum / finiteCount) : 0.0;
        }
    };

    auto collect_stats = [&](RangeStats &energyStats, RangeStats &timeStats) {
        openpni::io::listmode::ListmodeFileInput input;
        input.Open(inputPath.string());
        const auto segmentNum = input.SegmentNum();
        for (uint32_t segIdx = 0; segIdx < segmentNum; ++segIdx)
        {
            auto segment = input.ReadSegment(segIdx, segIdx + 1);
            const auto singles = openpni::distributed::coin::readSinglesFromSegment(segment);
            if (singles.empty())
            {
                continue;
            }
            for (const auto &single : singles)
            {
                energyStats.add(static_cast<double>(single.energy));
                timeStats.add(static_cast<double>(single.timevalue_pico));
            }
        }
    };

    RangeStats energyStats;
    RangeStats timeStats;
    collect_stats(energyStats, timeStats);

    std::cout << "Energy stats: count=" << energyStats.count
              << ", nonFinite=" << energyStats.nonFinite
              << ", min=" << (std::isfinite(energyStats.min) ? energyStats.min : 0.0)
              << ", max=" << (std::isfinite(energyStats.max) ? energyStats.max : 0.0)
              << ", mean=" << energyStats.mean() << std::endl;
    std::cout << "Time stats (pico): count=" << timeStats.count
              << ", nonFinite=" << timeStats.nonFinite
              << ", min=" << (std::isfinite(timeStats.min) ? timeStats.min : 0.0)
              << ", max=" << (std::isfinite(timeStats.max) ? timeStats.max : 0.0)
              << ", mean=" << timeStats.mean() << std::endl;

    constexpr std::size_t kBins = 20;
    auto build_hist = [&](double minVal, double maxVal, auto valueGetter) {
        std::vector<uint64_t> hist(kBins, 0);
        if (!std::isfinite(minVal) || !std::isfinite(maxVal) || maxVal <= minVal)
        {
            return hist;
        }
        const double range = maxVal - minVal;

        openpni::io::listmode::ListmodeFileInput input;
        input.Open(inputPath.string());
        const auto segmentNum = input.SegmentNum();
        for (uint32_t segIdx = 0; segIdx < segmentNum; ++segIdx)
        {
            auto segment = input.ReadSegment(segIdx, segIdx + 1);
            const auto singles = openpni::distributed::coin::readSinglesFromSegment(segment);
            if (singles.empty())
            {
                continue;
            }
            for (const auto &single : singles)
            {
                const double value = valueGetter(single);
                if (!std::isfinite(value))
                {
                    continue;
                }
                const double scaled = (value - minVal) / range;
                const std::size_t bin = std::min<std::size_t>(kBins - 1, static_cast<std::size_t>(scaled * kBins));
                hist[bin]++;
            }
        }
        return hist;
    };

    const auto energyHist = build_hist(
        energyStats.min,
        energyStats.max,
        [](const auto &single) { return static_cast<double>(single.energy); });

    const auto timeHist = build_hist(
        timeStats.min,
        timeStats.max,
        [](const auto &single) { return static_cast<double>(single.timevalue_pico); });

    if (!energyHist.empty())
    {
        std::cout << "Energy histogram (" << kBins << " bins):";
        for (const auto count : energyHist)
        {
            std::cout << " " << count;
        }
        std::cout << std::endl;
    }

    if (!timeHist.empty())
    {
        std::cout << "Time histogram (" << kBins << " bins, pico):";
        for (const auto count : timeHist)
        {
            std::cout << " " << count;
        }
        std::cout << std::endl;
    }
}

namespace
{
std::optional<std::string> parse_arg_value(const std::string &arg, const std::string &key)
{
    if (arg == key)
    {
        return std::nullopt;
    }
    const std::string prefix = key + "=";
    if (arg.rfind(prefix, 0) == 0)
    {
        return arg.substr(prefix.size());
    }
    return std::nullopt;
}

void print_usage(const char *exe)
{
    std::cout << "Usage: " << exe << " [--cali_path PATH] [--data_path PATH] [--out_path PATH]" << std::endl;
}
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h")
        {
            print_usage(argv[0]);
            return 0;
        }

        if (auto value = parse_arg_value(arg, "--cali_path"))
        {
            cali_path = *value;
            continue;
        }
        if (auto value = parse_arg_value(arg, "--data_path"))
        {
            data_path = *value;
            continue;
        }
        if (auto value = parse_arg_value(arg, "--out_path"))
        {
            out_path = *value;
            continue;
        }

        if (arg == "--cali_path" || arg == "--data_path" || arg == "--out_path")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "Missing value for " << arg << std::endl;
                print_usage(argv[0]);
                return 1;
            }
            const std::string next = argv[++i];
            if (arg == "--cali_path")
            {
                cali_path = next;
            }
            else if (arg == "--data_path")
            {
                data_path = next;
            }
            else
            {
                out_path = next;
            }
            continue;
        }

        std::cerr << "Unknown argument: " << arg << std::endl;
        print_usage(argv[0]);
        return 1;
    }

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

   // 工具调用，批量转换50100原始数据
   convert_50100_rawdata_batch_process();

    std::cout << "[Test 3] Testing 50100 callback mode..." << std::endl;
   test_50100_930_callback();

//     //export_singles_payload_only("/media/lenovo/1TB/50100data/pni_res/singles/singles_50100_part1.lsingle");

    // 工具调用，批量转换50100单事件数据
    convert_50100_singles_batch_process();

    // //工具调用，批量转换RS单事件数据为PNI singles
    // convert_rs_singles_batch_process();

    // analyze_first_pni_singles_in_folder(path_pre + "/pni_singles");

    // std::cout << "[Test 4] Splitting 50100 rawdatas channels..." << std::endl;
    // split_930_data();
    
    std::cout << "\n======================================" << std::endl;
    std::cout << "     All tests completed!" << std::endl;
    std::cout << "======================================" << std::endl;

    return 0;
}

// g++ -fdiagnostics-color=always -g /media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/tests/test_pni_r2c.cpp /media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/build/GpuSort.o -o /media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/build/test_pni_r2c.o -std=c++20 -O3 -march=native -fopenmp $(pkg-config --cflags --libs libpni) -ltbb -L/usr/local/cuda/lib64 -lcudart && ./build/test_pni_r2c.o