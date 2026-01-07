#include <pni/experimental/node/Coincidence.hpp>
#include <pni/io/IO.hpp>
#include <pni/process/Acquisition.hpp>
#include <pni/process/ListmodeProcessing.hpp>
#include <pni/CudaPtr.hpp>
#include <pni/basic/PetDataType.h>
#include <json.hpp>
#include <memory>
#include <string>
#include <cstdint>
#include <vector>
#include <iostream>
#include <pni/detector/BDMBiD.hpp>
#include "../src/core/aquisition-and-r2s/R2S.hpp"
#include "../src/core/testTool.hpp"
#include "../src/core/merge-and-coin/MergeAndCoin.hpp"

void test_bdmbid()
{
    std::string rawdataPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/raw_data/rawdata_100.data";
    std::string resPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid";
    std::vector<std::string> calibrationFilePaths = {
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_00.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_01.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_02.data",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_03.data"};

    auto config = createBDMBiDConfig(rawdataPath, resPath, calibrationFilePaths);
    processR2S(config);
}
void test_bdm2()
{
    std::string rawdataPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdm2/raw_data/2_PET_2Bed pet 600s-bed0.raw";
    std::string resPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdm2";
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

    auto config = createBDM2Config(rawdataPath, resPath, calibrationFilePaths);
    processR2S(config);
}
int main()
{
    // 示例：打印文件信息
    // print_rawdata_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/raw_data/split_Data/rawdata_100_channel_0.raw");
    // print_rawdata_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/raw_data/split_Data/rawdata_100_channel_1.raw");
    // print_rawdata_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/raw_data/split_Data/rawdata_100_channel_2.raw");
    // print_rawdata_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/raw_data/split_Data/rawdata_100_channel_3.raw");

    // 示例：运行 BDMBiD 测试
    // test_bdmbid();
    // std::string resPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split";
    // std::vector<std::string> calibrationFilePaths = {
    //     "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_00.data",
    //     "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_01.data",
    //     "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_02.data",
    //     "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/calibration/channel_03.data"};

    // for (int i = 0; i < 4; i++)
    // {
    //     std::string singleRawdataPath = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/bdmbid/raw_data/split_Data/rawdata_100_channel_" + std::to_string(i) + ".raw";

    //     auto config = createBDMBiDConfig(singleRawdataPath, resPath, calibrationFilePaths, "singles_channel_" + std::to_string(i), {uint16_t(i)});
    //     processR2S(config);
    // }

    //  test_bdm2();

    // 测试single合并

    // print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_0.single");
    // print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_1.single");
    // print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_2.single");
    // print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_3.single");
    std::vector<std::string> files = {
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_0.single",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_1.single",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_2.single",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_3.single"};
    auto time_ms = timer(
        [&]
        {
            CoincidenceProcessConfig mergeConfig;
            mergeConfig.enable = true;
            mergeConfig.channelNum = 4;
            mergeConfig.crystalsPerChannel = 400 * 8;
            mergeConfig.outputDir = "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/coin";

            merge_single_files(files, "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/merged.single", mergeConfig);
        },
        1);

    // print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/singles.single");
    // print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/merged.single");

    std::cout << "Merging time: " << time_ms << " ms" << std::endl;

    return 0;
}

// g++ -fdiagnostics-color=always -g /media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/tests/test_pni_r2c.cpp \
-o /media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/build/test_pni_r2c.o \
-std=c++20 -O3 -march=native -fopenmp $(pkg-config --cflags --libs libpni) -ltbb && ./build/test_pni_r2c.o