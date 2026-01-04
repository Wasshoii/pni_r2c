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
#include "R2S.hpp"
#include "testTool.hpp"
#include "MergeAndCoin.hpp"

// template <typename T>
// void save_vector(std::vector<T> const &data, std::string fileName)
// {
//     std::ofstream file(fileName);
//     if (!file.is_open())
//         std::cerr << "Cannot open file " << fileName << std::endl;
//     file.write((char *)&data[0], data.size() * sizeof(T));
// }

// #pragma pack(push, 1)
// struct LocalListmode
// {
//     unsigned short channelIndex1;
//     unsigned short crystalIndex1;
//     unsigned short channelIndex2;
//     unsigned short crystalIndex2;
//     unsigned short time1_2pico;
// };
// #pragma pack(pop)

// void appendCoinToDataFile(
//     const std::string &dir,
//     const std::string &filename,
//     std::span<openpni::experimental::node::LocalListmode const> coins)
// {
//     std::string filepath = dir + "/" + filename + ".data";

//     std::ofstream ofs(filepath, std::ios::binary | std::ios::app);
//     if (!ofs)
//     {
//         throw std::runtime_error("Failed to open file: " + filepath);
//         return;
//     }

//     SegmentHeader header;
//     header.count = static_cast<uint32_t>(coins.size());
//     header.timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
//                               std::chrono::system_clock::now().time_since_epoch())
//                               .count();

//     ofs.write(reinterpret_cast<const char *>(&header), sizeof(header));

//     const void *srcPtr = coins.data();
//     size_t bytes = coins.size() * sizeof(openpni::experimental::node::LocalListmode);

//     if (isDevicePointer(srcPtr))
//     {
//         // GPU -> Host copy
//         std::vector<openpni::experimental::node::LocalListmode> hostBuf(coins.size());
//         cudaError_t err = cudaMemcpy(hostBuf.data(), srcPtr, bytes, cudaMemcpyDeviceToHost);
//         if (err != cudaSuccess)
//         {
//             throw std::runtime_error("cudaMemcpyDeviceToHost failed: " +
//                                      std::string(cudaGetErrorString(err)));
//         }
//         ofs.write(reinterpret_cast<const char *>(hostBuf.data()), bytes);
//     }
//     else
//     {
//         // Host -> File direct write
//         ofs.write(reinterpret_cast<const char *>(srcPtr), bytes);
//     }
// }

// #include "testTool.hpp"

// std::vector<uint32_t> rearrange(std::vector<uint32_t> vector)
// {
//     auto copy = std::vector<uint32_t>(vector.size(), 0);
//     for (auto layer = 0; layer < 4; layer++)
//     {
//         for (auto i = 0; i < 169 * 4 * 48; i++)
//         {
//             auto detector = i / (169 * 4);
//             auto crystal = i % (169 * 4);
//             auto dRing = detector / 24;
//             auto inRing = detector % 24;
//             auto v = crystal / (13 * 4);
//             auto u = crystal % (13 * 4);
//             auto x = v + inRing * 13;
//             auto y = u + dRing * 13 * 4;
//             copy[x + y * 312 + layer * 312 * 104] = vector[i + layer * 312 * 104];
//         }
//     }
//     return copy;
// }

// bool loadDataFile(
//     const std::string &filepath,
//     std::vector<std::vector<openpni::basic::LocalSingle>> &resultStorage)
// {
//     std::ifstream ifs(filepath, std::ios::binary);
//     if (!ifs)
//     {
//         throw std::runtime_error("Cannot open data file: " + filepath);
//     }

//     size_t totalBytesRead = 0;

//     uint32_t channelNum = resultStorage.size();
//     while (true)
//     {
//         SegmentHeader header;
//         if (!ifs.read(reinterpret_cast<char *>(&header), sizeof(header)))
//         {
//             break; // EOF 正常结束
//         }
//         if (header.magic != SEG_MAGIC)
//         {
//             throw std::runtime_error("Segment magic mismatch, file corrupted: " + filepath);
//         }

//         size_t segmentBytes = size_t(header.count) * sizeof(openpni::experimental::interface::LocalSingle);
//         totalBytesRead += sizeof(header) + segmentBytes;
//         if (totalBytesRead > MAX_READ_BYTES)
//         {
//             // 超过最大读取限制，避免 OOM
//             break;
//         }

//         // 为本段读取分配临时缓冲区（host 内存）
//         std::vector<openpni::experimental::interface::LocalSingle> buf(header.count);
//         if (!ifs.read(reinterpret_cast<char *>(buf.data()), segmentBytes))
//         {
//             throw std::runtime_error("Unexpected EOF when reading singles data.");
//         }

//         // 分发到 48 通道对应存储
//         for (const auto &s : buf)
//         {
//             if (s.channelIndex >= channelNum)
//             {
//                 // 若出现异常通道，跳过或可做 log
//                 continue;
//             }
//             openpni::basic::LocalSingle out;
//             out.crystalIndex = s.crystalIndex;
//             out.timevalue_pico = s.timevalue_pico;
//             out.energy = s.energy;

//             resultStorage[s.channelIndex].push_back(out);
//         }
//     }
//     return true;
// }

// bool loadCoinFile(
//     const std::string &filepath,
//     std::vector<std::vector<openpni::basic::LocalSingle>> &resultStorage)
// {
//     std::ifstream ifs(filepath, std::ios::binary);
//     if (!ifs)
//     {
//         throw std::runtime_error("Cannot open data file: " + filepath);
//     }

//     size_t totalBytesRead = 0;

//     uint32_t channelNum = resultStorage.size();
//     while (true)
//     {
//         SegmentHeader header;
//         if (!ifs.read(reinterpret_cast<char *>(&header), sizeof(header)))
//         {
//             break; // EOF 正常结束
//         }
//         if (header.magic != SEG_MAGIC)
//         {
//             throw std::runtime_error("Segment magic mismatch, file corrupted: " + filepath);
//         }

//         size_t segmentBytes = size_t(header.count) * sizeof(openpni::experimental::interface::LocalSingle);
//         totalBytesRead += sizeof(header) + segmentBytes;
//         if (totalBytesRead > MAX_READ_BYTES)
//         {
//             // 超过最大读取限制，避免 OOM
//             break;
//         }

//         // 为本段读取分配临时缓冲区（host 内存）
//         std::vector<openpni::experimental::interface::LocalSingle> buf(header.count);
//         if (!ifs.read(reinterpret_cast<char *>(buf.data()), segmentBytes))
//         {
//             throw std::runtime_error("Unexpected EOF when reading singles data.");
//         }

//         // 分发到 48 通道对应存储
//         for (const auto &s : buf)
//         {
//             if (s.channelIndex >= channelNum)
//             {
//                 // 若出现异常通道，跳过或可做 log
//                 continue;
//             }
//             openpni::basic::LocalSingle out;
//             out.crystalIndex = s.crystalIndex;
//             out.timevalue_pico = s.timevalue_pico;
//             out.energy = s.energy;

//             resultStorage[s.channelIndex].push_back(out);
//         }
//     }
//     return true;
// }

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

    print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_0.single");
    print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_1.single");
    print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_2.single");
    print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_3.single");
    std::vector<std::string> files = {
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_0.single",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_1.single",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_2.single",
        "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/singles_channel_3.single"};
    auto time_ms = timer(
        [&]
        {
            merge_single_files(files, "/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/merged.single", true);
        },
        1);

    std::cout << "Merging time: " << time_ms << " ms" << std::endl;

    print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/singles.single");
    print_single_file_info("/media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c/Data/result/Bdmbid/split/merged.single");

    return 0;
}

// g++ -std=c++20 raw2coin.cpp $(pkg-config --cflags --libs libpni) -o r2s_test