#include <pni/experimental/node/Coincidence.hpp>
#include <pni/io/IO.hpp>
#include <pni/process/Acquisition.hpp>
#include <pni/experimental/node/BDMBiDR2S.hpp>
#include <pni/experimental/node/ConvergedR2S.hpp>
#include <pni/process/ListmodeProcessing.hpp>
#include <pni/CudaPtr.hpp>
#include <pni/basic/PetDataType.h>
#include <fstream>
#include <json.hpp>
#include <memory>
#include <string>
#include <cstdint>
#include <vector>
#include <iostream>
#include <pni/detector/BDMBiD.hpp>

constexpr size_t MAX_READ_BYTES = size_t(3ULL * 1024ULL * 1024ULL * 1024ULL); // 3GB
constexpr uint32_t SEG_MAGIC = 0x53474D54;                                    // 'SGMT'
std::string rawdataPath = "/home/ustc/桌面/BiD/rawdata_100.data";
std::vector<std::string> calibrationFilePaths = {
    "/home/ustc/桌面/BiD/CaliFile/20251222_235129/channel_00.data",
    "/home/ustc/桌面/BiD/CaliFile/20251222_235129/channel_01.data",
    "/home/ustc/桌面/BiD/CaliFile/20251222_235129/channel_02.data",
    "/home/ustc/桌面/BiD/CaliFile/20251222_235129/channel_03.data"};

struct SegmentHeader
{
    uint32_t magic = SEG_MAGIC; // 'SGMT' 用于校验与同步
    uint32_t count;             // 本段包含的事件条数
    uint64_t timestamp_ns;      // 可记录写入时刻 or 数据采样时刻（可选）
};
template <typename T>
void save_vector(std::vector<T> const &data, std::string fileName)
{
    std::ofstream file(fileName);
    if (!file.is_open())
        std::cerr << "Cannot open file " << fileName << std::endl;
    file.write((char *)&data[0], data.size() * sizeof(T));
}

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

void appendSinglesToDataFile(
    const std::string &dir,
    const std::string &filename,
    std::span<openpni::experimental::interface::LocalSingle const> singles)
{
    std::string filepath = dir + "/" + filename + ".data";

    std::ofstream ofs(filepath, std::ios::binary | std::ios::app);
    if (!ofs)
    {
        throw std::runtime_error("Failed to open file: " + filepath);
        return;
    }

    SegmentHeader header;
    header.count = static_cast<uint32_t>(singles.size());
    header.timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();

    ofs.write(reinterpret_cast<const char *>(&header), sizeof(header));

    // for (uint32_t i = 0; i < 100; i++)
    // {
    //     std::cout << "ChannelIndex: " << singles[i].channelIndex << " CrystalIndex: " << singles[i].crystalIndex << " Energy: " << singles[i].energy << " TimeValuePico: "
    //               << singles[i].timevalue_pico << std::endl;
    // }

    const void *srcPtr = singles.data();
    size_t bytes = singles.size() * sizeof(openpni::experimental::interface::LocalSingle);

    if (isDevicePointer(srcPtr))
    {
        // GPU -> Host copy
        std::vector<openpni::experimental::interface::LocalSingle> hostBuf(singles.size());
        cudaError_t err = cudaMemcpy(hostBuf.data(), srcPtr, bytes, cudaMemcpyDeviceToHost);
        if (err != cudaSuccess)
        {
            throw std::runtime_error("cudaMemcpyDeviceToHost failed: " +
                                     std::string(cudaGetErrorString(err)));
        }
        ofs.write(reinterpret_cast<const char *>(hostBuf.data()), bytes);
    }
    else
    {
        // Host -> File direct write
        ofs.write(reinterpret_cast<const char *>(srcPtr), bytes);
    }
}

#pragma pack(push, 1)
struct LocalListmode
{
    unsigned short channelIndex1;
    unsigned short crystalIndex1;
    unsigned short channelIndex2;
    unsigned short crystalIndex2;
    unsigned short time1_2pico;
};
#pragma pack(pop)

void appendCoinToDataFile(
    const std::string &dir,
    const std::string &filename,
    std::span<openpni::experimental::node::LocalListmode const> coins)
{
    std::string filepath = dir + "/" + filename + ".data";

    std::ofstream ofs(filepath, std::ios::binary | std::ios::app);
    if (!ofs)
    {
        throw std::runtime_error("Failed to open file: " + filepath);
        return;
    }

    SegmentHeader header;
    header.count = static_cast<uint32_t>(coins.size());
    header.timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();

    ofs.write(reinterpret_cast<const char *>(&header), sizeof(header));

    const void *srcPtr = coins.data();
    size_t bytes = coins.size() * sizeof(openpni::experimental::node::LocalListmode);

    if (isDevicePointer(srcPtr))
    {
        // GPU -> Host copy
        std::vector<openpni::experimental::node::LocalListmode> hostBuf(coins.size());
        cudaError_t err = cudaMemcpy(hostBuf.data(), srcPtr, bytes, cudaMemcpyDeviceToHost);
        if (err != cudaSuccess)
        {
            throw std::runtime_error("cudaMemcpyDeviceToHost failed: " +
                                     std::string(cudaGetErrorString(err)));
        }
        ofs.write(reinterpret_cast<const char *>(hostBuf.data()), bytes);
    }
    else
    {
        // Host -> File direct write
        ofs.write(reinterpret_cast<const char *>(srcPtr), bytes);
    }
}

std::vector<uint32_t> rearrange(std::vector<uint32_t> vector)
{
    auto copy = std::vector<uint32_t>(vector.size(), 0);
    for (auto layer = 0; layer < 4; layer++)
    {
        for (auto i = 0; i < 169 * 4 * 48; i++)
        {
            auto detector = i / (169 * 4);
            auto crystal = i % (169 * 4);
            auto dRing = detector / 24;
            auto inRing = detector % 24;
            auto v = crystal / (13 * 4);
            auto u = crystal % (13 * 4);
            auto x = v + inRing * 13;
            auto y = u + dRing * 13 * 4;
            copy[x + y * 312 + layer * 312 * 104] = vector[i + layer * 312 * 104];
        }
    }
    return copy;
}

bool loadDataFile(
    const std::string &filepath,
    std::vector<std::vector<openpni::basic::LocalSingle>> &resultStorage)
{
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs)
    {
        throw std::runtime_error("Cannot open data file: " + filepath);
    }

    size_t totalBytesRead = 0;

    uint32_t channelNum = resultStorage.size();
    while (true)
    {
        SegmentHeader header;
        if (!ifs.read(reinterpret_cast<char *>(&header), sizeof(header)))
        {
            break; // EOF 正常结束
        }
        if (header.magic != SEG_MAGIC)
        {
            throw std::runtime_error("Segment magic mismatch, file corrupted: " + filepath);
        }

        size_t segmentBytes = size_t(header.count) * sizeof(openpni::experimental::interface::LocalSingle);
        totalBytesRead += sizeof(header) + segmentBytes;
        if (totalBytesRead > MAX_READ_BYTES)
        {
            // 超过最大读取限制，避免 OOM
            break;
        }

        // 为本段读取分配临时缓冲区（host 内存）
        std::vector<openpni::experimental::interface::LocalSingle> buf(header.count);
        if (!ifs.read(reinterpret_cast<char *>(buf.data()), segmentBytes))
        {
            throw std::runtime_error("Unexpected EOF when reading singles data.");
        }

        // 分发到 48 通道对应存储
        for (const auto &s : buf)
        {
            if (s.channelIndex >= channelNum)
            {
                // 若出现异常通道，跳过或可做 log
                continue;
            }
            openpni::basic::LocalSingle out;
            out.crystalIndex = s.crystalIndex;
            out.timevalue_pico = s.timevalue_pico;
            out.energy = s.energy;

            resultStorage[s.channelIndex].push_back(out);
        }
    }
    return true;
}

bool loadCoinFile(
    const std::string &filepath,
    std::vector<std::vector<openpni::basic::LocalSingle>> &resultStorage)
{
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs)
    {
        throw std::runtime_error("Cannot open data file: " + filepath);
    }

    size_t totalBytesRead = 0;

    uint32_t channelNum = resultStorage.size();
    while (true)
    {
        SegmentHeader header;
        if (!ifs.read(reinterpret_cast<char *>(&header), sizeof(header)))
        {
            break; // EOF 正常结束
        }
        if (header.magic != SEG_MAGIC)
        {
            throw std::runtime_error("Segment magic mismatch, file corrupted: " + filepath);
        }

        size_t segmentBytes = size_t(header.count) * sizeof(openpni::experimental::interface::LocalSingle);
        totalBytesRead += sizeof(header) + segmentBytes;
        if (totalBytesRead > MAX_READ_BYTES)
        {
            // 超过最大读取限制，避免 OOM
            break;
        }

        // 为本段读取分配临时缓冲区（host 内存）
        std::vector<openpni::experimental::interface::LocalSingle> buf(header.count);
        if (!ifs.read(reinterpret_cast<char *>(buf.data()), segmentBytes))
        {
            throw std::runtime_error("Unexpected EOF when reading singles data.");
        }

        // 分发到 48 通道对应存储
        for (const auto &s : buf)
        {
            if (s.channelIndex >= channelNum)
            {
                // 若出现异常通道，跳过或可做 log
                continue;
            }
            openpni::basic::LocalSingle out;
            out.crystalIndex = s.crystalIndex;
            out.timevalue_pico = s.timevalue_pico;
            out.energy = s.energy;

            resultStorage[s.channelIndex].push_back(out);
        }
    }
    return true;
}

auto timer(auto func, auto time)
{
    auto now = std::chrono::steady_clock::now();
    for (int i = 0; i < time; i++)
        func();
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - now).count();
}

int main()
{
    std::cout << "Starting R2S processing test..." << std::endl;
    auto mRawFileInput = std::make_unique<openpni::io::RawFileInput>();
    mRawFileInput->open(rawdataPath);
    auto Header = mRawFileInput->header();
    auto channelNum = Header.channelNum;
    auto segmentNum = Header.segmentNum;

    std::vector<openpni::experimental::interface::SingleGenerator *> generatorsVector;
    std::cout << "Loading " << channelNum << " channels' calibration data..." << std::endl;

    for (uint32_t i = 0; i < channelNum; i++)
    {
        auto generator = new openpni::experimental::node::BDMBiDR2S();

        generator->setChannelIndex(i);
        generator->loadCalibration(calibrationFilePaths[i]);
        generatorsVector.push_back(generator);
    }

    auto R2S = openpni::experimental::node::ConvergedR2S();
    auto Coin = openpni::experimental::node::Coincidence();
    std::vector<uint32_t> crystalNumOfEachChannel(channelNum, 400 * 8);
    Coin.setTotalCrystalNumOfEachChannel(crystalNumOfEachChannel);
    std::cout << "Setting up ConvergedR2S with generators..." << std::endl;
    R2S.setChannels(generatorsVector);
    std::cout << "success" << std::endl;

    for (uint64_t i = 0; i < segmentNum; i++)
    {
        auto segment = mRawFileInput->readSegment(i, i + 1);
        auto view = segment.view(mRawFileInput->header(), mRawFileInput->segmentHeader(i));
        auto count = view.count;
        if (!count || !view.data)
        {
            std::cout << "No data in segment " << i << std::endl;
            continue;
        }

        try
        {
            auto time_ms = timer(
                [&]
                {
                    auto d_data = openpni::experimental::node::DPackets::fromHost(view.data, view.offset, view.length, view.channel, view.count);
                    openpni::experimental::node::CoincidenceProtocol protocol;
                    auto r2sResults = R2S.r2s_cuda(d_data.raw, d_data.offset, d_data.length, d_data.channel, d_data.count);
                    // appendSinglesToDataFile("/home/ustc/桌面/BiD", "singles", r2sResults[1]);
                    auto [prompt, delay] = Coin.getDListmode(r2sResults, protocol);
                    appendCoinToDataFile("/home/ustc/桌面/BiD", "prompt", prompt);
                    appendCoinToDataFile("/home/ustc/桌面/BiD", "delay", delay);
                    // auto r2sTest = R2S.r2s_cpu(view.data, view.offset, view.length, view.channel, view.count);
                    // appendSinglesToDataFile("/home/ustc/桌面/BiD", "singles", r2sTest[1]);
                },
                1);
            std::cout << "Data = " << view.count * 1024 << " bytes, time = " << time_ms << ", Speed = " << double(view.count * 1024) / 1024 / 1024 / (time_ms / 1) << " MB/ms\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "Exception at segment " << i << ": " << e.what() << std::endl;
            throw;
        }
    }
    // auto crystalCountMap = Coin.dumpCrystalCountMap();
    // crystalCountMap = rearrange(crystalCountMap);
    // save_vector(crystalCountMap, "Crystal");
    // std::cout << "R2S processing test completed." << std::endl;

    // std::vector<std::vector<openpni::basic::LocalSingle>> singlesIn;
    // singlesIn.resize(48);

    // loadDataFile("/home/ustc/DataTest/data/singles.data", singlesIn);

    // CoinTest(singlesIn);
    return 0;
}

// g++ -std=c++20 test.cpp $(pkg-config --cflags --libs libpni) -o r2s_test