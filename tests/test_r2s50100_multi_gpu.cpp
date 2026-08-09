#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <cuda_runtime.h>
#include <pni/PnI-Config.hpp>
#include <pni/io/IO.hpp>

#include "core/io/IOAdapter.hpp"
#include "core/r2s/R2S.hpp"
#include "../src/tools/testTool.hpp"

namespace
{
    std::string path_pre = "/media/lenovo/1TB/50100data/test_9120";
    std::string out_path = path_pre;
    std::string cali_path = "/media/lenovo/1TB/50100data/pni_res/caliFile";

    std::string envOrDefault(const char *name, const std::string &fallback)
    {
        if (const char *value = std::getenv(name))
        {
            return value;
        }
        return fallback;
    }

    std::optional<std::string> parseArgValue(const std::string &arg, const std::string &key)
    {
        const std::string prefix = key + "=";
        if (arg.rfind(prefix, 0) == 0)
        {
            return arg.substr(prefix.size());
        }
        return std::nullopt;
    }

    struct SinglesKey
    {
        uint16_t channel = 0;
        uint16_t crystal = 0;
        float energy = 0.0f;
        uint64_t time = 0;

        auto tie() const { return std::tie(channel, crystal, energy, time); }

        bool operator<(const SinglesKey &other) const { return tie() < other.tie(); }
        bool operator==(const SinglesKey &other) const { return tie() == other.tie(); }
    };

    std::map<SinglesKey, uint64_t> countSingles(std::span<const openpni::Single> singles)
    {
        std::map<SinglesKey, uint64_t> hist;
        for (const auto &single : singles)
        {
            SinglesKey key{
                single.channelIndex,
                single.crystalIndex,
                single.energy_ev,
                single.timevalue_100fs};
            ++hist[key];
        }
        return hist;
    }

    bool compareSinglesHistograms(
        const std::map<SinglesKey, uint64_t> &a,
        const std::map<SinglesKey, uint64_t> &b)
    {
        if (a == b)
        {
            return true;
        }

        std::cerr << "Singles histogram mismatch:\n";
        std::cerr << "  legacy entries=" << a.size() << " multi-gpu entries=" << b.size() << '\n';
        return false;
    }

    std::vector<uint16_t> makeNode0ChannelIndices()
    {
        std::vector<uint16_t> channel_indices;
        channel_indices.reserve(288);
        for (uint16_t ch = 0; ch < 288; ++ch)
        {
            channel_indices.push_back(ch);
        }
        return channel_indices;
    }

    bool ensureMergedNode0Raw(bool allow_merge)
    {
        namespace fs = std::filesystem;

        const std::string merged_dir = out_path + "/pni_raw_node0";
        const auto existing = openpni::distributed::r2s::collectRawDataFiles(merged_dir);
        if (!existing.empty())
        {
            std::cout << "Using existing merged raw dir: " << merged_dir
                      << " (" << existing.size() << " files)\n";
            return true;
        }

        if (!allow_merge)
        {
            std::cerr << "Merged raw dir is empty: " << merged_dir << '\n';
            return false;
        }

        const std::string ring0_dir = path_pre + "/pni_raw_ring0";
        const std::string ring1_dir = path_pre + "/pni_raw_ring1";
        if (!fs::exists(ring0_dir) || !fs::is_directory(ring0_dir) ||
            !fs::exists(ring1_dir) || !fs::is_directory(ring1_dir))
        {
            std::cerr << "Merged raw not found and ring dirs missing:\n"
                      << "  " << ring0_dir << '\n'
                      << "  " << ring1_dir << '\n';
            return false;
        }

        std::cout << "Merging ring0+ring1 into " << merged_dir << " ...\n";
        const bool ok = merge_rawdata_dirs_by_clock(
            {ring0_dir, ring1_dir},
            merged_dir,
            576,
            "pniRaw-",
            ".bin",
            true,
            2);
        if (!ok)
        {
            std::cerr << "merge_rawdata_dirs_by_clock failed\n";
            return false;
        }

        return !openpni::distributed::r2s::collectRawDataFiles(merged_dir).empty();
    }

    openpni::distributed::r2s::R2SProcessConfig make9120Node0Config(bool use_legacy_single_gpu)
    {
        const std::vector<uint16_t> channel_indices = makeNode0ChannelIndices();
        auto config = openpni::distributed::r2s::createBDM50100_9120Config(
            "",
            out_path + "/pni_singles_multi_gpu_test",
            {cali_path, cali_path},
            "singles_9120_multi_gpu_test",
            channel_indices,
            4);

        config.saveData2SingleFile = false;
        config.asyncFileWrite = false;
        config.enableMultiGpu = !use_legacy_single_gpu;
        config.useEnergyCut = true;
        config.energyCutLow = 421000.0f;
        config.energyCutHigh = 1000000.0f;
        config.progressLogInterval = 0;
        return config;
    }

    bool run9120Node0R2S(
        bool use_legacy_single_gpu,
        const std::string &raw_file_path,
        uint32_t max_segments,
        std::vector<openpni::Single> &out_singles)
    {
        out_singles.clear();

        openpni::distributed::coreio::RawDataFileReader raw_file_reader;
        try
        {
            raw_file_reader.Open(raw_file_path);
        }
        catch (const std::exception &e)
        {
            std::cerr << "Failed to open raw file: " << raw_file_path
                      << " error=" << e.what() << '\n';
            return false;
        }

        const auto channel_num = raw_file_reader.Info().channelNum;
        const uint32_t segment_num = raw_file_reader.SegmentNum();
        const uint32_t segments_to_process = max_segments == 0
                                                 ? segment_num
                                                 : std::min(max_segments, segment_num);

        auto config = make9120Node0Config(use_legacy_single_gpu);
        config.onSinglesSpanReady = [&](std::span<const openpni::Single> singles,
                                        uint64_t,
                                        uint32_t) -> bool {
            if (openpni::distributed::r2s::isDevicePointer(singles.data()))
            {
                const auto host_singles = openpni::distributed::r2s::materializeSinglesOnHost(singles);
                out_singles.insert(out_singles.end(), host_singles.begin(), host_singles.end());
            }
            else
            {
                out_singles.insert(out_singles.end(), singles.begin(), singles.end());
            }
            return true;
        };

        openpni::distributed::r2s::R2SStreamProcessor processor(config);
        if (!processor.initialize(static_cast<uint16_t>(channel_num)))
        {
            std::cerr << "Failed to initialize R2S processor (legacy_single_gpu="
                      << (use_legacy_single_gpu ? "true" : "false") << ")\n";
            return false;
        }

        for (uint32_t seg = 0; seg < segments_to_process; ++seg)
        {
            auto segment = raw_file_reader.ReadSegment(seg, seg + 1);
            const auto view = segment.View();
            if (!processor.processSegment(view))
            {
                std::cerr << "processSegment failed at segment " << seg
                          << " (legacy_single_gpu=" << (use_legacy_single_gpu ? "true" : "false")
                          << ")\n";
                return false;
            }
        }

        return processor.finalize();
    }
}

int main(int argc, char **argv)
{
    bool allow_merge = true;
    uint32_t max_segments = 1;
    std::string raw_file_override;

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h")
        {
            std::cout << "Usage: " << argv[0]
                      << " [--data_path PATH] [--out_path PATH] [--cali_path PATH]"
                      << " [--raw_file PATH] [--max_segments N] [--no-merge]\n";
            return 0;
        }
        if (arg == "--no-merge")
        {
            allow_merge = false;
            continue;
        }
        if (auto value = parseArgValue(arg, "--data_path"))
        {
            path_pre = *value;
            out_path = path_pre;
            continue;
        }
        if (auto value = parseArgValue(arg, "--out_path"))
        {
            out_path = *value;
            continue;
        }
        if (auto value = parseArgValue(arg, "--cali_path"))
        {
            cali_path = *value;
            continue;
        }
        if (auto value = parseArgValue(arg, "--raw_file"))
        {
            raw_file_override = *value;
            continue;
        }
        if (auto value = parseArgValue(arg, "--max_segments"))
        {
            max_segments = static_cast<uint32_t>(std::max(0, std::atoi(value->c_str())));
            continue;
        }
    }

    path_pre = envOrDefault("R2C_MULTI_GPU_DATA_PATH", path_pre);
    out_path = envOrDefault("R2C_MULTI_GPU_OUT_PATH", out_path);
    cali_path = envOrDefault("R2C_MULTI_GPU_CALI_DIR", cali_path);

    int device_count = 0;
    const cudaError_t count_err = cudaGetDeviceCount(&device_count);
    if (count_err != cudaSuccess || device_count < 1)
    {
        std::cerr << "SKIP: no CUDA device available\n";
        return 0;
    }

    std::cout << "\n========== BDM50100 9120 Two-Ring Multi-GPU Test ==========\n";
    std::cout << "Data root: " << path_pre << '\n';
    std::cout << "Output:    " << out_path << '\n';
    std::cout << "Cali:      " << cali_path << '\n';
    std::cout << "GPU count: " << device_count << '\n';

    std::string raw_file_path = raw_file_override;
    if (raw_file_path.empty())
    {
        if (!ensureMergedNode0Raw(allow_merge))
        {
            std::cerr << "SKIP: failed to prepare merged node0 raw data\n";
            return 0;
        }

        const std::string merged_dir = out_path + "/pni_raw_node0";
        const auto raw_files = openpni::distributed::r2s::collectRawDataFiles(merged_dir);
        if (raw_files.empty())
        {
            std::cerr << "SKIP: no raw files in " << merged_dir << '\n';
            return 0;
        }
        raw_file_path = raw_files.front().path;
    }

    std::cout << "Raw file:  " << raw_file_path << '\n';
    std::cout << "Segments:  " << max_segments << " (0 = all)\n";
    std::cout << "Channels:  0..287 (9120 node0, ring0+ring1)\n";

    std::vector<openpni::Single> legacy_singles;
    std::vector<openpni::Single> multi_gpu_singles;

    const auto legacy_start = std::chrono::steady_clock::now();
    if (!run9120Node0R2S(true, raw_file_path, max_segments, legacy_singles))
    {
        return 1;
    }
    const auto legacy_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - legacy_start)
                               .count();

    const auto multi_start = std::chrono::steady_clock::now();
    if (!run9120Node0R2S(false, raw_file_path, max_segments, multi_gpu_singles))
    {
        return 1;
    }
    const auto multi_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - multi_start)
                              .count();

    const auto legacy_hist = countSingles(legacy_singles);
    const auto multi_hist = countSingles(multi_gpu_singles);

    std::cout << "\n========== Results ==========\n";
    std::cout << "legacy singles=" << legacy_singles.size()
              << " elapsed_ms=" << legacy_ms << '\n';
    std::cout << "multi-gpu singles=" << multi_gpu_singles.size()
              << " elapsed_ms=" << multi_ms << '\n';

    if (!compareSinglesHistograms(legacy_hist, multi_hist))
    {
        return 1;
    }

    std::cout << "BDM50100 9120 two-ring multi-GPU consistency test passed\n";
    return 0;
}
