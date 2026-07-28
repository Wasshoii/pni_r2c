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
#include <pni/io/ListmodeIO.hpp>
#include <pni/node/misc/Coincidence.hpp>
#include <pni/tools/UniPtr.hpp>

#include "core/streaming/StreamingCoincidence.hpp"
#include "core/streaming/multi_gpu/CoincidenceMultiGpuEngine.hpp"

namespace fs = std::filesystem;
using openpni::distributed::streaming::TimeAlignerConfig;
using openpni::distributed::streaming::createBDM50100_9120AlignerConfig;
namespace multi_gpu = openpni::distributed::streaming::multi_gpu;

namespace
{
    std::string data_root = "/media/lenovo/1TB/50100data/test_9120";

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

    struct ListmodeKey
    {
        uint16_t channel1 = 0;
        uint16_t crystal1 = 0;
        uint16_t channel2 = 0;
        uint16_t crystal2 = 0;
        int32_t dt = 0;
        uint32_t timestamp = 0;

        auto tie() const
        {
            return std::tie(channel1, crystal1, channel2, crystal2, dt, timestamp);
        }

        bool operator<(const ListmodeKey &other) const { return tie() < other.tie(); }
        bool operator==(const ListmodeKey &other) const { return tie() == other.tie(); }
    };

    ListmodeKey makeKey(const openpni::Listmode &lm)
    {
        return ListmodeKey{
            lm.channelIndex1,
            lm.crystalIndex1,
            lm.channelIndex2,
            lm.crystalIndex2,
            lm.time1_2_100fs,
            lm.timestamp_100us};
    }

    std::map<ListmodeKey, uint64_t> countListmodes(std::span<const openpni::Listmode> events)
    {
        std::map<ListmodeKey, uint64_t> hist;
        for (const auto &lm : events)
        {
            ++hist[makeKey(lm)];
        }
        return hist;
    }

    bool compareHistograms(
        const std::map<ListmodeKey, uint64_t> &a,
        const std::map<ListmodeKey, uint64_t> &b,
        const char *label)
    {
        if (a == b)
        {
            return true;
        }
        std::cerr << label << " histogram mismatch: legacy entries=" << a.size()
                  << " multi-gpu entries=" << b.size() << '\n';
        return false;
    }

    std::vector<std::string> collectSinglesFiles(const std::string &dir)
    {
        std::vector<std::string> files;
        if (!fs::exists(dir))
        {
            return files;
        }
        for (const auto &entry : fs::directory_iterator(dir))
        {
            if (!entry.is_regular_file())
            {
                continue;
            }
            const auto path = entry.path();
            if (path.extension() == ".lsingle")
            {
                files.push_back(path.string());
            }
        }
        std::sort(files.begin(), files.end());
        return files;
    }

    std::vector<openpni::Single> readSinglesFromSegment(
        openpni::io::listmode::ListmodeFileSegment &segment)
    {
        const auto data = segment.GetHAnyData();
        if (!data.local_crystal_index1 || !data.channel_index1 || !data.absolute_timestamp1_100fs)
        {
            throw std::runtime_error("Single segment missing required fields");
        }

        std::vector<openpni::Single> singles(data.count);
        for (std::size_t i = 0; i < data.count; ++i)
        {
            singles[i].channelIndex = data.channel_index1[i];
            singles[i].crystalIndex = data.local_crystal_index1[i];
            singles[i].timevalue_100fs = data.absolute_timestamp1_100fs[i];
            singles[i].energy = data.energy1 ? data.energy1[i] : 0.0f;
        }
        return singles;
    }

    bool loadSinglesBatch(
        const std::vector<std::string> &files,
        size_t max_singles,
        std::vector<openpni::Single> &out)
    {
        out.clear();
        for (const auto &filePath : files)
        {
            openpni::io::listmode::ListmodeFileInput inputFile;
            inputFile.Open(filePath);
            if (inputFile.Header().FileTypeName() !=
                openpni::io::listmode::fields::file_type_single_listmode)
            {
                continue;
            }

            for (uint32_t segIdx = 0; segIdx < inputFile.SegmentNum(); ++segIdx)
            {
                auto segment = inputFile.ReadSegment(segIdx);
                auto singles = readSinglesFromSegment(segment);
                out.insert(out.end(), singles.begin(), singles.end());
                if (max_singles > 0 && out.size() >= max_singles)
                {
                    out.resize(max_singles);
                    return true;
                }
            }
        }
        return !out.empty();
    }

    bool runLegacyCoincidence(
        const std::vector<openpni::Single> &singles,
        const TimeAlignerConfig &config,
        std::vector<openpni::Listmode> &prompt_out,
        std::vector<openpni::Listmode> &delay_out)
    {
        prompt_out.clear();
        delay_out.clear();

        openpni::Coincidence coin;
        std::vector<uint32_t> crystal_nums(config.channelNum, config.crystalsPerChannel);
        coin.setTotalCrystalNumOfEachChannel(crystal_nums);

        openpni::tools::UniPtr<openpni::Single> single_buffer{"legacy_s2c_singles"};
        openpni::tools::UniPtr<openpni::Listmode> coin_buffer{"legacy_s2c_coins"};

        single_buffer.CopyFromHost(std::span<const openpni::Single>(singles));
        std::vector<std::span<openpni::Single const>> inputList;
        inputList.push_back(single_buffer.CudaRStdSpan());

        auto [prompt, delay] = coin.getDListmode(inputList, config.coinProtocol);

        if (!prompt.empty())
        {
            coin_buffer.CopyFromCuda(prompt);
            const auto host = coin_buffer.HostRStdSpan();
            prompt_out.assign(host.begin(), host.end());
        }
        if (!delay.empty())
        {
            coin_buffer.CopyFromCuda(delay);
            const auto host = coin_buffer.HostRStdSpan();
            delay_out.assign(host.begin(), host.end());
        }
        return true;
    }

    bool runMultiGpuCoincidence(
        const std::vector<openpni::Single> &singles,
        const TimeAlignerConfig &config,
        std::vector<openpni::Listmode> &prompt_out,
        std::vector<openpni::Listmode> &delay_out)
    {
        prompt_out.clear();
        delay_out.clear();

        auto engine_config = multi_gpu::makeCoincidenceMultiGpuEngineConfig(config);
        multi_gpu::CoincidenceMultiGpuEngine engine;
        if (!engine.initialize(engine_config))
        {
            std::cerr << "Failed to initialize CoincidenceMultiGpuEngine\n";
            return false;
        }

        const auto result = engine.processSinglesSync(std::span<const openpni::Single>(singles));
        prompt_out.assign(result.prompt.begin(), result.prompt.end());
        delay_out.assign(result.delay.begin(), result.delay.end());
        return true;
    }
} // namespace

int main(int argc, char **argv)
{
    size_t max_singles = 2'000'000;

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h")
        {
            std::cout << "Usage: " << argv[0]
                      << " [--data_path PATH] [--max_singles N]\n";
            return 0;
        }
        if (auto value = parseArgValue(arg, "--data_path"))
        {
            data_root = *value;
            continue;
        }
        if (auto value = parseArgValue(arg, "--max_singles"))
        {
            max_singles = static_cast<size_t>(std::max(0, std::atoi(value->c_str())));
            continue;
        }
    }

    data_root = envOrDefault("R2C_COIN_MULTI_GPU_DATA_PATH", data_root);

    int device_count = 0;
    const cudaError_t count_err = cudaGetDeviceCount(&device_count);
    if (count_err != cudaSuccess || device_count < 1)
    {
        std::cerr << "SKIP: no CUDA device available\n";
        return 0;
    }

    const std::string node0_dir = data_root + "/pni_singles_node0";
    const std::string node1_dir = data_root + "/pni_singles_node1";
    auto files0 = collectSinglesFiles(node0_dir);
    auto files1 = collectSinglesFiles(node1_dir);
    if (files0.empty() && files1.empty())
    {
        std::cerr << "SKIP: no .lsingle files under " << data_root << '\n';
        return 0;
    }

    std::vector<std::string> files = files0;
    files.insert(files.end(), files1.begin(), files1.end());

    openpni::CoincidenceProtocol protocol;
    protocol.timeWindow_ps = 2000;
    protocol.delayTime_ps = 2000000;
    protocol.energyLower_eV = 421000.0f;
    protocol.energyUpper_eV = 1000000.0f;

    TimeAlignerConfig config = createBDM50100_9120AlignerConfig("/tmp/coin_multi_gpu_test", protocol);
    config.enableMultiGpu = true;

    std::vector<openpni::Single> singles;
    if (!loadSinglesBatch(files, max_singles, singles))
    {
        std::cerr << "SKIP: failed to load singles\n";
        return 0;
    }

    std::cout << "\n========== Coincidence Multi-GPU Consistency Test ==========\n";
    std::cout << "Data root: " << data_root << '\n';
    std::cout << "GPU count: " << device_count << '\n';
    std::cout << "Singles:   " << singles.size() << '\n';
    std::cout << "Channels:  " << config.channelNum << '\n';

    std::vector<openpni::Listmode> legacy_prompt;
    std::vector<openpni::Listmode> legacy_delay;
    std::vector<openpni::Listmode> multi_prompt;
    std::vector<openpni::Listmode> multi_delay;

    const auto legacy_start = std::chrono::steady_clock::now();
    if (!runLegacyCoincidence(singles, config, legacy_prompt, legacy_delay))
    {
        return 1;
    }
    const auto legacy_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - legacy_start)
                               .count();

    const auto multi_start = std::chrono::steady_clock::now();
    if (!runMultiGpuCoincidence(singles, config, multi_prompt, multi_delay))
    {
        return 1;
    }
    const auto multi_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - multi_start)
                              .count();

    std::cout << "\n========== Results ==========\n";
    std::cout << "legacy prompt=" << legacy_prompt.size()
              << " delay=" << legacy_delay.size()
              << " elapsed_ms=" << legacy_ms << '\n';
    std::cout << "multi-gpu prompt=" << multi_prompt.size()
              << " delay=" << multi_delay.size()
              << " elapsed_ms=" << multi_ms << '\n';

    const bool prompt_ok = compareHistograms(
        countListmodes(legacy_prompt), countListmodes(multi_prompt), "prompt");
    const bool delay_ok = compareHistograms(
        countListmodes(legacy_delay), countListmodes(multi_delay), "delay");

    if (!prompt_ok || !delay_ok)
    {
        return 1;
    }

    std::cout << "Coincidence multi-GPU consistency test passed\n";
    return 0;
}
