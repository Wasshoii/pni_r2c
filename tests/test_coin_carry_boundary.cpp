/**
 * @file test_coin_carry_boundary.cpp
 * @brief 三模式对比：整段金标准 / 分段无 carry / 分段有 carry，验证尾部保留能否找回边界丢失。
 *
 * Usage:
 *   test_coin_carry_boundary [--singles-dir PATH] [--max-singles N]
 *                            [--segment-duration-ps N]  (default: 100 ms)
 *                            [--channel-num N] [--crystals-per-channel N]
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <vector>

#include <cuda_runtime.h>
#include <pni/PnI-Config.hpp>
#include <pni/io/IO.hpp>
#include <pni/io/ListmodeIO.hpp>
#include <pni/node/misc/Coincidence.hpp>
#include <pni/tools/UniPtr.hpp>

namespace fs = std::filesystem;

namespace
{
    constexpr uint16_t kDefaultChannelNum = 48 * 3;
    constexpr uint32_t kDefaultCrystalsPerChannel = 6 * 6 * 8;
    constexpr float kEnergyLower_eV = 421000.0f;
    constexpr float kEnergyUpper_eV = 1000000.0f;
    constexpr int kDefaultTimeWindowPs = 2000;
    constexpr int kDefaultDelayTimePs = 2'000'000;
    // 100 ms in picoseconds.
    constexpr uint64_t kDefaultSegmentDurationPs = 100ull * 1'000'000'000ull;

    // Default data under repo Data/singles (override with --singles-dir).
    const std::string kDefaultSinglesDir = "/media/lenovo/1TB/pni_dis_r2c/Data/singles";

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
        bool operator<(const ListmodeKey &o) const { return tie() < o.tie(); }
        bool operator==(const ListmodeKey &o) const { return tie() == o.tie(); }
    };

    ListmodeKey makeKey(const openpni::Listmode &lm)
    {
        return ListmodeKey{
            lm.channelIndex1, lm.crystalIndex1,
            lm.channelIndex2, lm.crystalIndex2,
            lm.time1_2_100fs, lm.timestamp_100us};
    }

    using Hist = std::map<ListmodeKey, uint64_t>;

    Hist countListmodes(const std::vector<openpni::Listmode> &events)
    {
        Hist hist;
        for (const auto &lm : events)
        {
            ++hist[makeKey(lm)];
        }
        return hist;
    }

    uint64_t histTotal(const Hist &h)
    {
        uint64_t n = 0;
        for (const auto &[_, c] : h)
        {
            n += c;
        }
        return n;
    }

    /** Multiset difference |a| - |b| counting only keys where a has more. */
    uint64_t histDeficit(const Hist &ref, const Hist &other)
    {
        uint64_t deficit = 0;
        for (const auto &[k, cref] : ref)
        {
            const auto it = other.find(k);
            const uint64_t cother = (it == other.end()) ? 0 : it->second;
            if (cref > cother)
            {
                deficit += (cref - cother);
            }
        }
        return deficit;
    }

    uint64_t histExtra(const Hist &ref, const Hist &other)
    {
        return histDeficit(other, ref);
    }

    void printDiffSamples(const Hist &ref, const Hist &other, const char *label, size_t maxSamples = 8)
    {
        size_t printed = 0;
        for (const auto &[k, cref] : ref)
        {
            const auto it = other.find(k);
            const uint64_t cother = (it == other.end()) ? 0 : it->second;
            if (cref == cother)
            {
                continue;
            }
            std::cerr << "  [" << label << "] missing/extra key ch=" << k.channel1 << "/" << k.channel2
                      << " xtal=" << k.crystal1 << "/" << k.crystal2
                      << " dt=" << k.dt << " ts=" << k.timestamp
                      << " ref=" << cref << " other=" << cother << '\n';
            if (++printed >= maxSamples)
            {
                break;
            }
        }
        for (const auto &[k, cother] : other)
        {
            if (ref.count(k))
            {
                continue;
            }
            std::cerr << "  [" << label << "] only-in-other ch=" << k.channel1 << "/" << k.channel2
                      << " xtal=" << k.crystal1 << "/" << k.crystal2
                      << " dt=" << k.dt << " ts=" << k.timestamp
                      << " count=" << cother << '\n';
            if (++printed >= maxSamples * 2)
            {
                break;
            }
        }
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
            if (entry.path().extension() == ".lsingle")
            {
                files.push_back(entry.path().string());
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
            singles[i].energy_ev = data.energy1 ? data.energy1[i] : 0.0f;
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

    void autoAdjustEnergyWindow(
        const std::vector<openpni::Single> &singles,
        openpni::CoincidenceProtocol &protocol)
    {
        uint64_t inEv = 0;
        uint64_t inKev = 0;
        const float loK = protocol.energyLower_eV / 1000.0f;
        const float hiK = protocol.energyUpper_eV / 1000.0f;
        for (const auto &s : singles)
        {
            if (s.energy_ev >= protocol.energyLower_eV && s.energy_ev <= protocol.energyUpper_eV)
            {
                ++inEv;
            }
            if (s.energy_ev >= loK && s.energy_ev <= hiK)
            {
                ++inKev;
            }
        }
        std::cout << "[Energy] in eV-window=" << inEv << " in keV-window=" << inKev
                  << " / " << singles.size() << '\n';
        if (inEv == 0 && inKev > 0)
        {
            protocol.energyLower_eV = loK;
            protocol.energyUpper_eV = hiK;
            std::cout << "[Energy] Auto-switch window to keV: "
                      << protocol.energyLower_eV << "~" << protocol.energyUpper_eV << '\n';
        }
    }

    void appendDeviceListmodes(
        std::span<const openpni::Listmode> deviceSpan,
        std::vector<openpni::Listmode> &out)
    {
        if (deviceSpan.empty())
        {
            return;
        }
        const size_t offset = out.size();
        out.resize(offset + deviceSpan.size());
        const cudaError_t err = cudaMemcpy(
            out.data() + offset,
            deviceSpan.data(),
            deviceSpan.size() * sizeof(openpni::Listmode),
            cudaMemcpyDeviceToHost);
        if (err != cudaSuccess)
        {
            throw std::runtime_error(std::string("cudaMemcpy D2H failed: ") + cudaGetErrorString(err));
        }
    }

    void runCoincidence(
        openpni::Coincidence &coin,
        const std::vector<openpni::Single> &singles,
        const openpni::CoincidenceProtocol &protocol,
        uint64_t carryCutoffTime_100fs,
        std::vector<openpni::Listmode> &promptOut,
        std::vector<openpni::Listmode> &delayOut)
    {
        if (singles.empty())
        {
            return;
        }
        // Fresh UniPtr per call avoids Clear()/shrink cuda size-mismatch hazards.
        openpni::tools::UniPtr<openpni::Single> singleBuf{"carry_boundary_batch"};
        singleBuf.CopyFromHost(std::span<const openpni::Single>(singles));
        std::vector<std::span<openpni::Single const>> inputList;
        inputList.push_back(singleBuf.CudaRStdSpan());

        const auto result =
            coin.getDListmode(inputList, protocol, carryCutoffTime_100fs);

        appendDeviceListmodes(result.prompt, promptOut);
        appendDeviceListmodes(result.delay, delayOut);
        coin.clearCrystalCountMap();
    }

    struct SegmentBounds
    {
        size_t begin = 0; // index into sorted singles
        size_t end = 0;   // exclusive
        uint64_t tBegin = 0;
        uint64_t tEnd = 0; // exclusive cut (last segment: max+1)
    };

    std::vector<SegmentBounds> buildTimeSegments(
        const std::vector<openpni::Single> &sorted,
        uint64_t segmentDuration_100fs)
    {
        std::vector<SegmentBounds> segs;
        if (sorted.empty() || segmentDuration_100fs == 0)
        {
            return segs;
        }
        const uint64_t t0 = sorted.front().timevalue_100fs;
        const uint64_t tLast = sorted.back().timevalue_100fs;

        size_t idx = 0;
        uint64_t cut = t0 + segmentDuration_100fs;
        while (idx < sorted.size())
        {
            SegmentBounds s;
            s.begin = idx;
            s.tBegin = (segs.empty() ? t0 : segs.back().tEnd);
            // Advance cut until it covers at least one event, or finish.
            while (cut <= s.tBegin)
            {
                cut += segmentDuration_100fs;
            }
            if (cut > tLast)
            {
                s.end = sorted.size();
                s.tEnd = tLast + 1;
                segs.push_back(s);
                break;
            }
            while (idx < sorted.size() && sorted[idx].timevalue_100fs < cut)
            {
                ++idx;
            }
            s.end = idx;
            s.tEnd = cut;
            if (s.end > s.begin)
            {
                segs.push_back(s);
            }
            cut += segmentDuration_100fs;
            if (idx >= sorted.size())
            {
                break;
            }
        }
        return segs;
    }

    uint64_t overlapLength_100fs(const openpni::CoincidenceProtocol &protocol)
    {
        return (static_cast<uint64_t>(protocol.delayTime_ps) +
                static_cast<uint64_t>(protocol.timeWindow_ps)) *
               10ull;
    }

    void updateCarry(
        const std::vector<openpni::Single> &processed,
        uint64_t watermark,
        uint64_t overlap_100fs,
        std::vector<openpni::Single> &carry)
    {
        std::vector<openpni::Single> next;
        if (processed.empty() || watermark == 0)
        {
            carry = std::move(next);
            return;
        }
        const uint64_t carryBegin = watermark > overlap_100fs ? watermark - overlap_100fs : 0;
        next.reserve(processed.size() / 8 + 8);
        for (const auto &s : processed)
        {
            if (s.timevalue_100fs > carryBegin && s.timevalue_100fs <= watermark)
            {
                next.push_back(s);
            }
        }
        carry = std::move(next);
    }

    struct ModeResult
    {
        std::vector<openpni::Listmode> prompt;
        std::vector<openpni::Listmode> delay;
        double elapsed_ms = 0;
        size_t segmentCount = 0;
        double avgCarry = 0;
    };

    ModeResult runFull(
        openpni::Coincidence &coin,
        const std::vector<openpni::Single> &singles,
        const openpni::CoincidenceProtocol &protocol)
    {
        ModeResult r;
        const auto t0 = std::chrono::high_resolution_clock::now();
        runCoincidence(coin, singles, protocol, 0, r.prompt, r.delay);
        const auto t1 = std::chrono::high_resolution_clock::now();
        r.elapsed_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        r.segmentCount = 1;
        return r;
    }

    ModeResult runSegmented(
        openpni::Coincidence &coin,
        const std::vector<openpni::Single> &sorted,
        const std::vector<SegmentBounds> &segs,
        const openpni::CoincidenceProtocol &protocol,
        bool useCarry)
    {
        ModeResult r;
        r.segmentCount = segs.size();
        const uint64_t O = overlapLength_100fs(protocol);
        std::vector<openpni::Single> carry;
        uint64_t carrySum = 0;
        uint64_t prevWatermark = 0;

        const auto t0 = std::chrono::high_resolution_clock::now();
        for (size_t si = 0; si < segs.size(); ++si)
        {
            const auto &seg = segs[si];
            std::vector<openpni::Single> batch;
            batch.reserve((useCarry ? carry.size() : 0) + (seg.end - seg.begin));
            if (useCarry)
            {
                batch.insert(batch.end(), carry.begin(), carry.end());
                carrySum += carry.size();
            }
            batch.insert(batch.end(), sorted.begin() + static_cast<std::ptrdiff_t>(seg.begin),
                         sorted.begin() + static_cast<std::ptrdiff_t>(seg.end));

            const uint64_t cutoff = (useCarry && !carry.empty()) ? prevWatermark : 0;
            runCoincidence(coin, batch, protocol, cutoff, r.prompt, r.delay);

            if (useCarry)
            {
                uint64_t W = 0;
                for (size_t i = seg.begin; i < seg.end; ++i)
                {
                    W = std::max(W, sorted[i].timevalue_100fs);
                }
                updateCarry(batch, W, O, carry);
                prevWatermark = W;
            }
        }
        const auto t1 = std::chrono::high_resolution_clock::now();
        r.elapsed_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        r.avgCarry = (useCarry && !segs.empty())
                         ? static_cast<double>(carrySum) / static_cast<double>(segs.size())
                         : 0.0;
        return r;
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
} // namespace

int main(int argc, char **argv)
{
    std::string singlesDir = kDefaultSinglesDir;
    size_t maxSingles = 0; // 0 = all
    uint64_t segmentDurationPs = kDefaultSegmentDurationPs; // default 100 ms
    uint16_t channelNum = kDefaultChannelNum;
    uint32_t crystalsPerChannel = kDefaultCrystalsPerChannel;
    openpni::CoincidenceProtocol protocol;
    protocol.timeWindow_ps = kDefaultTimeWindowPs;
    protocol.delayTime_ps = kDefaultDelayTimePs;
    protocol.energyLower_eV = kEnergyLower_eV;
    protocol.energyUpper_eV = kEnergyUpper_eV;

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h")
        {
            std::cout << "Usage: " << argv[0]
                      << " [--singles-dir=PATH] [--max-singles=N]\n"
                      << "       [--segment-duration-ps=N]  (default: 1e11 = 100 ms)\n"
                      << "       [--channel-num=N] [--crystals-per-channel=N]\n";
            return 0;
        }
        if (auto v = parseArgValue(arg, "--singles-dir"))
        {
            singlesDir = *v;
        }
        else if (auto v = parseArgValue(arg, "--max-singles"))
        {
            maxSingles = static_cast<size_t>(std::stoull(*v));
        }
        else if (auto v = parseArgValue(arg, "--segment-duration-ps"))
        {
            segmentDurationPs = std::stoull(*v);
        }
        else if (auto v = parseArgValue(arg, "--channel-num"))
        {
            channelNum = static_cast<uint16_t>(std::stoul(*v));
        }
        else if (auto v = parseArgValue(arg, "--crystals-per-channel"))
        {
            crystalsPerChannel = static_cast<uint32_t>(std::stoul(*v));
        }
        else
        {
            std::cerr << "Unknown arg: " << arg << '\n';
            return 2;
        }
    }

    const auto files = collectSinglesFiles(singlesDir);
    if (files.empty())
    {
        std::cerr << "No .lsingle under " << singlesDir << '\n';
        return 2;
    }

    std::vector<openpni::Single> singles;
    std::cout << "========== Coin Carry Boundary Test ==========\n";
    std::cout << "Singles dir: " << singlesDir << " (" << files.size() << " files)\n";
    if (!loadSinglesBatch(files, maxSingles, singles))
    {
        std::cerr << "Failed to load singles\n";
        return 2;
    }
    std::cout << "Loaded singles: " << singles.size() << '\n';
    {
        uint16_t maxCh = 0;
        uint16_t maxXtal = 0;
        for (const auto &s : singles)
        {
            maxCh = std::max<uint16_t>(maxCh, s.channelIndex & 0x7FFF);
            maxXtal = std::max<uint16_t>(maxXtal, s.crystalIndex & 0x7FFF);
        }
        std::cout << "Data max channelIndex=" << maxCh << " max crystalIndex=" << maxXtal << '\n';
        if (maxCh >= channelNum)
        {
            std::cerr << "WARNING: max channelIndex " << maxCh
                      << " >= channelNum " << channelNum
                      << "; bumping channelNum to " << (maxCh + 1) << '\n';
            channelNum = static_cast<uint16_t>(maxCh + 1);
        }
        if (maxXtal >= crystalsPerChannel)
        {
            std::cerr << "WARNING: max crystalIndex " << maxXtal
                      << " >= crystalsPerChannel " << crystalsPerChannel
                      << "; bumping crystalsPerChannel to " << (maxXtal + 1) << '\n';
            crystalsPerChannel = static_cast<uint32_t>(maxXtal) + 1;
        }
    }

    std::sort(singles.begin(), singles.end(),
              [](const openpni::Single &a, const openpni::Single &b) {
                  return a.timevalue_100fs < b.timevalue_100fs;
              });

    autoAdjustEnergyWindow(singles, protocol);

    const uint64_t O = overlapLength_100fs(protocol);
    if (segmentDurationPs == 0)
    {
        segmentDurationPs = kDefaultSegmentDurationPs;
    }
    const uint64_t segmentDuration_100fs = segmentDurationPs * 10ull;
    std::cout << "[Segment] duration_ps=" << segmentDurationPs
              << " (" << (static_cast<double>(segmentDurationPs) / 1e9) << " ms)"
              << " duration_100fs=" << segmentDuration_100fs
              << " overlap_O_100fs=" << O << '\n';

    const auto segs = buildTimeSegments(singles, segmentDuration_100fs);
    if (segs.size() < 2)
    {
        std::cerr << "WARNING: only " << segs.size()
                  << " segment(s); decrease --segment-duration-ps to observe boundary loss\n";
    }
    std::cout << "Segments: " << segs.size() << '\n';
    if (!segs.empty())
    {
        uint64_t sum = 0;
        for (const auto &s : segs)
        {
            sum += (s.end - s.begin);
        }
        std::cout << "Avg singles/seg: "
                  << (static_cast<double>(sum) / static_cast<double>(segs.size())) << '\n';
    }

    std::vector<uint32_t> crystalNums(channelNum, crystalsPerChannel);
    auto makeCoin = [&]() {
        auto c = std::make_unique<openpni::Coincidence>();
        c->setTotalCrystalNumOfEachChannel(crystalNums);
        return c;
    };

    ModeResult m1;
    ModeResult m2;
    ModeResult m3;

    {
        auto coin = makeCoin();
        std::cout << "\n--- M1: full batch (gold) ---\n" << std::flush;
        m1 = runFull(*coin, singles, protocol);
        std::cout << "prompt=" << m1.prompt.size() << " delay=" << m1.delay.size()
                  << " time_ms=" << m1.elapsed_ms << std::endl;
    }
    {
        auto coin = makeCoin();
        std::cout << "\n--- M2: segmented, no carry ---\n" << std::flush;
        m2 = runSegmented(*coin, singles, segs, protocol, false);
        std::cout << "prompt=" << m2.prompt.size() << " delay=" << m2.delay.size()
                  << " time_ms=" << m2.elapsed_ms << std::endl;
    }
    {
        auto coin = makeCoin();
        std::cout << "\n--- M3: segmented, with carry ---\n" << std::flush;
        m3 = runSegmented(*coin, singles, segs, protocol, true);
        std::cout << "prompt=" << m3.prompt.size() << " delay=" << m3.delay.size()
                  << " avg_carry=" << m3.avgCarry << " time_ms=" << m3.elapsed_ms << std::endl;
    }

    const Hist h1p = countListmodes(m1.prompt);
    const Hist h1d = countListmodes(m1.delay);
    const Hist h2p = countListmodes(m2.prompt);
    const Hist h2d = countListmodes(m2.delay);
    const Hist h3p = countListmodes(m3.prompt);
    const Hist h3d = countListmodes(m3.delay);

    const uint64_t lossP = histDeficit(h1p, h2p);
    const uint64_t lossD = histDeficit(h1d, h2d);
    const uint64_t residualP = histDeficit(h1p, h3p) + histExtra(h1p, h3p);
    const uint64_t residualD = histDeficit(h1d, h3d) + histExtra(h1d, h3d);
    // For residual equality check use exact match
    const bool m3PromptEq = (h1p == h3p);
    const bool m3DelayEq = (h1d == h3d);

    std::cout << "\n========== Comparison ==========\n";
    std::cout << "M1 prompt/delay totals: " << histTotal(h1p) << " / " << histTotal(h1d) << '\n';
    std::cout << "M2 prompt/delay totals: " << histTotal(h2p) << " / " << histTotal(h2d) << '\n';
    std::cout << "M3 prompt/delay totals: " << histTotal(h3p) << " / " << histTotal(h3d) << '\n';
    std::cout << "Boundary loss M1-M2 (prompt/delay): " << lossP << " / " << lossD << '\n';
    std::cout << "Residual |M1-M3| (prompt/delay keys asymmetric): " << residualP << " / " << residualD
              << '\n';
    if (lossP > 0)
    {
        const double rec =
            1.0 - static_cast<double>(histDeficit(h1p, h3p)) / static_cast<double>(lossP);
        std::cout << "Prompt recovery vs loss: " << rec << '\n';
    }
    if (lossD > 0)
    {
        const double rec =
            1.0 - static_cast<double>(histDeficit(h1d, h3d)) / static_cast<double>(lossD);
        std::cout << "Delay recovery vs loss: " << rec << '\n';
    }

    if (lossP == 0 && lossD == 0)
    {
        std::cerr << "WARNING: no boundary loss observed (M2==M1). "
                     "Try smaller --segment-duration-ps.\n";
    }

    int exitCode = 0;
    if (!m3PromptEq || !m3DelayEq)
    {
        std::cerr << "FAIL: M3 does not match M1 gold standard\n";
        if (!m3PromptEq)
        {
            printDiffSamples(h1p, h3p, "prompt");
        }
        if (!m3DelayEq)
        {
            printDiffSamples(h1d, h3d, "delay");
        }
        exitCode = 1;
    }
    else
    {
        std::cout << "PASS: M3 ≡ M1 (carry recovered boundary pairs)\n";
    }

    return exitCode;
}
