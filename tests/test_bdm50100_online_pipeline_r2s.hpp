#pragma once

/**
 * @file test_bdm50100_online_pipeline_r2s.hpp
 * @brief R2S baseline + AsyncRawDataToR2SBridge wrapper isolated from coin headers
 *        (pni/node/Coincidence.hpp vs pni/node/misc/Coincidence.hpp conflict).
 */

#include <pni/io/IO.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace bdm50100_online
{

struct BaselineDigest
{
    uint64_t callbacks = 0;
    uint64_t totalSingles = 0;
    uint64_t checksumXor = 0;
};

struct BaselineResult
{
    bool success = false;
    uint32_t processedSegments = 0;
    uint64_t processedPackets = 0;
    BaselineDigest digest;
};

std::vector<std::string> collectBdmCalibrationFiles(const std::string &caliDir);

BaselineResult runOfflineBaseline(
    const std::string &rawPath,
    const std::vector<std::string> &calibrationFiles,
    uint32_t segmentLimit,
    uint64_t packetLimit,
    float energyLow_eV,
    float energyHigh_eV,
    uint32_t chunkPackets = 65536);

using SinglesReadyFn = std::function<bool(
    std::vector<openpni::Single> &&singles,
    uint64_t clockMs,
    uint32_t durationMs)>;

struct BridgeStats
{
    uint64_t enqueuedSegments = 0;
    uint64_t processedSegments = 0;
    uint64_t droppedSegments = 0;
    uint64_t enqueueFullHits = 0;
    bool healthy = true;
};

class OnlineR2SBridge
{
public:
    OnlineR2SBridge(
        const std::vector<std::string> &calibrationFiles,
        SinglesReadyFn onSingles,
        float energyLow_eV,
        float energyHigh_eV,
        const std::string &singlesOutputDir = "Data/result/Bdm50100/online_pipeline_singles");
    ~OnlineR2SBridge();

    OnlineR2SBridge(const OnlineR2SBridge &) = delete;
    OnlineR2SBridge &operator=(const OnlineR2SBridge &) = delete;

    bool start(uint16_t channelCount);
    bool stop();
    BridgeStats stats() const;
    void setReleaseFn(std::function<void(uint64_t)> releaseFn);
    std::function<bool(const openpni::RawDataView &)> makeRawDataCallback();
    const std::string &singlesOutputDir() const;

private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;
};

} // namespace bdm50100_online
