#include "core/r2s/R2S.hpp"

#include <cctype>
#include <chrono>
#include <glog/logging.h>
#include <set>
#include <sstream>

namespace openpni::distributed::r2s
{
    namespace
    {
        bool hasExtension(const std::filesystem::path &path, const std::vector<std::string> &extensions)
        {
            if (extensions.empty())
            {
                return true;
            }

            const std::string ext = path.extension().string();
            for (const auto &rawExt : extensions)
            {
                if (rawExt.empty())
                {
                    continue;
                }

                if (rawExt[0] == '.')
                {
                    if (ext == rawExt)
                    {
                        return true;
                    }
                }
                else if (ext == std::string(".") + rawExt)
                {
                    return true;
                }
            }

            return false;
        }

        bool tryExtractNumericSuffix(const std::string &filename,
                                     const std::string &prefix,
                                     const std::string &suffix,
                                     uint64_t &value)
        {
            if (prefix.empty() || suffix.empty())
            {
                return false;
            }

            if (filename.size() <= prefix.size() + suffix.size())
            {
                return false;
            }

            if (filename.compare(0, prefix.size(), prefix) != 0)
            {
                return false;
            }

            if (filename.compare(filename.size() - suffix.size(), suffix.size(), suffix) != 0)
            {
                return false;
            }

            const size_t numberOffset = prefix.size();
            const size_t numberLength = filename.size() - prefix.size() - suffix.size();
            const std::string numberText = filename.substr(numberOffset, numberLength);
            if (numberText.empty())
            {
                return false;
            }

            for (unsigned char ch : numberText)
            {
                if (std::isdigit(ch) == 0)
                {
                    return false;
                }
            }

            try
            {
                value = std::stoull(numberText);
            }
            catch (const std::exception &)
            {
                return false;
            }

            return true;
        }
    }

    bool isDevicePointer(const void *ptr)
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

    std::vector<Single> materializeSinglesOnHost(std::span<Single const> singles)
    {
        std::vector<Single> hostSingles;
        if (singles.empty())
        {
            return hostSingles;
        }

        hostSingles.resize(singles.size());
        const Single *dataPtr = singles.data();
        if (isDevicePointer(dataPtr))
        {
            cudaError_t err = cudaMemcpy(hostSingles.data(), dataPtr,
                                         singles.size() * sizeof(Single),
                                         cudaMemcpyDeviceToHost);
            if (err != cudaSuccess)
            {
                throw std::runtime_error("cudaMemcpyDeviceToHost failed: " +
                                         std::string(cudaGetErrorString(err)));
            }
            return hostSingles;
        }

        std::copy(singles.begin(), singles.end(), hostSingles.begin());
        return hostSingles;
    }

    std::vector<std::string> collectCalibrationFiles(
        const std::string &directory,
        const std::vector<std::string> &extensions,
        bool sortByName,
        const std::string &namePrefix,
        const std::string &nameSuffix)
    {
        std::vector<std::string> files;

        if (directory.empty())
        {
            return files;
        }

        std::error_code ec;
        const std::filesystem::path dirPath(directory);
        if (!std::filesystem::exists(dirPath, ec) || !std::filesystem::is_directory(dirPath, ec))
        {
            LOG(ERROR) << "Calibration directory not found: " << directory;
            return files;
        }

        for (const auto &entry : std::filesystem::directory_iterator(dirPath, ec))
        {
            if (ec)
            {
                LOG(ERROR) << "Failed to iterate calibration directory: " << directory
                           << ", error=" << ec.message();
                break;
            }

            if (!entry.is_regular_file(ec))
            {
                continue;
            }

            const auto &path = entry.path();
            if (!hasExtension(path, extensions))
            {
                continue;
            }

            files.push_back(path.string());
        }

        if (sortByName && !namePrefix.empty() && !nameSuffix.empty())
        {
            std::sort(files.begin(), files.end(), [&](const std::string &a, const std::string &b)
                      {
                          const std::string nameA = std::filesystem::path(a).filename().string();
                          const std::string nameB = std::filesystem::path(b).filename().string();
                          uint64_t valueA = 0;
                          uint64_t valueB = 0;
                          const bool hasNumberA = tryExtractNumericSuffix(nameA, namePrefix, nameSuffix, valueA);
                          const bool hasNumberB = tryExtractNumericSuffix(nameB, namePrefix, nameSuffix, valueB);

                          if (hasNumberA && hasNumberB)
                          {
                              if (valueA != valueB)
                              {
                                  return valueA < valueB;
                              }
                              return nameA < nameB;
                          }

                          return nameA < nameB;
                      });
        }
        else if (sortByName)
        {
            std::sort(files.begin(), files.end(), [](const std::string &a, const std::string &b)
                      {
                          return std::filesystem::path(a).filename().string() <
                                 std::filesystem::path(b).filename().string();
                      });
        }
        
        LOG(INFO) << "Collected " << files.size() << " calibration files from: " << directory;

        return files;
    }

    bool appendSinglesToSingleFile(
        openpni::distributed::coreio::RollingFileWriter<
            openpni::distributed::coreio::SinglesFileWriter,
            openpni::distributed::coreio::SingleWriterOptions> &outputFile,
        std::span<Single const> singles,
        uint64_t clock_ms,
        uint32_t duration_ms)
    {
        if (singles.empty())
        {
            return true;
        }

        // 估算本次写入的字节数，用于分卷阈值判断（仅在 maxFileSizeBytes > 0 时生效）
        constexpr size_t ESTIMATED_SINGLE_BYTES = 16;
        const uint64_t sizeEstimate = singles.size() * ESTIMATED_SINGLE_BYTES;

        try
        {
            if (isDevicePointer(singles.data()))
            {
                auto hostSingles = materializeSinglesOnHost(singles);
                return outputFile.AppendSegment(
                    sizeEstimate,
                    std::span<const Single>(hostSingles.data(), hostSingles.size()),
                    clock_ms,
                    duration_ms);
            }

            return outputFile.AppendSegment(sizeEstimate, singles, clock_ms, duration_ms);
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "Error appending singles to file: " << e.what();
            return false;
        }
    }

    openpni::interface::ISingleGenerator *createSingleGenerator(
        const R2SProcessConfig &config,
        uint16_t localIndex,
        uint16_t globalChannelIndex)
    {
        openpni::interface::ISingleGenerator *generator = nullptr;

        switch (config.detectorType)
        {
        case DetectorType::BDM2:
            generator = new openpni::BDM2R2S();
            break;
        case DetectorType::BDM50100:
        {
            auto *g50100 = new openpni::device::bdm50100_v2::BDM50100R2S();
            openpni::device::bdm50100_v2::BDM50100R2SParams params{};
            params.matchXTalkEnabled = config.matchXTalkEnabled;
            params.timeWindow = config.timeWindow;
            params.timeShift = config.timeShift;
            params.crossTalkEnabled = config.crossTalkEnabled;
            params.crossTalkTimeWindow = config.crossTalkTimeWindow;
            params.__deviceId = config.__deviceId;
            params.energyThresholds = config.energyThresholds;
            g50100->setParams(params);
            generator = g50100;
            break;
        }
        default:
            throw std::runtime_error("Unknown detector type");
        }

        generator->SetChannelIndex(localIndex);
        generator->LoadCalibration(config.calibrationFiles[globalChannelIndex]);

        return generator;
    }

    AsyncSingleFileWriter::AsyncSingleFileWriter(size_t maxQueueSize)
        : m_maxQueueSize(maxQueueSize)
    {
    }

    AsyncSingleFileWriter::~AsyncSingleFileWriter()
    {
        stop();
    }

    bool AsyncSingleFileWriter::open(const std::string &sessionDir, const std::string &filePrefix,
                                      uint32_t totalCrystals,
                                      openpni::distributed::coreio::SingleWriterOptions options)
    {
        const bool opened = m_output.Open(
            sessionDir, filePrefix, "lsingle", std::move(options),
            [totalCrystals](openpni::distributed::coreio::SinglesFileWriter &w, const std::string &path)
            {
                w.Open(path, totalCrystals);
            });

        if (!opened)
        {
            LOG(ERROR) << "[AsyncWriter] Failed to open output file with prefix " << filePrefix
                       << " in " << sessionDir;
            return false;
        }

        m_running = true;
        m_writerThread = std::thread([this]
                                     { writerLoop(); });

        LOG(INFO) << "[AsyncWriter] Started, queue size: " << m_maxQueueSize;
        return true;
    }

    bool AsyncSingleFileWriter::submit(std::vector<Single> &&singles, uint64_t clock_ms, uint32_t duration_ms)
    {
        if (!m_running.load())
        {
            return false;
        }

        std::unique_lock<std::mutex> lock(m_mutex);
        m_cvNotFull.wait(lock, [this]
                         { return m_queue.size() < m_maxQueueSize || !m_running.load(); });

        if (!m_running.load())
        {
            return false;
        }

        m_queue.push({std::move(singles), clock_ms, duration_ms});
        m_pendingCount++;
        lock.unlock();
        m_cvNotEmpty.notify_one();

        return true;
    }

    void AsyncSingleFileWriter::stop()
    {
        if (!m_running.exchange(false))
        {
            return;
        }

        m_cvNotEmpty.notify_all();
        m_cvNotFull.notify_all();

        if (m_writerThread.joinable())
        {
            m_writerThread.join();
        }

        LOG(INFO) << "[AsyncWriter] Stopped, total written: " << m_totalWritten.load()
                  << " singles";
    }

    void AsyncSingleFileWriter::flush()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cvFlushed.wait(lock, [this]
                         { return m_queue.empty() || !m_running.load(); });
    }

    void AsyncSingleFileWriter::writerLoop()
    {
        while (m_running.load() || !m_queue.empty())
        {
            AsyncWriteTask task;
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_cvNotEmpty.wait(lock, [this]
                                  { return !m_queue.empty() || !m_running.load(); });

                if (m_queue.empty())
                {
                    continue;
                }

                task = std::move(m_queue.front());
                m_queue.pop();
                m_pendingCount--;
            }

            m_cvNotFull.notify_one();

            if (!task.singles.empty())
            {
                constexpr size_t ESTIMATED_SINGLE_BYTES = 16;
                bool success = m_output.AppendSegment(
                    task.singles.size() * ESTIMATED_SINGLE_BYTES,
                    std::span<const Single>(task.singles.data(), task.singles.size()),
                    task.clock_ms,
                    task.duration_ms);

                if (success)
                {
                    m_totalWritten += task.singles.size();
                }
                else
                {
                    LOG(ERROR) << "[AsyncWriter] Failed to write segment";
                }
            }

            if (m_queue.empty())
            {
                m_cvFlushed.notify_all();
            }
        }
    }

    R2SStreamProcessor::R2SStreamProcessor(const R2SProcessConfig &config)
        : m_config(config)
    {
    }

    R2SStreamProcessor::~R2SStreamProcessor()
    {
        finalize();
    }

    bool R2SStreamProcessor::initialize(uint16_t inputChannelNum)
    {
        if (m_initialized)
        {
            return true;
        }

        m_inputChannelNum = inputChannelNum;

        if (m_config.useEnergyCut && m_config.energyCutHigh < m_config.energyCutLow)
        {
            LOG(WARNING) << "Energy window invalid: high < low, disabling energy cut";
            m_config.useEnergyCut = false;
        }

        try
        {
            const auto detectorTypeName = [this]() -> const char *
            {
                switch (m_config.detectorType)
                {
                case DetectorType::BDM2:
                    return "BDM2";
                case DetectorType::BDM50100:
                    return "BDM50100";
                case DetectorType::BDM100100:
                    return "BDM100100";
                case DetectorType::BDMBiD:
                    return "BDMBiD";
                default:
                    return "Unknown";
                }
            };

            LOG(INFO) << "Starting R2S processing...";
            LOG(INFO) << "Detector: " << detectorTypeName();
            if (m_inputChannelNum > 0)
            {
                LOG(INFO) << "Input channels: " << m_inputChannelNum;
            }

            if (!prepareChannelsToProcess())
            {
                m_hadError = true;
                return false;
            }

            if (!prepareGenerators())
            {
                m_hadError = true;
                return false;
            }

            LOG(INFO) << "Setting up ConvergedR2S with generators...";
            m_r2s.SetChannels(m_generatorsVector);
            LOG(INFO) << "Setup complete.";

            if (!prepareOutput())
            {
                m_hadError = true;
                return false;
            }

            m_initialized = true;
            m_finalized = false;
            return true;
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "Error initializing R2S stream processor: " << e.what();
            m_hadError = true;
            cleanupGenerators();
            return false;
        }
    }

    bool R2SStreamProcessor::processSegment(const openpni::RawDataView &view)
    {
        if (!m_initialized)
        {
            LOG(ERROR) << "R2S stream processor is not initialized";
            m_hadError = true;
            return false;
        }

        const uint64_t segmentId = m_totalSegmentsProcessed++;

        const bool needPerfLog =
            m_config.progressLogInterval > 0 &&
            (segmentId == 0 || segmentId % m_config.progressLogInterval == 0);

        if (!view.count || !view.data)
        {
            if (needPerfLog)
            {
                LOG(INFO) << "Segment " << segmentId << ": No data, skipping";
            }
            return true;
        }

        openpni::RawDataView effectiveView = view;
        std::vector<uint64_t> filteredOffset;
        std::vector<uint16_t> filteredLength;
        std::vector<uint16_t> filteredChannel;

        if (m_filterUnassignedChannels && view.channel != nullptr)
        {
            filteredOffset.reserve(static_cast<size_t>(view.count));
            filteredLength.reserve(static_cast<size_t>(view.count));
            filteredChannel.reserve(static_cast<size_t>(view.count));
            for (uint64_t i = 0; i < view.count; ++i)
            {
                const uint16_t ch = view.channel[i];
                if (m_assignedChannelSet.find(ch) == m_assignedChannelSet.end())
                {
                    continue;
                }
                filteredOffset.push_back(view.offset ? view.offset[i] : 0);
                filteredLength.push_back(view.length ? view.length[i] : 0);
                filteredChannel.push_back(ch);
            }

            if (filteredChannel.empty())
            {
                if (needPerfLog)
                {
                    LOG(INFO) << "Segment " << segmentId
                              << ": No packets for assigned channels, skipping";
                }
                return true;
            }

            effectiveView.offset = filteredOffset.data();
            effectiveView.length = filteredLength.data();
            effectiveView.channel = filteredChannel.data();
            effectiveView.count = filteredChannel.size();
        }

        m_totalRawPackets += effectiveView.count;

        // Remap global channel IDs to local (0-based) indices for GPU kernel
        std::vector<uint16_t> remappedChannel;
        if (m_filterUnassignedChannels && !m_globalToLocalChannel.empty())
        {
            remappedChannel.resize(static_cast<size_t>(effectiveView.count));
            for (uint64_t i = 0; i < effectiveView.count; ++i)
            {
                remappedChannel[i] = m_globalToLocalChannel[effectiveView.channel[i]];
            }
            effectiveView.channel = remappedChannel.data();
        }

        const uint64_t clockMs = effectiveView.clock_ms;
        const uint32_t durationMs =
            effectiveView.duration_ms > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())
                ? std::numeric_limits<uint32_t>::max()
                : static_cast<uint32_t>(effectiveView.duration_ms);

        const auto perfStart = needPerfLog ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};

        try
        {
            auto d_data = openpni::DPackets::FromHost(
                effectiveView.data,
                effectiveView.offset,
                effectiveView.length,
                effectiveView.channel,
                effectiveView.count);

            auto r2sResults = m_r2s.R2S_CUDA(
                d_data.raw,
                d_data.offset,
                d_data.length,
                d_data.channel,
                d_data.count);

            const cudaError_t cudaSyncErr = cudaDeviceSynchronize();
            if (cudaSyncErr != cudaSuccess)
            {
                LOG(ERROR) << "CUDA sync error after R2S_CUDA: " << cudaGetErrorString(cudaSyncErr);
                m_hadError = true;
                return false;
            }

            const cudaError_t cudaLastErr = cudaGetLastError();
            if (cudaLastErr != cudaSuccess)
            {
                LOG(ERROR) << "CUDA async error after R2S_CUDA: " << cudaGetErrorString(cudaLastErr);
                m_hadError = true;
                return false;
            }

            if (m_config.r2sResultIndex >= r2sResults.size())
            {
                LOG(ERROR) << "Error: r2sResultIndex " << m_config.r2sResultIndex
                           << " is out of range, result size=" << r2sResults.size();
                m_hadError = true;
                return false;
            }

            auto &singlesSpan = r2sResults[m_config.r2sResultIndex];

            if (m_config.useEnergyCut)
            {
                if (!singlesSpan.empty() && isDevicePointer(singlesSpan.data()))
                {
                    const uint64_t filteredCount = openpni::distributed::r2s::d_filterSinglesByEnergy_R2S(
                        const_cast<Single *>(singlesSpan.data()),
                        singlesSpan.size(),
                        m_config.energyCutLow,
                        m_config.energyCutHigh);
                    singlesSpan = std::span<Single const>(singlesSpan.data(), filteredCount);
                }
            }

            if (m_config.sortDataByTime)
            {
                if (!singlesSpan.empty() && isDevicePointer(singlesSpan.data()))
                {
                    openpni::distributed::r2s::d_sortSinglesByTime_R2S(
                        const_cast<Single *>(singlesSpan.data()),
                        singlesSpan.size());
                }
            }

            // Restore global channel indices if local remapping was applied
            std::vector<Single> hostSinglesRestored;
            std::span<Single const> dispatchSpan = singlesSpan;
            if (m_filterUnassignedChannels && !m_localToGlobalChannel.empty() && !singlesSpan.empty())
            {
                hostSinglesRestored = materializeSinglesOnHost(singlesSpan);
                for (auto &s : hostSinglesRestored)
                {
                    if (s.channelIndex < m_localToGlobalChannel.size())
                    {
                        s.channelIndex = m_localToGlobalChannel[s.channelIndex];
                    }
                }
                dispatchSpan = std::span<Single const>(hostSinglesRestored.data(), hostSinglesRestored.size());
            }

            const bool callbackSuccess = dispatchSinglesToCallback(dispatchSpan, clockMs, durationMs);
            const bool fileSuccess = dispatchSinglesToFile(dispatchSpan, clockMs, durationMs);

            m_totalSingles += dispatchSpan.size();

            if (needPerfLog)
            {
                const auto perfEnd = std::chrono::steady_clock::now();
                const auto timeMs = std::chrono::duration_cast<std::chrono::milliseconds>(perfEnd - perfStart).count();

                LOG(INFO) << "Segment " << segmentId
                          << ": Processed " << effectiveView.count << " packets, generated "
                          << singlesSpan.size() << " singles";

                if (timeMs > 0)
                {
                    const double speedMbPerSec =
                        static_cast<double>(effectiveView.count * 1024ULL) / 1024.0 / 1024.0 /
                        (static_cast<double>(timeMs) / 1000.0);
                    LOG(INFO) << " speed = " << speedMbPerSec << " MB/s";
                }

                LOG(INFO) << "";
            }

            if (!callbackSuccess || !fileSuccess)
            {
                m_hadError = true;
            }

            return callbackSuccess && fileSuccess;
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "Exception at segment " << segmentId << ": " << e.what();
            m_hadError = true;
            return false;
        }
    }

    bool R2SStreamProcessor::finalize()
    {
        if (m_finalized)
        {
            return !m_hadError;
        }

        m_finalized = true;

        if (m_asyncWriter)
        {
            LOG(INFO) << "Waiting for async writer to complete...";
            m_asyncWriter->flush();
            m_asyncWriter->stop();
            LOG(INFO) << "Async writer completed, written: " << m_asyncWriter->getTotalWritten()
                      << " singles";
        }

        LOG(INFO) << "\n=== Processing Complete ===";
        LOG(INFO) << "Total raw packets: " << m_totalRawPackets;
        LOG(INFO) << "Total singles: " << m_totalSingles;
        LOG(INFO) << "Singles/Packet ratio: "
                  << (m_totalRawPackets > 0 ? static_cast<double>(m_totalSingles) / m_totalRawPackets : 0.0);

        if (m_config.saveData2SingleFile && m_hasStreamingCallback)
        {
            LOG(INFO) << "Output file: " << m_outputFilePath
                      << (m_config.asyncFileWrite ? " (async)" : " (sync)");
            LOG(INFO) << "Data also streamed via callback";
        }
        else if (m_config.saveData2SingleFile)
        {
            LOG(INFO) << "Output file: " << m_outputFilePath
                      << (m_config.asyncFileWrite ? " (async)" : " (sync)");
        }
        else
        {
            LOG(INFO) << "Data streamed via callback";
        }

        LOG(INFO) << "===========================\n";

        cleanupGenerators();
        m_initialized = false;
        return !m_hadError;
    }

    bool R2SStreamProcessor::prepareChannelsToProcess()
    {
        m_channelsToProcess.clear();
        m_assignedChannelSet.clear();
        m_filterUnassignedChannels = false;

        if (m_config.channelIndices.empty())
        {
            for (uint16_t i = 0; i < m_config.channelNums; i++)
            {
                m_channelsToProcess.push_back(i);
            }
            LOG(INFO) << "Processing all channels [0, " << m_config.channelNums << ")";
        }
        else
        {
            m_channelsToProcess = m_config.channelIndices;
            LOG(INFO) << "Processing assigned channels (" << m_channelsToProcess.size() << "): ";
            for (auto ch : m_channelsToProcess)
            {
                LOG(INFO) << ch << " ";
            }
            LOG(INFO) << "";

            // 整机上界用 channelNums；若已知 raw 头通道数，再核对文件头是否覆盖分配通道
            for (auto ch : m_channelsToProcess)
            {
                if (ch >= m_config.channelNums)
                {
                    LOG(ERROR) << "Error: Assigned channel index " << ch
                               << " exceeds instrument channelNums=" << m_config.channelNums;
                    return false;
                }
                if (m_inputChannelNum > 0 && ch >= m_inputChannelNum)
                {
                    LOG(ERROR) << "Error: Assigned channel index " << ch
                               << " exceeds raw file channelNum=" << m_inputChannelNum;
                    return false;
                }
            }

            m_filterUnassignedChannels = true;
        }

        m_assignedChannelSet.insert(m_channelsToProcess.begin(), m_channelsToProcess.end());
        return true;
    }

    bool R2SStreamProcessor::prepareGenerators()
    {
        if (m_config.forceFullCalibrationLoad && m_config.detectorType == DetectorType::BDM50100)
        {
            LOG(INFO) << "BDM50100 calibration mode: loading generators for assigned channels only"
                      << " (count=" << m_channelsToProcess.size() << ")";
        }

        for (auto channelIndex : m_channelsToProcess)
        {
            if (static_cast<size_t>(channelIndex) >= m_config.calibrationFiles.size() ||
                m_config.calibrationFiles[channelIndex].empty())
            {
                LOG(ERROR) << "Error: Missing calibration file for assigned channel " << channelIndex
                           << " (calibrationFiles.size=" << m_config.calibrationFiles.size() << ")";
                return false;
            }
        }

        LOG(INFO) << "Loading " << m_channelsToProcess.size() << " channels' calibration data...";

        // Build global <-> local channel mapping
        const uint16_t maxGlobalCh = *std::max_element(m_channelsToProcess.begin(), m_channelsToProcess.end());
        m_globalToLocalChannel.assign(static_cast<size_t>(maxGlobalCh) + 1, UINT16_MAX);
        m_localToGlobalChannel.resize(m_channelsToProcess.size());

        uint16_t localIdx = 0;
        for (auto channelIndex : m_channelsToProcess)
        {
            m_globalToLocalChannel[channelIndex] = localIdx;
            m_localToGlobalChannel[localIdx] = channelIndex;

            try
            {
                auto generator = createSingleGenerator(m_config, localIdx, channelIndex);
                m_generatorsVector.push_back(generator);
            }
            catch (const std::exception &e)
            {
                LOG(ERROR) << "Error creating generator for channel " << channelIndex << ": " << e.what();
                cleanupGenerators();
                return false;
            }
            ++localIdx;
        }

        return true;
    }

    bool R2SStreamProcessor::openOutputWithPrefix(const std::string &filePrefix)
    {
        const uint32_t totalCrystals = m_config.channelNums * m_config.crystalsPerChannel;

        openpni::distributed::coreio::SingleWriterOptions opts;
        opts.io.maxFileSizeBytes = m_config.singlesMaxFileSizeBytes;
        opts.io.enableOverrideExistingFile = m_config.singlesOverwriteExisting;

        m_outputFilePath = m_config.resultPath + "/" + filePrefix + ".lsingle";

        if (m_config.asyncFileWrite)
        {
            m_asyncWriter = std::make_unique<AsyncSingleFileWriter>(m_config.asyncWriteQueueSize);
            if (!m_asyncWriter->open(m_config.resultPath, filePrefix, totalCrystals, opts))
            {
                return false;
            }
            LOG(INFO) << "Output file (async): " << m_outputFilePath;
        }
        else
        {
            const bool opened = m_singleOutput.Open(
                m_config.resultPath, filePrefix, "lsingle", opts,
                [totalCrystals](openpni::distributed::coreio::SinglesFileWriter &w, const std::string &path)
                {
                    w.Open(path, totalCrystals);
                });
            if (!opened)
            {
                LOG(ERROR) << "Failed to open singles output file: " << m_outputFilePath;
                return false;
            }
            LOG(INFO) << "Output file (sync): " << m_outputFilePath;
        }

        LOG(INFO) << "Total crystals: " << totalCrystals;
        return true;
    }

    bool R2SStreamProcessor::reopenOutput(const std::string &filePrefix)
    {
        if (!m_config.saveData2SingleFile)
        {
            m_config.outputFileName = filePrefix;
            return true;
        }

        if (m_asyncWriter)
        {
            m_asyncWriter->flush();
            m_asyncWriter->stop();
            m_asyncWriter.reset();
        }
        else
        {
            m_singleOutput.Stop();
        }

        m_config.outputFileName = filePrefix;
        return openOutputWithPrefix(filePrefix);
    }

    bool R2SStreamProcessor::prepareOutput()
    {
        m_hasStreamingCallback =
            static_cast<bool>(m_config.onSinglesSpanReady) ||
            static_cast<bool>(m_config.onSinglesReady);

        if (!m_config.saveData2SingleFile && !m_hasStreamingCallback)
        {
            LOG(ERROR) << "Error: saveData2SingleFile is false and no callback is set";
            return false;
        }

        if (m_config.saveData2SingleFile)
        {
            if (!openOutputWithPrefix(m_config.outputFileName))
            {
                return false;
            }
        }

        if (m_config.saveData2SingleFile && m_hasStreamingCallback)
        {
            LOG(INFO) << "Dual mode: data will be saved to file AND sent via callback";
        }
        else if (!m_config.saveData2SingleFile && m_hasStreamingCallback)
        {
            LOG(INFO) << "Streaming mode: data will be sent via callback only";
        }

        return true;
    }

    bool R2SStreamProcessor::dispatchSinglesToCallback(std::span<Single const> singles, uint64_t clockMs, uint32_t durationMs)
    {
        if (m_config.onSinglesSpanReady)
        {
            const bool callbackSuccess = m_config.onSinglesSpanReady(singles, clockMs, durationMs);
            if (!callbackSuccess)
            {
                LOG(ERROR) << "Callback onSinglesSpanReady returned false, stopping";
            }
            return callbackSuccess;
        }

        if (m_config.onSinglesReady)
        {
            auto hostSingles = materializeSinglesOnHost(singles);
            const bool callbackSuccess = m_config.onSinglesReady(std::move(hostSingles), clockMs, durationMs);
            if (!callbackSuccess)
            {
                LOG(ERROR) << "Callback onSinglesReady returned false, stopping";
            }
            return callbackSuccess;
        }

        return true;
    }

    bool R2SStreamProcessor::dispatchSinglesToFile(std::span<Single const> singles, uint64_t clockMs, uint32_t durationMs)
    {
        if (!m_config.saveData2SingleFile)
        {
            return true;
        }

        if (m_config.asyncFileWrite)
        {
            auto hostSingles = materializeSinglesOnHost(singles);
            const bool fileSuccess = m_asyncWriter->submit(
                std::move(hostSingles),
                clockMs,
                durationMs);
            if (!fileSuccess)
            {
                LOG(ERROR) << "Failed to submit segment to async writer";
            }
            return fileSuccess;
        }

        const bool fileSuccess = appendSinglesToSingleFile(
            m_singleOutput,
            singles,
            clockMs,
            durationMs);
        if (!fileSuccess)
        {
            LOG(ERROR) << "Failed to append segment to single file";
        }
        return fileSuccess;
    }

    void R2SStreamProcessor::cleanupGenerators()
    {
        for (auto *generator : m_generatorsVector)
        {
            delete generator;
        }
        m_generatorsVector.clear();
    }

    AsyncRawDataToR2SBridge::AsyncRawDataToR2SBridge(const R2SProcessConfig &r2sConfig)
        : AsyncRawDataToR2SBridge(r2sConfig, Config())
    {
    }

    AsyncRawDataToR2SBridge::AsyncRawDataToR2SBridge(
        const R2SProcessConfig &r2sConfig,
        const Config &config)
        : m_r2sProcessor(r2sConfig),
          m_queue(config.queue),
          m_config(config)
    {
    }

    AsyncRawDataToR2SBridge::~AsyncRawDataToR2SBridge()
    {
        stop();
    }

    bool AsyncRawDataToR2SBridge::start(uint16_t inputChannelNum)
    {
        if (m_started.exchange(true, std::memory_order_acq_rel))
        {
            LOG(ERROR) << "[RawDataR2SBridge] already started";
            return false;
        }

        m_failed.store(false, std::memory_order_release);
        m_running.store(true, std::memory_order_release);

        if (!m_r2sProcessor.initialize(inputChannelNum))
        {
            m_running.store(false, std::memory_order_release);
            m_started.store(false, std::memory_order_release);
            return false;
        }

        m_consumerThread = std::thread([this]
                                       { consumerLoop(); });
        return true;
    }

    bool AsyncRawDataToR2SBridge::stop()
    {
        if (!m_started.exchange(false, std::memory_order_acq_rel))
        {
            return !m_failed.load(std::memory_order_acquire);
        }

        m_running.store(false, std::memory_order_release);
        if (m_consumerThread.joinable())
        {
            m_consumerThread.join();
        }

        const bool finalizeOk = m_r2sProcessor.finalize();
        return finalizeOk && !m_failed.load(std::memory_order_acquire);
    }

    bool AsyncRawDataToR2SBridge::enqueueRawData(const openpni::RawDataView &view)
    {
        if (!m_running.load(std::memory_order_acquire) || m_failed.load(std::memory_order_acquire))
        {
            return false;
        }

        while (m_running.load(std::memory_order_acquire) && !m_failed.load(std::memory_order_acquire))
        {
            if (m_queue.tryPushCopy(view))
            {
                m_enqueuedSegments.fetch_add(1, std::memory_order_relaxed);
                return true;
            }

            const uint64_t fullHits = m_enqueueFullHits.fetch_add(1, std::memory_order_relaxed) + 1;
            if (m_config.queueFullWarnEvery > 0 && fullHits % m_config.queueFullWarnEvery == 0)
            {
                LOG(ERROR) << "[RawDataR2SBridge] queue is full, depth="
                           << m_queue.size() << "/" << m_queue.capacity();
            }

            if (!m_config.blockWhenQueueFull)
            {
                if (m_config.dropWhenQueueFull)
                {
                    m_droppedSegments.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }

                return false;
            }

            if (m_config.queueFullBackoffUs > 0)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(m_config.queueFullBackoffUs));
            }
        }

        return false;
    }

    std::function<bool(const openpni::RawDataView &)> AsyncRawDataToR2SBridge::makeRawDataCallback()
    {
        return [this](const openpni::RawDataView &view)
        {
            return enqueueRawData(view);
        };
    }

    AsyncRawDataToR2SBridge::Stats AsyncRawDataToR2SBridge::stats() const
    {
        Stats s;
        s.enqueuedSegments = m_enqueuedSegments.load(std::memory_order_relaxed);
        s.processedSegments = m_processedSegments.load(std::memory_order_relaxed);
        s.droppedSegments = m_droppedSegments.load(std::memory_order_relaxed);
        s.enqueueFullHits = m_enqueueFullHits.load(std::memory_order_relaxed);
        s.queuePeakDepth = m_queue.peakSize();
        s.healthy = !m_failed.load(std::memory_order_acquire);
        return s;
    }

    bool AsyncRawDataToR2SBridge::healthy() const
    {
        return !m_failed.load(std::memory_order_acquire);
    }

    void AsyncRawDataToR2SBridge::consumerLoop()
    {
        while ((m_running.load(std::memory_order_acquire) || !m_queue.empty()) &&
               !m_failed.load(std::memory_order_acquire))
        {
            const bool consumed = m_queue.tryConsumeOne(
                [this](RawDataSegmentBuffer &segment)
                {
                    auto view = segment.toRawView();
                    if (!m_r2sProcessor.processSegment(view))
                    {
                        m_failed.store(true, std::memory_order_release);
                        m_running.store(false, std::memory_order_release);
                        return;
                    }

                    m_processedSegments.fetch_add(1, std::memory_order_relaxed);
                });

            if (!consumed)
            {
                if (m_config.consumerIdleBackoffUs > 0)
                {
                    std::this_thread::sleep_for(std::chrono::microseconds(m_config.consumerIdleBackoffUs));
                }
            }
        }
    }

    namespace
    {
        std::optional<uint64_t> tryParseRawStartClock(
            const std::string &filename,
            const std::string &namePrefix,
            const std::string &nameSuffix)
        {
            if (filename.size() < namePrefix.size() + nameSuffix.size())
            {
                return std::nullopt;
            }
            if (filename.compare(0, namePrefix.size(), namePrefix) != 0)
            {
                return std::nullopt;
            }
            if (filename.compare(filename.size() - nameSuffix.size(), nameSuffix.size(), nameSuffix) != 0)
            {
                return std::nullopt;
            }
            const size_t start = namePrefix.size();
            const size_t end = filename.size() - nameSuffix.size();
            if (end <= start)
            {
                return std::nullopt;
            }
            const std::string clockBody = filename.substr(start, end - start);
            const size_t partSep = clockBody.find_first_of("_-");
            const std::string clockDigits = partSep == std::string::npos
                                                ? clockBody
                                                : clockBody.substr(0, partSep);
            if (clockDigits.empty())
            {
                return std::nullopt;
            }
            try
            {
                return static_cast<uint64_t>(std::stoull(clockDigits));
            }
            catch (...)
            {
                return std::nullopt;
            }
        }

        bool placeRingCalibrationFiles(
            std::vector<std::string> &calibrationFiles,
            uint16_t ringIndex,
            uint16_t channelsPerRing,
            const std::string &dir)
        {
            if (dir.empty())
            {
                return true;
            }

            auto ringFiles = collectCalibrationFiles(dir, {".bin"}, true, "bdm_", ".bin");
            if (ringFiles.size() < static_cast<size_t>(channelsPerRing))
            {
                LOG(WARNING) << "createBDM50100_9120Config: ring " << ringIndex
                             << " calibration dir \"" << dir << "\" has " << ringFiles.size()
                             << " files, expected >= " << channelsPerRing;
            }

            const size_t base = static_cast<size_t>(ringIndex) * static_cast<size_t>(channelsPerRing);
            const size_t n = std::min(ringFiles.size(), static_cast<size_t>(channelsPerRing));
            for (size_t i = 0; i < n; ++i)
            {
                calibrationFiles[base + i] = ringFiles[i];
            }
            return true;
        }
    } // namespace

    bool processR2S(const R2SProcessConfig &config)
    {
        try
        {
            openpni::distributed::coreio::RawDataFileReader rawFileInput;
            rawFileInput.Open(config.rawdataPath);

            const auto &info = rawFileInput.Info();
            auto channelNum = info.channelNum;
            auto segmentNum = info.segmentNum;

            LOG(INFO) << "Channels: " << channelNum;
            LOG(INFO) << "Segments: " << segmentNum;

            R2SProcessConfig runtimeConfig = config;
            const auto parsedClock = tryParseRawStartClock(
                std::filesystem::path(config.rawdataPath).filename().string(),
                "pniRaw-",
                ".bin");
            if (parsedClock)
            {
                runtimeConfig.outputFileName = makeSinglesOutputPrefix(config.outputFileName, *parsedClock, false);
            }

            R2SStreamProcessor processor(runtimeConfig);
            if (!processor.initialize(channelNum))
            {
                return false;
            }

            LOG(INFO) << "Processing " << segmentNum << " segments...";
            for (uint64_t i = 0; i < segmentNum; i++)
            {
                auto segment = rawFileInput.ReadSegment(static_cast<uint32_t>(i), static_cast<uint32_t>(i + 1));
                auto view = segment.View();

                if (!processor.processSegment(view))
                {
                    LOG(ERROR) << "R2S processing failed at segment " << i;
                    return false;
                }
            }

            return processor.finalize();
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "Error in processR2S: " << e.what();
            return false;
        }
    }

    std::vector<RawDataFileEntry> collectRawDataFiles(
        const std::string &directory,
        const std::string &namePrefix,
        const std::string &nameSuffix)
    {
        std::vector<RawDataFileEntry> entries;
        if (directory.empty())
        {
            return entries;
        }

        std::error_code ec;
        const std::filesystem::path dirPath(directory);
        if (!std::filesystem::exists(dirPath, ec) || !std::filesystem::is_directory(dirPath, ec))
        {
            LOG(ERROR) << "Rawdata directory not found: " << directory;
            return entries;
        }

        for (const auto &entry : std::filesystem::directory_iterator(dirPath, ec))
        {
            if (ec || !entry.is_regular_file(ec))
            {
                continue;
            }
            const std::string name = entry.path().filename().string();
            auto clockOpt = tryParseRawStartClock(name, namePrefix, nameSuffix);
            if (!clockOpt)
            {
                continue;
            }
            entries.push_back(RawDataFileEntry{*clockOpt, entry.path().string()});
        }

        std::sort(entries.begin(), entries.end(),
                  [](const RawDataFileEntry &a, const RawDataFileEntry &b)
                  {
                      if (a.startClock != b.startClock)
                      {
                          return a.startClock < b.startClock;
                      }
                      return a.path < b.path;
                  });

        LOG(INFO) << "Collected " << entries.size() << " rawdata files from: " << directory;
        return entries;
    }

    std::string makeSinglesOutputPrefix(
        const std::string &basePrefix,
        uint64_t inputClock,
        bool appendRunTimestamp)
    {
        std::ostringstream oss;
        oss << basePrefix << '_' << inputClock;
        if (appendRunTimestamp)
        {
            const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count();
            oss << '_' << nowMs;
        }
        return oss.str();
    }

    bool processR2SDirectory(
        R2SProcessConfig config,
        const std::string &rawdataDir,
        bool appendRunTimestamp,
        const std::string &rawNamePrefix,
        const std::string &rawNameSuffix)
    {
        const auto files = collectRawDataFiles(rawdataDir, rawNamePrefix, rawNameSuffix);
        if (files.empty())
        {
            LOG(ERROR) << "processR2SDirectory: no raw files in " << rawdataDir;
            return false;
        }

        const std::string outputPrefixBase = config.outputFileName;
        bool openedFirst = false;
        uint16_t inputChannelNum = 0;
        std::unique_ptr<R2SStreamProcessor> processor;

        try
        {
            for (const auto &file : files)
            {
                openpni::distributed::coreio::RawDataFileReader rawFileInput;
                rawFileInput.Open(file.path);
                const auto &info = rawFileInput.Info();
                if (!openedFirst)
                {
                    inputChannelNum = info.channelNum;
                    config.outputFileName = makeSinglesOutputPrefix(
                        outputPrefixBase, file.startClock, appendRunTimestamp);
                    processor = std::make_unique<R2SStreamProcessor>(config);
                    if (!processor->initialize(inputChannelNum))
                    {
                        return false;
                    }
                    openedFirst = true;
                }
                else
                {
                    if (info.channelNum != inputChannelNum)
                    {
                        LOG(WARNING) << "processR2SDirectory: channelNum mismatch for "
                                     << file.path << " (" << info.channelNum
                                     << " vs " << inputChannelNum << ")";
                    }
                    const std::string nextPrefix = makeSinglesOutputPrefix(
                        outputPrefixBase, file.startClock, appendRunTimestamp);
                    if (!processor->reopenOutput(nextPrefix))
                    {
                        LOG(ERROR) << "processR2SDirectory: failed to reopen output for " << file.path;
                        processor->finalize();
                        return false;
                    }
                }

                LOG(INFO) << "Processing raw file: " << file.path
                          << " segments=" << info.segmentNum;
                for (uint32_t i = 0; i < info.segmentNum; ++i)
                {
                    auto segment = rawFileInput.ReadSegment(i, i + 1);
                    if (!processor->processSegment(segment.View()))
                    {
                        LOG(ERROR) << "processR2SDirectory: failed at " << file.path
                                   << " segment " << i;
                        processor->finalize();
                        return false;
                    }
                }
            }

            return processor ? processor->finalize() : false;
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "Error in processR2SDirectory: " << e.what();
            if (processor)
            {
                processor->finalize();
            }
            return false;
        }
    }

    R2SProcessConfig createBDM2Config(
        const std::string &rawdataPath,
        const std::string &resultPath,
        const std::vector<std::string> &calibrationFiles,
        std::string outputFileName,
        const std::vector<uint16_t> &channelIndices)
    {
        R2SProcessConfig config;
        config.rawdataPath = rawdataPath;
        config.resultPath = resultPath;
        config.calibrationFiles = calibrationFiles;
        config.detectorType = DetectorType::BDM2;
        config.crystalsPerChannel = 169 * 4;
        config.r2sResultIndex = 0;
        config.outputFileName = outputFileName;
        config.channelNums = 48;
        config.channelIndices = channelIndices;
        return config;
    }

    R2SProcessConfig createBDM50100Config(
        const std::string &rawdataPath,
        const std::string &resultPath,
        const std::vector<std::string> &calibrationFiles,
        std::string outputFileName,
        const std::vector<uint16_t> &channelIndices)
    {
        R2SProcessConfig config;
        config.rawdataPath = rawdataPath;
        config.resultPath = resultPath;
        config.calibrationFiles = calibrationFiles;
        config.detectorType = DetectorType::BDM50100;
        config.crystalsPerChannel = 6 * 6 * 8;
        config.r2sResultIndex = 2;
        config.outputFileName = outputFileName;
        config.channelNums = 48 * 3;
        config.channelIndices = channelIndices;
        config.forceFullCalibrationLoad = true;
        // 与原 createSingleGenerator 中的硬编码默认值保持一致，避免行为变化
        config.matchXTalkEnabled = true;
        config.crossTalkEnabled = true;
        config.crossTalkTimeWindow = 2.0f;
        return config;
    }

    R2SProcessConfig createBDM50100_9120Config(
        const std::string &rawdataPath,
        const std::string &resultPath,
        const std::vector<std::string> &calibrationDirs,
        std::string outputFileName,
        const std::vector<uint16_t> &channelIndices,
        uint16_t instrumentRingCount)
    {
        constexpr uint16_t kChannelsPerRing = 48 * 3;
        constexpr uint32_t kCrystalsPerChannel = 6 * 6 * 8;
        constexpr uint16_t kMaxRingCount = std::numeric_limits<uint16_t>::max() / kChannelsPerRing;

        if (instrumentRingCount == 0)
        {
            LOG(WARNING) << "createBDM50100_9120Config: instrumentRingCount must be >= 1, fallback to 4";
            instrumentRingCount = 4;
        }
        else if (instrumentRingCount > kMaxRingCount)
        {
            LOG(ERROR) << "createBDM50100_9120Config: instrumentRingCount " << instrumentRingCount
                       << " overflows channelNums, clamped to " << kMaxRingCount;
            instrumentRingCount = kMaxRingCount;
        }

        const uint16_t channelNums = static_cast<uint16_t>(kChannelsPerRing * instrumentRingCount);
        std::vector<std::string> calibrationFiles(static_cast<size_t>(channelNums));

        std::vector<uint16_t> ringsToLoad;
        if (calibrationDirs.size() == static_cast<size_t>(instrumentRingCount))
        {
            for (uint16_t ring = 0; ring < instrumentRingCount; ++ring)
            {
                ringsToLoad.push_back(ring);
            }
            for (size_t i = 0; i < ringsToLoad.size(); ++i)
            {
                placeRingCalibrationFiles(
                    calibrationFiles, ringsToLoad[i], kChannelsPerRing, calibrationDirs[i]);
            }
        }
        else if (!channelIndices.empty())
        {
            std::set<uint16_t> ringSet;
            for (auto ch : channelIndices)
            {
                ringSet.insert(static_cast<uint16_t>(ch / kChannelsPerRing));
            }
            ringsToLoad.assign(ringSet.begin(), ringSet.end());
            if (calibrationDirs.size() != ringsToLoad.size())
            {
                LOG(WARNING) << "createBDM50100_9120Config: calibrationDirs.size()=" << calibrationDirs.size()
                             << " != covered ring count=" << ringsToLoad.size()
                             << "; pairing by min size in ring-index order";
            }
            const size_t n = std::min(calibrationDirs.size(), ringsToLoad.size());
            for (size_t i = 0; i < n; ++i)
            {
                placeRingCalibrationFiles(
                    calibrationFiles, ringsToLoad[i], kChannelsPerRing, calibrationDirs[i]);
            }
        }
        else
        {
            const size_t n = std::min(calibrationDirs.size(), static_cast<size_t>(instrumentRingCount));
            for (size_t i = 0; i < n; ++i)
            {
                placeRingCalibrationFiles(
                    calibrationFiles, static_cast<uint16_t>(i), kChannelsPerRing, calibrationDirs[i]);
            }
        }

        R2SProcessConfig config;
        config.rawdataPath = rawdataPath;
        config.resultPath = resultPath;
        config.calibrationFiles = std::move(calibrationFiles);
        config.detectorType = DetectorType::BDM50100;
        config.crystalsPerChannel = kCrystalsPerChannel;
        config.r2sResultIndex = 2;
        config.outputFileName = outputFileName;
        config.channelNums = channelNums;
        config.channelIndices = channelIndices;
        config.forceFullCalibrationLoad = true;
        config.matchXTalkEnabled = true;
        config.crossTalkEnabled = true;
        config.crossTalkTimeWindow = 2.0f;

        size_t missing = 0;
        if (config.channelIndices.empty())
        {
            for (uint16_t ch = 0; ch < channelNums; ++ch)
            {
                if (config.calibrationFiles[ch].empty())
                {
                    ++missing;
                }
            }
        }
        else
        {
            for (auto ch : config.channelIndices)
            {
                if (ch >= config.calibrationFiles.size() || config.calibrationFiles[ch].empty())
                {
                    ++missing;
                }
            }
        }
        if (missing > 0)
        {
            LOG(WARNING) << "createBDM50100_9120Config: " << missing
                         << " assigned channels lack calibration files "
                         << "(instrument channelNums=" << channelNums << ")";
        }

        return config;
    }

} // namespace openpni::distributed::r2s
