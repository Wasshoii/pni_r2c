#include "core/r2s/R2S.hpp"

#include <cctype>
#include <glog/logging.h>

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
        openpni::distributed::coreio::SinglesFileWriter &outputFile,
        std::span<Single const> singles,
        uint64_t clock_ms,
        uint32_t duration_ms)
    {
        if (singles.empty())
        {
            return true;
        }

        try
        {
            if (isDevicePointer(singles.data()))
            {
                auto hostSingles = materializeSinglesOnHost(singles);
                return outputFile.AppendSegment(
                    std::span<const Single>(hostSingles.data(), hostSingles.size()),
                    clock_ms,
                    duration_ms);
            }

            return outputFile.AppendSegment(singles, clock_ms, duration_ms);
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "Error appending singles to file: " << e.what();
            return false;
        }
    }

    openpni::interface::ISingleGenerator *createSingleGenerator(
        DetectorType type,
        uint16_t channelIndex,
        const std::string &calibrationFile)
    {
        openpni::interface::ISingleGenerator *generator = nullptr;

        switch (type)
        {
        case DetectorType::BDM2:
            generator = new openpni::BDM2R2S();
            break;
        case DetectorType::BDM50100:
        {
            auto *g50100 = new openpni::device::bdm50100_v2::BDM50100R2S();
            openpni::device::bdm50100_v2::BDM50100R2SParams params{};
            params.matchXTalkEnabled = true;
            params.crossTalkEnabled = true;
            params.__deviceId = 0;
            params.crossTalkTimeWindow = 2.0f; 
            g50100->setParams(params);
            generator = g50100;
            break;
        }
        default:
            throw std::runtime_error("Unknown detector type");
        }

        generator->SetChannelIndex(channelIndex);
        generator->LoadCalibration(calibrationFile);

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

    bool AsyncSingleFileWriter::open(const std::string &filePath, uint32_t totalCrystals)
    {
        openpni::distributed::coreio::SingleWriterOptions opts;
        opts.backend = openpni::distributed::coreio::IOBackendContext::Get().singlesWriter;
        m_output = std::make_unique<openpni::distributed::coreio::SinglesFileWriter>(std::move(opts));
        m_output->Open(filePath, totalCrystals);

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
                bool success = m_output->AppendSegment(
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

        m_totalRawPackets += view.count;

        const uint64_t clockMs = view.clock_ms;
        const uint32_t durationMs =
            view.duration_ms > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())
                ? std::numeric_limits<uint32_t>::max()
                : static_cast<uint32_t>(view.duration_ms);

        const auto perfStart = needPerfLog ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};

        try
        {
            auto d_data = openpni::DPackets::FromHost(
                view.data,
                view.offset,
                view.length,
                view.channel,
                view.count);

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

            if (m_config.sortDataByTime)
            {
                if (!singlesSpan.empty() && isDevicePointer(singlesSpan.data()))
                {
                    openpni::distributed::r2s::d_sortSinglesByTime_R2S(
                        const_cast<Single *>(singlesSpan.data()),
                        singlesSpan.size());
                }
            }

            const bool callbackSuccess = dispatchSinglesToCallback(singlesSpan, clockMs, durationMs);
            const bool fileSuccess = dispatchSinglesToFile(singlesSpan, clockMs, durationMs);

            m_totalSingles += singlesSpan.size();

            if (needPerfLog)
            {
                const auto perfEnd = std::chrono::steady_clock::now();
                const auto timeMs = std::chrono::duration_cast<std::chrono::milliseconds>(perfEnd - perfStart).count();

                LOG(INFO) << "Segment " << segmentId
                          << ": Processed " << view.count << " packets, generated "
                          << singlesSpan.size() << " singles";

                if (timeMs > 0)
                {
                    const double speedMbPerSec =
                        static_cast<double>(view.count * 1024ULL) / 1024.0 / 1024.0 /
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

        if (m_config.channelIndices.empty())
        {
            for (uint16_t i = 0; i < m_config.channelNums; i++)
            {
                m_channelsToProcess.push_back(i);
            }
            LOG(INFO) << "Processing all channels";
        }
        else
        {
            m_channelsToProcess = m_config.channelIndices;
            LOG(INFO) << "Processing selected channels: ";
            for (auto ch : m_channelsToProcess)
            {
                LOG(INFO) << ch << " ";
            }
            LOG(INFO) << "";

            const uint16_t validateRange = std::max<uint16_t>(m_config.channelNums, m_inputChannelNum);
            for (auto ch : m_channelsToProcess)
            {
                if (ch >= validateRange)
                {
                    LOG(ERROR) << "Error: Channel index " << ch
                               << " is out of range (0-" << (validateRange - 1) << ")";
                    return false;
                }
            }
        }

        return true;
    }

    bool R2SStreamProcessor::prepareGenerators()
    {
        const bool useFullCalibrationLoad =
            m_config.forceFullCalibrationLoad && m_config.detectorType == DetectorType::BDM50100;

        if (useFullCalibrationLoad)
        {
            LOG(INFO) << "Full calibration load enabled for BDM50100";
        }

        size_t requiredCalibrationCount = 0;
        if (m_config.channelIndices.empty() || useFullCalibrationLoad)
        {
            requiredCalibrationCount = m_config.channelNums;
        }
        else
        {
            uint16_t maxChannelIndex = 0;
            for (auto channelIndex : m_config.channelIndices)
            {
                maxChannelIndex = std::max<uint16_t>(maxChannelIndex, channelIndex);
            }
            requiredCalibrationCount = static_cast<size_t>(maxChannelIndex) + 1;
        }

        if (m_config.calibrationFiles.size() < requiredCalibrationCount)
        {
            LOG(ERROR) << "Error: Not enough calibration files. Need at least " << requiredCalibrationCount
                       << ", got " << m_config.calibrationFiles.size();
            return false;
        }

        
        LOG(INFO) << "Loading " << m_channelsToProcess.size() << " channels' calibration data...";

        if (m_config.channelIndices.empty() || useFullCalibrationLoad)
        {
            for (size_t i = 0; i < m_config.channelNums; i++)
            {
                try
                {
                    auto generator = createSingleGenerator(
                        m_config.detectorType,
                        static_cast<uint16_t>(i),
                        m_config.calibrationFiles[i]);
                    m_generatorsVector.push_back(generator);
                }
                catch (const std::exception &e)
                {
                    LOG(ERROR) << "Error creating generator for channel " << i << ": " << e.what();
                    cleanupGenerators();
                    return false;
                }
            }
        }
        else
        {
            for (size_t i = 0; i < m_config.channelIndices.size(); i++)
            {
                try
                {
                    auto channelIndex = m_config.channelIndices[i];
                    auto generator = createSingleGenerator(
                        m_config.detectorType,
                        channelIndex,
                        m_config.calibrationFiles[channelIndex]);
                    m_generatorsVector.push_back(generator);
                }
                catch (const std::exception &e)
                {
                    LOG(ERROR) << "Error creating generator for channel " << m_config.channelIndices[i]
                               << ": " << e.what();
                    cleanupGenerators();
                    return false;
                }
            }
        }

        return true;
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
            const uint32_t totalCrystals = m_config.channelNums * m_config.crystalsPerChannel;
            m_outputFilePath = m_config.resultPath + "/" + m_config.outputFileName + ".lsingle";

            if (m_config.asyncFileWrite)
            {
                m_asyncWriter = std::make_unique<AsyncSingleFileWriter>(m_config.asyncWriteQueueSize);
                m_asyncWriter->open(m_outputFilePath, totalCrystals);
                LOG(INFO) << "Output file (async): " << m_outputFilePath;
            }
            else
            {
                openpni::distributed::coreio::SingleWriterOptions opts;
                opts.backend = openpni::distributed::coreio::IOBackendContext::Get().singlesWriter;
                m_singleOutput = std::make_unique<openpni::distributed::coreio::SinglesFileWriter>(std::move(opts));
                m_singleOutput->Open(m_outputFilePath, totalCrystals);
                LOG(INFO) << "Output file (sync): " << m_outputFilePath;
            }

            LOG(INFO) << "Total crystals: " << totalCrystals;
        }

        if (m_config.saveData2SingleFile && m_hasStreamingCallback)
        {
            LOG(INFO) << "Dual mode: data will be saved to file AND sent via callback";
        }
        else if (m_hasStreamingCallback)
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
            *m_singleOutput,
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

    bool processR2S(const R2SProcessConfig &config)
    {
        try
        {
            openpni::distributed::coreio::RawDataFileReader rawFileInput(
                openpni::distributed::coreio::IOBackendContext::Get().rawdataReader);
            rawFileInput.Open(config.rawdataPath);

            const auto &info = rawFileInput.Info();
            auto channelNum = info.channelNum;
            auto segmentNum = info.segmentNum;

            LOG(INFO) << "Channels: " << channelNum;
            LOG(INFO) << "Segments: " << segmentNum;

            R2SStreamProcessor processor(config);
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
        return config;
    }

} // namespace openpni::distributed::r2s
