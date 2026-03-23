#include "core/r2s/R2S.hpp"

namespace openpni::distributed::r2s
{
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

    std::vector<GlobalSingle> convertLocalToGlobalSingles(
        std::span<Single const> singles,
        uint32_t crystalsPerChannel)
    {
        std::vector<GlobalSingle> globalSingles;
        globalSingles.reserve(singles.size());

        std::vector<Single> hostBuf;
        const Single *dataPtr = singles.data();

        if (isDevicePointer(dataPtr))
        {
            hostBuf.resize(singles.size());
            cudaError_t err = cudaMemcpy(hostBuf.data(), dataPtr,
                                         singles.size() * sizeof(Single),
                                         cudaMemcpyDeviceToHost);
            if (err != cudaSuccess)
            {
                throw std::runtime_error("cudaMemcpyDeviceToHost failed: " +
                                         std::string(cudaGetErrorString(err)));
            }
            dataPtr = hostBuf.data();
        }

        for (size_t i = 0; i < singles.size(); i++)
        {
            GlobalSingle gs;
            gs.globalCrystalIndex = dataPtr[i].channelIndex * crystalsPerChannel + dataPtr[i].crystalIndex;
            gs.energy = dataPtr[i].energy;
            gs.timeValue_pico = dataPtr[i].timevalue_pico;
            globalSingles.push_back(gs);
        }

        return globalSingles;
    }

    bool appendSinglesToSingleFile(
        openpni::io::v1::single::SingleFileOutput &outputFile,
        std::span<Single const> singles,
        uint32_t crystalsPerChannel,
        uint64_t clock_ms,
        uint32_t duration_ms)
    {
        if (singles.empty())
        {
            return true;
        }

        try
        {
            auto globalSingles = convertLocalToGlobalSingles(singles, crystalsPerChannel);
            return outputFile.appendSegment(globalSingles.data(), globalSingles.size(),
                                            clock_ms, duration_ms);
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error appending singles to file: " << e.what() << std::endl;
            return false;
        }
    }

    openpni::interface::SingleGenerator *createSingleGenerator(
        DetectorType type,
        uint16_t channelIndex,
        const std::string &calibrationFile)
    {
        openpni::interface::SingleGenerator *generator = nullptr;

        switch (type)
        {
        case DetectorType::BDM2:
            generator = new openpni::BDM2R2S();
            break;
        default:
            throw std::runtime_error("Unknown detector type");
        }

        generator->setChannelIndex(channelIndex);
        generator->loadCalibration(calibrationFile);

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
        m_output = std::make_unique<openpni::io::v1::single::SingleFileOutput>();
        m_output->setBytes4CrystalIndex(openpni::io::v1::single::CrystalIndexType::UINT32);
        m_output->setBytes4TimeValue(openpni::io::v1::single::TimeValueType::UINT64);
        m_output->setBytes4Energy(openpni::io::v1::single::EnergyType::FLT32);
        m_output->setTotalCrystalNum(totalCrystals);
        m_output->open(filePath);

        m_running = true;
        m_writerThread = std::thread([this]
                                     { writerLoop(); });

        std::cout << "[AsyncWriter] Started, queue size: " << m_maxQueueSize << std::endl;
        return true;
    }

    bool AsyncSingleFileWriter::submit(std::vector<GlobalSingle> &&singles, uint64_t clock_ms, uint32_t duration_ms)
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

        std::cout << "[AsyncWriter] Stopped, total written: " << m_totalWritten.load()
                  << " singles" << std::endl;
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
                bool success = m_output->appendSegment(
                    task.singles.data(),
                    task.singles.size(),
                    task.clock_ms,
                    task.duration_ms);

                if (success)
                {
                    m_totalWritten += task.singles.size();
                }
                else
                {
                    std::cerr << "[AsyncWriter] Failed to write segment" << std::endl;
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
            std::cout << "Starting R2S processing..." << std::endl;
            std::cout << "Detector: " << (m_config.detectorType == DetectorType::BDM2 ? "BDM2" : "BDMBiD") << std::endl;
            if (m_inputChannelNum > 0)
            {
                std::cout << "Input channels: " << m_inputChannelNum << std::endl;
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

            std::cout << "Setting up ConvergedR2S with generators..." << std::endl;
            m_r2s.SetChannels(m_generatorsVector);
            std::cout << "Setup complete." << std::endl;

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
            std::cerr << "Error initializing R2S stream processor: " << e.what() << std::endl;
            m_hadError = true;
            cleanupGenerators();
            return false;
        }
    }

    bool R2SStreamProcessor::processSegment(const openpni::RawDataView &view)
    {
        if (!m_initialized)
        {
            std::cerr << "R2S stream processor is not initialized" << std::endl;
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
                std::cout << "Segment " << segmentId << ": No data, skipping" << std::endl;
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

            if (m_config.r2sResultIndex >= r2sResults.size())
            {
                std::cerr << "Error: r2sResultIndex " << m_config.r2sResultIndex
                          << " is out of range, result size=" << r2sResults.size() << std::endl;
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

                std::cout << "Segment " << segmentId
                          << ": Processed " << view.count << " packets, generated "
                          << singlesSpan.size() << " singles";

                if (timeMs > 0)
                {
                    const double speedMbPerSec =
                        static_cast<double>(view.count * 1024ULL) / 1024.0 / 1024.0 /
                        (static_cast<double>(timeMs) / 1000.0);
                    std::cout << ", speed=" << speedMbPerSec << " MB/s";
                }

                std::cout << std::endl;
            }

            if (!callbackSuccess || !fileSuccess)
            {
                m_hadError = true;
            }

            return callbackSuccess && fileSuccess;
        }
        catch (const std::exception &e)
        {
            std::cerr << "Exception at segment " << segmentId << ": " << e.what() << std::endl;
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
            std::cout << "Waiting for async writer to complete..." << std::endl;
            m_asyncWriter->flush();
            m_asyncWriter->stop();
            std::cout << "Async writer completed, written: " << m_asyncWriter->getTotalWritten()
                      << " singles" << std::endl;
        }

        std::cout << "\n=== Processing Complete ===" << std::endl;
        std::cout << "Total raw packets: " << m_totalRawPackets << std::endl;
        std::cout << "Total singles: " << m_totalSingles << std::endl;
        std::cout << "Singles/Packet ratio: "
                  << (m_totalRawPackets > 0 ? static_cast<double>(m_totalSingles) / m_totalRawPackets : 0.0)
                  << std::endl;

        if (m_config.saveData2SingleFile && m_hasStreamingCallback)
        {
            std::cout << "Output file: " << m_outputFilePath
                      << (m_config.asyncFileWrite ? " (async)" : " (sync)") << std::endl;
            std::cout << "Data also streamed via callback" << std::endl;
        }
        else if (m_config.saveData2SingleFile)
        {
            std::cout << "Output file: " << m_outputFilePath
                      << (m_config.asyncFileWrite ? " (async)" : " (sync)") << std::endl;
        }
        else
        {
            std::cout << "Data streamed via callback" << std::endl;
        }

        std::cout << "===========================\n"
                  << std::endl;

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
            std::cout << "Processing all channels" << std::endl;
        }
        else
        {
            m_channelsToProcess = m_config.channelIndices;
            std::cout << "Processing selected channels: ";
            for (auto ch : m_channelsToProcess)
            {
                std::cout << ch << " ";
            }
            std::cout << std::endl;

            const uint16_t validateRange = std::max<uint16_t>(m_config.channelNums, m_inputChannelNum);
            for (auto ch : m_channelsToProcess)
            {
                if (ch >= validateRange)
                {
                    std::cerr << "Error: Channel index " << ch
                              << " is out of range (0-" << (validateRange - 1) << ")" << std::endl;
                    return false;
                }
            }
        }

        return true;
    }

    bool R2SStreamProcessor::prepareGenerators()
    {
        size_t requiredCalibrationCount = 0;
        if (m_config.channelIndices.empty())
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
            std::cerr << "Error: Not enough calibration files. Need at least " << requiredCalibrationCount
                      << ", got " << m_config.calibrationFiles.size() << std::endl;
            return false;
        }

        std::cout << "Loading " << m_channelsToProcess.size() << " channels' calibration data..." << std::endl;

        if (m_config.channelIndices.empty())
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
                    std::cerr << "Error creating generator for channel " << i << ": " << e.what() << std::endl;
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
                    std::cerr << "Error creating generator for channel " << m_config.channelIndices[i]
                              << ": " << e.what() << std::endl;
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
            std::cerr << "Error: saveData2SingleFile is false and no callback is set" << std::endl;
            return false;
        }

        if (m_config.saveData2SingleFile)
        {
            const uint32_t totalCrystals = m_config.channelNums * m_config.crystalsPerChannel;
            m_outputFilePath = m_config.resultPath + "/" + m_config.outputFileName + ".single";

            if (m_config.asyncFileWrite)
            {
                m_asyncWriter = std::make_unique<AsyncSingleFileWriter>(m_config.asyncWriteQueueSize);
                m_asyncWriter->open(m_outputFilePath, totalCrystals);
                std::cout << "Output file (async): " << m_outputFilePath << std::endl;
            }
            else
            {
                m_singleOutput = std::make_unique<openpni::io::v1::single::SingleFileOutput>();
                m_singleOutput->setBytes4CrystalIndex(openpni::io::v1::single::CrystalIndexType::UINT32);
                m_singleOutput->setBytes4TimeValue(openpni::io::v1::single::TimeValueType::UINT64);
                m_singleOutput->setBytes4Energy(openpni::io::v1::single::EnergyType::FLT32);
                m_singleOutput->setTotalCrystalNum(totalCrystals);
                m_singleOutput->open(m_outputFilePath);
                std::cout << "Output file (sync): " << m_outputFilePath << std::endl;
            }

            std::cout << "Total crystals: " << totalCrystals << std::endl;
        }

        if (m_config.saveData2SingleFile && m_hasStreamingCallback)
        {
            std::cout << "Dual mode: data will be saved to file AND sent via callback" << std::endl;
        }
        else if (m_hasStreamingCallback)
        {
            std::cout << "Streaming mode: data will be sent via callback only" << std::endl;
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
                std::cerr << "Callback onSinglesSpanReady returned false, stopping" << std::endl;
            }
            return callbackSuccess;
        }

        if (m_config.onSinglesReady)
        {
            auto globalSingles = convertLocalToGlobalSingles(singles, m_config.crystalsPerChannel);
            const bool callbackSuccess = m_config.onSinglesReady(std::move(globalSingles), clockMs, durationMs);
            if (!callbackSuccess)
            {
                std::cerr << "Callback onSinglesReady returned false, stopping" << std::endl;
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
            auto globalSinglesForFile = convertLocalToGlobalSingles(singles, m_config.crystalsPerChannel);
            const bool fileSuccess = m_asyncWriter->submit(
                std::move(globalSinglesForFile),
                clockMs,
                durationMs);
            if (!fileSuccess)
            {
                std::cerr << "Failed to submit segment to async writer" << std::endl;
            }
            return fileSuccess;
        }

        const bool fileSuccess = appendSinglesToSingleFile(
            *m_singleOutput,
            singles,
            m_config.crystalsPerChannel,
            clockMs,
            durationMs);
        if (!fileSuccess)
        {
            std::cerr << "Failed to append segment to single file" << std::endl;
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
            std::cerr << "[RawDataR2SBridge] already started" << std::endl;
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
                std::cerr << "[RawDataR2SBridge] queue is full, depth="
                          << m_queue.size() << "/" << m_queue.capacity() << std::endl;
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
            auto mRawFileInput = std::make_unique<openpni::io::v1::RawFileInput>();
            mRawFileInput->open(config.rawdataPath);

            auto header = mRawFileInput->header();
            auto channelNum = header.channelNum;
            auto segmentNum = header.segmentNum;

            std::cout << "Channels: " << channelNum << std::endl;
            std::cout << "Segments: " << segmentNum << std::endl;

            R2SStreamProcessor processor(config);
            if (!processor.initialize(channelNum))
            {
                return false;
            }

            std::cout << "Processing " << segmentNum << " segments..." << std::endl;
            for (uint64_t i = 0; i < segmentNum; i++)
            {
                auto segment = mRawFileInput->readSegment(i, i + 1);
                auto segHeader = mRawFileInput->segmentHeader(i);
                auto view = segment.view(header, segHeader);
                view.clock_ms = segHeader.clock;
                view.duration_ms = segHeader.duration;

                if (!processor.processSegment(view))
                {
                    std::cerr << "R2S processing failed at segment " << i << std::endl;
                    return false;
                }
            }

            return processor.finalize();
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error in processR2S: " << e.what() << std::endl;
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

} // namespace openpni::distributed::r2s
