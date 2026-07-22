#pragma once

#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <string>
#include <fstream>
#include <atomic>
#include <cstdint>
// manifest written as newline-delimited JSON without external json dependency

#include "core/io/IOAdapter.hpp"
#include "core/acquisition/AcquisitionServer.hpp"

namespace openpni::distributed::coreio
{
    struct ShardedManifestRecord
    {
        uint64_t segment_id = 0;
        uint64_t clock_ms = 0;
        uint32_t duration_ms = 0;
        size_t shard_id = 0;
        std::string file;
        bool committed = false;
    };

    // Sharded writer: 将采集段并发地写入到多个磁盘根目录下的 shard
    class ShardedRawFileOutput : public openpni::distributed::acquisition::IRawFileOutput
    {
    public:
        using FileReadyCallback = std::function<void(const std::string &)>;

        explicit ShardedRawFileOutput(const openpni::distributed::acquisition::StorageConfig &cfg);
        ~ShardedRawFileOutput();

        void SetFileReadyCallback(FileReadyCallback cb) override;
        bool Write(const ::openpni::RawDataView &data) override;
        void Stop() override;

    private:
        struct OwnedSegment
        {
            std::vector<uint8_t> dataBuf;
            std::vector<uint16_t> lengths;
            std::vector<uint64_t> offsets;
            std::vector<uint16_t> channels;
            uint64_t clock_ms = 0;
            uint32_t duration_ms = 0;
            uint16_t channelNum = 0;
        };

        using RawRollingWriter = RollingFileWriter<RawDataFileWriter, RawDataWriterOptions>;

        struct ShardState
        {
            std::mutex mutex;
            std::condition_variable cv;
            std::vector<std::shared_ptr<OwnedSegment>> ring;
            size_t ring_head = 0;
            size_t ring_tail = 0;
            size_t ring_count = 0;
            std::thread worker;
            RawRollingWriter writer;
            bool opened = false;
            bool draining = false;
        };

        void ShardWorkerLoop(size_t shardIndex);
        bool SerializeSegment(const ::openpni::RawDataView &view, std::shared_ptr<OwnedSegment> &out);
        void WriteManifestLine(const std::string &jsonLine);
        void WriteManifestPrepare(uint64_t segmentId, const OwnedSegment &segment, size_t shardIndex, const std::string &filePath);
        void WriteManifestCommit(uint64_t segmentId);
        size_t SelectShard(const OwnedSegment &segment) const;
        bool OpenNextFile(ShardState &shard, size_t shardIndex);
        void CloseShardFile(ShardState &shard);

        openpni::distributed::acquisition::StorageConfig cfg_;
        FileReadyCallback callback_;

        std::vector<std::string> shardRoots_;
        std::vector<std::unique_ptr<ShardState>> shards_;

        size_t maxQueueDepth_ = 1024;

        std::atomic<bool> running_{false};

        // manifest
        int manifestFd_ = -1;
        std::mutex manifestMutex_;
        std::atomic<uint64_t> segmentCounter_{1};

        // simple round-robin counter
        mutable std::atomic<uint64_t> rrCounter_{0};
    };

    std::vector<ShardedManifestRecord> RecoverShardedManifest(const std::string &manifestPath);

} // namespace openpni::distributed::coreio
