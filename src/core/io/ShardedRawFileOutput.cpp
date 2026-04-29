#include "core/io/ShardedRawFileOutput.hpp"

#include <chrono>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <cerrno>
#include <unordered_map>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

namespace openpni::distributed::coreio
{
    using namespace std::chrono_literals;

    static std::string MakeFilename(uint64_t timestamp, int seq)
    {
        std::ostringstream ss;
        ss << "raw_" << timestamp << "_" << std::setfill('0') << std::setw(4) << seq << ".raw";
        return ss.str();
    }

    static bool extractUInt64(const std::string &line, const char *key, uint64_t *out)
    {
        const auto pos = line.find(key);
        if (pos == std::string::npos)
        {
            return false;
        }
        size_t start = pos + std::strlen(key);
        while (start < line.size() && (line[start] == ' ' || line[start] == '"' || line[start] == ':'))
        {
            ++start;
        }
        size_t end = start;
        while (end < line.size() && std::isdigit(static_cast<unsigned char>(line[end])))
        {
            ++end;
        }
        if (end == start)
        {
            return false;
        }
        try
        {
            *out = std::stoull(line.substr(start, end - start));
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    static bool extractString(const std::string &line, const char *key, std::string *out)
    {
        const auto pos = line.find(key);
        if (pos == std::string::npos)
        {
            return false;
        }
        size_t start = pos + std::strlen(key);
        if (start >= line.size())
        {
            return false;
        }
        while (start < line.size() && (line[start] == ' ' || line[start] == ':' || line[start] == '"'))
        {
            ++start;
        }
        size_t end = line.find('"', start);
        if (end == std::string::npos)
        {
            return false;
        }
        *out = line.substr(start, end - start);
        return true;
    }

    static bool extractType(const std::string &line, std::string *out)
    {
        return extractString(line, "\"type\":\"", out);
    }

    ShardedRawFileOutput::ShardedRawFileOutput(const openpni::distributed::acquisition::StorageConfig &cfg)
        : cfg_(cfg)
    {
        if (!cfg_.output_roots.empty())
        {
            shardRoots_ = cfg_.output_roots;
        }
        else
        {
            shardRoots_.push_back(cfg_.output_root);
        }

        maxQueueDepth_ = cfg_.async_queue_depth;
        if (maxQueueDepth_ == 0)
        {
            maxQueueDepth_ = 1;
        }
        shards_.reserve(shardRoots_.size());
        for (size_t i = 0; i < shardRoots_.size(); ++i)
        {
            shards_.push_back(std::make_unique<ShardState>());
            shards_.back()->ring.resize(maxQueueDepth_);
        }

        // prepare manifest in first shard root
        std::string manifestDir = (shardRoots_.empty() ? cfg_.output_root : shardRoots_[0]);
        manifestDir = manifestDir + "/" + cfg_.session_name;
        std::error_code ec;
        std::filesystem::create_directories(manifestDir, ec);
        std::string manifestPath = manifestDir + "/" + cfg_.manifest_filename;
        manifestFd_ = open(manifestPath.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);

        running_.store(true);
        // start shard worker threads
        for (size_t i = 0; i < shardRoots_.size(); ++i)
        {
            shards_[i]->worker = std::thread(&ShardedRawFileOutput::ShardWorkerLoop, this, i);
        }
    }

    ShardedRawFileOutput::~ShardedRawFileOutput()
    {
        Stop();
    }

    void ShardedRawFileOutput::SetFileReadyCallback(FileReadyCallback cb)
    {
        callback_ = std::move(cb);
    }

    bool ShardedRawFileOutput::SerializeSegment(const ::openpni::RawDataView &view, std::shared_ptr<OwnedSegment> &out)
    {
        if (view.count == 0)
        {
            // nothing
            return false;
        }

        out = std::make_shared<OwnedSegment>();
        out->channelNum = view.channelNum;
        out->clock_ms = view.clock_ms;
        out->duration_ms = view.duration_ms;
        out->lengths.resize(view.count);
        out->offsets.resize(view.count);
        out->channels.resize(view.count);

        // compute total data size
        size_t total = 0;
        for (size_t i = 0; i < view.count; ++i)
        {
            out->lengths[i] = view.length[i];
            out->offsets[i] = view.offset[i];
            out->channels[i] = view.channel[i];
            total += view.length[i];
        }

        out->dataBuf.resize(total);
        size_t dest = 0;
        for (size_t i = 0; i < view.count; ++i)
        {
            const auto len = view.length[i];
            if (len)
            {
                std::memcpy(out->dataBuf.data() + dest, view.data + view.offset[i], len);
                dest += len;
            }
        }

        return true;
    }

    bool ShardedRawFileOutput::Write(const ::openpni::RawDataView &data)
    {
        if (!running_.load())
            return false;

        std::shared_ptr<OwnedSegment> seg;
        if (!SerializeSegment(data, seg))
            return false;

        const size_t shardIndex = SelectShard(*seg);
        auto &shard = *shards_[shardIndex];

        std::unique_lock lock(shard.mutex);
        if (shard.ring_count >= shard.ring.size())
        {
            if (cfg_.use_spill_to_disk)
            {
                // fallback: append a spill marker and let upstream decide whether to retry
                std::ostringstream ss;
                ss << "{\"type\":\"spill\",\"shard_id\":" << shardIndex << ",\"clock_ms\":" << seg->clock_ms << "}\n";
                WriteManifestLine(ss.str());
                return true;
            }

            if (cfg_.fail_on_queue_full)
            {
                shard.cv.wait(lock, [&]() { return !running_.load() || shard.ring_count < shard.ring.size(); });
            }
            else
            {
                // drop
                return false;
            }
        }

        shard.ring[shard.ring_tail] = std::move(seg);
        shard.ring_tail = (shard.ring_tail + 1) % shard.ring.size();
        ++shard.ring_count;
        lock.unlock();
        shard.cv.notify_one();
        return true;
    }

    size_t ShardedRawFileOutput::SelectShard(const OwnedSegment &segment) const
    {
        if (shardRoots_.empty())
        {
            return 0;
        }

        if (cfg_.shard_strategy == openpni::distributed::acquisition::StorageConfig::ShardStrategy::HashByChannel && !segment.channels.empty())
        {
            const auto channel = static_cast<size_t>(segment.channels.front());
            return channel % shardRoots_.size();
        }

        return static_cast<size_t>(rrCounter_.fetch_add(1) % shardRoots_.size());
    }

    bool ShardedRawFileOutput::OpenNextFile(ShardState &shard, size_t shardIndex)
    {
        const auto now = std::chrono::system_clock::now();
        const auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        std::string filename = MakeFilename(timestamp, static_cast<int>(shard.fileSeq++));
        std::string sessionDir = shardRoots_[shardIndex] + "/" + cfg_.session_name;
        std::error_code ec;
        std::filesystem::create_directories(sessionDir, ec);

        shard.currentPath = sessionDir + "/" + filename;
        try
        {
            openpni::distributed::coreio::RawDataWriterOptions options;
            options.channelNum = cfg_.channel_num;
            options.io.reservedBytes = cfg_.total_reserved_gib * 1024ull * 1024ull * 1024ull;
            options.backend = openpni::distributed::coreio::IOBackendContext::Get().rawdataWriter;
            shard.writer = std::make_unique<openpni::distributed::coreio::RawDataFileWriter>(std::move(options));
            shard.writer->Open(shard.currentPath);
            shard.currentSize = 0;
            return true;
        }
        catch (...)
        {
            shard.currentPath.clear();
            shard.writer.reset();
            return false;
        }
    }

    void ShardedRawFileOutput::CloseShardFile(ShardState &shard)
    {
        if (shard.writer)
        {
            shard.writer.reset();
        }
        if (!shard.currentPath.empty() && callback_)
        {
            callback_(shard.currentPath);
        }
        shard.currentPath.clear();
        shard.currentSize = 0;
    }

    void ShardedRawFileOutput::ShardWorkerLoop(size_t shardIndex)
    {
        auto &shard = *shards_[shardIndex];

        while (true)
        {
            std::shared_ptr<OwnedSegment> seg;
            {
                std::unique_lock lock(shard.mutex);
                shard.cv.wait(lock, [&]() { return !running_.load() || shard.ring_count > 0; });
                if (shard.ring_count == 0)
                {
                    if (!running_.load())
                    {
                        break;
                    }
                    continue;
                }
                seg = std::move(shard.ring[shard.ring_head]);
                shard.ring[shard.ring_head].reset();
                shard.ring_head = (shard.ring_head + 1) % shard.ring.size();
                --shard.ring_count;
                shard.cv.notify_all();
            }

            if (!seg)
                continue;

            if (!shard.writer)
            {
                if (!OpenNextFile(shard, shardIndex))
                {
                    continue;
                }
            }

            // build RawDataView pointing to owned buffers
            ::openpni::RawDataView view;
            view.data = seg->dataBuf.data();
            view.length = seg->lengths.data();
            view.offset = seg->offsets.data();
            view.channel = seg->channels.data();
            view.count = seg->lengths.size();
            view.clock_ms = seg->clock_ms;
            view.duration_ms = seg->duration_ms;
            view.channelNum = seg->channelNum;

            const uint64_t segmentId = segmentCounter_.fetch_add(1, std::memory_order_relaxed);
            WriteManifestPrepare(segmentId, *seg, shardIndex, shard.currentPath);

            bool ok = shard.writer->AppendSegment(view);
            if (ok)
            {
                // update size estimate
                size_t added = 0;
                for (auto l : seg->lengths) added += l + 32;
                shard.currentSize += added;

                WriteManifestCommit(segmentId);
            }
            else
            {
                // write failed - consider requeue or mark
            }

            if (shard.currentSize > cfg_.max_file_size_mb * 1024ull * 1024ull)
            {
                CloseShardFile(shard);
            }
        }

        CloseShardFile(shard);
    }

    void ShardedRawFileOutput::WriteManifestLine(const std::string &jsonLine)
    {
        std::lock_guard lock(manifestMutex_);
        if (manifestFd_ >= 0)
        {
            ssize_t toWrite = static_cast<ssize_t>(jsonLine.size());
            const char *buf = jsonLine.c_str();
            while (toWrite > 0)
            {
                ssize_t w = write(manifestFd_, buf, toWrite);
                if (w <= 0)
                    break;
                buf += w;
                toWrite -= w;
            }
            if (cfg_.fsync_each_segment)
            {
                fsync(manifestFd_);
            }
        }
    }

    void ShardedRawFileOutput::WriteManifestPrepare(uint64_t segmentId, const OwnedSegment &segment, size_t shardIndex, const std::string &filePath)
    {
        std::ostringstream ss;
        ss << "{\"type\":\"prepare\""
           << ",\"segment_id\":" << segmentId
           << ",\"segment_clock_ms\":" << segment.clock_ms
           << ",\"duration_ms\":" << segment.duration_ms
           << ",\"shard_id\":" << shardIndex
           << ",\"file\":\"" << filePath << "\"}"
           << "\n";
        WriteManifestLine(ss.str());
    }

    void ShardedRawFileOutput::WriteManifestCommit(uint64_t segmentId)
    {
        std::ostringstream ss;
        ss << "{\"type\":\"commit\",\"segment_id\":" << segmentId << "}\n";
        WriteManifestLine(ss.str());
    }

    void ShardedRawFileOutput::Stop()
    {
        running_.store(false);
        for (auto &shard : shards_)
        {
            if (shard)
                shard->cv.notify_all();
        }
        for (auto &shard : shards_)
        {
            if (shard && shard->worker.joinable())
            {
                shard->worker.join();
            }
        }
        if (manifestFd_ >= 0)
            close(manifestFd_);
    }

    std::vector<ShardedManifestRecord> RecoverShardedManifest(const std::string &manifestPath)
    {
        std::ifstream ifs(manifestPath);
        if (!ifs)
        {
            return {};
        }

        std::unordered_map<uint64_t, ShardedManifestRecord> records;
        std::string line;
        while (std::getline(ifs, line))
        {
            std::string type;
            if (!extractType(line, &type))
            {
                continue;
            }

            if (type == "prepare")
            {
                ShardedManifestRecord rec;
                if (!extractUInt64(line, "\"segment_id\"", &rec.segment_id))
                {
                    continue;
                }
                extractUInt64(line, "\"segment_clock_ms\"", &rec.clock_ms);
                uint64_t duration = 0;
                extractUInt64(line, "\"duration_ms\"", &duration);
                rec.duration_ms = static_cast<uint32_t>(duration);
                uint64_t shardId = 0;
                extractUInt64(line, "\"shard_id\"", &shardId);
                rec.shard_id = static_cast<size_t>(shardId);
                extractString(line, "\"file\"", &rec.file);
                records[rec.segment_id] = rec;
            }
            else if (type == "commit")
            {
                uint64_t segmentId = 0;
                if (extractUInt64(line, "\"segment_id\"", &segmentId))
                {
                    auto it = records.find(segmentId);
                    if (it != records.end())
                    {
                        it->second.committed = true;
                    }
                }
            }
        }

        std::vector<ShardedManifestRecord> committed;
        committed.reserve(records.size());
        for (auto &entry : records)
        {
            if (entry.second.committed)
            {
                committed.push_back(entry.second);
            }
        }

        std::sort(committed.begin(), committed.end(), [](const ShardedManifestRecord &a, const ShardedManifestRecord &b) {
            if (a.clock_ms != b.clock_ms)
            {
                return a.clock_ms < b.clock_ms;
            }
            return a.segment_id < b.segment_id;
        });

        return committed;
    }

} // namespace openpni::distributed::coreio
