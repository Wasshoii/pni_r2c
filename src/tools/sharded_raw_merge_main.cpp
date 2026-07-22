#include <pni/PnI-Config.hpp>

#include <filesystem>
#include <iostream>
#include <string>
#include <unordered_map>

#include "core/io/IOAdapter.hpp"
#include "core/io/ShardedRawFileOutput.hpp"

namespace
{
    namespace coreio = openpni::distributed::coreio;

    struct Args
    {
        std::string manifestPath;
        std::string outputPath;
    };

    void printUsage(const char *prog)
    {
        std::cout << "Usage: " << prog << " --manifest <path> --output <path>\n";
    }

    bool parseArgs(int argc, char **argv, Args *out)
    {
        for (int i = 1; i < argc; ++i)
        {
            const std::string arg = argv[i];
            if (arg == "--manifest")
            {
                if (i + 1 >= argc)
                {
                    return false;
                }
                out->manifestPath = argv[++i];
                continue;
            }
            if (arg == "--output")
            {
                if (i + 1 >= argc)
                {
                    return false;
                }
                out->outputPath = argv[++i];
                continue;
            }
            if (arg == "--help")
            {
                return false;
            }
            std::cerr << "Unknown argument: " << arg << "\n";
            return false;
        }
        return !out->manifestPath.empty() && !out->outputPath.empty();
    }

    struct FileCursor
    {
        FileCursor() = default;

        coreio::RawDataFileReader reader;
        coreio::RawDataFileInfo info{};
        uint32_t nextIndex = 0;
        bool opened = false;
    };
}

int main(int argc, char **argv)
{
    Args args;
    if (!parseArgs(argc, argv, &args))
    {
        printUsage(argv[0]);
        return 1;
    }

    if (!std::filesystem::exists(args.manifestPath))
    {
        std::cerr << "Manifest not found: " << args.manifestPath << "\n";
        return 2;
    }

    const auto records = coreio::RecoverShardedManifest(args.manifestPath);
    if (records.empty())
    {
        std::cerr << "No committed segments found in manifest.\n";
        return 3;
    }

    std::unordered_map<std::string, FileCursor> cursors;

    coreio::RawDataFileWriter writer({});
    bool writerOpened = false;
    coreio::RawDataFileInfo outputInfo{};

    for (const auto &rec : records)
    {
        if (rec.file.empty())
        {
            std::cerr << "Manifest record has empty file path.\n";
            return 4;
        }

        auto it = cursors.find(rec.file);
        if (it == cursors.end())
        {
            it = cursors.emplace(rec.file, FileCursor()).first;
        }

        FileCursor &cursor = it->second;
        if (!cursor.opened)
        {
            cursor.reader.Open(rec.file);
            cursor.info = cursor.reader.Info();
            cursor.opened = true;
        }

        if (!writerOpened)
        {
            outputInfo = cursor.info;
            coreio::RawDataWriterOptions options;
            options.channelNum = outputInfo.channelNum;
            options.channelTypeNames = outputInfo.channelTypeNames;
            writer = coreio::RawDataFileWriter(std::move(options));
            writer.Open(args.outputPath);
            writerOpened = true;
        }
        else if (cursor.info.channelNum != outputInfo.channelNum)
        {
            std::cerr << "Channel count mismatch for file: " << rec.file << "\n";
            return 5;
        }

        const auto seg = cursor.reader.ReadSegment(cursor.nextIndex++);
        if (!seg.Valid())
        {
            std::cerr << "Failed to read segment from file: " << rec.file << "\n";
            return 6;
        }

        if (!writer.AppendSegment(seg.View()))
        {
            std::cerr << "Failed to append segment to output file.\n";
            return 7;
        }
    }

    std::cout << "Merged " << records.size() << " segments into: " << args.outputPath << "\n";
    return 0;
}
