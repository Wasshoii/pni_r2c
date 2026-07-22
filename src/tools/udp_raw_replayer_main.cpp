#include <pni/PnI-Config.hpp>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <pni/io/IO.hpp>

#include "core/io/IOAdapter.hpp"

namespace
{
    struct ProgramOptions
    {
        std::string rawPath;
        std::string sourceIp = "127.0.0.1";
        std::string destinationIp = "127.0.0.1";
        uint16_t sourcePortBase = 17100;
        uint16_t destinationPortBase = 18100;
        uint16_t channelCount = 4;
        uint16_t channelOffset = 0;
        uint32_t maxSegments = 80;
        uint32_t interPacketUs = 2;
        uint32_t repeat = 1;
        bool helpOnly = false;
    };

    struct ReplayResult
    {
        bool success = false;
        uint64_t sentPackets = 0;
        uint64_t sentBytes = 0;
        uint64_t skippedPackets = 0;
        uint32_t replayedSegments = 0;
    };

    class RawFileUdpReplayer
    {
    public:
        explicit RawFileUdpReplayer(ProgramOptions opts)
            : m_opts(std::move(opts))
        {
        }

        ReplayResult replayOnce()
        {
            ReplayResult result;

            try
            {
                openpni::distributed::coreio::RawDataFileReader input;
                input.Open(m_opts.rawPath);

                const auto &info = input.Info();
                const uint32_t segmentsToReplay = std::min<uint32_t>(info.segmentNum, m_opts.maxSegments);

                std::vector<int> sockets(m_opts.channelCount, -1);
                std::vector<sockaddr_in> destinations(m_opts.channelCount);

                if (!createSockets(sockets, destinations))
                {
                    closeSockets(sockets);
                    return result;
                }

                for (uint32_t seg = 0; seg < segmentsToReplay; ++seg)
                {
                    auto segment = input.ReadSegment(seg, seg + 1);
                    auto view = segment.View();

                    if (!view.data || !view.length || !view.offset || !view.channel)
                    {
                        continue;
                    }

                    for (uint64_t i = 0; i < view.count; ++i)
                    {
                        const uint16_t rawChannel = view.channel[i];
                        if (rawChannel < m_opts.channelOffset)
                        {
                            result.skippedPackets += 1;
                            continue;
                        }

                        const uint16_t ch = static_cast<uint16_t>(rawChannel - m_opts.channelOffset);
                        if (ch >= m_opts.channelCount)
                        {
                            result.skippedPackets += 1;
                            continue;
                        }

                        const uint16_t packetLength = view.length[i];
                        const uint64_t packetOffset = view.offset[i];
                        const uint8_t *payload = view.data + packetOffset;

                        const ssize_t written = ::sendto(
                            sockets[ch],
                            payload,
                            packetLength,
                            0,
                            reinterpret_cast<const sockaddr *>(&destinations[ch]),
                            sizeof(sockaddr_in));

                        if (written > 0)
                        {
                            result.sentPackets += 1;
                            result.sentBytes += static_cast<uint64_t>(written);
                        }
                        else
                        {
                            std::cerr << "[UdpReplayer] sendto failed at segment=" << seg
                                      << " packet=" << i
                                      << " raw_channel=" << rawChannel
                                      << " local_channel=" << ch
                                      << std::endl;
                            closeSockets(sockets);
                            return result;
                        }

                        if (m_opts.interPacketUs > 0)
                        {
                            std::this_thread::sleep_for(std::chrono::microseconds(m_opts.interPacketUs));
                        }
                    }

                    result.replayedSegments += 1;
                }

                closeSockets(sockets);
                result.success = true;
                return result;
            }
            catch (const std::exception &e)
            {
                std::cerr << "[UdpReplayer] exception: " << e.what() << std::endl;
                return result;
            }
        }

    private:
        bool createSockets(std::vector<int> &sockets, std::vector<sockaddr_in> &destinations)
        {
            for (uint16_t ch = 0; ch < m_opts.channelCount; ++ch)
            {
                const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
                if (fd < 0)
                {
                    std::cerr << "[UdpReplayer] socket create failed, channel=" << ch << std::endl;
                    return false;
                }

                sockaddr_in srcAddr{};
                srcAddr.sin_family = AF_INET;
                srcAddr.sin_port = htons(static_cast<uint16_t>(m_opts.sourcePortBase + ch));
                if (::inet_pton(AF_INET, m_opts.sourceIp.c_str(), &srcAddr.sin_addr) != 1)
                {
                    std::cerr << "[UdpReplayer] invalid source ip: " << m_opts.sourceIp << std::endl;
                    ::close(fd);
                    return false;
                }

                if (::bind(fd, reinterpret_cast<const sockaddr *>(&srcAddr), sizeof(srcAddr)) != 0)
                {
                    std::cerr << "[UdpReplayer] bind failed on source port=" << (m_opts.sourcePortBase + ch) << std::endl;
                    ::close(fd);
                    return false;
                }

                sockaddr_in dstAddr{};
                dstAddr.sin_family = AF_INET;
                dstAddr.sin_port = htons(static_cast<uint16_t>(m_opts.destinationPortBase + ch));
                if (::inet_pton(AF_INET, m_opts.destinationIp.c_str(), &dstAddr.sin_addr) != 1)
                {
                    std::cerr << "[UdpReplayer] invalid destination ip: " << m_opts.destinationIp << std::endl;
                    ::close(fd);
                    return false;
                }

                sockets[ch] = fd;
                destinations[ch] = dstAddr;
            }

            return true;
        }

        static void closeSockets(std::vector<int> &sockets)
        {
            for (int &fd : sockets)
            {
                if (fd >= 0)
                {
                    ::close(fd);
                    fd = -1;
                }
            }
        }

        ProgramOptions m_opts;
    };

    void printUsage(const char *prog)
    {
        std::cout << "Usage: " << prog << " --raw-path <path> [options]\n"
                  << "Options:\n"
                  << "  --source-ip <ip>              source bind ip (default: 127.0.0.1)\n"
                  << "  --destination-ip <ip>         destination ip (default: 127.0.0.1)\n"
                  << "  --source-port-base <port>     source base port (default: 17100)\n"
                  << "  --destination-port-base <port> destination base port (default: 18100)\n"
                  << "  --channel-count <n>           channel count (default: 4)\n"
                  << "  --channel-offset <n>          raw channel offset mapped to local [0..channel-count-1] (default: 0)\n"
                  << "  --max-segments <n>            max segments replayed each run (default: 80)\n"
                  << "  --inter-packet-us <n>         delay between packets in us (default: 2)\n"
                  << "  --repeat <n>                  replay repeat times (default: 1)\n"
                  << "  --help                        print this message\n"
                  << std::endl;
    }

    bool parseArgs(int argc, char **argv, ProgramOptions *opts)
    {
        for (int i = 1; i < argc; ++i)
        {
            const std::string arg = argv[i];
            auto needValue = [&](const std::string &name) -> const char *
            {
                if (i + 1 >= argc)
                {
                    std::cerr << "Missing value for " << name << std::endl;
                    return nullptr;
                }
                return argv[++i];
            };

            if (arg == "--help")
            {
                printUsage(argv[0]);
                opts->helpOnly = true;
                return true;
            }
            if (arg == "--raw-path")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->rawPath = v;
                continue;
            }
            if (arg == "--source-ip")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->sourceIp = v;
                continue;
            }
            if (arg == "--destination-ip")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->destinationIp = v;
                continue;
            }
            if (arg == "--source-port-base")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->sourcePortBase = static_cast<uint16_t>(std::stoul(v));
                continue;
            }
            if (arg == "--destination-port-base")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->destinationPortBase = static_cast<uint16_t>(std::stoul(v));
                continue;
            }
            if (arg == "--channel-count")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->channelCount = static_cast<uint16_t>(std::stoul(v));
                continue;
            }
            if (arg == "--max-segments")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->maxSegments = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--channel-offset")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->channelOffset = static_cast<uint16_t>(std::stoul(v));
                continue;
            }
            if (arg == "--inter-packet-us")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->interPacketUs = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--repeat")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->repeat = static_cast<uint32_t>(std::stoul(v));
                continue;
            }

            std::cerr << "Unknown argument: " << arg << std::endl;
            return false;
        }

        if (opts->rawPath.empty())
        {
            std::cerr << "--raw-path is required" << std::endl;
            return false;
        }

        if (opts->channelCount == 0)
        {
            std::cerr << "--channel-count must be > 0" << std::endl;
            return false;
        }

        if (opts->repeat == 0)
        {
            std::cerr << "--repeat must be > 0" << std::endl;
            return false;
        }

        return true;
    }

} // namespace

int main(int argc, char **argv)
{
    ProgramOptions opts;
    if (!parseArgs(argc, argv, &opts))
    {
        return 1;
    }
    if (opts.helpOnly)
    {
        return 0;
    }

    std::cout << "===========================================" << std::endl;
    std::cout << "  App: UDP Raw Replay Sender" << std::endl;
    std::cout << "===========================================" << std::endl;
    std::cout << "rawPath             : " << opts.rawPath << std::endl;
    std::cout << "sourceIp            : " << opts.sourceIp << std::endl;
    std::cout << "destinationIp       : " << opts.destinationIp << std::endl;
    std::cout << "sourcePortBase      : " << opts.sourcePortBase << std::endl;
    std::cout << "destinationPortBase : " << opts.destinationPortBase << std::endl;
    std::cout << "channelCount        : " << opts.channelCount << std::endl;
    std::cout << "channelOffset       : " << opts.channelOffset << std::endl;
    std::cout << "maxSegments         : " << opts.maxSegments << std::endl;
    std::cout << "interPacketUs       : " << opts.interPacketUs << std::endl;
    std::cout << "repeat              : " << opts.repeat << std::endl;

    RawFileUdpReplayer replayer(opts);

    ReplayResult result;
    for (uint32_t i = 0; i < opts.repeat; ++i)
    {
        result = replayer.replayOnce();
        if (!result.success)
        {
            break;
        }
    }

    std::cout << "[UdpReplayer] success=" << (result.success ? "true" : "false")
              << " sentPackets=" << result.sentPackets
              << " sentBytes=" << result.sentBytes
              << " skippedPackets=" << result.skippedPackets
              << " replayedSegments=" << result.replayedSegments
              << std::endl;

    return result.success ? 0 : 2;
}
