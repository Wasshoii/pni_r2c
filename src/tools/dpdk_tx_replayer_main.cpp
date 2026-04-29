#include <arpa/inet.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

extern "C"
{
#include <rte_byteorder.h>
#include <rte_eal.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_ip.h>
#include <rte_mbuf.h>
#include <rte_udp.h>
}

namespace
{
    struct ProgramOptions
    {
        uint16_t portId = 0;
        std::string sourceIp = "10.10.1.10";
        std::string destinationIp = "10.10.1.20";
        std::string destinationMac;
        uint16_t sourcePortBase = 17100;
        uint16_t destinationPortBase = 18100;
        uint16_t channelCount = 1;
        uint16_t channelOffset = 0;
        uint16_t payloadSize = 512;
        uint16_t burstSize = 32;
        uint32_t mbufCount = 32768;
        uint32_t durationSec = 10;
        uint32_t reportIntervalMs = 1000;
        uint64_t pps = 0; // 0 means maximum speed.
        std::string ealArgs = "--in-memory --file-prefix=dpdk_tx_replayer";
        bool helpOnly = false;
    };

    std::atomic<bool> g_stop{false};

    void onSignal(int)
    {
        g_stop.store(true);
    }

    void printUsage(const char *prog)
    {
        std::cout
            << "Usage: " << prog << " --dst-mac <mac> [options]\n"
            << "Options:\n"
            << "  --port-id <n>                 DPDK tx port id (default: 0)\n"
            << "  --source-ip <ip>              IPv4 source in packet (default: 10.10.1.10)\n"
            << "  --destination-ip <ip>         IPv4 destination in packet (default: 10.10.1.20)\n"
            << "  --dst-mac <mac>               destination MAC, e.g. aa:bb:cc:dd:ee:ff (required)\n"
            << "  --source-port-base <port>     UDP source base port (default: 17100)\n"
            << "  --destination-port-base <port> UDP destination base port (default: 18100)\n"
            << "  --channel-count <n>           channel count (default: 1)\n"
            << "  --channel-offset <n>          local channel offset (default: 0)\n"
            << "  --payload-size <n>            payload bytes per packet (default: 512)\n"
            << "  --burst-size <n>              tx burst size (default: 32)\n"
            << "  --mbuf-count <n>              mbuf count in pool (default: 32768)\n"
            << "  --pps <n>                     packets per second target, 0 means max speed\n"
            << "  --duration-sec <n>            send duration in seconds (default: 10)\n"
            << "  --report-interval-ms <n>      report interval in ms (default: 1000)\n"
            << "  --eal-args \"...\"             extra EAL args string (default: --in-memory --file-prefix=dpdk_tx_replayer)\n"
            << "  --help                        print this message\n";
    }

    bool parseIPv4(const std::string &ip, uint32_t *outNetworkOrder)
    {
        in_addr addr{};
        if (::inet_pton(AF_INET, ip.c_str(), &addr) != 1)
        {
            return false;
        }
        *outNetworkOrder = addr.s_addr;
        return true;
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
            if (arg == "--port-id")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->portId = static_cast<uint16_t>(std::stoul(v));
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
            if (arg == "--dst-mac")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->destinationMac = v;
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
            if (arg == "--payload-size")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->payloadSize = static_cast<uint16_t>(std::stoul(v));
                continue;
            }
            if (arg == "--burst-size")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->burstSize = static_cast<uint16_t>(std::stoul(v));
                continue;
            }
            if (arg == "--mbuf-count")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->mbufCount = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--pps")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->pps = static_cast<uint64_t>(std::stoull(v));
                continue;
            }
            if (arg == "--duration-sec")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->durationSec = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--report-interval-ms")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->reportIntervalMs = static_cast<uint32_t>(std::stoul(v));
                continue;
            }
            if (arg == "--eal-args")
            {
                const char *v = needValue(arg);
                if (!v)
                {
                    return false;
                }
                opts->ealArgs = v;
                continue;
            }

            std::cerr << "Unknown argument: " << arg << std::endl;
            return false;
        }

        if (opts->destinationMac.empty())
        {
            std::cerr << "--dst-mac is required" << std::endl;
            return false;
        }
        if (opts->channelCount == 0)
        {
            std::cerr << "--channel-count must be > 0" << std::endl;
            return false;
        }
        if (opts->burstSize == 0)
        {
            std::cerr << "--burst-size must be > 0" << std::endl;
            return false;
        }
        if (opts->payloadSize < 16)
        {
            std::cerr << "--payload-size must be >= 16" << std::endl;
            return false;
        }

        return true;
    }

    std::vector<std::string> splitBySpace(const std::string &s)
    {
        std::istringstream iss(s);
        std::vector<std::string> parts;
        std::string item;
        while (iss >> item)
        {
            parts.push_back(item);
        }
        return parts;
    }

    bool buildPacket(rte_mbuf *m,
                     const rte_ether_addr &srcMac,
                     const rte_ether_addr &dstMac,
                     uint32_t srcIp,
                     uint32_t dstIp,
                     uint16_t srcPort,
                     uint16_t dstPort,
                     uint16_t payloadSize,
                     uint8_t fillByte)
    {
        const uint16_t pktSize = static_cast<uint16_t>(sizeof(rte_ether_hdr) + sizeof(rte_ipv4_hdr) + sizeof(rte_udp_hdr) + payloadSize);
        char *buf = reinterpret_cast<char *>(rte_pktmbuf_append(m, pktSize));
        if (buf == nullptr)
        {
            return false;
        }

        auto *eth = reinterpret_cast<rte_ether_hdr *>(buf);
        rte_ether_addr_copy(&dstMac, &eth->dst_addr);
        rte_ether_addr_copy(&srcMac, &eth->src_addr);
        eth->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

        auto *ip = reinterpret_cast<rte_ipv4_hdr *>(eth + 1);
        ip->version_ihl = RTE_IPV4_VHL_DEF;
        ip->type_of_service = 0;
        ip->total_length = rte_cpu_to_be_16(static_cast<uint16_t>(sizeof(rte_ipv4_hdr) + sizeof(rte_udp_hdr) + payloadSize));
        ip->packet_id = 0;
        ip->fragment_offset = 0;
        ip->time_to_live = 64;
        ip->next_proto_id = IPPROTO_UDP;
        ip->src_addr = srcIp;
        ip->dst_addr = dstIp;
        ip->hdr_checksum = 0;

        auto *udp = reinterpret_cast<rte_udp_hdr *>(ip + 1);
        udp->src_port = rte_cpu_to_be_16(srcPort);
        udp->dst_port = rte_cpu_to_be_16(dstPort);
        udp->dgram_len = rte_cpu_to_be_16(static_cast<uint16_t>(sizeof(rte_udp_hdr) + payloadSize));
        udp->dgram_cksum = 0;

        uint8_t *payload = reinterpret_cast<uint8_t *>(udp + 1);
        std::memset(payload, fillByte, payloadSize);

        ip->hdr_checksum = rte_ipv4_cksum(ip);
        udp->dgram_cksum = rte_ipv4_udptcp_cksum(ip, udp);

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

    uint32_t srcIp = 0;
    uint32_t dstIp = 0;
    if (!parseIPv4(opts.sourceIp, &srcIp))
    {
        std::cerr << "Invalid --source-ip: " << opts.sourceIp << std::endl;
        return 1;
    }
    if (!parseIPv4(opts.destinationIp, &dstIp))
    {
        std::cerr << "Invalid --destination-ip: " << opts.destinationIp << std::endl;
        return 1;
    }

    rte_ether_addr dstMac{};
    if (rte_ether_unformat_addr(opts.destinationMac.c_str(), &dstMac) != 0)
    {
        std::cerr << "Invalid --dst-mac: " << opts.destinationMac << std::endl;
        return 1;
    }

    std::vector<std::string> ealParts = splitBySpace(opts.ealArgs);
    std::vector<std::string> allArgs;
    allArgs.emplace_back(argv[0]);
    allArgs.insert(allArgs.end(), ealParts.begin(), ealParts.end());

    std::vector<char *> ealArgv;
    ealArgv.reserve(allArgs.size());
    for (auto &a : allArgs)
    {
        ealArgv.push_back(a.data());
    }

    const int ealRc = rte_eal_init(static_cast<int>(ealArgv.size()), ealArgv.data());
    if (ealRc < 0)
    {
        std::cerr << "rte_eal_init failed" << std::endl;
        return 2;
    }

    const uint16_t portId = opts.portId;
    if (portId >= rte_eth_dev_count_avail())
    {
        std::cerr << "Invalid port id: " << portId << std::endl;
        return 2;
    }

    if (rte_eth_dev_socket_id(portId) < 0)
    {
        std::cerr << "Invalid socket id for port: " << portId << std::endl;
        return 2;
    }

    rte_mempool *mbufPool = rte_pktmbuf_pool_create(
        "tx_pool",
        opts.mbufCount,
        256,
        0,
        RTE_MBUF_DEFAULT_BUF_SIZE,
        rte_socket_id());

    if (mbufPool == nullptr)
    {
        std::cerr << "Failed to create mbuf pool" << std::endl;
        return 2;
    }

    rte_eth_conf portConf{};
    portConf.txmode.mq_mode = RTE_ETH_MQ_TX_NONE;

    if (rte_eth_dev_configure(portId, 0, 1, &portConf) < 0)
    {
        std::cerr << "rte_eth_dev_configure failed" << std::endl;
        return 2;
    }

    if (rte_eth_tx_queue_setup(portId, 0, 1024, rte_eth_dev_socket_id(portId), nullptr) < 0)
    {
        std::cerr << "rte_eth_tx_queue_setup failed" << std::endl;
        return 2;
    }

    if (rte_eth_dev_start(portId) < 0)
    {
        std::cerr << "rte_eth_dev_start failed" << std::endl;
        return 2;
    }

    rte_ether_addr srcMac{};
    if (rte_eth_macaddr_get(portId, &srcMac) != 0)
    {
        std::cerr << "rte_eth_macaddr_get failed" << std::endl;
        return 2;
    }

    std::cout << "===========================================\n";
    std::cout << "  Tool: DPDK TX Replayer\n";
    std::cout << "===========================================\n";
    std::cout << "portId             : " << portId << "\n";
    std::cout << "sourceIp           : " << opts.sourceIp << "\n";
    std::cout << "destinationIp      : " << opts.destinationIp << "\n";
    std::cout << "destinationMac     : " << opts.destinationMac << "\n";
    std::cout << "sourcePortBase     : " << opts.sourcePortBase << "\n";
    std::cout << "destinationPortBase: " << opts.destinationPortBase << "\n";
    std::cout << "channelCount       : " << opts.channelCount << "\n";
    std::cout << "channelOffset      : " << opts.channelOffset << "\n";
    std::cout << "payloadSize        : " << opts.payloadSize << "\n";
    std::cout << "burstSize          : " << opts.burstSize << "\n";
    std::cout << "mbufCount          : " << opts.mbufCount << "\n";
    std::cout << "durationSec        : " << opts.durationSec << "\n";
    std::cout << "reportIntervalMs   : " << opts.reportIntervalMs << "\n";
    std::cout << "pps                : " << opts.pps << "\n";

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    const uint32_t totalChannels = opts.channelCount;
    const uint16_t portSrcBase = opts.sourcePortBase;
    const uint16_t portDstBase = opts.destinationPortBase;

    auto start = std::chrono::steady_clock::now();
    auto lastReport = start;

    uint64_t totalSent = 0;
    uint64_t totalBytes = 0;
    uint64_t lastTotalSent = 0;

    while (!g_stop.load())
    {
        auto now = std::chrono::steady_clock::now();
        const auto elapsedSec = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
        if (opts.durationSec > 0 && static_cast<uint32_t>(elapsedSec) >= opts.durationSec)
        {
            break;
        }

        std::vector<rte_mbuf *> burst(opts.burstSize, nullptr);
        uint16_t ready = 0;
        for (uint16_t i = 0; i < opts.burstSize; ++i)
        {
            burst[i] = rte_pktmbuf_alloc(mbufPool);
            if (!burst[i])
            {
                break;
            }

            const uint16_t ch = static_cast<uint16_t>((totalSent + i) % totalChannels);
            const uint16_t srcPort = static_cast<uint16_t>(portSrcBase + ch);
            const uint16_t dstPort = static_cast<uint16_t>(portDstBase + ch);
            const uint8_t fillByte = static_cast<uint8_t>((ch + opts.channelOffset) & 0xFFu);

            if (!buildPacket(burst[i], srcMac, dstMac, srcIp, dstIp, srcPort, dstPort, opts.payloadSize, fillByte))
            {
                rte_pktmbuf_free(burst[i]);
                burst[i] = nullptr;
                break;
            }
            ready += 1;
        }

        if (ready == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        const uint16_t sent = rte_eth_tx_burst(portId, 0, burst.data(), ready);
        for (uint16_t i = sent; i < ready; ++i)
        {
            rte_pktmbuf_free(burst[i]);
        }

        totalSent += sent;
        totalBytes += static_cast<uint64_t>(sent) * opts.payloadSize;

        if (opts.pps > 0)
        {
            const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastReport).count();
            const uint64_t expected = (opts.pps * static_cast<uint64_t>(elapsedMs)) / 1000ull;
            const uint64_t actual = totalSent - lastTotalSent;
            if (actual > expected)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        }

        if (opts.reportIntervalMs > 0 && now - lastReport >= std::chrono::milliseconds(opts.reportIntervalMs))
        {
            const double dt = std::chrono::duration<double>(now - lastReport).count();
            const double pps = (totalSent - lastTotalSent) / dt;
            std::cout << "[DPDK-TX] sent=" << totalSent
                      << " pps=" << pps
                      << " bytes=" << totalBytes
                      << std::endl;
            lastReport = now;
            lastTotalSent = totalSent;
        }
    }

    rte_eth_dev_stop(portId);
    rte_eth_dev_close(portId);

    std::cout << "[DPDK-TX] done, totalSent=" << totalSent
              << " totalBytes=" << totalBytes
              << std::endl;

    return 0;
}
