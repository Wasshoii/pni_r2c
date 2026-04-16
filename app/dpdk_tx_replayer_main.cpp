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

    const uint16_t nbPorts = rte_eth_dev_count_avail();
    if (opts.portId >= nbPorts)
    {
        std::cerr << "Invalid --port-id " << opts.portId << ", available ports=" << nbPorts << std::endl;
        return 2;
    }

    std::cout << "===========================================\n";
    std::cout << "  App: DPDK TX UDP Replayer\n";
    std::cout << "===========================================\n";
    std::cout << "portId              : " << opts.portId << "\n";
    std::cout << "sourceIp            : " << opts.sourceIp << "\n";
    std::cout << "destinationIp       : " << opts.destinationIp << "\n";
    std::cout << "destinationMac      : " << opts.destinationMac << "\n";
    std::cout << "sourcePortBase      : " << opts.sourcePortBase << "\n";
    std::cout << "destinationPortBase : " << opts.destinationPortBase << "\n";
    std::cout << "channelCount        : " << opts.channelCount << "\n";
    std::cout << "channelOffset       : " << opts.channelOffset << "\n";
    std::cout << "payloadSize         : " << opts.payloadSize << "\n";
    std::cout << "burstSize           : " << opts.burstSize << "\n";
    std::cout << "pps                 : " << opts.pps << (opts.pps == 0 ? " (max)" : "") << "\n";
    std::cout << "durationSec         : " << opts.durationSec << "\n";

    rte_mempool *mp = rte_pktmbuf_pool_create(
        "dpdk_tx_replayer_pool",
        opts.mbufCount,
        256,
        0,
        RTE_MBUF_DEFAULT_BUF_SIZE,
        rte_socket_id());
    if (mp == nullptr)
    {
        std::cerr << "rte_pktmbuf_pool_create failed" << std::endl;
        return 3;
    }

    rte_eth_conf portConf{};
    const int confRc = rte_eth_dev_configure(opts.portId, 0, 1, &portConf);
    if (confRc < 0)
    {
        std::cerr << "rte_eth_dev_configure failed: " << confRc << std::endl;
        return 3;
    }

    const int txqRc = rte_eth_tx_queue_setup(opts.portId, 0, 1024, rte_eth_dev_socket_id(opts.portId), nullptr);
    if (txqRc < 0)
    {
        std::cerr << "rte_eth_tx_queue_setup failed: " << txqRc << std::endl;
        return 3;
    }

    const int startRc = rte_eth_dev_start(opts.portId);
    if (startRc < 0)
    {
        std::cerr << "rte_eth_dev_start failed: " << startRc << std::endl;
        return 3;
    }

    rte_eth_promiscuous_enable(opts.portId);

    rte_ether_addr srcMac{};
    rte_eth_macaddr_get(opts.portId, &srcMac);

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    const auto t0 = std::chrono::steady_clock::now();
    auto tLast = t0;
    uint64_t sentPkts = 0;
    uint64_t sentBytes = 0;
    uint64_t droppedPkts = 0;
    uint64_t chSeq = 0;

    while (!g_stop.load())
    {
        const auto now = std::chrono::steady_clock::now();
        const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - t0).count();
        if (elapsed >= opts.durationSec)
        {
            break;
        }

        if (opts.pps > 0)
        {
            const double elapsedSec = std::chrono::duration<double>(now - t0).count();
            const uint64_t shouldSend = static_cast<uint64_t>(elapsedSec * static_cast<double>(opts.pps));
            if (sentPkts >= shouldSend)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
                continue;
            }
        }

        std::vector<rte_mbuf *> burst;
        burst.reserve(opts.burstSize);

        for (uint16_t i = 0; i < opts.burstSize; ++i)
        {
            rte_mbuf *m = rte_pktmbuf_alloc(mp);
            if (m == nullptr)
            {
                droppedPkts += 1;
                continue;
            }

            const uint16_t ch = static_cast<uint16_t>((chSeq % opts.channelCount) + opts.channelOffset);
            const uint16_t srcPort = static_cast<uint16_t>(opts.sourcePortBase + ch);
            const uint16_t dstPort = static_cast<uint16_t>(opts.destinationPortBase + ch);
            const uint8_t fillByte = static_cast<uint8_t>(ch & 0xFF);

            if (!buildPacket(m, srcMac, dstMac, srcIp, dstIp, srcPort, dstPort, opts.payloadSize, fillByte))
            {
                rte_pktmbuf_free(m);
                droppedPkts += 1;
                continue;
            }

            burst.push_back(m);
            chSeq += 1;
        }

        if (!burst.empty())
        {
            const uint16_t n = static_cast<uint16_t>(burst.size());
            const uint16_t tx = rte_eth_tx_burst(opts.portId, 0, burst.data(), n);

            for (uint16_t i = tx; i < n; ++i)
            {
                rte_pktmbuf_free(burst[i]);
                droppedPkts += 1;
            }

            sentPkts += tx;
            sentBytes += static_cast<uint64_t>(tx) * static_cast<uint64_t>(opts.payloadSize + sizeof(rte_udp_hdr));
        }

        const auto sinceLastMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - tLast).count();
        if (sinceLastMs >= opts.reportIntervalMs)
        {
            const double sec = std::chrono::duration<double>(now - t0).count();
            const double ppsNow = (sec > 0.0) ? (static_cast<double>(sentPkts) / sec) : 0.0;
            const double mbpsNow = (sec > 0.0) ? ((static_cast<double>(sentBytes) * 8.0) / sec / 1e6) : 0.0;

            std::cout << "[DpdkTx] elapsed_s=" << sec
                      << " sent_pkts=" << sentPkts
                      << " dropped_pkts=" << droppedPkts
                      << " avg_pps=" << ppsNow
                      << " avg_mbps(l4)=" << mbpsNow
                      << std::endl;
            tLast = now;
        }
    }

    const double totalSec = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const double avgPps = (totalSec > 0.0) ? (static_cast<double>(sentPkts) / totalSec) : 0.0;
    const double avgMbps = (totalSec > 0.0) ? ((static_cast<double>(sentBytes) * 8.0) / totalSec / 1e6) : 0.0;

    std::cout << "[DpdkTx] finished elapsed_s=" << totalSec
              << " sent_pkts=" << sentPkts
              << " dropped_pkts=" << droppedPkts
              << " avg_pps=" << avgPps
              << " avg_mbps(l4)=" << avgMbps
              << std::endl;

    rte_eth_dev_stop(opts.portId);
    rte_eth_dev_close(opts.portId);
    rte_eal_cleanup();
    return 0;
}
