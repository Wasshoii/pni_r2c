#include <pni/PnI-Config.hpp>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "core/acquisition/AcquisitionServer.hpp"
#include "grpcService/AcquisitionMaster.hpp"
#include "grpcNode/acquisitionNode.hpp"

namespace
{
    namespace acq = openpni::distributed::acquisition;
    namespace grpcnode = openpni::distributed::grpcnode;

    uint32_t ipToInt(const std::string &ip)
    {
        std::stringstream ss(ip);
        std::string token;
        uint32_t result = 0;

        for (int i = 0; i < 4; ++i)
        {
            if (!std::getline(ss, token, '.'))
            {
                return 0;
            }
            const uint32_t octet = static_cast<uint32_t>(std::stoul(token));
            if (octet > 255)
            {
                return 0;
            }
            result = (result << 8) | octet;
        }

        return result;
    }

    std::string stateToString(acq::NodeState state)
    {
        switch (state)
        {
        case acq::STATE_IDLE:
            return "IDLE";
        case acq::STATE_CONFIGURED:
            return "CONFIGURED";
        case acq::STATE_RUNNING:
            return "RUNNING";
        case acq::STATE_ERROR:
            return "ERROR";
        default:
            return "UNKNOWN";
        }
    }

    std::string ipToString(uint32_t ip)
    {
        return std::to_string((ip >> 24) & 0xFF) + "." +
               std::to_string((ip >> 16) & 0xFF) + "." +
               std::to_string((ip >> 8) & 0xFF) + "." +
               std::to_string(ip & 0xFF);
    }

    void fillDetectorMapping(acq::AcquisitionTask &task)
    {
        for (uint16_t i = 0; i < 8; ++i)
        {
            auto *source = task.add_detector_sources();
            source->set_detector_id("detector-" + std::to_string(i));

            // Fake detector source IPs for integration testing.
            const uint16_t subnet = static_cast<uint16_t>((i / 4) + 1);
            const uint16_t host = static_cast<uint16_t>((i % 4) + 10);
            source->set_ip_source(ipToInt("10.10." + std::to_string(subnet) + "." + std::to_string(host)));
            source->set_port_source(static_cast<uint32_t>(7000 + i));
        }

        auto *dest = task.mutable_destination_rule();
        // Keep destination assignment deterministic for test visibility.
        dest->set_ip_destination(ipToInt("239.255.0.1"));
        dest->set_port_destination_base(8100);
        dest->set_port_destination_stride(1);

        // Do not set channel_index_rule explicitly; controller default is base=0, stride=1.

        // Fill DPDK options for protocol completeness; ignored for SOCKET runtime.
        auto *dpdk = task.mutable_dpdk_options();
        dpdk->set_copy_thread_num(12);
        dpdk->set_rx_rings_per_port(2);
        dpdk->set_rte_mbuf_double_pointer_size_multiply(48);
        dpdk->set_rte_mbuf_double_pointer_num_multiply(3);
        dpdk->add_bind_ips("192.168.100.10");
        dpdk->add_bind_ips("192.168.100.11");
    }

    void printGlobalTaskSummary(const acq::AcquisitionTask &task)
    {
        std::cout << "[InitTest/Config] algorithm_type=" << task.algorithm_type()
                  << " storage_unit_size=" << task.storage_unit_size()
                  << " min_packet_size=" << task.min_packet_size()
                  << " max_buffer_size=" << task.max_buffer_size()
                  << " time_switch_buffer_ms=" << task.time_switch_buffer_ms()
                  << " session='" << task.session_name() << "'"
                  << " reserved_storage_gib=" << task.reserved_storage_gib()
                  << " max_file_size_mb=" << task.max_file_size_mb()
                  << " detector_sources=" << task.detector_sources_size()
                  << std::endl;

        if (task.has_destination_rule())
        {
            std::cout << "[InitTest/Config] destination_rule dst="
                      << ipToString(task.destination_rule().ip_destination())
                      << " port_base=" << task.destination_rule().port_destination_base()
                      << " port_stride=" << task.destination_rule().port_destination_stride()
                      << std::endl;
        }

        if (task.has_channel_index_rule())
        {
            std::cout << "[InitTest/Config] channel_index_rule base=" << task.channel_index_rule().channel_index_base()
                      << " stride=" << task.channel_index_rule().channel_index_stride()
                      << std::endl;
        }
        else
        {
            std::cout << "[InitTest/Config] channel_index_rule=<implicit default: base=0, stride=1>" << std::endl;
        }

        if (task.has_dpdk_options())
        {
            const auto &dpdk = task.dpdk_options();
            std::cout << "[InitTest/Config] dpdk(copy_threads=" << dpdk.copy_thread_num()
                      << ",rx_rings_per_port=" << dpdk.rx_rings_per_port()
                      << ",bind_ips=" << dpdk.bind_ips_size()
                      << ",ppsize_mul=" << dpdk.rte_mbuf_double_pointer_size_multiply()
                      << ",ppnum_mul=" << dpdk.rte_mbuf_double_pointer_num_multiply()
                      << ")"
                      << std::endl;
        }

        for (int i = 0; i < task.detector_sources_size(); ++i)
        {
            const auto &src = task.detector_sources(i);
            std::cout << "[InitTest/Config] detector=" << src.detector_id()
                      << " src=" << ipToString(src.ip_source()) << ":" << src.port_source()
                      << std::endl;
        }
    }

} // namespace

int main()
{
    const std::string masterAddress = "127.0.0.1:50091";

    acq::AcquisitionTask globalTask;
    globalTask.set_algorithm_type(acq::ALGORITHM_TYPE_SOCKET);
    // Keep defaults close to acquisition.cpp for easier behavior comparison.
    globalTask.set_storage_unit_size(1024);
    globalTask.set_min_packet_size(1024);
    globalTask.set_max_buffer_size(4ull * 1024ull * 1024ull * 1024ull);
    globalTask.set_time_switch_buffer_ms(1000);
    globalTask.set_session_name("acq_init_auto_map");
    globalTask.set_reserved_storage_gib(20);
    globalTask.set_max_file_size_mb(512);
    fillDetectorMapping(globalTask);
    printGlobalTaskSummary(globalTask);

    acq::AcquisitionMaster master;
    master.Initialize(globalTask);
    master.StartServer(masterAddress);

    grpcnode::AcquisitionGrpcNode::InitOptions nodeOpt1;
    nodeOpt1.masterAddress = masterAddress;
    nodeOpt1.nodeId = "acq-node-1";
    nodeOpt1.nodeAddress = "127.0.0.1";
    nodeOpt1.outputRoot = "Data/raw_data";
    nodeOpt1.sessionNamePrefix = "init_node_1";

    grpcnode::AcquisitionGrpcNode::InitOptions nodeOpt2;
    nodeOpt2.masterAddress = masterAddress;
    nodeOpt2.nodeId = "acq-node-2";
    nodeOpt2.nodeAddress = "127.0.0.1";
    nodeOpt2.outputRoot = "Data/raw_data";
    nodeOpt2.sessionNamePrefix = "init_node_2";

    grpcnode::AcquisitionGrpcNode node1(nodeOpt1);
    grpcnode::AcquisitionGrpcNode node2(nodeOpt2);

    std::thread t1([&]()
                   {
                       const bool ok = node1.run();
                       std::cout << "[NodeThread] node1 exit, ok=" << (ok ? "true" : "false") << std::endl; });

    std::thread t2([&]()
                   {
                       const bool ok = node2.run();
                       std::cout << "[NodeThread] node2 exit, ok=" << (ok ? "true" : "false") << std::endl; });

    const bool connected = master.WaitForConnectedNodes(2, 10000);
    std::cout << "[InitTest] Connected nodes = " << master.ConnectedNodeCount()
              << ", wait result=" << (connected ? "true" : "false") << std::endl;

    master.DistributeTasks();
    std::cout << "[InitTest] Distributed tasks using detector_sources -> channel mapping" << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(2500));

    auto snapshot = master.SnapshotNodes();
    size_t configuredCount = 0;
    for (const auto &node : snapshot)
    {
        std::cout << "[InitTest] node=" << node.info.node_id()
                  << " connected=" << (node.connected ? "true" : "false")
                  << " state=" << stateToString(node.lastStatus.state())
                  << " err='" << node.lastStatus.error_message() << "'"
                  << std::endl;

        if (node.lastStatus.state() == acq::STATE_CONFIGURED)
        {
            ++configuredCount;
        }
    }

    const bool initSuccess = connected && configuredCount >= 2;
    std::cout << "[InitTest] Initialization " << (initSuccess ? "SUCCESS" : "FAILED")
              << " (configured=" << configuredCount << ")" << std::endl;

    master.SendShutdown("init-test-done");
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    master.StopServer();

    if (t1.joinable())
    {
        t1.join();
    }
    if (t2.joinable())
    {
        t2.join();
    }

    return initSuccess ? 0 : 1;
}
