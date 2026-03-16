#include <pni/PnI-Config.hpp>

#include "distributed/AcquisitionMaster.hpp"
#include "grpcNode/acquisitionNode.hpp"

int main()
{
    openpni::distributed::acquisition::AcquisitionTask task;
    task.set_storage_unit_size(1024);
    task.set_max_buffer_size(4ull * 1024ull * 1024ull * 1024ull);
    task.set_time_switch_buffer_ms(1000);
    task.set_min_packet_size(1024);

    openpni::distributed::acquisition::AcquisitionMaster master;
    master.Initialize(task);

    openpni::distributed::grpcnode::AcquisitionGrpcNode::InitOptions nodeOpts;
    openpni::distributed::grpcnode::AcquisitionGrpcNode node(nodeOpts);

    (void)node;
    return 0;
}
