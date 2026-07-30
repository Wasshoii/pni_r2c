#pragma once

#include "dataplane/rdma/RdmaTypes.hpp"
#include "protos/coincidence.pb.h"

#include <cstring>

namespace openpni::distributed::dataplane::rdma
{

inline coincidence::DataPlaneKind toProto(DataPlaneKind kind)
{
    switch (kind)
    {
    case DataPlaneKind::RdmaRoceV2:
        return coincidence::DATA_PLANE_RDMA_ROCE_V2;
    case DataPlaneKind::InProcess:
        return coincidence::DATA_PLANE_INPROCESS;
    default:
        return coincidence::DATA_PLANE_UNSPECIFIED;
    }
}

inline DataPlaneKind fromProto(coincidence::DataPlaneKind kind)
{
    switch (kind)
    {
    case coincidence::DATA_PLANE_RDMA_ROCE_V2:
        return DataPlaneKind::RdmaRoceV2;
    case coincidence::DATA_PLANE_INPROCESS:
        return DataPlaneKind::InProcess;
    default:
        return DataPlaneKind::Unspecified;
    }
}

inline void fillProtoEndpoint(const RdmaEndpointInfo &src, coincidence::RdmaEndpoint *dst)
{
    if (!dst)
    {
        return;
    }
    dst->set_kind(toProto(src.kind));
    dst->set_gid(src.gid.data(), src.gid.size());
    dst->set_lid(src.lid);
    dst->set_qp_num(src.qpNum);
    dst->set_psn(src.psn);
    dst->set_rkey(src.rkey);
    dst->set_base_addr(src.baseAddr);
    dst->set_slot_count(src.slotCount);
    dst->set_slot_stride(src.slotStride);
    dst->set_notify_rkey(src.notifyRkey);
    dst->set_notify_addr(src.notifyAddr);
    dst->set_notify_capacity(src.notifyCapacity);
    dst->set_consumer_rkey(src.consumerRkey);
    dst->set_consumer_addr(src.consumerAddr);
    dst->set_device_name(src.deviceName);
    dst->set_port_num(src.portNum);
    dst->set_gid_index(src.gidIndex);
    dst->set_inprocess_handle(src.inprocessHandle);
}

inline RdmaEndpointInfo fromProtoEndpoint(const coincidence::RdmaEndpoint &src)
{
    RdmaEndpointInfo dst;
    dst.kind = fromProto(src.kind());
    if (src.gid().size() >= 16)
    {
        std::memcpy(dst.gid.data(), src.gid().data(), 16);
    }
    dst.lid = src.lid();
    dst.qpNum = src.qp_num();
    dst.psn = src.psn();
    dst.rkey = src.rkey();
    dst.baseAddr = src.base_addr();
    dst.slotCount = src.slot_count();
    dst.slotStride = src.slot_stride();
    dst.notifyRkey = src.notify_rkey();
    dst.notifyAddr = src.notify_addr();
    dst.notifyCapacity = src.notify_capacity();
    dst.consumerRkey = src.consumer_rkey();
    dst.consumerAddr = src.consumer_addr();
    dst.deviceName = src.device_name();
    dst.portNum = src.port_num() == 0 ? 1 : src.port_num();
    dst.gidIndex = src.gid_index();
    dst.inprocessHandle = src.inprocess_handle();
    return dst;
}

} // namespace openpni::distributed::dataplane::rdma
