# ProtoConvert

> #include<dataplane/rdma/ProtoConvert.hpp>

## 目的

gRPC `coincidence.RdmaEndpoint` / `DataPlaneKind` 与 [RdmaTypes](RdmaTypes.md) 之间的字段拷贝。不含 singles payload，不参与热路径。

命名空间：`openpni::distributed::dataplane::rdma`。

## 核心接口

```cpp
coincidence::DataPlaneKind toProto(DataPlaneKind kind);
DataPlaneKind fromProto(coincidence::DataPlaneKind kind);
void fillProtoEndpoint(const RdmaEndpointInfo &src, coincidence::RdmaEndpoint *dst);
RdmaEndpointInfo fromProtoEndpoint(const coincidence::RdmaEndpoint &src);
```

- `toProto` / `fromProto`：RoCE ↔ `DATA_PLANE_RDMA_ROCE_V2`，InProcess ↔ `DATA_PLANE_INPROCESS`，其余为 Unspecified。
- `fillProtoEndpoint`：`dst==nullptr` 则直接返回。GID 按 16 字节写入 proto bytes。
- `fromProtoEndpoint`：`gid` 长度不足 16 则保持默认零；`port_num==0` 时本地视为 1。

映射字段包括 slot 基址、notify、`consumerSeq`、credit 镜像、`inprocess_handle`。与 `RdmaEndpointInfo` 一一对应。

## 使用提示

- 仅 OpenDataPlane 请求/响应使用。改端点字段时同时改 proto、本头文件与握手测试。
- `inprocess_handle` 出现在 proto 里只为同进程测试；跨机 RoCE 路径应忽略该 token。
