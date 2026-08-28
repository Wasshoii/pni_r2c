# 接口说明

按 `include/` 层级说明各模块职责，体例对齐 libpni [接口说明](../../../pni-standard-project/docs/接口说明)。一篇对应一个头文件；跨模块关系只放在「通路」页。

部署、握手、实验剖面见 [app以及实验配置](../app以及实验配置/App%20与实验配置文档索引.md)，不在本目录重复。

`openpni::Single` 的字段与单位以 libpni [CommonDataType](../../../pni-standard-project/docs/接口说明/core/CommonDataType.md) 为准。本仓库只强调其 **16 字节 pack(1)** 布局，作为 R2S 输出与 RDMA 槽 payload 的契约。

## 本批：R2S → RDMA

先读 [通路/R2S到RDMA.md](通路/R2S到RDMA.md)，再按依赖下钻。

### 通路

- [通路/R2S到RDMA.md](通路/R2S到RDMA.md)

### core/r2s

- [core/r2s/R2S.md](core/r2s/R2S.md)
- [core/r2s/multi_gpu/R2S50100MultiGpuEngine.md](core/r2s/multi_gpu/R2S50100MultiGpuEngine.md)
- [core/r2s/multi_gpu/SPSCProcessor.md](core/r2s/multi_gpu/SPSCProcessor.md)
- [core/r2s/multi_gpu/R2S50100Compute.md](core/r2s/multi_gpu/R2S50100Compute.md)
- [core/r2s/multi_gpu/DPacketsAsync.md](core/r2s/multi_gpu/DPacketsAsync.md)
- [core/r2s/multi_gpu/PinnedHostCopy.md](core/r2s/multi_gpu/PinnedHostCopy.md)

### 衔接

- [core/streaming/PackedSingle.md](core/streaming/PackedSingle.md)
- [grpcService/CoincidenceClient.md](grpcService/CoincidenceClient.md)

### dataplane/rdma

- [dataplane/rdma/README.md](dataplane/rdma/README.md) — 总览：RoCE 原理、槽环、热路径拷贝（档 1：D2H 直写 TX）
- [dataplane/rdma/GPUDirectRDMA.md](dataplane/rdma/GPUDirectRDMA.md) — 档 2 设计（未实现）
- [dataplane/rdma/RdmaTypes.md](dataplane/rdma/RdmaTypes.md)
- [dataplane/rdma/SlotProtocol.md](dataplane/rdma/SlotProtocol.md)
- [dataplane/rdma/HugepageArena.md](dataplane/rdma/HugepageArena.md)
- [dataplane/rdma/SlotRing.md](dataplane/rdma/SlotRing.md)
- [dataplane/rdma/RdmaContext.md](dataplane/rdma/RdmaContext.md)
- [dataplane/rdma/RdmaWriteSender.md](dataplane/rdma/RdmaWriteSender.md)
- [dataplane/rdma/RdmaRecvServer.md](dataplane/rdma/RdmaRecvServer.md)
- [dataplane/rdma/ProtoConvert.md](dataplane/rdma/ProtoConvert.md)

## 后续（本批不写）

- `include/core/acquisition/**`：采集与 DPDK
- `include/core/streaming/StreamingCoincidence.hpp` 及符合 GPU
- `include/grpcNode/**`：进程封装
- timesync
