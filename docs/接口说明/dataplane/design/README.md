# 数据面通路：现状汇报（桌面审查）

本目录是给指导师兄的**实现现状**说明，不是头文件字段手册。审查范围是当前仓库代码，**未上机、未跑测试**。

完整正文：[数据面通路现状与评价.md](数据面通路现状与评价.md)。下周实现清单：[优化方案.md](优化方案.md)（不含 GPUDirect）。

字段级 API 仍看同级 [../rdma/README.md](../rdma/README.md)。通路边界看 [../../通路/R2S到RDMA.md](../../通路/R2S到RDMA.md)。GPUDirect（未实现，本期不做）看 [../rdma/GPUDirectRDMA.md](../rdma/GPUDirectRDMA.md)。

## 三句话结论

1. 热路径不是 GPU 直发网卡。DPDK 把 raw 放在 **host**；多卡 R2S 做 **H2D + kernel**，singles 留在 `d_singles`；**有序消费线程按 `next()` 的 submit 序**再 D2H 进本地 2×4 MiB TX 槽，用 RoCE `WRITE_WITH_IMM` 打到 coin 的 64×4 MiB 接收环。多卡乱的是算完时刻，不是发送序。
2. 「TX 里有数据了」不会走 gRPC。就绪信号是数据面的 immediate；反压是 coin 把 8 字节 `consumerSeq` WRITE 回 worker 的 credit 镜像。
3. 按 30 **Gibit**/s（≈ 3.75 GiB/s）估，单 worker 一条 100 GbE QP **带宽上够**。**加大 TX/RX 几乎不加线速**，只加排队和更晚的反压；多卡只并行计算，发送仍是一条线程 + 一条 QP + 两槽 ping-pong。更容易顶满的是 coin ingest / 时间对齐器反压。

## 和 `rdma/` 的分工

| 目录 | 写什么 |
|------|--------|
| [数据面通路现状与评价.md](数据面通路现状与评价.md) | 端到端怎么走、多卡如何有序发出、缓冲加大有没有用、两端怎么同步 |
| [优化方案.md](优化方案.md) | 下周可改的实现清单（不含 GPUDirect / 接收零拷贝） |
| `dataplane/rdma/` | 各头文件字段与调用约定 |

`rdma/` 里个别表述可能滞后；以本目录对照代码的结论为准。本次**没有改**那些页。

## 审查范围

- [`include/dataplane/rdma`](../../../../include/dataplane/rdma)、[`src/dataplane/rdma`](../../../../src/dataplane/rdma)
- [`CoincidenceClient.cpp`](../../../../src/grpcService/CoincidenceClient.cpp) 的 `fillRoceTxAndCommit`
- [`CoincidenceServiceImpl.cpp`](../../../../src/grpcService/CoincidenceServiceImpl.cpp) 的 `ingestRdmaSlot`
- [`R2S.cpp`](../../../../src/core/r2s/R2S.cpp) 的 `consumerLoop` / `completeOldestMultiGpuSegment`

生产接线是 `app_acq_r2s_node`。`r2sNode.cpp` 的 `PersistentNodeStreamSender`（先 host 物化再发送）**不是**热路径。
