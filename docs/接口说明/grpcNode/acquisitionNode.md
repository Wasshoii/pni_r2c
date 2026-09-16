# AcquisitionGrpcNode

> #include<grpcNode/acquisitionNode.hpp>

## 目的

采集+R2S 进程的控制面封装：连上 `AcquisitionMaster`，收 `CONFIGURE/START/STOP/SHUTDOWN`，创建 `DistributedAcquisitionNode`。数据面仍走进程内 `RawDataView` 回调，不经 gRPC。

命名空间：`openpni::distributed::grpcnode`。

## 核心接口

```cpp
class AcquisitionGrpcNode {
  void setRawDataReadyCallback(RawDataReadyCallback callback);
  void setDeferRawDataRelease(bool defer);
  std::function<void(uint64_t)> makeRawDataReleaseFn();
  bool run();
  void stop();
};
```

`app_acq_r2s_node` 在 bridge 开启时：`setDeferRawDataRelease(true)`，`setReleaseFn(node.makeRawDataReleaseFn())`，`setRawDataReadyCallback(bridge.makeRawDataCallback())`。

## 运行时选择

| 条件 | 实现 |
|------|------|
| `ALGORITHM_TYPE_SOCKET` 或未开 DPDK | `SocketAcquisition` |
| `ALGORITHM_TYPE_DPDK` 且 `PNI_STANDARD_CONFIG_ENABLE_DPDK=1` | `InitDPDKNew` + `DPDKAcquisitionNew` |
| 请求 DPDK 但 libpni 未开 DPDK | 配置阶段报错 |

`InitDPDKNew` 使用 `DpdkOptions.bind_ips`、`rx_rings_per_port`、`mbuf_pool_size`、`mbuf_cache_size`、`local_loopback_iface`、`extra_eal_args`。`extra_eal_args` 用于绑核 / hugepage / `--file-prefix`，避免和 R2S、RDMA 抢 lcore。

旧 `DPDKAcquisition` / `InitDPDK` 不再调用。部署与 hugepage 见 [DPDK采集配置与使用](../../app以及实验配置/DPDK采集配置与使用.md)。通路见 [采集到R2S](../通路/采集到R2S.md)。
