# AcquisitionServer

> #include<core/acquisition/AcquisitionServer.hpp>

## 目的

把控制面 `AcquisitionTask` 转成 libpni `AcquisitionInfo`，并用 `DistributedAcquisitionNode<Algo>` 驱动一次采集循环：`Read()` → 可选写 raw 文件 → `RawDataReadyCallback`。热路径算法是 `SocketAcquisition` 或 `DPDKAcquisitionNew`（旧 `DPDKAcquisition` 不再接入）。

命名空间：`openpni::distributed::acquisition`。

## 核心接口

### NodeAcquisitionConfig / MakeNodeAcquisitionConfig

```cpp
struct NodeAcquisitionConfig {
  enum class RuntimeType { Socket, Dpdk };
  struct DpdkConfig {
    uint32_t rx_rings_per_port = 1;
    std::vector<std::string> bind_ips;
    uint32_t mbuf_pool_size = 0;
    uint32_t mbuf_cache_size = 0;
    std::string local_loopback_iface;
    std::vector<std::string> extra_eal_args;
  };
  RuntimeType runtime_type = RuntimeType::Socket;
  uint32_t min_packet_size;
  uint32_t max_packet_size; // storageUnitSize
  uint64_t max_buffer_size;
  uint32_t time_switch_buffer_ms;
  DpdkConfig dpdk;
  std::vector<Channel> channels;
};

NodeAcquisitionConfig MakeNodeAcquisitionConfig(const AcquisitionTask &task);
openpni::AcquisitionInfo MakeAcquisitionInfo(const NodeAcquisitionConfig &config);
```

- `ALGORITHM_TYPE_DPDK` → `RuntimeType::Dpdk`；其余（含未指定）→ Socket。
- `MakeAcquisitionInfo` 始终设置 `hostMemoryType = CUDAHost`，供后续 H2D 走 pinned DMA，见 [采集到R2S](../../通路/采集到R2S.md)。
- `local_loopback_iface` 非空时节点侧 `InitDPDKNew` 打开 loopback vdev。
- proto 里旧字段 `copy_thread_num`、双指针乘数会被忽略。

### DistributedAcquisitionNode

```cpp
template <typename AlgoType = openpni::SocketAcquisition>
class DistributedAcquisitionNode {
  void SetRawDataReadyCallback(RawDataReadyCallback cb);
  void SetDeferRawDataRelease(bool defer);
  std::function<void(uint64_t)> MakeRawDataReleaseFn();
  bool Start();
  void Stop();
};
```

- `SetDeferRawDataRelease(true)`：写文件（若启用）先于 callback；callback 成功则由调用方 `Release(count)`，失败则本节点立即归还。
- `MakeRawDataReleaseFn()` 绑定到当前 `AlgoType::Release`，供 `AsyncRawDataToR2SBridge` 在 R2S 完成后按 **Read 顺序** 归还。
- DPDKNew 的 `Read()` 在无包时会阻塞到下一时间片或 `Stop()`；空 `optional` 上的 sleep 主要给 Socket。

## 使用提示

- 回调内不可持久化 `RawDataView` 指针；异步处理必须租约或拷贝。
- 多队列 DPDKNew 的 `Release(n)` 是 FIFO。必须与 `completeOldestMultiGpuSegment` 一样按提交顺序归还，不能乱序 complete。
