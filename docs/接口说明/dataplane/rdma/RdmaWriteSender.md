# RdmaWriteSender

> #include<dataplane/rdma/RdmaWriteSender.hpp>

## 目的

节点侧发送器：把连续 packed singles 写入 coin 接收环。RoCE 用 `WRITE_WITH_IMM`；否则同进程 memcpy。由 [CoincidenceClient](../../grpcService/CoincidenceClient.md) 在 OpenDataPlane 之后 `connect`。

命名空间：`openpni::distributed::dataplane::rdma`。

## 核心接口

### Config / TxSlotLease

```cpp
struct Config {
  uint32_t nodeId = 0;
  std::string deviceName;
  size_t stagingSlotBytes = kDefaultSlotBytes;
  bool preferHugePages = true;
  bool forceInProcess = false;
  bool requireRoce = false;
  int gidIndex = -1;
  uint32_t txSlotCount = 8;
};

struct TxSlotLease {
  uint32_t localIndex;
  SlotHeader *header;
  uint8_t *payload;
  size_t payloadCapacity;
};
```

- `txSlotCount`：本地已注册 TX 暂存槽（默认 8；与远端 ring 槽数不是同一个数）。
- `requireRoce` 且对端/本地不是 RoCE 则 `connect` 失败。

### 握手与发送

```cpp
bool prepareLocalEndpoint(RdmaEndpointInfo *outLocal);
bool connect(const RdmaEndpointInfo &coinEndpoint);
bool sendPackedSingles(uint64_t chunkId, uint64_t computerClockMs, uint32_t durationMs,
                       const void *singlesPacked, uint32_t singlesCount);
bool acquireTxSlot(TxSlotLease *out);
bool commitTxSlot(const TxSlotLease &lease, const SlotHeader &hdr, uint32_t singlesCount);
void abortTxSlot(const TxSlotLease &lease);
```

- `prepareLocalEndpoint`：建本地 QP（INIT）及 credit 镜像 MR，填 worker 端点给 OpenDataPlane。
- `connect`：用 coin 端点完成 RC，或按 `inprocessHandle` 找到 [RdmaNodeRecvSession](RdmaRecvServer.md)。
- `sendPackedSingles`：按远端 `slotStride` 切槽，填 [SlotHeader](SlotProtocol.md) 与 payload，等待 credit 后写出。RoCE 拷进已注册 TX 槽；InProcess 从源缓冲直接 memcpy 进接收环。
- `acquireTxSlot` / `commitTxSlot`：CoincidenceClient 在 RoCE 上于生产线程填槽，sender 再提交。`abortTxSlot` 在入队失败时释放 lease。等待 TX/credit 时不长时间持有发送锁，以便填槽与 WRITE 重叠。

### 反压

`commitTxSlot` / `sendPackedSingles` 在需要的 `producerSeq` 超过 `consumerSeq + slotCount` 时阻塞等待 coin 推进 credit。

观测：

```cpp
uint64_t slotsInFlight() const;
uint32_t creditRemaining() const;
DataPlaneKind kind() const;
```

与调试日志 `pend` / `rdma` 的对应关系见 [状态机与调试.md](../../../app以及实验配置/状态机与调试.md)，本页不教调参。

## 典型流程

1. `prepareLocalEndpoint` → gRPC OpenDataPlane
2. `connect(coinEndpoint)`
3. 循环 `sendPackedSingles`
4. `close()`

## 使用提示

- 热路径应由 CoincidenceClient 调用；直接用本类时必须先交换与 ring 一致的 `slotCount`/`slotStride`/rkey。
- `singlesPacked` 必须是 16 字节 stride 的 `Single` 数组，见 [PackedSingle](../../core/streaming/PackedSingle.md)。
