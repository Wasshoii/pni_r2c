# RdmaTypes

> #include<dataplane/rdma/RdmaTypes.hpp>

## 目的

数据面公共类型：传输种类、握手用端点、ingest 回调看到的槽视图。不包含 verbs 句柄。

命名空间：`openpni::distributed::dataplane::rdma`。

## 核心接口

### DataPlaneKind

```cpp
enum class DataPlaneKind : uint32_t {
  Unspecified = 0,
  RdmaRoceV2 = 1,
  InProcess = 2,  // 同进程 memcpy，无 IB 设备时（CI / localhost）
};
```

### RdmaEndpointInfo

OpenDataPlane 交换的端点。RoCE 字段用于 RC QP；`inprocessHandle` 仅本进程有效。

| 字段 | 含义 |
|------|------|
| `kind` | RoCE 或 InProcess |
| `gid` / `lid` / `qpNum` / `psn` | 对端 QP |
| `rkey` / `baseAddr` | 接收环 slots 的 MR |
| `slotCount` / `slotStride` | 槽个数与每槽字节（含 64B 头） |
| `notifyRkey` / `notifyAddr` / `notifyCapacity` | 尾部 notify 环 |
| `consumerRkey` / `consumerAddr` | coin 侧 `consumerSeq` |
| `creditMirrorRkey` / `creditMirrorAddr` | worker 侧 credit 镜像；coin WRITE 推进 |
| `deviceName` / `portNum` / `gidIndex` | 本地网卡选择 |
| `inprocessHandle` | InProcess 会话 token，跨进程无效 |

gRPC 映射见 [ProtoConvert](ProtoConvert.md)。环布局见 [SlotRing](SlotRing.md)。

### SlotChunkView

```cpp
struct SlotChunkView {
  uint32_t nodeId;
  uint64_t chunkId;
  uint64_t computerClockMs;
  uint32_t durationMs;
  uint16_t flags;
  uint32_t singlesCount;
  const void *singlesPacked;  // 16 字节 packed Single 数组
};
```

- `singlesPacked` 指向接收环内 payload，**仅在 `IngestSlotFn` 返回前有效**。
- `flags` 为 [SlotProtocol](SlotProtocol.md) 的 SOF/EOF/PARTIAL。同一 `chunkId` 可对应连续多槽。

## 使用提示

- 不要把 `inprocessHandle` 或 raw 指针地址发到另一台机器当 RoCE 地址用。
- ingest 回调内若要异步处理 singles，必须在返回前拷贝。
