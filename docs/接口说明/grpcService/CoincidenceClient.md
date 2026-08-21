# CoincidenceClient

> #include<grpcService/CoincidenceClient.hpp>

## 目的

worker 侧符合客户端：gRPC 完成 Register / OpenDataPlane / WaitForStart / Heartbeat，热路径把 singles 交给 [RdmaWriteSender](../dataplane/rdma/RdmaWriteSender.md)。R2S 通过 `onSinglesSpanReady` 调用 `sendSingles(span)`，见 [R2S到RDMA](../通路/R2S到RDMA.md)。

本页只写发送与数据面。编排状态机、PAUSE/RESUME、日志字段见 [状态机与调试.md](../../app以及实验配置/状态机与调试.md)。

命名空间：`openpni::distributed::streaming`。

## 核心接口

### CoincidenceClientConfig（数据面相关）

```cpp
struct CoincidenceClientConfig {
  std::string serverAddress;
  uint32_t nodeId;
  bool requireRoce = false;
  bool forceInProcess = false;
  std::string rdmaDeviceName;
  int gidIndex = -1;
  uint32_t txSlotCount = 0;
  uint32_t requestedSlotCount = 0;
  uint32_t requestedSlotBytes = 0;
  size_t maxPendingChunks = 100;
  size_t batchSize = 1000;
  bool remapLocalToGlobalChannels = false;
  uint32_t globalChannelOffset = 0;
  bool waitForStartSignal = true;
  // ... 重连、心跳间隔等控制面字段
};
```

- `requireRoce`：对端必须是 RoCE，不允许静默落到 InProcess。
- `forceInProcess`：同进程 memcpy（CI / 单机编排测试）。
- `txSlotCount`：本地 TX 暂存槽数，交给 `RdmaWriteSender::Config`（0 用 sender 默认 **8**）。
- `requestedSlotCount` / `requestedSlotBytes`：OpenDataPlane 向 coin 请求的接收环尺寸。
- `maxPendingChunks`：入队上限；RoCE 热路径 payload 已在 TX 槽里，实际深度还受 `txSlotCount` 限制。
- `remapLocalToGlobalChannels`：发送前把 `channelIndex` 加上 `globalChannelOffset`。

### sendSingles

```cpp
bool sendSingles(const std::vector<Single> &singles,
                 uint64_t computerClock_ms,
                 uint32_t duration_ms);
bool sendSingles(std::span<const Single> singles,
                 uint64_t computerClock_ms,
                 uint32_t duration_ms);
bool sendSingles(std::vector<Single> &&singles,
                 uint64_t computerClock_ms,
                 uint32_t duration_ms);
```

- **RoCE**：调用线程按槽 `acquireTxSlot` + memcpy 进 TX MR，入队已填 lease；`senderLoop` 只 `waitForCredit` + `commitTxSlot`（WRITE）。一块过大拆多槽，同一 `chunkId`。PAUSE 仍挡在入队侧。
- **InProcess**：不填 TX；`span` 拷进 pending（或 `sendSinglesView` 借指针），sender 直写接收环。
- 队列达到 `maxPendingChunks` 时阻塞，直到发送推进或 `stop` / `STOP_PRODUCE`。

### sendSinglesView

```cpp
bool sendSinglesView(std::span<const Single> singles,
                     uint64_t computerClock_ms,
                     uint32_t duration_ms);
```

- InProcess：只入队指针，不拷 payload。调用方保证 span 在该 chunk 真正发出前一直有效（preload 缓冲：直到 `waitUntilIdle` / `notifyProducerComplete`）。
- RoCE：与 `sendSingles(span)` 相同，拷进 TX 槽后即可释放调用方缓冲。
- 开启 channel remap 时退回拷贝路径（不能改调用方缓冲）。

`computerClock_ms` / `duration_ms` 写入槽头，符合对齐仍以 PET `timevalue_100fs` 为准。

### 数据面握手（start 内部）

1. `RegisterNode`
2. `OpenDataPlane`：`prepareLocalEndpoint` 得到本端 `RdmaEndpointInfo`，经 [ProtoConvert](../dataplane/rdma/ProtoConvert.md) 放入请求；响应里的 coin 端点交给 `RdmaWriteSender::connect`
3. `WaitForStart`（可关）
4. 启动 sender 线程与 heartbeat 线程

`dataPlaneKind()` 返回实际连接类型（RoCE 或 InProcess）。

### 发送侧观测

```cpp
uint64_t getTotalSinglesSent() const;
size_t getPendingMessageCount() const;
uint32_t rdmaSlotCount() const;
uint64_t rdmaSlotsInFlight() const;
uint32_t rdmaCreditRemaining() const;
bool waitUntilIdle();
bool notifyProducerComplete();
```

- `rdmaSlotsInFlight` / `rdmaCreditRemaining` 来自 sender 对远端 `consumerSeq` 的镜像，对应调试日志里的 `rdma` / credit。
- `waitUntilIdle`：等到 pending 队列空且当前 slot 写完。
- `notifyProducerComplete`：先 `waitUntilIdle`，再发 gRPC 完成通知。

## 典型流程

1. 构造 `CoincidenceClient`，`start()` 完成握手。
2. R2S 回调里 `sendSingles`。
3. 生产结束 `notifyProducerComplete()`，再 `stop()`。

## 使用提示

- `span` 仅在回调返回前有效：RoCE 下 `sendSingles(span)` 在返回前已拷进 TX 槽；InProcess 拷进 pending。不要假设 span 在回调返回后仍指向 GPU/租约缓冲。
- 不要绕过本类直接对未 connect 的 `RdmaWriteSender` 发包；槽尺寸与 rkey 来自 OpenDataPlane。
- Heartbeat 上的 PAUSE/STOP 会挡住或结束生产，细节见状态机文档。
