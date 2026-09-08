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
- `txSlotCount`：本地 TX 暂存槽数，交给 `RdmaWriteSender::Config`（0 用 sender 默认 **2**）。device D2H 管线深等于该槽数；上机可 A/B `2` vs `4`，若 `waitForCredit` 变长则改回 2。不要把默认改成几十槽。
- `requestedSlotCount` / `requestedSlotBytes`：OpenDataPlane 向 coin 请求的接收环尺寸（服务端可忽略）。
- `maxPendingChunks`：JSON 兼容字段；热路径不再作为发送队列深度。
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

- **RoCE**：调用线程（必须是有序 `next()` 的消费线程）按槽 `acquireTxSlot`。50100 热路径源为 **device**，按 GPU 缓存 `cudaStreamNonBlocking`（切卡不销毁其它卡的 stream）`cudaMemcpyAsync` + event（管线深 = `txSlotCount`，默认 2）直写已 `cudaHostRegister` 的 TX payload，再 remap 并当场 `commitTxSlot`（等 credit + WRITE）。源为 host 时 memcpy。一块过大拆多槽，同一 `chunkId`。PAUSE 挡在填槽前。`OpenDataPlane` 成功后对 TX mmap `cudaHostRegister`；**RoCE 下注册失败则握手失败**，不进入 WaitForStart。InProcess 不要求 register。`stop()` 先销毁各 GPU 的 D2H stream/event，再 `cudaHostUnregister`，再 `close()`。
- **InProcess**：调用线程里 `sendPackedSingles`（一次 memcpy 进接收环）。device span 先 D2H；`sendSinglesView` 对 host 直接发，调用期间 span 必须有效。

### sendSinglesView

```cpp
bool sendSinglesView(std::span<const Single> singles,
                     uint64_t computerClock_ms,
                     uint32_t duration_ms);
```

- InProcess：调用期间从 host span `sendPackedSingles`，不拷 payload。**device span 不能借**，退回 `sendSingles` 做 D2H。
- RoCE：与 `sendSingles(span)` 相同，async D2H（device）或 memcpy（host）进 TX 并 commit 后即可释放调用方缓冲。
- 开启 channel remap 时退回拷贝路径（不能改调用方缓冲）。

`computerClock_ms` / `duration_ms` 写入槽头，符合对齐仍以 PET `timevalue_100fs` 为准。

### 数据面握手（start 内部）

1. `RegisterNode`
2. `OpenDataPlane`：`prepareLocalEndpoint` 得到本端 `RdmaEndpointInfo`，经 [ProtoConvert](../dataplane/rdma/ProtoConvert.md) 放入请求；响应里的 coin 端点交给 `RdmaWriteSender::connect`
3. `WaitForStart`（可关）
4. 启动 heartbeat 线程。`sendSingles` 在调用线程上填槽并提交。

`dataPlaneKind()` 返回实际连接类型（RoCE 或 InProcess）。

### 发送侧观测

```cpp
uint64_t getTotalSinglesSent() const;
size_t getPendingMessageCount() const;
uint32_t rdmaSlotCount() const;
uint32_t txStagingSlotCount() const;
uint64_t rdmaSlotsInFlight() const;
uint32_t rdmaCreditRemaining() const;
uint64_t txD2hStreamCreateCount() const;
bool waitUntilIdle();
bool notifyProducerComplete();
```

- `getPendingMessageCount()`：本地 TX busy 槽数。
- `txStagingSlotCount()`：本地 TX 槽数（默认 2）。
- `rdmaSlotsInFlight` / `rdmaCreditRemaining` 来自 sender 对远端 `consumerSeq` 的镜像，对应调试日志里的 `rdma` / credit。
- `txD2hStreamCreateCount()`：每 GPU 首次建 copy stream 的次数；多卡交替不应每段递增。
- `waitUntilIdle`：等到当前 `sendSingles` 返回（无在飞提交）。
- `notifyProducerComplete`：先 `waitUntilIdle`，再发 gRPC 完成通知。

## 典型流程

1. 构造 `CoincidenceClient`，`start()` 完成握手。
2. R2S 回调里 `sendSingles`。
3. 生产结束 `notifyProducerComplete()`，再 `stop()`。

## 使用提示

- `span` 仅在回调返回前有效：RoCE 下 `sendSingles(span)` 在返回前已 async D2H（device）或 memcpy（host）并提交 WRITE；InProcess 对 host 当场 memcpy 进环，对 device 先 D2H。不要假设 span 在回调返回后仍指向 GPU/租约缓冲。
- 不要绕过本类直接对未 connect 的 `RdmaWriteSender` 发包；槽尺寸与 rkey 来自 OpenDataPlane。
- 不要在 GPU worker 里 `acquireTxSlot`：多卡完成序 ≠ 段序，会打乱 QP `seq` 与符合水位线。
- Heartbeat 上的 PAUSE/STOP 会挡住或结束生产，细节见状态机文档。
