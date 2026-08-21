# RdmaRecvServer

> #include<dataplane/rdma/RdmaRecvServer.hpp>

## 目的

coin 侧接收：每节点一个 [SlotRing](SlotRing.md) 会话，poll notify 后调用 ingest。支持 RoCE RC 与 InProcess memcpy。

命名空间：`openpni::distributed::dataplane::rdma`。

## 核心接口

### Ingest 回调与模式

```cpp
using IngestSlotFn = std::function<bool(const SlotChunkView &view)>;
using CreditOnlyFn = std::function<void(uint32_t nodeId, uint32_t singlesCount)>;

enum class IngestMode : uint8_t {
  Full = 0,        // 生产默认：零拷贝 SlotChunkView
  CreditOnly = 1,  // 只推进 credit，可选计数；测试/压测
};
```

- `Full`：每槽一次回调；`view.singlesPacked` 指向环内内存，返回前有效。多槽逻辑块共享 `chunkId`，SOF/EOF 在 `view.flags`。
- 回调返回 `false` 表示 ingest 失败（例如符合 ring 满）；实现会记日志且该槽 credit 行为见源码（失败路径不应假设已释放给 sender）。

### RdmaNodeRecvSession

```cpp
struct Config {
  uint32_t nodeId;
  uint32_t slotCount = kDefaultSlotCount;
  size_t slotBytes = kDefaultSlotBytes;
  bool preferHugePages = true;
  std::string deviceName;
  bool forceInProcess = false;
  bool requireRoce = false;
  int gidIndex = -1;
  int recvWr = 128;
};

bool prepare();
RdmaEndpointInfo localEndpoint() const;
bool acceptRemote(const RdmaEndpointInfo &remote);
int pollOnce(int maxSlots = 8);
```

- `prepare`：分配环并注册 MR，或生成 `inprocessHandle`。
- `localEndpoint()` 经 gRPC 发给 worker。
- `acceptRemote`：对端 QP 进入 RTR/RTS，或 InProcess 绑定 sender。
- `pollOnce`：处理最多 `maxSlots` 个就绪槽，返回实际 ingest 数。

### RdmaRecvServer

拥有 `nodeId → session` 映射，可选后台 poll 线程。

```cpp
void setIngest(IngestSlotFn fn);
std::shared_ptr<RdmaNodeRecvSession> ensureSession(uint32_t nodeId);
void start();
void stop();
static std::shared_ptr<RdmaNodeRecvSession> findInProcessSession(uint64_t handle);
```

- `ensureSession`：OpenDataPlane 时按 node 创建会话。
- `startPoller==true`（默认）时 `start()` 起 `pollLoop`。
- `findInProcessSession`：同进程 sender `connect` 用全局弱引用表查找 handle。

## 使用提示

- 生产路径 `IngestMode::Full`，把 view 交给符合引擎；不要在回调返回后使用 `singlesPacked`。
- `CreditOnly` 不把 payload 送入符合，只用于数据面通路压测。
- 会话数等于已 OpenDataPlane 的 worker 数；与 coin `expectedNodeCount` 对齐见编排文档。
