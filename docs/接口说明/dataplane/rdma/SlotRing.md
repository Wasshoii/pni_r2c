# SlotRing

> #include<dataplane/rdma/SlotRing.hpp>

## 目的

coin 接收侧（及 sender 理解的远端布局）连续缓冲：槽数组、notify 环、credit 字。

布局：

```
[ slot0 | slot1 | ... | slotN-1 | NotifyEntry[N] | uint64_t consumerSeq ]
```

每槽步长 `slotStride`（默认 4 MiB，含 64B [SlotHeader](SlotProtocol.md)）。`consumerSeq` 是消费者已完成的生产者序号；sender 用它算 credit。

命名空间：`openpni::distributed::dataplane::rdma`。

## 核心接口

```cpp
struct SlotRingConfig {
  uint32_t slotCount = kDefaultSlotCount;  // 64
  size_t slotBytes = kDefaultSlotBytes;    // 含头
  bool preferHugePages = true;
};

class SlotRing {
  bool init(const SlotRingConfig &cfg);
  void reset();
  SlotHeader *slotHeader(uint32_t index);
  uint8_t *slotPayload(uint32_t index);
  NotifyEntry *notifyBase();
  std::atomic<uint64_t> *consumerSeq();
  size_t maxSingles() const;
};
```

- `init` 分配 [HugepageArena](HugepageArena.md)，切出 notify 与 `consumerSeq` 指针。
- `slotPayload(i)` 指向头之后的 packed singles。
- `baseAddr` / `notifyAddr` / `consumerSeqAddr` 填入 [RdmaEndpointInfo](RdmaTypes.md) 供握手。

## 使用提示

- 下标必须 `< slotCount`。生产者 `seq` 映射槽号见 `slotIndexFromSeq`。
- `consumerSeq` 由 recv ingest 成功后推进；sender `waitForCredit` 读远端或本地 credit 镜像。
- 整环注册为一次 MR；notify 与 slots 共用 rkey 基址不同 offset（端点里分开填 addr）。
