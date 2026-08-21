# SlotProtocol

> #include<dataplane/rdma/SlotProtocol.hpp>

## 目的

RoCE / InProcess 共用的槽头与通知条目固定布局。payload 紧跟 64 字节头，为连续 16 字节 `openpni::Single`（与 [PackedSingle](../../core/streaming/PackedSingle.md) 一致）。

命名空间：`openpni::distributed::dataplane::rdma`。

## 核心接口

### 常量

```cpp
inline constexpr uint32_t kSlotMagic = 0x52324441u; // 'R2DA'
inline constexpr uint16_t kSlotVersion = 1;
inline constexpr size_t kSlotHeaderBytes = 64;
inline constexpr size_t kPackedSingleBytes = 16;
inline constexpr uint32_t kSlotFlagSof = 1u << 0;
inline constexpr uint32_t kSlotFlagEof = 1u << 1;
inline constexpr uint32_t kSlotFlagPartial = 1u << 2;
inline constexpr size_t kDefaultSlotBytes = 4u * 1024u * 1024u; // 4 MiB
inline constexpr uint32_t kDefaultSlotCount = 64;
```

- 一块逻辑 chunk 超过单槽容量时拆槽：首槽 SOF，中间 PARTIAL，末槽 EOF；`chunkId` 相同。
- 默认 4 MiB 槽去掉 64B 头后约 262143 条 singles。

### SlotHeader

`#pragma pack(1)`，`sizeof == 64`。

```cpp
struct SlotHeader {
  uint32_t magic;
  uint16_t version;
  uint16_t flags;
  uint32_t nodeId;
  uint32_t singlesCount;
  uint64_t chunkId;
  uint64_t computerClockMs;
  uint32_t durationMs;
  uint32_t seq;      // 生产者 1-based 序号
  uint32_t crc32;    // 0 = 未使用
  uint8_t reserved[20];
};
```

- `seq` 与 Immediate Data 的 32-bit wrap 对应（`seqToImm`）。
- `slotIndexFromSeq(seq, slotCount)`：`(seq - 1) % slotCount`。

### NotifyEntry

```cpp
struct NotifyEntry {
  uint64_t seq;        // 0 = 空；非 0 = 该槽就绪
  uint32_t slotIndex;
  uint32_t singlesCount;
  uint64_t chunkId;
};
```

接收侧 poll notify 环得知哪一槽可 ingest，无需先扫全部 slot 头。

### maxSinglesPerSlot / clearSlotHeader

- `maxSinglesPerSlot(slotStrideBytes)`：`(stride - 64) / 16`。
- `clearSlotHeader`：置零后写回 magic/version。

## 使用提示

- 改头大小或 Single 宽度必须同步 sender、recv、proto 与测试。
- 符合计算仍用 PET `timevalue_100fs`；`computerClockMs` 只是墙钟附带信息。
