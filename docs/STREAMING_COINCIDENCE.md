# 流式符合计算系统文档

## 概述

本系统实现了分布式 PET（正电子发射断层成像）数据处理的流式符合计算功能。核心设计思想是利用 PET 时钟板的精确时间戳（ps级）作为全局时间基准，通过滑动窗口机制实现高效的时间对齐和符合计算。

## 架构设计

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        流式时间对齐架构                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Node1 ──> [RingBuffer1] ──> stealThread1 ──> stolenDeque1 ─┐               │
│  Node2 ──> [RingBuffer2] ──> stealThread2 ──> stolenDeque2 ─┼─> coord      │
│  ...                                                        │   merge      │
│  NodeN ──> [RingBufferN] ──> stealThreadN ──> stolenDequeN ─┘   submit     │
│                                                                      │      │
│                                                              [Coincidence]  │
│                                                                      ▼      │
│                                                                    LMF      │
│                                                                             │
│  关键：以 PET 时钟的 timeValue_pico 作为全局时间基准                          │
│        水位线只定时间上界；条数预算在协调线程归并前缀上切，不按节点均分         │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. TimestampedSingleChunk

带时间戳的单事件数据块，从分布式节点接收的数据单元。

```cpp
struct TimestampedSingleChunk {
    uint16_t nodeId;           // 来源节点ID
    uint64_t chunkId;          // 块序号
    uint64_t computerClock_ms; // 计算机时钟（粗略参考）
    uint32_t duration_ms;      // 持续时间
    std::vector<GlobalSingle_t> singles;  // 单事件数据
    size_t consumed;           // 未消费起点；半块只推进此下标，不 assign/erase

    uint64_t minTime_pico;     // 未消费区间最小时间（PET时钟）
    uint64_t maxTime_pico;     // 未消费区间最大时间（PET时钟）
};
```

### 2. NodeRingBuffer

线程安全的环形缓冲区，每个节点一个。

**特性：**
- 每节点按到达序追加（RDMA `seq` / R2S `next()` 已保序）；`minTime` 回退只计数打日志，不重排
- 支持单生产者多消费者模式
- 阻塞式 push/pop 操作
- 非阻塞式 tryPop 操作
- 可关闭以停止所有等待线程

### 3. StreamingTimeAligner

核心时间对齐器，负责收集各节点数据并进行精确时间对齐。

**工作流程：**
1. 计算全局安全时间边界 = min(所有节点已到达 `maxEventTime`) − 符合窗（及可选 `networkLatencyMargin`）
2. 从各节点缓冲区提取 [上次边界, 当前安全边界] 范围内的数据
3. 合并排序后进行符合计算
4. 更新时间边界，重复

#### 触发机制

处理循环是**事件驱动 + 多条件触发**，不是固定节拍轮询。`NodeRingBuffer::push` 在释放锁
之后回调唤醒处理线程，处理线程在 `m_wakeCv.wait_for(processingIntervalMs)` 上等待——
`processingIntervalMs` 的语义因此是「无 push 时的最长空转等待」，而不是处理周期。

醒来后按顺序判定：

1. 水位线必须前进（`watermark > lastWatermark`）。缓冲已到高水位却推不动水位线，说明某
   节点数据滞后，此时**只背压 + 限流告警**，绝不越过水位线抽取。
2. 段跨度必须达到硬下界 `minSegmentOverlapFactor × 重叠窗`，否则继续攒。
3. 满足以下任一条件即处理：待处理量 ≥ `minSegmentSingles`、任一节点/内存池占用 ≥
   `bufferHighWaterRatio`、距上次处理超过 `maxProcessLatencyMs`。

待处理量用各节点 **stolen deque + 环** 的 chunk 条数前缀和（跨水位线的半块按整块上界估计），不扫 singles。
水位线只定时间上界；真正抽出时一次扫描，时间 ≤ watermark 且新条数 ≤
`maxSegmentSingles - carry`。每节点一条常驻 steal 线程把环里的**整块** swap 进有界 stolen
deque（默认 8 个 chunk），**不按水位切前缀**。段边界、条数预算、半块 `upper_bound` 只在
协调线程上做。协调线程从 N 个 deque 按全局时间前缀切预算；半块只推进 chunk 的 `consumed`
游标，merge 只读 `[consumed, consumed+takeN)`，锁外 **memcpy concat** 写入 pinned 输入槽
（不在主机做全局 2-way merge；内核会再按时间排序）。
不要按节点各抽 `budget/N`：会破坏跨节点符合。
硬下界仍优先于预算（超预算记 `oversizedSegments`）。Steal 相对 merge 超前：merge/GPU 在处理
段 i 时，steal 已在把后续 chunk 搬进 deque。停机顺序：steal join → 协调线程抽空 deque 并
`flushRemaining` → `signalNoMoreData`（非 extract-only）。

一段被显存预算钳掉尾巴时不会回去等待，而是立即继续下一段，直到追平水位线。

#### 三条不变式

1. 压力触发与延迟兜底只改变「**何时**」处理，绝不改变「**处理到哪里**」。抽取边界恒
   `<= calculateWatermark()`，跨节点对齐语义与触发策略完全解耦。
2. 抽取边界恒 `>= lastWatermark + minSegmentOverlapFactor × 重叠窗`（停机 flush 除外）。
3. 送入内核的批 = carry + 新数据，**两者合计**受 `maxSegmentSingles` 约束。

#### 两个下界的分工

`minSegmentSingles` 是**软下界**，管内核效率：批太小则 kernel 启动开销占比过高，所以不足
时继续攒；但缓冲压力或延迟到期可以突破它。

`minSegmentOverlapFactor` 是**硬下界**（以重叠窗为单位，随 `coinProtocol` 自动伸缩），管
carry 占比与边界单调性：段跨度小于重叠窗时 carry 占比趋近 100%，同一批数据被反复重算，
且抽取边界可能不再单调。压力和超时都**不能**突破它，唯一例外是停机 `flushRemaining`。
取 4 时 carry 相对开销约 25%，调大可进一步摊薄。

`carrySinglesTotal / totalSinglesProcessed` 就是 carry 重算开销比，可用来校准这个因子。

#### 单批上限（重要）

底层 `openpni::Coincidence::getDListmode` 对单批 singles 数存在上限，越界表现为 CUDA 非法
访存（硬崩溃，不是降级）。9120 双节点数据实测在 5×10^5 与 10^6 之间触发。`maxSegmentSingles`
就是这个上限的护栏，默认 262144；**不建议设为 0（不限制）**，突发流量下水位线一次推进很远
就会踩到内核上限。初始化时若检测到 0 或估算显存不足会打 WARNING。

#### 多 GPU 流水线与写盘顺序

多 GPU 路径把处理循环拆成 steal / 协调 / GPU / 收回 / 写盘，不再在抽取线程上同步 `submit`+`next`+写盘：

1. **Steal 线程**（每节点 1 条）：环非空且 deque 未满时短锁整块 `stealWholeFronts` 进该节点
   有界 stolen deque。不看水位、不切半块。满则背压在 deque cap，环堆积后走上游高水位。
2. **协调 / 抽取线程**：水位判定（`watermarkNs`）、等到各节点到齐（deque 可见或环/在途确认
   无 `<= W` 数据）、从 stolen deque 按全局时间前缀切预算（完整块按 chunk `maxTime` 批量弹出，
   半块对 singles 二分切 `takeN` 后只推进 `consumed`，merge 读 `[consumed, consumed+takeN)`）、
   锁外归并进 pinned 槽（`extractNs` = 切点/归并/carry；其中归并另记 `mergeNs`，steal
   工人记 `stealNs`）、`updateCarrySingles`（仍用主机数据、在 `submit` 之前），然后把有主 pinned 槽交给 `submitSingles`。等输入槽记 `slotWaitNs`。
   槽活到对应的 `nextResult()`。`extractOnly` 仍走 steal→merge→carry→推进水位，但不
   `submitSingles`、不建 GPU 工人；默认归并写入 pageable `m_extractHost`。`--merge-pinned`
   改走 pinned `m_stageMerged`，用来测生产写出墙。热路径对主机全局有序不再做 2-way
   `std::merge`：carry 前缀 + 各节点有序 run **memcpy concat** 只写 dest 一次（内核能量
   筛选后会再 `d_sortSinglesByTime`）。Carry 仍按 `[W-overlap, W]` 从各有序源二分切出再
   小归并。未到齐时不得把 `lastWatermark` 跳到全水位。
3. **GPU 工人**：`SPSCProcessor` 谁空谁接下一段（负载均衡）。时间顺序只由 submit 序和 `next()`
   序保证，不把 GPU i 绑死在第 i 段。
4. **收回线程**：按提交序 `nextResult()`，把 prompt/delay listmode 拷进有界写队列后立刻释放
   GPU lease 和输入槽。
5. **写盘线程**：FIFO 先写 `prompt.lmf` 再写 `delay.lmf`。队列满则收回阻塞 → GPU 环填满 →
   `submit` 阻塞 → 上游环形缓冲背压。停机时写线程排空后再 `RollingFileWriter::Stop()`。

`coinPipelineDepth` 默认 0，按 `gpuCount × instancePerGpu` 推导 ring；也可显式设在飞段数。
`ring_size = max(computeInstances+2, depth+2, 4)`。单 GPU 回退（`enableMultiGpu=false`）仍走
同步 `processCoincidence`，不为 legacy `Coincidence` 再做一套环。

流水线只改何时算、何时写，不改段边界与 cutoff；Test 9 分段不变性仍然成立。

#### 已知上游缺陷

`getDListmode(..., carryCutoffTime_100fs)` 的约定是「原始时间 ≤ cutoff 的事件视为尾部保
留，彼此之间不配对」。预编译内核只在 **delay** 路径实现了该抑制，**prompt** 路径忽略
cutoff。后果：每段的 carry 前缀内部的 prompt 配对会被重复计入，总 prompt 数比金标准多出
恰好 `carrySinglesTotal` 条；delay 数严格与分段方式无关。
`tests/correctness/test_coin_streaming_aligner.cpp` 的 Test 9a 用合成数据固化了这个契约，
Test 9 则断言「prompt 偏差恰等于 carry 条数」——偏差一旦超出 carry，就说明分段/carry 逻辑
真的出了回归。

### 4. CoincidenceServiceImpl

gRPC 控制面。**热路径是 OpenDataPlane + RDMA（或本机 InProcess）**，不是 `StreamSingles`。

编排顺序：

1. `RegisterNode`：校验 nodeId / detector / channels；**不** Start
2. `OpenDataPlane`：QP/GID/credit_mirror；`requireRoce` 拒绝 InProcess
3. Start：`registered==N && dataplane_open==N`（或显式 `Control.START`）
4. `NotifyProducerComplete` / Heartbeat `producer_complete` → drain 对齐器

其它 RPC：`WaitForStart`、`GetStatus`（含 dataplane / producers_complete）、`Control`（含 DRAIN）、`Heartbeat`。

`StreamSingles` 保留在 proto 中但不接线。

### 5. CoincidenceClient

Worker 侧唯一发送入口（`sendSingles` / `acquireTxSlot` 路径）。

启动：Register → OpenDataPlane → WaitForStart（不再按墙钟 sleep）。
结束：`notifyProducerComplete()` 等 pending 清空后再通知服务端。

## 配置参数

### TimeAlignerConfig

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `networkLatencyMargin_pico` | 0 | 额外 PET 水位裕量（皮秒）。**不是**墙钟等包 / RDMA 乱序等待。有序到达后再扣只增加持有量；排查块内时间回退时可再打开 |
| `maxChunksPerNode` | 100 | 每节点最大缓冲块数 |
| `processingIntervalMs` | 200ms | 无 push 事件时的最长空转等待（不是处理周期） |
| `maxSegmentSingles` | 262144 | 单段 singles 上限，**含 carry**。按显存与内核上限设置，0=不限制（不推荐） |
| `minSegmentSingles` | 65536 | 攒批软下界，压力或超时可突破。0=不攒批 |
| `minSegmentOverlapFactor` | 4 | 段跨度硬下界，单位是重叠窗个数。压力与超时都不能突破 |
| `bufferHighWaterRatio` | 0.80 | 任一节点缓冲或内存池占用超过此比例即立刻触发 |
| `maxProcessLatencyMs` | 50ms | 延迟兜底**触发**切段（仍不得越过水位），0=关闭。不是网络等待 |
| `listmodeWriteQueueCap` | 32 | 写队列深度，与 GPU `ring_size` 脱钩。队列满只背压，不丢已算出的 pair |
| `listmodeIoQueueSize` | 8 | Listmode/Unimode 两层 IO 环深度 |
| `nodeStallWarnMs` | 1000ms | 「高水位但水位线不前进」的告警限流间隔 |
| `allowStalledNodeBypass` | false | 开启后静默超时的节点会被剔出水位线计算（对齐降级） |
| `nodeStallTimeoutMs` | 5000ms | 仅在 `allowStalledNodeBypass=true` 时生效 |

`getTotalSafetyMargin()` = `networkLatencyMargin_pico` + `max(timeWindow, delayTime)`（均转为 100fs）。符合窗始终保留，与网络裕量拆开。

`ProcessingStatistics` 相应新增：`triggerByWatermark / triggerByPressure / triggerByDeadline
/ heldByMinDuration / watermarkStallEvents / degradedSegments / oversizedSegments /
maxNodeBacklogChunks / carrySinglesTotal`。

### 符合协议

```cpp
struct CoincidenceProtocol {
    int timeWindow_ps = 2'000;        // 2ns 符合时间窗口
    int delayTime_ps = 2'000'000;     // 2μs 延迟时间
    float energyLower_eV = 350'000;   // 350 keV 能量下限
    float energyUpper_eV = 650'000;   // 650 keV 能量上限
};
```

## 使用示例

### 服务器端

```cpp
#include "streaming-coin/CoincidenceServiceImpl.hpp"

using namespace openpni::distributed::streaming;

int main() {
    // 创建配置
    auto config = createBDM2AlignerConfig("/path/to/output");
    config.coinProtocol.timeWindow_ps = 2000;
    
    // 创建服务器
    CoincidenceServer server(config, 4, "0.0.0.0:50051");
    
    // 启动
    server.start();
    
    // 等待
    server.wait();
    
    return 0;
}
```

### 客户端

```cpp
#include "streaming-coin/CoincidenceClient.hpp"

using namespace openpni::distributed::streaming;

int main() {
    // 创建配置
    CoincidenceClientConfig config;
    config.serverAddress = "server:50051";
    config.nodeId = 0;
    config.detectorType = "BDM2";
    
    // 创建客户端
    CoincidenceClient client(config);
    client.start();
    
    // 发送数据
    std::vector<openpni::basic::GlobalSingle_t> singles = /* ... */;
    client.sendSingles(singles, clock_ms, duration_ms);
    
    // 停止
    client.stop();
    
    return 0;
}
```

## 文件结构

```
include/core/streaming/
└── StreamingCoincidence.hpp    # 核心流式处理组件

include/grpcService/
├── CoincidenceServiceImpl.hpp  # gRPC 服务端实现
└── CoincidenceClient.hpp       # gRPC 客户端实现

protos/
└── coincidence.proto           # gRPC 协议定义

tests/correctness/
└── test_coin_streaming_aligner.cpp  # 流式对齐（不经 RDMA）
```

## 设计亮点

### 1. PET 时钟作为全局基准

所有节点的 `timeValue_pico` 来自同一个 PET 时钟板，因此可以直接比较。这避免了依赖不可靠的计算机时钟同步。

### 2. 安全边界机制

只处理所有节点都已提供数据的时间范围，保证不丢失符合对：
```
SafeBoundary = min(各节点最小待处理时间) - SafetyMargin
```

### 3. 流式处理避免全局排序

通过滑动窗口机制，每次只对窗口内的数据进行排序，大大减少了内存和计算开销。

### 4. 内存可控

环形缓冲区限制了每个节点的待处理数据量，避免内存无限增长。

## 性能考虑

1. **并行排序**：使用 `std::execution::par_unseq` 进行并行排序
2. **零拷贝**：尽可能使用移动语义避免数据拷贝
3. **GPU 加速**：符合计算在 GPU 上执行
4. **异步 IO**：gRPC 流式传输，客户端异步发送

## 与原有代码对比

| 特性 | 原 MergeAndCoin | 新 StreamingCoincidence |
|------|-----------------|-------------------------|
| 时间同步 | 依赖计算机时钟(ms) | 依赖 PET 时钟(ps) |
| 数据处理 | 离线批处理 | 在线流式处理 |
| 内存占用 | 加载全部数据 | 滑动窗口，可控 |
| 延迟 | 高（需等待全部数据） | 低（实时处理） |
| 扩展性 | 单机 | 分布式 |
