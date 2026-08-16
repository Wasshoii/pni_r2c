# 流式符合计算系统文档

## 概述

本系统实现了分布式 PET（正电子发射断层成像）数据处理的流式符合计算功能。核心设计思想是利用 PET 时钟板的精确时间戳（ps级）作为全局时间基准，通过滑动窗口机制实现高效的时间对齐和符合计算。

## 架构设计

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        流式时间对齐架构                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Node1 ──┬──> [RingBuffer1] ──┐                                            │
│  Node2 ──┼──> [RingBuffer2] ──┼──> [TimeAligner] ──> [Coincidence] ──> LMF │
│  Node3 ──┼──> [RingBuffer3] ──┤                                            │
│  ...     │        ...         │                                            │
│  NodeN ──┴──> [RingBufferN] ──┘                                            │
│                                                                             │
│  关键：以 PET 时钟的 timeValue_pico 作为全局时间基准                          │
│        滑动窗口逐步推进，避免全局排序                                         │
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
    
    uint64_t minTime_pico;     // 块内最小时间（PET时钟）
    uint64_t maxTime_pico;     // 块内最大时间（PET时钟）
};
```

### 2. NodeRingBuffer

线程安全的环形缓冲区，每个节点一个。

**特性：**
- 支持单生产者多消费者模式
- 阻塞式 push/pop 操作
- 非阻塞式 tryPop 操作
- 可关闭以停止所有等待线程

### 3. StreamingTimeAligner

核心时间对齐器，负责收集各节点数据并进行精确时间对齐。

**工作流程：**
1. 计算全局安全时间边界 = min(所有节点最小待处理时间) - 安全边距
2. 从各节点缓冲区提取 [上次边界, 当前安全边界] 范围内的数据
3. 合并排序后进行符合计算
4. 更新时间边界，重复

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
| `alignmentWindow_pico` | 200ms | 对齐窗口大小 |
| `safetyMargin_pico` | 10ms | 安全边距，补偿网络延迟 |
| `maxChunksPerNode` | 100 | 每节点最大缓冲块数 |
| `processingIntervalMs` | 5ms | 处理循环间隔 |

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

tests/
└── test_streaming_coincidence.cpp  # 单元测试
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
