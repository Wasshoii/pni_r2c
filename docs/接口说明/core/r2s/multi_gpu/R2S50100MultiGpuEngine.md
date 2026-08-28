# R2S50100MultiGpuEngine

> #include<core/r2s/multi_gpu/R2S50100MultiGpuEngine.hpp>

## 目的

BDM50100 的多 GPU 任务并行入口：把一段 `RawDataView` 提交给共享任务队列，由各卡整段 `compute()`，再按 submit 序 `next()`。默认热路径上 `SegmentSinglesResult::span` 指向 **该卡 device** 上的 `d_singles`（不是引擎 pinned host）。由 [R2SStreamProcessor](../R2S.md) 在 `enableMultiGpu==true` 时持有，应用代码通常不必直接构造。

命名空间：`openpni::distributed::r2s::multi_gpu`。

内部用 [SPSCProcessor](SPSCProcessor.md) 调度多份 [R2S50100Compute](R2S50100Compute.md)。

## 核心接口

### R2S50100MultiGpuEngineConfig

```cpp
struct R2S50100MultiGpuEngineConfig {
  openpni::device::bdm50100_v2::BDM50100R2SParams r2s_params;
  std::vector<std::string> local_calib_files;
  std::vector<uint32_t> gpu_ids;
  uint32_t instance_per_gpu = 1;
  long double max_input_gibits = 0.0L;
  float input_burst_tolerance_coef = 1.2f;
  size_t ring_size;
  size_t queue_cap;
};
```

- `local_calib_files`：本节点要处理的通道校正，与 `R2SProcessConfig::channelIndices` 对齐。
- `gpu_ids` 空则由工厂填全部可见设备。
- `instance_per_gpu`：每张卡上的 compute 实例数（SPSC worker 数 = `gpu_ids.size() * instance_per_gpu`）。
- `max_input_gibits==0`：不为 singles 缓冲预留上限；`>0` 时按包长估算（policy 内保留，device `d_singles` 在 worker 上按实际条数 `Reserve`）。
- `ring_size` / `queue_cap`：未设 `maxInputGibits` 时默认来自 `R2S50100SPSCProcessor`（16），并至少为 `computePipelineDepth+2`。设了 `maxInputGibits` 时缩到 `max(pipeline+2, 4)`，避免每槽按最坏 singles 预留把显存打满。

### SegmentSinglesResult

```cpp
struct SegmentSinglesResult {
  std::span<const Single> span;
  uint64_t count = 0;
};
```

- `span` 指向该卡 **device** 上的 `d_singles`，生命周期直到下一次 `processSegmentSync` 或 `finalize`（held lease）。调用方若当 host 用必须 `materializeSinglesOnHost`。
- 调用方若要跨段持有，必须拷贝（D2H 或 D2D）。

### R2S50100MultiGpuEngine

```cpp
bool initialize(const R2S50100MultiGpuEngineConfig &config);
void submitView(const openpni::RawDataView *view);
R2S50100SPSCProcessor::OutputLease nextLease();
SegmentSinglesResult processSegmentSync(const openpni::RawDataView &view);
void finalize();
size_t gpuCount() const;
```

- `initialize`：按 GPU × 实例创建 `R2S50100Compute`，启动 SPSC。
- `submitView` / `nextLease`：有序提交与收取；`view` 必须活到对应 `nextLease` 返回（H2D 完成）。
- `processSegmentSync`：`submit` + `next`，lease 留在引擎内直到下一次 sync 调用。
- `finalize`：停止 worker、释放 GPU 资源。析构也会调用。

### 工厂

```cpp
R2S50100MultiGpuEngineConfig makeMultiGpuEngineConfig(
    const R2SProcessConfig &config,
    const std::vector<uint16_t> &channels_to_process);
bool shouldUseMultiGpu50100(const R2SProcessConfig &config);
```

- `shouldUseMultiGpu50100`：探测器为 BDM50100 且 `enableMultiGpu==true`。

## 计算缩放

现架构是 **整段任务并行**，不是一段 raw 切到 8 张卡上。

- 每张卡一个 [R2S50100Compute](R2S50100Compute.md)，持有节点 **全部** 通道校正。
- [SPSCProcessor](SPSCProcessor.md) 共享任务队列：worker 弹出整段 `RawDataView*`；`compute()` 内 H2D + `DRaw2Singles`，结果留在该卡 `d_singles`。
- `next()` 按 submit 序取出，**不参与 kernel 本身**。发送侧必须跟 `next()` 同序填 TX，见 [R2S到RDMA](../../../通路/R2S到RDMA.md)。

吞吐（段/s、Gib/s）在理想条件下可接近线性：

- 在飞段数 ≥ GPU 数（`computePipelineDepth` / 引擎 ring ≥ N）。
- 各卡算不同段，kernel 互不抢 SM。
- 只统计 device 上的 `DRaw2Singles` 时间，不计 host 填 TX、不计单线程 `next`。

即使只谈计算，也不是严格 N×：

- **单段时延几乎不降。** 8 卡不会把一段 350 ms 变成 44 ms。线性指 **多段吞吐**，不是单段加速。
- **`compute()` 含 H2D（及随后消费侧 D2H）。** 多路同时从同一 NUMA hugepage DMA，会打主机内存和 PCIe。
- **喂不饱就不线性。** Bridge 默认深度 2 时最多约 2 卡有活——这是供给问题，不是 worker 公式错。
- **同卡 `instancePerGpu>1`** 抢同一 SM，不是加卡。

## 使用提示

- 本类不是线程安全的多生产者：与 `R2SStreamProcessor` 一样由单消费者线程串行 submit/next。
- 应用代码应走 `R2SProcessConfig`，不要直接 include 本头文件，除非写引擎级测试。
- CUDA 包缓冲见 [DPacketsAsync](DPacketsAsync.md)。热路径不再经 [PinnedHostCopy](PinnedHostCopy.md) 落到引擎 pinned；D2H 在 `CoincidenceClient::fillRoceTxAndEnqueue` 或 `materializeSinglesOnHost`。
