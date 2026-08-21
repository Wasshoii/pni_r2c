# R2S50100MultiGpuEngine

> #include<core/r2s/multi_gpu/R2S50100MultiGpuEngine.hpp>

## 目的

BDM50100 的多 GPU 任务并行入口：把一段 `RawDataView` 同步转换成 pinned host 上的 `span<const Single>`。由 [R2SStreamProcessor](../R2S.md) 在 `enableMultiGpu==true` 时持有，应用代码通常不必直接构造。

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
- `max_input_gibits==0`：不为 singles 缓冲预留上限；`>0` 时按包长估算预留，见 `R2S50100SinglesResultPolicy`。
- `ring_size` / `queue_cap` 默认来自 `R2S50100SPSCProcessor`（16）。

### SegmentSinglesResult

```cpp
struct SegmentSinglesResult {
  std::span<const Single> span;
  uint64_t count = 0;
};
```

- `span` 指向 pinned host 缓冲，生命周期直到下一次 `processSegmentSync` 或 `finalize`。
- 调用方若要跨段持有，必须拷贝。

### R2S50100MultiGpuEngine

```cpp
bool initialize(const R2S50100MultiGpuEngineConfig &config);
SegmentSinglesResult processSegmentSync(const openpni::RawDataView &view);
void finalize();
size_t gpuCount() const;
```

- `initialize`：按 GPU × 实例创建 `R2S50100Compute`，启动 SPSC。
- `processSegmentSync`：提交一段 raw，阻塞直到有序结果可用。
- `finalize`：停止 worker、释放 GPU 资源。析构也会调用。

### 工厂

```cpp
R2S50100MultiGpuEngineConfig makeMultiGpuEngineConfig(
    const R2SProcessConfig &config,
    const std::vector<uint16_t> &channels_to_process);
bool shouldUseMultiGpu50100(const R2SProcessConfig &config);
```

- `shouldUseMultiGpu50100`：探测器为 BDM50100 且 `enableMultiGpu==true`。

## 使用提示

- 本类不是线程安全的多生产者：与 `R2SStreamProcessor` 一样由单消费者线程串行 `processSegmentSync`。
- 应用代码应走 `R2SProcessConfig`，不要直接 include 本头文件，除非写引擎级测试。
- CUDA 包缓冲与 D2H 见 [DPacketsAsync](DPacketsAsync.md)、[PinnedHostCopy](PinnedHostCopy.md)。
