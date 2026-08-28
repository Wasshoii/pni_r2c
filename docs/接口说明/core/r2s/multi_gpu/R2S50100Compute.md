# R2S50100Compute

> #include<core/r2s/multi_gpu/R2S50100Compute.cuh>

## 目的

一张 GPU 上的 BDM50100 计算实例：实现 `ICompute<RawDataView, SinglesResult>`。由 [R2S50100MultiGpuEngine](R2S50100MultiGpuEngine.md) 按 `gpuId × instance` 构造，放入 [SPSCProcessor](SPSCProcessor.md)。

**应用代码不要直接 include 本头文件。**

命名空间：`openpni::distributed::r2s::multi_gpu`。

## 核心接口

### SinglesResult

```cpp
struct SinglesResult {
  openpni::tools::HostUniquePtr<Single> singles;  // 仅 cut/sort/写盘/onSinglesReady 时再物化
  openpni::detail::CudaUniquePointer<Single> d_singles;
  int gpu_id = -1;
  uint64_t actualSinglesCount{0};
};
```

- kernel 输出先 **D2D** 到 `d_singles`（generator `temp` 会被下一轮覆盖）。默认不再 D2H 到 pinned。
- `gpu_id` 供消费线程 `cudaSetDevice` 后再 D2H。
- `actualSinglesCount` 为本次有效条数。

### R2S50100SinglesResultPolicy

- `make_result()` 不再在错误的 GPU 上下文里预留巨大 host pinned。
- `max_input_gibits` 仍用于估算上限；device 缓冲在 `compute()` 里按实际条数 `Reserve`。

### R2S50100ComputeConfig / R2S50100Compute

```cpp
struct R2S50100ComputeConfig {
  std::vector<std::string> local_calib_files;
  openpni::device::bdm50100_v2::BDM50100R2SParams r2s_params;
  int gpuId = 0;
};

void compute(const RawDataView *data, SinglesResult *out) override;
```

- 绑定 `gpuId`，内部持有 `BDM50100R2SArray` 与每通道 `BDM50100R2S`（libpni）。
- `compute`：host `RawDataView` → [DPacketsAsync](DPacketsAsync.md) 上 GPU → libpni device R2S → **device** `d_singles`。禁止在 `compute()` 里 `acquireTxSlot`（完成序 ≠ 段序）。
- 算法细节（晶体、串扰、能量）在 libpni，此处不展开。

`R2S50100SPSCProcessor` 是 `SPSCProcessor<RawDataView, SinglesResult, R2S50100SinglesResultPolicy>` 的别名。

## 使用提示

- 每个实例固定一张 GPU；多实例可共享同一 `gpuId`（`instancePerGpu>1`）。
- `out` 由 SPSC ring 复用；`compute` 必须写入 `actualSinglesCount`，不要假定容量等于有效条数。
