# SPSCProcessor

> #include<core/r2s/multi_gpu/SPSCProcessor.hpp>

## 目的

单提交线程、单消费线程、多 worker 的有序处理器。提交顺序与 `next()` 取出顺序一致；计算可在多个 `ICompute` 上并行。R2S 50100 将其特化为 `R2S50100SPSCProcessor`（`RawDataView` → `SinglesResult`）。

应用代码不要直接使用本模板，应通过 [R2S50100MultiGpuEngine](R2S50100MultiGpuEngine.md)。

命名空间：`openpni::distributed::r2s::multi_gpu`。

## 核心接口

### ICompute

```cpp
template <typename Data, typename Result>
class ICompute {
  virtual void compute(const Data *data, Result *out) = 0;
};
```

- worker 线程调用。`out` 由 ring slot 预分配，可按 `ResultPolicy` 复用。

### DefaultResultPolicy / OutputLease

- `make_result()` / `prepare_for_reuse()`：控制 slot 内 `Result` 的构造与复用。
- `OutputLease`：移动语义租约。析构时把 ring slot 标为可复用。`operator bool()` 为真表示 `Ready` 且指针有效。`status()` 为 `Ready` / `Failed` / `EndOfStream`。

### SPSCProcessor

```cpp
explicit SPSCProcessor(std::vector<ComputePtr> computes,
                       size_t ring_size,
                       size_t queue_cap,
                       Policy result_policy = {});
void submit(InputPtr data);
OutputLease next();
void signal_no_more_data();
```

- 构造时启动与 `computes.size()` 相等的 worker 线程。
- `submit`：仅提交线程调用；队列满则阻塞。
- `next`：仅消费线程调用；按提交序等待对应 slot 完成。返回的 lease 在析构前，该 slot 不会被下一轮覆盖。
- `signal_no_more_data`：之后 `next()` 在排空后得到 `EndOfStream`。
- 默认 `ring_size=16`、`queue_cap=16`。

## 使用提示

- 提交线程与消费线程必须各只有一个；worker 数量可变。
- `next()` 拿到的 `Result` 指针在 lease 释放后无效。引擎在 `processSegmentSync` 内把 `d_singles` 暴露为 span 前会保持 lease。
- 失败路径：某 worker `compute` 抛错或 slot `Failed` 时，消费侧应停止继续 `submit` 并 `signal_no_more_data`。

### 计算缩放

多 worker 是 **整段任务并行**：各卡同时算不同段，`next()` 仍按 submit 序。吞吐在在飞段数 ≥ GPU 数时可接近线性；单段时延几乎不降。同卡 `instancePerGpu>1` 抢 SM。含 H2D/D2H/PCIe 时不要把 N× 当成承诺。详见 [R2S50100MultiGpuEngine](R2S50100MultiGpuEngine.md)「计算缩放」。
