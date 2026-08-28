# DPacketsAsync

> #include<core/r2s/multi_gpu/DPacketsAsync.cuh>

## 目的

把 host 侧 `ISingleGenerator::PacketsInfo` / `RawDataView` 的包字节与索引拷到 device，供 [R2S50100Compute](R2S50100Compute.md) 调用 libpni CUDA R2S。

**应用代码不要直接 include。**

命名空间：`openpni::distributed::r2s::multi_gpu`。

## 核心接口

```cpp
struct DPacketsAsync {
  CudaUniquePointer<uint8_t> raw;
  CudaUniquePointer<uint64_t> offset;
  CudaUniquePointer<uint16_t> length;
  CudaUniquePointer<uint16_t> channel;
  uint64_t count{0};

  static DPacketsAsync FromHost(...);
  void ReserveFromHost(...);
};
```

- `raw` / `offset` / `length` / `channel` 与 libpni `PacketsInfo` 同语义：第一个包起点是 `raw + offset[0]`，不是 `raw[0]` 必有效。
- `FromHost`：分配并拷贝，返回新对象。
- `ReserveFromHost`：在已有对象上按本段尺寸扩容再拷贝，供 compute 实例复用缓冲。

内存域：输入必须是 host 指针；输出指针仅在对应 CUDA 设备上有效。

## 使用提示

- 仅由 `R2S50100Compute::compute` 使用。
- 与 [PinnedHostCopy](PinnedHostCopy.md) 方向相反：本文件是 H2D 包数据。50100 热路径 singles 不再经 PinnedHostCopy。
