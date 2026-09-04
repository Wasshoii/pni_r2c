# PinnedHostCopy

> #include<core/r2s/multi_gpu/PinnedHostCopy.cuh>

## 目的

把 device 上的 `span<const T>` 异步拷到 CUDA pinned host（`HostUniquePtr`），并在函数内 `cudaStreamSynchronize`。S2C 等路径仍用；**50100 R2S 热路径已不再经此落到引擎 pinned**，singles 留在 `d_singles`，D2H 在 TX 填槽的 copy stream 或 `materializeSinglesOnHost`。

**应用代码不要直接 include。** 上层应使用 [R2S.md](../R2S.md) 的 `materializeSinglesOnHost`。

命名空间：`openpni::distributed::r2s::multi_gpu`。

## 核心接口

```cpp
template <typename T>
HostUniquePtr<T> make_cuda_host_ptr_from_dcopy(std::span<const T> d_src, cudaStream_t stream);

template <typename T>
void copy_from_device_to_pinned_host_async(HostUniquePtr<T> &h_dst,
                                           std::span<const T> d_src,
                                           cudaStream_t stream);
```

- 空 span：返回/重置为空缓冲。
- 失败时通过 libpni `cuda_throw` 抛异常。
- 当前实现在 memcpy 后立即同步 `stream`，调用返回后 host 侧数据已可见。

## 使用提示

- `d_src` 必须是当前设备可访问的 device 指针。
- 与 [DPacketsAsync](DPacketsAsync.md) 配套：包 H2D、singles D2H。
