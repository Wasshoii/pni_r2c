# HugepageArena

> #include<dataplane/rdma/HugepageArena.hpp>

## 目的

连续内存池：优先大页，失败则匿名 mmap。供 [SlotRing](SlotRing.md) 与 TX/credit 缓冲做 `ibv_reg_mr`。

命名空间：`openpni::distributed::dataplane::rdma`。

## 核心接口

```cpp
class HugepageArena {
  bool allocate(size_t bytes, bool preferHugePages = true);
  void release();
  void *data() noexcept;
  size_t size() const noexcept;
  bool usesHugePages() const noexcept;
};
```

- `allocate`：长度向上取整到页大小。失败返回 false，对象保持空。
- 不可拷贝，可移动。析构 `release`。
- `usesHugePages()` 表示实际是否落到 hugetlb，不是请求值。

## 使用提示

- 大页未预留时仍应能靠 mmap 跑通（性能较差）。实验机预留见 [DPDK采集配置与使用.md](../../../app以及实验配置/DPDK采集配置与使用.md) 与 [RDMA多机实验.md](../../../app以及实验配置/RDMA多机实验.md) 的 preflight。
- 必须在 `ibv_reg_mr` 之前 `allocate` 成功；不要对空 arena 取 `data()` 去注册。
