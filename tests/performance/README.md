# 性能测试

本目录放吞吐、时延、soak 等**性能**用例，通过标准与 [correctness](../correctness) 分开。不要用正确性断言当通过门槛。

命名与正确性相同：`test_<模块>_<描述>`（`r2s` / `rdma` / `coin` / `pni` / `app` 等）。

## 用例

| 目标 | 用途 |
|------|------|
| `test_r2s_50100_single_ring` | 50100 单环 R2S 吞吐（生产 `processSegment` 多 GPU；预读真实 segment；callback 做类 RDMA 填槽） |

跨机 RoCE soak 仍走 `app/experiments/rdma_cluster/`，见 `docs/app以及实验配置/RDMA多机实验.md`。
