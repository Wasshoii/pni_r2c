# 测试

本目录按 **单元模块 / 通信 / 集成** 说明 `bin/test/*`。跨机 soak 与剖面判据见 [RDMA多机实验.md](../app以及实验配置/RDMA多机实验.md)，不要和进程测试混用通过标准。

接口与握手以 [接口说明](../接口说明/README.md) 和 [状态机与调试.md](../app以及实验配置/状态机与调试.md) 为准。[STREAMING_COINCIDENCE.md](../STREAMING_COINCIDENCE.md) 是旧综述，热路径已不是 gRPC `StreamSingles`。

## 和 app 的关系

| app | 角色 | 进程测试主要覆盖 |
|-----|------|------------------|
| `app_acq_r2s_node` | worker：R2S + `CoincidenceClient` 发送 | synthetic、R2S GPU、RDMA 发送、本机 R2S gRPC |
| `app_coin_master` | 符合-only：RDMA 收 + 对齐 + 符合 | 编排握手、L2 收包、符合 GPU、L3 回放、R2S→coin |

timesync、AcquisitionMaster **现行 app 未接线或会忽略**，对应测试只保模块。

```mermaid
flowchart TB
  subgraph unit [单元模块]
    syn[test_synthetic_singles]
    loop[test_rdma_dataplane_loopback]
    cfg[test_app_config]
    r2sGpu[test_r2s50100_multi_gpu]
    coinGpu[test_coincidence_multi_gpu]
    carry[test_coin_carry_boundary]
  end
  subgraph comm [通信]
    orch[test_rdma_orchestration]
    ingress[test_local_grpc_singles_ingress]
  end
  subgraph integ [集成]
    smoke[test_app_inprocess_smoke]
    r2sGrpc[test_local_grpc_r2s]
    coinStream[test_local_grpc_coin_stream]
    r2sCoin[test_local_grpc_r2s_coin]
  end
  worker[app_acq_r2s_node]
  coin[app_coin_master]
  syn --> worker
  r2sGpu --> worker
  loop --> worker
  cfg --> worker
  cfg --> coin
  orch --> worker
  orch --> coin
  smoke --> worker
  smoke --> coin
  ingress --> coin
  r2sGrpc --> worker
  coinStream --> coin
  r2sCoin --> worker
  r2sCoin --> coin
  coinGpu --> coin
  carry --> coin
```

分类页：[单元模块.md](单元模块.md) · [通信.md](通信.md) · [集成.md](集成.md)

## 测试一览

### 单元模块

| 目标 | 用途 |
|------|------|
| `test_synthetic_singles` | 测试 worker synthetic 源的配对公式、分块拼接与时间单调 |
| `test_rdma_dataplane_loopback` | 测试本机 RDMA 槽环写入、credit 与 SOF/EOF 标志是否正确 |
| `test_r2s50100_multi_gpu` | 测试 50100 单事件转换在多 GPU 上的正确性 |
| `test_coincidence_multi_gpu` | 测试符合计算在多 GPU 上与单 GPU 结果一致 |
| `test_coin_carry_boundary` | 测试流式分段符合在窗口边界用 carry 能否找回丢失的符合 |
| `test_app_config` | 测试 AppConfig 能否解析 example 与 InProcess 冒烟 JSON（有 app 二进制再跑 --dry-run） |
| `test_distributed_clock_sync` | 测试多节点时钟漂移校正算法（不走真实 gRPC） |
| `test_pni_r2c` | 用盘上 raw 离线跑 50100 R2S，产出 singles 供对照（非 CI） |
| `test_pni_coin` | 用盘上 singles 离线做符合，检查 prompt/delay 通道分布（非 CI） |

### 通信

| 目标 | 用途 |
|------|------|
| `test_rdma_orchestration` | 测试 worker↔coin 的 Register / OpenDataPlane / Start 握手、PAUSE、InProcess 发送与通道 remap |
| `test_local_grpc_singles_ingress` | 测试 `.lsingle` 经数据面灌入 coin 接收环的连续性与吞吐（不算符合） |
| `test_acquisition_control_init` | 测试采集 Master 向节点下发任务并进入 CONFIGURED |
| `test_distributed_clock_sync_grpc` | 测试 timesync 的 gRPC 同步、多轮校正与并发客户端 |
| `test_local_grpc_coin` | 提供不跑符合的 RDMA 接收端，供手工对端联调 |

### 集成

| 目标 | 用途 |
|------|------|
| `test_app_inprocess_smoke` | 测试真实 `app_coin_master` + `app_acq_r2s_node` 的 InProcess synthetic 握手与收发 |
| `test_local_grpc_r2s` | 测试两路 R2S 节点经 RDMA 发到本机接收端（不含符合计算） |
| `test_local_grpc_coin_stream` | 测试预计算 singles 灌入真实 coin 后能否产出 prompt/delay |
| `test_local_grpc_r2s_coin` | 测试本机双 R2S + coin 全链路（R2S → RDMA → 符合） |
| `test_streaming_coincidence` | 测试流式时间对齐与符合（不经 RDMA，直接灌 singles） |
| `test_bdm50100_online_pipeline` | 模拟采集环走完 R2S 与符合并落盘，不做正确性断言 |

## 编译

`./build.sh --tests` 依次 configure+build **core / pni / cuda** 三档，编该档已打开的全部测试（含实验室脚本）。不是扫 `tests/` 目录名。

```bash
cd pni_r2c
./build.sh --tests
# 产物：bin/test/<目标名>
```

分档：

```bash
cmake --preset linux-release-tests-core && cmake --build --preset build-tests-core -j
cmake --preset linux-release-tests-pni  && cmake --build --preset build-tests-pni -j
cmake --preset linux-release-tests-cuda && cmake --build --preset build-tests-cuda -j
```

单项：`cmake --build --preset build-tests-pni -j --target test_synthetic_singles`

## 默认跑测 vs 手工

CTest 标签：`core` / `pni` / `cuda`，外加 `integration`（本机链路、常要数据或 GPU）和 `manual`（实验室脚本 / 默认会挂的 soak）。

```bash
# 推荐 CI：无数据也能过的断言
ctest --test-dir build/tests/core -L core --output-on-failure
ctest --test-dir build/tests/pni  -L pni  -LE "integration|manual" --output-on-failure
ctest --test-dir build/tests/cuda -L cuda -LE "integration|manual" --output-on-failure

# 显式（示例）
ctest --test-dir build/tests/pni -R test_rdma_orchestration --output-on-failure
./bin/test/test_local_grpc_singles_ingress --data-root /media/lenovo/1TB/50100data/test_9120
```

cuda 档几乎都依赖 9120/校正文件，默认 `-LE integration|manual` 可能没有可跑项。有数据时用 `-L cuda` 或 `-R <名>`。

## 过时与分层（源码保留）

- 已删除的 Makefile 入口：`test-acq-datapath-udp`、`test-acq-r2s-pipeline`、`test-acq-control-smoke`（无对应 `tests/*.cpp`）。
- `test_local_grpc_coin`：假 coin 接收端，被 `test_rdma_orchestration` / `test_local_grpc_singles_ingress` 替代；仍编译，标 `manual`。
- 实验室脚本：`test_pni_r2c`、`test_pni_coin`、`test_bdm50100_online_pipeline`（无正确性断言）— 照编，默认 ctest 不跑。
- `test_streaming_coincidence` 默认只跑 9120 soak（`test7Only`），缺数据或 120s 超时会挂。

## 后续补测（有条件再实现）

本轮只记文档，不写测试代码。

- **B5** 有 9120 `.lsingle` 时：同批 singles 离线符合 vs `CoinGrpcNode` 写出的 LMF 条数（允许窗边界阈值）。挂 cuda 档，标 `integration`。
- **B6** worker `source.type=lsingle_replay`（`sendReplayBySegment`）小 fixture，与 L2 `grpc_singles_replay` helper 对齐。缺数据 skip。
- **C7** 本机有 verbs 时：`CoincidenceClient::sendSingles(span)` 走 RoCE 回调填 TX，挂现有 `testSameProcessRoce` 的 skip 逻辑；无 RNIC 不算失败。
