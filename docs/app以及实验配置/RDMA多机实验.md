# RDMA 多机实验

本阶段跨机热路径是 **16 字节 packed singles + RoCE**，不是 raw UDP。  
Coin 进程只做符合；worker 仍是 `app_acq_r2s_node`（采集预留 + R2S + RDMA 发送），**不要**再拆独立 singles 进程。

采集收发包/落盘（阶段 1）不在本目录。本目录覆盖通路、正确性、回放、接收稳定性与符合极限（阶段 2/3），共用同一条 `worker → RoCE → coin`。

进程测试（握手/PAUSE、不替代跨机 soak）见 [测试说明.md](测试说明.md)。状态行判读见 [状态机与调试.md](状态机与调试.md)。

## 角色

| 机器 | 进程 | 职责 |
|------|------|------|
| Coin | `app_coin_master` | Register / OpenDataPlane / Start / Drain；RDMA recv + 对齐 + 符合 |
| Worker | `app_acq_r2s_node` | `source.type=synthetic` 或 `lsingle_replay`；握手后再发数 |

采集 `source.type=acquisition` 本阶段只留 `StubRawIngress`，不接 AcquisitionMaster。

## 握手顺序

1. `RegisterNode`（不 Start）
2. `OpenDataPlane`（QP / GID；`requireRoce=true` 拒绝 InProcess）
3. 全员 `registered==N && dataplane_open==N` 后 Start；`WaitForStart` 表示可以发数（默认不再睡墙钟）
4. 发完 `NotifyProducerComplete` → drain 对齐器 → 关 LMF

## 配置分层

把 `cluster.serverAddress` / `cluster.nodeAddress` 改成真实 IP。

- `cluster`：监听或连接地址、nodeId、expectedNodeCount
- `dataplane`：`requireRoce`、`deviceName`、`gidIndex`
- `coincidence`：探测器 profile、输出目录、时间/能量窗、`savePrompt` / `saveDelay`
- `source`：`synthetic` / `lsingle_replay`；`mode=pairs|stream`；速率与错开字段见 `APP_CONFIG.md`

Coin JSON 里的 `acquisitionControl` 会被 warn 并忽略。

## 三种「延迟」（不要混用）

| 含义 | 怎么配 | 测什么 |
|------|--------|--------|
| PET delay 窗 | `source.delayTimePs` 与 coin `protocol.delayTimePs` | 正确性里 delay 对 |
| 发送墙钟 | `singlesPerSec`、`rateJitterFraction`、`startDelayMs`、`pauseAfterMs` | 接收稳定性、偏斜 |
| 链路 RTT | 跨机真 RoCE | 日志 `rtt`；**不要**用软件 sleep 冒充 NIC |

## 实验剖面

脚本：`bash app/experiments/rdma_cluster/run_coin.sh <coin.json>`，worker 机 `run_worker.sh <worker.json>`。先起 coin，再起 worker。每台 RoCE 机器先跑 `rdma_preflight.sh`。

| 剖面 | Coin JSON | Worker JSON | 看什么 | 通过标准 |
|------|-----------|-------------|--------|----------|
| 通路 | `coin.json`（1 worker） | `worker0.json` | handshake、`complete` | sent ≈ received，能 drain |
| 正确性 | `coin_correctness.json` | `worker0_correctness.json` + `worker1_correctness.json` | 窗内 prompt/delay | 先 sent≈recv，再对数相对真值留容差（几何/能量窗可能小于 `promptPairs`） |
| 回放 | `coin_correctness.json`（或 `coin_2workers.json`） | `worker_replay.json`（改 `lsinglePath`；第二台对称一份） | 真实分布 | 同正确性；replay **按 segment 读出发送**，速率只用墙钟 `singlesPerSec` |
| 稳定性 | `coin_soak.json`（不写 LMF） | `worker0_soak.json` + `worker1_soak.json`（错开/pause） | `lag` `buf` credit `live` | 60s 内心跳不断、credit/buf 能恢复；不对对数 |
| 符合极限 | `coin_rate.json` | `worker0_rate.json` + `worker1_rate.json` | `recvRate` vs `procRate` `memUsagePct` | 稳态吞吐；`lag` 不持续发散；不对对数 |

`source.mode=stream` 必须同时设 `singlesPerSec>0` 与 `runSeconds>0`，边生成边发，避免全量进内存。`pairs` 与 `stream` 都按 `pushChunkSingles` 调用 `fillSyntheticChunk` 再发送。配对公式的单机断言见 `test_synthetic_singles`（[测试说明.md](测试说明.md)）。

`worker1_soak.json` 带 `startDelayMs` 与 `pauseAfterMs`，用来看一节点卡住时 watermark / `buf` 偏斜。

极限剖面 coin `runtime.runSeconds` 略大于 worker `source.runSeconds`，防止 worker 已 complete 而 coin 还在等。

## 编译

```bash
cmake --preset linux-release-apps-basic
cmake --build --preset build-apps-basic

cmake --preset linux-release-apps-cuda
cmake --build --preset build-apps-cuda
```

产物默认：`build/apps/basic/bin/app/app_coin_master`、`build/apps/cuda/bin/app/app_acq_r2s_node`。

## Preflight（每台 RoCE 机器）

```bash
bash app/experiments/rdma_cluster/rdma_preflight.sh
```

检查 verbs 设备、GID、hugepages。无 RNIC 时本地可用 `dataplane.forceInProcess=true` 且 `requireRoce=false` 做控制面联调，不能当作跨机验收。

## 2 机通路

1. Coin：`bash app/experiments/rdma_cluster/run_coin.sh app/config/rdma_cluster/coin.json`
2. Worker：改 `worker0.json` 的 `serverAddress` 为 Coin IP，然后  
   `bash app/experiments/rdma_cluster/run_worker.sh app/config/rdma_cluster/worker0.json`

## 3 机正确性 / soak / rate

1. 先起对应 `coin_*.json`（`expectedNodeCount=2`）
2. 两台 worker 分别用 `worker0_*.json` / `worker1_*.json`（改 IP；`peerNodeId` 互指）
3. Start 等两台都 OpenDataPlane

## Replay

`source.type=lsingle_replay` 且 `source.lsinglePath` 指向 `.lsingle` 文件或目录。先握手再推数。适合正确性规模的数据；极限 soak 请用 `mode=stream`，不要靠把整个 listmode 读进内存。

## 状态与调试

周期日志字段与瓶颈口诀见 [状态机与调试.md](状态机与调试.md)。
