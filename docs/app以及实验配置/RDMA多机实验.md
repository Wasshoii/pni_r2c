# RDMA 多机实验

本阶段跨机热路径是 **16 字节 packed singles + RoCE**，不是 raw UDP。  
Coin 进程只做符合；worker 仍是 `app_acq_r2s_node`（采集预留 + R2S + RDMA 发送），**不要**再拆独立 singles 进程。

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

## 配置

- Coin：`app/config/rdma_cluster/coin.json`（2 机一台 worker）或 `coin_2workers.json`（3 机两台 worker）
- Worker：`worker0.json` / `worker1.json`

把 `cluster.serverAddress` / `cluster.nodeAddress` 改成真实 IP。不要使用旧 `__A_MASTER_IP__` 模板。

分层字段：

- `cluster`：监听或连接地址、nodeId、expectedNodeCount
- `dataplane`：`requireRoce`、`deviceName`、`gidIndex`
- `coincidence`：探测器 profile、输出目录、时间/能量窗
- `source`：`synthetic` / `lsingle_replay` / `acquisition`（预留）

Coin JSON 里的 `acquisitionControl` 会被 warn 并忽略。

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

## 2 机

1. Coin 机：`bash app/experiments/rdma_cluster/run_coin.sh app/config/rdma_cluster/coin.json`
2. Worker 机：改 `worker0.json` 的 `serverAddress` 为 Coin IP，然后  
   `bash app/experiments/rdma_cluster/run_worker.sh app/config/rdma_cluster/worker0.json`

验收：worker 发完退出；coin 日志 `producersComplete=true`；sent ≈ received；进程能 drain 退出。

## 3 机（两台 worker synthetic）

1. Coin 用 `coin_2workers.json`（`expectedNodeCount=2`）
2. 两台 worker 分别用 `worker0.json` / `worker1.json`（`nodeId` 0/1，`peerNodeId` 互指）
3. 先起 coin，再同时起两个 worker（Start 等两台都 OpenDataPlane）

验收：prompt/delay 相对真值在阈值内（几何/能量窗可能导致不完全等于 `promptPairs`/`delayPairs`；先核对 received 计数）。

## Replay

`source.type=lsingle_replay` 且 `source.lsinglePath` 指向 `.lsingle` 文件或目录。同样必须先握手再推数。

## 状态与调试

周期日志字段、状态机与瓶颈口诀见 [状态机与调试.md](状态机与调试.md)。
