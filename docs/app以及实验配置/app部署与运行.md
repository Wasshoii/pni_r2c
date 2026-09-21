# App 部署与运行

## 目录与程序

app 目录包含两个当前实验用的可执行程序：

1. `bin/app/app_acq_r2s_node`（worker）
   - 同一进程：采集（`AcquisitionGrpcNode`）+ R2S + CoincidenceClient RDMA 发送
   - 默认数据源：`source.type=acquisition`（省略 `source` 即真实采集，连 AcquisitionMaster，走 gRPC + DPDKNew/Socket）
   - `synthetic` / `lsingle_replay` 须在 JSON 里显式写出；握手后再发数

2. `bin/app/app_coin_master`
   - 符合 + 可选 `AcquisitionMaster`（`acquisitionControl.enabled=true` 时在 `masterAddress` 上启动采集控制）
   - DPDK 无数据冒烟见 `dpdk_config/run_dpdk_nodata_smoketest.sh`

跨机实验见 `RDMA多机实验.md`。DPDK 无业务 raw 冒烟见 `dpdk_config/run_dpdk_nodata_smoketest.sh`。

## 配置分层

1. 示例配置（入门）
   - `app/config/examples/acq_r2s_node.example.json`
   - `app/config/examples/coin_master.example.json`

2. RDMA 集群（当前）
   - `app/config/rdma_cluster/*.json`
   - 启动脚本：`app/experiments/rdma_cluster/`

3. DPDK 无数据冒烟（真实采集路径）
   - `app/config/experiments/no_data_auto/*.json`（worker 写明 `"source": { "type": "acquisition" }`）

## 编译

```bash
cmake --preset linux-release-apps-basic
cmake --build --preset build-apps-basic

cmake --preset linux-release-apps-cuda
cmake --build --preset build-apps-cuda
```

## 基础运行

```bash
./bin/app/app_coin_master --config app/config/examples/coin_master.example.json
./bin/app/app_acq_r2s_node --config app/config/examples/acq_r2s_node.example.json
```

无 RNIC 时不要用示例里的 `requireRoce=true`；本机 InProcess 联调需 `dataplane.requireRoce=false` 且 `forceInProcess=true`。

## 配置校验（dry-run）

```bash
./bin/app/app_coin_master --config app/config/examples/coin_master.example.json --dry-run
./bin/app/app_acq_r2s_node --config app/config/examples/acq_r2s_node.example.json --dry-run
```

## 关于DPDK
   使用dpdk采集测试时，程序可能无法直接退出，可以使用如下指令停止程序
```bash
   # 查看是否还在跑
pgrep -a -f 'app_coin_master|app_acq_r2s_node|tool_dpdk_tx_replayer'

# 优雅停止
pkill -INT -f 'app_coin_master|app_acq_r2s_node|tool_dpdk_tx_replayer'

# 若还在，升级为 TERM
pkill -TERM -f 'app_coin_master|app_acq_r2s_node|tool_dpdk_tx_replayer'

# 最后兜底强杀
pkill -KILL -f 'app_coin_master|app_acq_r2s_node|tool_dpdk_tx_replayer'
```

## 关联文档

1. 跨机 RDMA：`RDMA多机实验.md`
2. 进程测试：[测试/README.md](../测试/README.md)
3. 状态行与调参：`状态机与调试.md`
4. DPDK 采集：`DPDK采集配置与使用.md`
