# App 部署与运行

## 目录与程序

app 目录包含两个当前实验用的可执行程序：

1. `bin/app/app_acq_r2s_node`（worker）
   - 同一进程：采集预留（`IRawIngress` stub）+ R2S + CoincidenceClient RDMA 发送
   - 本阶段数据源：`synthetic` 或 `lsingle_replay`；`acquisition` 未实现
   - 启动顺序：Coincidence 握手（Register → OpenDataPlane → Start）后再发数

2. `bin/app/app_coin_master`
   - 符合-only：RDMA recv + 时间对齐 + 符合
   - **不**启动 AcquisitionMaster（`acquisitionControl.enabled` 会被忽略）

跨机实验见 `RDMA多机实验.md`。DPDK 无业务 raw 冒烟见 `dpdk_config/run_dpdk_nodata_smoketest.sh`。

## 配置分层

1. 示例配置（入门）
   - `app/config/examples/acq_r2s_node.example.json`
   - `app/config/examples/coin_master.example.json`

2. RDMA 集群（当前）
   - `app/config/rdma_cluster/*.json`
   - 启动脚本：`app/experiments/rdma_cluster/`

3. DPDK 无数据冒烟（采集预留）
   - `app/config/experiments/no_data_auto/*.json`

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
pgrep -a -f 'build/apps/(basic/app_coin_master|cuda/app_acq_r2s_node)'

# 优雅停止
pkill -INT -f 'build/apps/(basic/app_coin_master|cuda/app_acq_r2s_node)'

# 若还在，升级为 TERM
pkill -TERM -f 'build/apps/(basic/app_coin_master|cuda/app_acq_r2s_node)'

# 最后兜底强杀
pkill -KILL -f 'build/apps/(basic/app_coin_master|cuda/app_acq_r2s_node)'
```

## 关联文档

1. 跨机 RDMA：`RDMA多机实验.md`
2. 状态行与调参：`状态机与调试.md`
3. DPDK 采集：`DPDK采集配置与使用.md`
