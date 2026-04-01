# App 部署与运行

## 目录与程序

app 目录包含三个核心可执行程序：

1. `bin/app_acq_r2s_node`
   - 采集子节点（受主控下发任务）
   - 采集 UDP 原始包并完成单事件转换
   - 通过 CoinClient 流式上送单事件

2. `bin/app_coin_master`
   - Coin 主控节点
   - 管理符合服务与流式处理
   - 可选启用 AcquisitionMaster 对采集节点下发配置与启停控制

3. `bin/app_udp_raw_replayer`
   - UDP 回放发包工具
   - 用于实验联调、压测和稳定性测试

## 配置分层

1. 示例配置（入门）
   - `app/config/examples/acq_r2s_node.example.json`
   - `app/config/examples/coin_master.example.json`

2. 实验配置（单机与阶段测试）
   - `app/config/experiments/*.json`

3. 三机模板（严格分布式）
   - `app/config/three_machine/templates/*.template.json`
   - 运行时渲染输出：`app/config/three_machine/runtime/*.json`

## 编译

```bash
make app-acq-r2s-node
make app-coin-master
make app-udp-replayer
```

## 基础运行

```bash
./bin/app_coin_master --config app/config/examples/coin_master.example.json
./bin/app_acq_r2s_node --config app/config/examples/acq_r2s_node.example.json
```

## 配置校验（dry-run）

```bash
./bin/app_coin_master --config app/config/examples/coin_master.example.json --dry-run
./bin/app_acq_r2s_node --config app/config/examples/acq_r2s_node.example.json --dry-run
```

## 关联文档

1. 实验脚本与性能测试：`实验配置与脚本.md`
2. 三机严格版：`三机严格版.md`
