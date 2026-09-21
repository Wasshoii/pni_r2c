# DPDK 仅采集双机测试

本目录测 **NIC → DPDKNew**，不经过 `app_coin_master` / `app_acq_r2s_node`，也不跑 R2S / RDMA。

生产 app 仍是分布式 `采集 → R2S → 符合`。这里自带精简 `AcquisitionMaster` + `AcquisitionGrpcNode`，因为采集节点不能单独 Start，必须有主控下发 Configure/Start。

## 产物

| 二进制 | 角色 |
|--------|------|
| `bin/test/test_dpdk_acq_master` | 只跑 AcquisitionMaster；节点连上后立即 Distribute + Start；周期打印速率 / unknown / imissed |
| `bin/test/test_dpdk_acq_node` | 只跑 AcquisitionGrpcNode + DPDK；周期打印同样计数；`--write-raw` 时落 raw 分卷 |
| `bin/tools/tool_dpdk_tx_replayer` | TX 机发包 |

CMake：`R2C_BUILD_TEST_DPDK_ACQ_PERF`（preset `linux-release-tests-pni` 已开）。标签 `pni performance manual`。无 `--config` 时打印 `SKIP` 并以 0 退出。

## 通道剖面

| `--profile` | `channelCount` | 默认 rings | 核数下界 `1+2×rings×ports` |
|-------------|----------------|------------|------------------------------|
| `930` | 144（单环） | 1 | 3 |
| `9120_2ring` | 288（双环） | 2 | 5 |

实验室发包：同一 `sourceIp` + `sourcePortBase+i`。真机 930 在 JSON 里填真实 `acquisition.detectorSources[]`。

## 编译

```bash
cmake --preset linux-release-tests-pni
cmake --build --preset build-tests-pni --target test_dpdk_acq_master test_dpdk_acq_node

cmake --preset linux-release-tools
cmake --build --preset build-tools --target tool_dpdk_tx_replayer
```

vfio / hugepage 见 [DPDK采集配置与使用.md](../../../docs/app以及实验配置/DPDK采集配置与使用.md)。TX 与 RX **不能共用同一块已绑 vfio 的网卡**。

改了 `pni-standard-project` 的 `AcquisitionStatus.imissed/ierrors` 后，需要重装正在链接的 libpni，测试节点才能读到 NIC 硬件丢包。

## 双机步骤

RX 机：

```bash
bash tests/performance/dpdk_acq/run_rx.sh \
  --bind-ip 10.10.1.20 \
  --source-ip 10.10.1.10 \
  --profile 930 \
  --duration-sec 20
```

TX 机（RX 已 START 后再发）：

```bash
bash tests/performance/dpdk_acq/run_tx.sh \
  --dst-mac aa:bb:cc:dd:ee:ff \
  --destination-ip 10.10.1.20 \
  --source-ip 10.10.1.10 \
  --profile 930 \
  --pps 500000 \
  --duration-sec 10
```

把 TX 日志拷到 RX 的 `tests/performance/dpdk_acq/logs/` 后：

```bash
bash tests/performance/dpdk_acq/summarize_logs.sh
```

930 真机落盘：RX 加 `--write-raw`。写盘不当 144/288 通道吞吐门槛。

## 统计口径

| 量 | 日志 |
|----|------|
| 瞬时 pps / 带宽 | `[DPDK-ACQ-MASTER] ... speed_mpps= bandwidth_MBps=`（带宽字段来自 NodeStatus，实现是 MiB/s） |
| 映射丢失 | `unknown`（四元组未命中或长度过滤） |
| NIC 硬件丢 | `imissed` / `ierrors`（`rte_eth_stats`） |
| 端到端丢包率 | `(tx_sent - total_rx_packets) / tx_sent` |

通过标准（性能，无硬阈值）：`unknown` 相对 sent 可忽略；把 `imissed` 与端到端 loss 写入报告；`buffer_used` 不顶满。
