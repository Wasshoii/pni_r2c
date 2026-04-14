# DPDK 采集配置与使用（基于 PNI）

本文档面向当前 `pni_r2c` 项目，补充“如何在本项目中启用并使用 DPDK 采集”。

参考来源：
- PNI 文档：`pni-standard-project/docs/项目介绍/DPDK支持.md`
- 本项目采集控制与 app 配置文档

## 1. 适用范围与前置假设

1. 操作系统：Linux。
2. 假设 DPDK 已安装（如未安装，先按 PNI 文档完成安装与基础验证）。
3. 本文重点是“本项目如何接入 DPDK 采集路径”。

## 2. 先判断机器是否具备 DPDK 运行条件

建议先做四类检查。

### 2.1 安装层检查

```bash
command -v dpdk-testpmd
command -v dpdk-devbind.py
command -v dpdk-hugepages.py
pkg-config --modversion libdpdk
```

判定：
1. 三个工具都可执行。
2. `libdpdk` 可返回版本号。

### 2.2 运行层检查（EAL 能否初始化）

```bash
dpdk-testpmd --no-pci --no-huge -- --total-num-mbufs=2048 --nb-cores=1
```

判定：
1. 输出中有 `EAL: Detected ...`。
2. 无动态链接或 EAL 初始化错误。

### 2.3 网卡绑定状态检查

```bash
dpdk-devbind.py --status
```

判定：
1. 目标网卡不能长期停留在普通内核驱动（如 `r8169`、`ixgbe`、`i40e`）用于 DPDK 数据面。
2. 用于 DPDK 的端口应切到 `vfio-pci`（或受支持的 UIO 驱动）。

### 2.4 HugePages 与 IOMMU 检查

```bash
grep -E 'HugePages_Total|HugePages_Free|Hugepagesize' /proc/meminfo
dpdk-hugepages.py -s
lsmod | grep -E 'vfio|uio|igb_uio' || true
test -e /dev/vfio/vfio && echo "/dev/vfio/vfio exists" || echo "missing"
```

判定：
1. `HugePages_Total` 大于 0。
2. `vfio` 相关能力可用。

## 3. 本项目中 DPDK 采集链路的关键点

当前项目链路为：

1. `app_coin_master` 下发 `AcquisitionTask`。
2. `app_acq_r2s_node` 接收任务并创建采集 runtime。
3. 当 `algorithm_type=ALGORITHM_TYPE_DPDK` 且构建启用 DPDK 时，节点进入 `openpni::DPDKAcquisition`。

同时，DPDK 初始化参数来自任务中的 `dpdk_options`，尤其是：

1. `copy_thread_num`
2. `rx_rings_per_port`
3. `rte_mbuf_double_pointer_size_multiply`
4. `rte_mbuf_double_pointer_num_multiply`
5. `bind_ips`

## 4. 配置步骤（项目视角）

## 4.1 网络与系统准备

1. 选择用于 DPDK 的网卡端口，并将业务 IP 与端口规划清楚。
2. 按 PNI 文档配置 HugePages。
3. 将目标网卡绑定到 `vfio-pci`。

示例（仅示意，按你的网卡 PCI 地址替换）：

```bash
sudo modprobe vfio-pci
sudo dpdk-devbind.py --status
sudo dpdk-devbind.py --bind=vfio-pci 0000:04:00.0
sudo dpdk-devbind.py --status
```

## 4.2 app 配置准备

在 `coin_master` 配置文件（如 `app/config/experiments/coin_master_correctness.json`）中，至少配置：

1. `acquisitionControl.acquisitionAlgorithm`（`socket` 或 `dpdk`）
2. `acquisitionControl.dpdkCopyThreadNum`
3. `acquisitionControl.dpdkRxRingsPerPort`
4. `acquisitionControl.dpdkMbufDoublePointerSizeMultiply`
5. `acquisitionControl.dpdkMbufDoublePointerNumMultiply`
6. `acquisitionControl.dpdkBindIps`（`acquisitionAlgorithm=dpdk` 时必填）
7. `acquisitionControl.detectorSources[]`、`destinationIp`、端口规划
8. （推荐）`acquisitionControl.nodeOverrides[]` 做节点级覆盖

并确保发包端（`app_udp_raw_replayer`）端口与采集控制下发端口匹配。

## 4.3 配置示例（关键字段）

```json
{
   "acquisitionControl": {
      "enabled": true,
      "acquisitionAlgorithm": "dpdk",
      "dpdkCopyThreadNum": 8,
      "dpdkRxRingsPerPort": 1,
      "dpdkMbufDoublePointerSizeMultiply": 32,
      "dpdkMbufDoublePointerNumMultiply": 2,
      "dpdkBindIps": ["10.10.1.10", "10.10.1.11"]
   }
}
```

说明：
1. 当 `acquisitionAlgorithm=dpdk` 时，`dpdkBindIps` 不能为空，且必须是有效 IPv4。
2. `dpdkBindIps` 应填写 DPDK 绑定网卡对应的数据面 IP，而不是管理面 IP。
3. 若未启用 DPDK 构建，节点在启动阶段会明确报错。

### 4.3.1 节点级覆盖（nodeOverrides）

可在全局配置基础上，对指定 nodeId 覆盖参数。

覆盖字段：

1. `nodeId`：目标节点 ID（必须与采集节点注册 ID 一致）
2. `acquisitionAlgorithm`：`inherit`、`socket`、`dpdk`
3. `dpdkCopyThreadNum`：大于 0 时覆盖
4. `dpdkRxRingsPerPort`：大于 0 时覆盖
5. `dpdkMbufDoublePointerSizeMultiply`：大于 0 时覆盖
6. `dpdkMbufDoublePointerNumMultiply`：大于 0 时覆盖
7. `dpdkBindIps`：非空时覆盖

示例：

```json
{
   "acquisitionControl": {
      "acquisitionAlgorithm": "dpdk",
      "dpdkBindIps": ["10.10.1.10", "10.10.1.11"],
      "nodeOverrides": [
         {
            "nodeId": "acq-r2s-node-0",
            "acquisitionAlgorithm": "dpdk",
            "dpdkCopyThreadNum": 12,
            "dpdkRxRingsPerPort": 2,
            "dpdkBindIps": ["10.10.1.10"]
         },
         {
            "nodeId": "acq-r2s-node-1",
            "acquisitionAlgorithm": "inherit",
            "dpdkBindIps": ["10.10.1.11"]
         }
      ]
   }
}
```

覆盖规则：

1. 先应用全局 `acquisitionControl`。
2. 节点命中 `nodeOverrides` 时，再应用节点覆盖。
3. 最终算法为 `dpdk` 时，最终 `bind_ips` 必须非空且为合法 IPv4。

## 4.4 启动顺序（单机联调）

建议顺序：

1. 启动 `app_coin_master`。
2. 启动一个或多个 `app_acq_r2s_node`。
3. 最后启动 `app_udp_raw_replayer` 回放 raw 文件发包。

示例（复用现有 raw 回放参数风格）：

```bash
./bin/app_udp_raw_replayer \
  --raw-path "Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw" \
  --source-ip 127.0.0.1 --destination-ip 127.0.0.1 \
  --source-port-base 17100 --destination-port-base 18100 \
  --channel-count 4 --channel-offset 0 --max-segments 80 --inter-packet-us 2 --repeat 1
```

## 5. 排障清单

## 5.1 典型报错与定位

1. 报错：`DPDK runtime requested but this build does not enable DPDK`
   - 含义：程序构建时未启用 DPDK 宏或相关依赖未正确链接。
2. 报错：`DPDK initialization failed: ...`
   - 常见原因：网卡未绑定、HugePages 未配置、`bind_ips` 与实际环境不匹配。
3. `testpmd` 显示 `No probed ethernet devices`
   - 常见原因：没有可用 DPDK 端口（仍被内核驱动占用，或驱动/网卡不匹配）。

## 5.2 最小可行自检

```bash
dpdk-devbind.py --status
dpdk-hugepages.py -s
dpdk-testpmd --no-pci --no-huge -- --total-num-mbufs=2048 --nb-cores=1
```

满足以下条件后，再做业务联调：

1. DPDK 工具链可用。
2. HugePages 就绪。
3. 至少一个目标网卡端口被 DPDK 驱动接管。

## 6. 是否需要提供 DPDK 一键配置脚本（假设 DPDK 已安装）

结论：建议提供，但不建议“完全无确认的一键到底”，应提供“半自动、可回滚、分阶段”的脚本。

原因：

1. 真实场景中，网卡绑定是高风险操作。
   - 绑定到 `vfio-pci` 后，原网络连接可能中断（尤其远程 SSH 场景）。
2. 不同机器的网卡型号、PCI 地址、NUMA、IOMMU 状态差异很大。
3. HugePages 配置可能与机器内存压力、其他业务冲突。

推荐做法：

1. 提供 `dpdk_precheck.sh`
   - 只读检查，不改系统。
   - 输出是否满足：工具、HugePages、vfio、IOMMU、候选网卡。
2. 提供 `dpdk_apply.sh`
   - 显式参数化：PCI 地址、HugePages 数量、页大小。
   - 执行前二次确认。
   - 记录变更日志。
3. 提供 `dpdk_rollback.sh`
   - 将网卡切回原内核驱动。
   - 清理或恢复 HugePages 配置。

建议优先级：

1. 先补 `precheck` 与 `rollback`。
2. 再补 `apply`。
3. 最后再考虑“全自动一键”，且仅用于本地控制台场景，不建议默认用于远程环境。

## 7. 真实分布式场景的配置建议

当前参数模型可运行，但在真实分布式场景建议进一步细化：

1. 建议将 DPDK 参数按节点分组，而不是全局一套。
   - 例如 B1/B2 网卡型号、NUMA、核数不同，`dpdkCopyThreadNum` 与 `dpdkRxRingsPerPort` 可能需要不同值。
2. `dpdkBindIps` 建议与节点配置模板联动，避免在主控配置中硬编码跨机器 IP。
3. 已支持节点级 CPU/NUMA 约束配置（在 `app_acq_r2s_node` 的 `runtime` 段），用于高吞吐稳定性优化。
4. 已支持节点侧 `bind_ips` 本机网卡归属检查，降低配置漂移风险。

### 7.1 节点侧高负载约束（已实现）

在 `acq_r2s_node` 配置中，可使用 `runtime` 字段控制约束：

```json
{
   "runtime": {
      "enableCpuAffinity": true,
      "cpuAffinityCores": [2, 3, 4, 5],
      "strictBindIpsOwnershipCheck": true,
      "strictNumaTopologyCheck": true,
      "requireBindIpsSingleNuma": true,
      "requireCpuAffinityOnNuma": true,
      "expectedNumaNode": 0
   }
}
```

约束语义：

1. `enableCpuAffinity=true`：节点进程启动时执行 `sched_setaffinity`。
2. `strictBindIpsOwnershipCheck=true`：配置下发的 `dpdkBindIps` 必须命中本机网卡 IPv4，否则配置失败。
3. `strictNumaTopologyCheck=true`：启用 NUMA 拓扑校验。
4. `requireBindIpsSingleNuma=true`：要求所有 `bind_ips` 落在同一 NUMA 节点。
5. `requireCpuAffinityOnNuma=true`：要求 `cpuAffinityCores` 与 `bind_ips`（或 `expectedNumaNode`）NUMA 一致。
6. `expectedNumaNode>=0`：显式指定目标 NUMA 节点；为 `-1` 表示不强制指定。

可行落地方案：

1. Phase-1（低风险）：保留当前全局配置，新增节点级覆盖配置（可选）。
2. Phase-2（中风险）：在模板生成阶段（three/four_machine）按节点自动渲染 `dpdkBindIps` 与 DPDK 参数。
3. Phase-3（高性能）：引入 NUMA/CPU 亲和参数并在节点侧执行约束检查。
