# App JSON 配置说明

本文档说明 pni_r2c/app/config 下 app 所需 JSON 配置字段的含义与用法。

当前多机实验配置按 **cluster / dataplane / coincidence / source** 分层。示例见 `app/config/examples/` 与 `app/config/rdma_cluster/`。跨机步骤见 `docs/app以及实验配置/RDMA多机实验.md`。

`dataplane.requireRoce` 默认 **true**。无 RNIC 的本机联调需显式设 `requireRoce=false` 且 `forceInProcess=true`。

## acq_r2s_node.json（worker）

### cluster（覆盖 coinClient 连接字段）
- serverAddress: Coin gRPC 地址。
- nodeId: Coincidence 节点 ID（0..N-1）。
- nodeAddress: 本机上报地址（RoCE GID 选择参考）。
- channelCount: 本节点通道数。

### dataplane
- requireRoce: 为 true 时拒绝 InProcess 回退。
- forceInProcess: 强制同进程 memcpy（仅本机测试）。
- deviceName: RNIC 名，空则自动。
- gidIndex: GID 索引，-1 自动。
- txSlotCount: 本地 TX 槽数（0 用 RdmaWriteSender 默认 **2**）。device D2H 管线深等于该槽数。上机可 A/B `2` vs `4`；若 `waitForCredit` 变长则改回 2。不要把默认改成几十槽。TX 不用 hugepage。slotCount / slotBytes: 向 coin 请求的接收环尺寸（0 表示服务端默认，当前服务端忽略请求值）。

### source
- type: `synthetic` | `lsingle_replay` | `acquisition`（本阶段 acquisition 只留 StubRawIngress）。
- mode: `pairs`（默认，有限 prompt/delay 对）| `stream`（按时长边生成边发；必须 `singlesPerSec>0` 且 `runSeconds>0`）。
- lsinglePath: replay 时的 `.lsingle` 文件或目录（按 segment 发送，不一次读入全部）。
- promptPairs / delayPairs / delayTimePs: synthetic 配对公式；`stream` 下时间戳按同一公式递增。
- singlesPerSec / pushChunkSingles: 墙钟节流与分块；0 表示尽快发（仅 `pairs`）。
- rateJitterFraction: `[0,1]`，节流睡眠乘随机因子。
- startDelayMs: Start 之后、发数之前的墙钟等待（节点错开）。
- pauseAfterMs / pauseDurationMs: worker 本地停发脉冲（RDMA 不断）；0 关闭。
- runSeconds: `stream` 发送时长；replay 时也可作为提前结束。
- peerNodeId / localChannel / peerChannel: 双节点 synthetic 配对。

### rawIngress
- enabled: 仅 `source.type=acquisition` 时有意义。本阶段启动 stub、不产生 raw。

### acqNode（采集预留，synthetic/replay 不用）
- masterAddress: 采集控制主控地址（gRPC）。
- nodeId: 采集节点 ID（用于日志和业务标识）。
- nodeAddress: 节点对外上报码/对齐使用的地址。
- outputRoot: 单盘 rawdata 输出根目录（outputRoots 为空时使用）。
- outputRoots: 多盘输出根目录数组，非空时启用分盘写入。
- shardStrategy: 分片策略，支持 RoundRobin / HashByChannel / FreeSpaceAware。
- manifestFilename: 分盘写入的 manifest 文件名（保存于会话目录）。
- sessionNamePrefix: 会话目录前缀。
- maxFileSizeMb: 单文件分卷大小阈值（MB）。
- reservedStorageGiB: 磁盘保留空间（GiB）。
- statusIntervalMs: 状态上报间隔（毫秒）。
- enableRawFileWrite: 是否写 raw 文件。false 时仅走内存回调链路。
- asyncQueueDepth: 异步队列深度（分盘写入时每节点总队列深度）。
- writerThreadsPerShard: 每个分片的写线程数。
- useSpillToDisk: 队列满时是否允许落盘溢写（true 时不阻塞）。
- failOnQueueFull: 队列满时是否阻塞/报错（true 时优先保证一致性）。
- fsyncEachSegment: 每段写入后是否 fsync。

### r2s
- calibrationDir: 标定文件目录。
- resultDir: R2S 输出目录。
- channelIndices: 本节点采集通道索引列表。
- sortDataByTime: 是否在 libpni 段内排序之外再按时间排序（默认 false；50100 段内已排序）。
- saveData2SingleFile: 是否写到单文件。
- asyncFileWrite: R2S 写盘是否异步。

### bridge
- enabled: 是否启用 RawData 到 R2S 的异步桥接。
- queueCapacity: 队列容量（slot 数）。
- reservePacketsPerSlot: 每个 slot 预留包数。
- reserveBytesPerSlot: 每个 slot 预留字节数。
- blockWhenQueueFull: 队列满时是否阻塞写入。
- queueFullWarnEvery: 队列满警告间隔。
- inputChannelCount: 进入桥接的通道数。

### coinClient
- enabled: 是否启用 CoincidenceClient。
- serverAddress: Coin 服务地址。
- nodeId: Coin 节点 ID。
- nodeAddress: Coin 节点地址。
- channelCount: 通道数。
- detectorType: 探测器类型（BDM2 / BDM50100）。
- remapLocalToGlobalChannels: 是否重映射通道。
- globalChannelOffset: 全局通道偏移。
- crystalsPerChannel: 每通道晶体数。
- maxPendingChunks: JSON 兼容字段；热路径不再作为发送队列深度（`pend` 为本地 TX busy / `txSlotCount`）。
- batchSize: 批量发送大小。
- heartbeatIntervalMs: 心跳间隔（默认 1000 ms）。主控通过心跳下发 Pause/Stop 并收集速率/缓冲。
- waitForStartSignal: 是否等待启动信号。
- waitForStartTimeoutMs: 启动等待超时。
- waitForStartRpcTimeoutMs: RPC 等待超时。
- waitForStartRetryIntervalMs: 重试间隔。

### runtime
- shutdownGraceMs: 退出前等待时间。
- enableCpuAffinity: 是否绑定 CPU 亲和性。
- cpuAffinityCores: 绑定的 CPU core 列表。
- strictBindIpsOwnershipCheck: 严格检查 DPDK bind IP。
- strictNumaTopologyCheck: 严格 NUMA 拓扑检查。
- requireBindIpsSingleNuma: 要求 bind IP 在单 NUMA。
- requireCpuAffinityOnNuma: 要求 CPU 亲和性与 NUMA 对齐。
- expectedNumaNode: 指定期望的 NUMA node。

## 使用示例

1) 单盘写 rawdata

- 将 outputRoots 留空或不填，使用 outputRoot。

2) 分盘写 rawdata

- 设置 outputRoots 为多个磁盘根目录。
- 配置 shardStrategy 与 manifestFilename。

示例片段：

```json
{
  "acqNode": {
    "outputRoot": "Data/raw_data",
    "outputRoots": ["/mnt/ssd1/raw", "/mnt/ssd2/raw"],
    "shardStrategy": "RoundRobin",
    "manifestFilename": "session_manifest.jsonl",
    "enableRawFileWrite": true
  }
}
```

## coin_master.json（coincidence-only）

### cluster
- listenAddress: 符合服务监听地址。
- expectedNodeCount: 预期 worker 数。Start 条件为 `registered==N && dataplane_open==N`。

### dataplane
- 同 worker：`requireRoce` / `deviceName` / `gidIndex` / slot 参数。

### coincidence
- detectorProfile: `BDM2` 或 `BDM50100_9120`。
- outputDir: LMF 输出目录。
- savePrompt / saveDelay: 是否写 prompt/delay LMF。极限 soak/rate 剖面可关，去掉写盘。
- protocol: timeWindowPs / delayTimePs / energyLowerEV / energyUpperEV。

### runtime
- runSeconds: 0 表示等到 `allProducersComplete` 或 SIGINT。
- statusIntervalMs: 状态打印间隔。

### acquisitionControl
- **本阶段忽略**。若 `enabled=true` 会 warn 并强制关闭。采集主控不再是 coin 的默认职责。

旧 `coinMaster` / `aligner` 段仍可解析，便于过渡。

备注：环境变量仍可作为覆盖手段（如 PNI_R2C_RAW_OUTPUT_ROOTS/PNI_R2C_RAW_SHARD_STRATEGY），但建议以 JSON 为主配置来源。
