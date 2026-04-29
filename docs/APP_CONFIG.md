# App JSON 配置说明

本文档说明 pni_r2c/app/config 下 app 所需 JSON 配置字段的含义与用法。

## acq_r2s_node.json

### acqNode
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
- sortDataByTime: 是否按时间排序后再处理。
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
- maxPendingChunks: 最大待发送 chunk 数。
- batchSize: 批量发送大小。
- heartbeatIntervalMs: 心跳间隔。
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

备注：环境变量仍可作为覆盖手段（如 PNI_R2C_RAW_OUTPUT_ROOTS/PNI_R2C_RAW_SHARD_STRATEGY），但建议以 JSON 为主配置来源。
