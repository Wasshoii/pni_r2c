# R2S

> #include<core/r2s/R2S.hpp>

## 目的

将一段 `openpni::RawDataView`（原始包视图）转换为 `openpni::Single` 序列。本头文件是分布式 worker 的 R2S 入口：配置、流式处理、离线批处理，以及采集线程到 R2S 线程的零拷贝租约桥。

晶体解码与校正算法在 libpni `ISingleGenerator` / `ConvergedR2S`（BDM2、BDM50100 等），本文不复述。`Single` 字段见 [CommonDataType](../../../../../pni-standard-project/docs/接口说明/core/CommonDataType.md)。通路总览见 [R2S到RDMA](../../通路/R2S到RDMA.md)。

命名空间：`openpni::distributed::r2s`。

## 核心接口

### DetectorType

```cpp
enum class DetectorType { BDM2, BDMBiD, BDM50100, BDM100100, Unknown };
```

- `BDMBiD` 已从 pni-core 移除，枚举保留仅为兼容。
- BDM50100 在 `enableMultiGpu==true` 时走 [R2S50100MultiGpuEngine](multi_gpu/R2S50100MultiGpuEngine.md)；其余类型走 libpni `ConvergedR2S`。

### SinglesReadyCallback / SinglesSpanReadyCallback

```cpp
using SinglesReadyCallback = std::function<bool(
    std::vector<Single> &&singles, uint64_t clock_ms, uint32_t duration_ms)>;

using SinglesSpanReadyCallback = std::function<bool(
    std::span<Single const> singles, uint64_t clock_ms, uint32_t duration_ms)>;
```

- `onSinglesSpanReady` 已设置时，`processR2S` / `R2SStreamProcessor` **优先**调用它。50100 多 GPU 默认路径上 span 可能是 **device 指针**；回调必须 `cudaMemcpy` D2H（例如 `CoincidenceClient::sendSingles`）或 `materializeSinglesOnHost`，不可对显存做 host `memcpy`。
- `span` 仅在回调返回前有效。回调内若异步使用（入队、跨线程发送），必须先拷贝。
- 返回 `false` 表示调用方要求停止后续处理。
- 可与 `saveData2SingleFile` 同时开启：一边写 `.lsingle`，一边流式送出。

worker 热路径设 `onSinglesSpanReady` → `CoincidenceClient::sendSingles`，见 [CoincidenceClient](../../grpcService/CoincidenceClient.md)。

### R2SProcessConfig

分布式与算法参数合一。常用字段：

- `rawdataPath` / `resultPath`：离线单文件输入与输出目录。目录批处理用 `processR2SDirectory`，不要把目录塞进 `rawdataPath`。
- `calibrationFiles`：按下标 = 全局通道号；未分配通道可为占位空串。
- `channelNums`：整机通道上界；`channelIndices` 非空则本节点只处理该集合，空则处理 `[0, channelNums)`。
- `detectorType`、`crystalsPerChannel`、`r2sResultIndex`：探测器与 `ConvergedR2S` 注册下标。
- `onSinglesReady` / `onSinglesSpanReady`：流式输出。
- `sortDataByTime`：默认 **false**。50100 段内排序由 libpni 完成；再开则在 host 上额外 H2D/sort/D2H。
- `computePipelineDepth`：多 GPU 在飞段数，默认 1（`processSegment` 内 submit 后立刻完成）。`AsyncRawDataToR2SBridge` 在配置为 1 时改用 `leaseQueueCapacity`。
- `saveData2SingleFile`、`asyncFileWrite`、`singlesMaxFileSizeBytes`：写盘；`maxFileSizeBytes==0` 表示不分卷。
- `enableMultiGpu` / `gpuIds` / `instancePerGpu`：仅 BDM50100。`gpuIds` 空则使用全部可见 GPU。
- `matchXTalkEnabled`、`timeWindow`、`timeShift`、`crossTalkEnabled`、能量窗等：传给 libpni generator，由 `create*Config` 工厂填默认值。

### 辅助函数

```cpp
bool isDevicePointer(const void *ptr);
std::vector<Single> materializeSinglesOnHost(std::span<Single const> singles);
std::vector<std::string> collectCalibrationFiles(
    const std::string &directory,
    const std::vector<std::string> &extensions = {},
    bool sortByName = true,
    const std::string &namePrefix = {},
    const std::string &nameSuffix = {});
openpni::interface::ISingleGenerator *createSingleGenerator(
    const R2SProcessConfig &config,
    uint16_t localIndex,
    uint16_t globalChannelIndex);
```

- `materializeSinglesOnHost`：若 span 已在 host 则拷贝；若在 device 则 D2H。用于 span 回调之后仍要持有数据。
- `createSingleGenerator` 返回的指针由 `R2SStreamProcessor` 持有并在 `cleanupGenerators` 释放；通道无关算法参数从完整 `R2SProcessConfig` 读取，不在函数内硬编码。

### R2SStreamProcessor

流式入口：一次 initialize，多段 `processSegment`。

```cpp
class R2SStreamProcessor {
public:
  explicit R2SStreamProcessor(const R2SProcessConfig &config);
  bool initialize(uint16_t inputChannelNum = 0);
  bool processSegment(
      const openpni::RawDataView &view,
      std::shared_ptr<void> inputKeepAlive = {});
  bool reopenOutput(const std::string &filePrefix);
  bool finalize();
};
```

- `initialize`：按 `channelIndices` 建 generator 或 50100 多 GPU 引擎，可选打开 `.lsingle` 输出。
- `processSegment`：转换本段 raw；结果经 span/vector 回调和/或写盘。
- `reopenOutput`：目录批处理切换输出前缀。
- `finalize`：刷写异步队列、释放 generator。

BDM50100 且 `enableMultiGpu` 时内部使用 `R2S50100MultiGpuEngine`（`submitView` + `nextLease`）。默认无 energy cut / 无外部 sort / 无通道 remap 时，`onSinglesSpanReady` 拿到 **device** span，回调返回前 output lease 有效（覆盖整段切槽 D2H）。H2D 优先对输入做 `cudaHostRegister`；失败则 R2S 线程 pin-bounce（槽数 = 流水深度），拷完即可归还采集包槽。`computePipelineDepth>1` 时 submit 后延迟 `next`，`finalize()` 排空在飞段。采集桥在未 bounce 时把 `RawDataLease` 交进 `processSegment` 的 keep-alive，直到对应段 H2D 完成。多卡吞吐缩放前提见 [R2S50100MultiGpuEngine](multi_gpu/R2S50100MultiGpuEngine.md)。

### RawDataLease / RawDataLeaseSpscRingQueue

```cpp
struct RawDataLease {
  openpni::RawDataView view{};
  std::function<void()> releaseFn;
  void release();
  void disarm();
};
```

- 持有采集 `RawDataView`；析构或 `release()` 时调用 `releaseFn` 归还包槽。
- `disarm()`：入队失败时取消归还，避免析构误 Release（调用方仍拥有 view）。

`RawDataLeaseSpscRingQueue`：单生产者 `tryPushLease`、单消费者 `tryConsumeOne`。队列满时 `tryPushLease` 会 `disarm` 后返回 `false`。

### AsyncRawDataToR2SBridge

采集线程只入队租约，不复制 packet bytes；独立线程跑 `R2SStreamProcessor`。

```cpp
bool start(uint16_t inputChannelNum = 0);
void setReleaseFn(ReleaseFn releaseFn);  // 须在 start 前
bool enqueueRawData(const openpni::RawDataView &view);
```

- `Config::leaseQueueCapacity` 建议 1～2（在途段数）。
- `blockWhenQueueFull==true`：反压等待；`false` 且 `dropWhenQueueFull` 时可丢段，丢弃仍归还包槽。
- 本阶段不描述 DPDK/Socket 采集实现。

### 离线批处理

```cpp
bool processR2S(const R2SProcessConfig &config);
bool processR2SDirectory(R2SProcessConfig config, const std::string &rawdataDir, ...);
R2SProcessConfig createBDM2Config(...);
R2SProcessConfig createBDM50100Config(...);
R2SProcessConfig createBDM50100_9120Config(..., uint16_t instrumentRingCount = 4);
```

- `processR2S`：读单个 raw 文件。
- `processR2SDirectory`：扫描 `pniRaw-<clock>.bin`，一次 initialize、按 `inputClock` 切换输出前缀。
- `createBDM50100_9120Config`：`channelNums = 48*3*instrumentRingCount`（默认 576）；本节点职责由 `channelIndices` 表达。校正目录按环落到 `calibrationFiles` 全局下标。

`AsyncSingleFileWriter`：独立线程写 `.lsingle`，`submit` 在队列满时返回 false。

## 典型流程

1. `createBDM50100Config` / `createBDM2Config` 填校正与通道。
2. 设置 `onSinglesReady`（或 span 回调）和/或写盘标志。
3. 流式：`R2SStreamProcessor::initialize` → 循环 `processSegment` → `finalize`。
4. 在线采集：`AsyncRawDataToR2SBridge::setReleaseFn` → `start` → 采集回调里 `enqueueRawData`。
5. 离线：`processR2S` 或 `processR2SDirectory`。

## 使用提示

- `RawDataView` 不拥有内存；桥接路径必须在 R2S 用完后 `release`，否则采集槽泄漏。
- span 回调与 `CoincidenceClient::sendSingles` 若跨线程，不要直接传 span，应物化或使用 `onSinglesReady` 的 `vector&&`。
- 校正文件下标必须与全局通道一致，否则 singles 的 `channelIndex` 与对端几何对不上。
