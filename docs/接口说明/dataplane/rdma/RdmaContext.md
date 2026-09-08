# RdmaContext

> #include<dataplane/rdma/RdmaContext.hpp>

## 目的

verbs 设备与单连接 RC QP 的薄封装。[RdmaWriteSender](RdmaWriteSender.md) / [RdmaRecvServer](RdmaRecvServer.md) 在 RoCE 路径使用；无设备时上层走 InProcess，不强制打开本模块。

命名空间：`openpni::distributed::dataplane::rdma`。

## 核心接口

### RdmaDevice

```cpp
static bool hasVerbsDevice();
static std::vector<std::string> listDeviceNames();
bool open(const std::string &deviceName = "", int gidIndex = -1);
ibv_pd *pd() const;
bool queryGid(std::array<uint8_t, 16> *outGid, uint16_t *outLid) const;
int queryActiveMtu() const;
```

- `deviceName` 空：选第一个可用 IB 设备。
- `gidIndex < 0`：实现选择默认 GID。
- `hasVerbsDevice()` 供测试与 `requireRoce` 判断。

### RdmaConnection

拥有 CQ + RC QP；MR 单独 `registerMemory` / `deregister`。

```cpp
bool create(RdmaDevice &dev, int cqEntries = 1024, int maxSendWr = 256, int maxRecvWr = 128);
bool connectTo(const RdmaEndpointInfo &remote, uint8_t localPort, const std::array<uint8_t, 16> &localGid);
bool postWriteImm(...);
bool postRecv(uint64_t wrId);
int pollCq(RdmaWorkCompletion *out, int maxCompletions);
```

- QP 状态：Init → RTR → RTS（`connectTo` 或分步 `transitionTo*`）。
- 热路径发送用 `WRITE_WITH_IMM`；imm 携带 `seq` 的 32-bit wrap。payload WRITE 全部 signaled（TX 槽靠 CQ 回收）。credit WRITE 可由上层选择 unsignaled；`RdmaConnection` 跟踪全部 SQ 占用，接近 `maxSendWr` 时强制 signaled，避免 unsignaled 塞满发送队列。
- `RdmaWorkCompletion`：`wrId`、`immData`、`status`、`isRecv`、`hasImm`。

本页不描述 verbs 寄存器。失败时函数返回 false，由 sender/recv 记录日志。

## 使用提示

- 无 RNIC 时不要 `requireRoce`；编排测试用 `forceInProcess`。
- `registerMemory` 的地址必须来自已分配的 [HugepageArena](HugepageArena.md) 或等价页对齐缓冲。
