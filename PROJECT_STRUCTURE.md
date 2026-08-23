# PROJECT_STRUCTURE

## 1. 目录总览（当前）
```text
r2c/
├── include/
│   ├── core/
│   │   ├── acquisition/AcquisitionServer.hpp
│   │   ├── r2s/R2S.hpp
│   │   └── streaming/StreamingCoincidence.hpp
│   ├── grpcNode/
│   │   ├── acquisitionNode.hpp
│   │   ├── r2sNode.hpp
│   │   ├── coinNode.hpp
│   │   └── GrpcNodes.hpp
│   └── grpcService/
│       ├── AcquisitionMaster.hpp
│       ├── CoincidenceClient.hpp
│       ├── CoincidenceServiceImpl.hpp
│       ├── GrpcServices.hpp
│       └── timesync/
│           ├── TimeSyncCommon.hpp
│           ├── TimeSyncClient.hpp
│           ├── TimeSyncServer.hpp
│           ├── TimeSyncServiceImpl.hpp
│           └── DistributedClockSyncManager.hpp
├── src/
│   ├── core/
│   │   ├── acquisition/AcquisitionServer.cpp
│   │   ├── r2s/R2S.cpp
│   │   ├── streaming/StreamingCoincidence.cpp
│   │   └── merge-and-coin/MergeAndCoin.hpp
│   ├── grpcNode/
│   │   ├── acquisitionNode.cpp
│   │   ├── r2sNode.cpp
│   │   └── coinNode.cpp
│   ├── grpcService/
│   │   ├── AcquisitionMaster.cpp
│   │   ├── CoincidenceClient.cpp
│   │   └── CoincidenceServiceImpl.cpp
│   └── tools/
│       ├── SinglesProcess.cu
│       ├── SinglesProcess.hpp
│       └── testTool.hpp
├── protos/
├── tests/
├── docs/
├── Data/
└── README.md
```

## 2. 模块职责
### 2.1 core
- 采集、R2S、流式符合的核心处理逻辑。
- 尽量保持与具体 gRPC 节点编排解耦。

### 2.2 grpcService
- 服务端/控制端实现。
- 负责 master 控制、符合服务、timesync 服务侧能力。

### 2.3 grpcNode
- 节点侧封装与运行入口。
- 面向测试与实际部署流程，连接 core 与 grpcService。
- 当前已做头源分离和 PImpl 降耦，头文件以接口为主。

### 2.4 tools
- CUDA 单事件处理和测试辅助工具。

### 2.5 tests
- 按单元模块 / 通信 / 集成覆盖，见 `docs/测试/README.md`。

## 3. 依赖关系（维护要求）
推荐依赖方向：

`core <- grpcService <- grpcNode <- tests/app`

约束：
- `core` 不依赖 `grpcNode`。
- `include` 公共头不暴露不必要的实现细节。
- 第三方重依赖（grpc/protobuf/cuda）优先放到 `.cpp`。

## 4. 构建与产物
- 构建入口：`./build.sh` 与 CMake preset
- 中间产物：`build/`
- 可执行文件：`bin/`
- 测试说明：`docs/测试/README.md`

## 5. 文档维护规则
- 目录或模块职责变更时，同步更新本文件与 `README.md`。
- 文档以“当前可执行事实”为准，不保留历史迁移描述。
- 保持简洁：优先写结论、路径和命令，避免冗长叙述。
