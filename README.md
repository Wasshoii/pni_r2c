# R2C: Distributed PET Raw-to-Coin Pipeline

## 1. 项目简介
R2C 是一个面向 PET 场景的分布式数据处理项目，覆盖以下主链路：

RawData 采集 -> R2S 单事件转换 -> 流式符合计算 -> 结果输出

项目同时提供：
- 采集控制（master/node）
- 本地 gRPC R2S 与符合接收链路
- 分布式时钟同步（timesync）
- 单元/集成测试与端到端测试

## 2. 快速开始
### 2.1 构建与测试
构建统一使用 CMake + build.sh，Makefile 仅保留测试运行入口。

```bash
# 一键构建 app/test/tools
./build.sh --all

# 仅构建测试
./build.sh --tests
```

```bash
# 查看测试入口
make help

# 基础测试
make test
make test-grpc

# 完整链路（需 OpenPnI/CUDA/TBB）
make test-local-grpc-r2s
make test-local-grpc-coin
make test-acq-control-init
make test-acq-datapath-udp
make test-acq-r2s-pipeline
```

更详细的构建说明见 docs/BUILD_AND_SETUP.md。

### 2.2 依赖
- 基础：g++, grpc, protobuf
- 完整：OpenPnI, CUDA Toolkit, TBB

## 3. 当前代码结构
详见 `PROJECT_STRUCTURE.md`。核心目录如下：

- `include/core`: 核心处理接口（acquisition/r2s/streaming）
- `include/grpcNode`: 节点侧封装（acquisition/r2s/coin）
- `include/grpcService`: 服务侧封装（master/coin/timesync）
- `src/core`: 核心实现
- `src/grpcNode`: 节点实现
- `src/grpcService`: 服务实现
- `tests`: 测试程序
- `protos`: protobuf/gRPC 定义与生成文件

## 4. 代码编写规范（项目约定）
以下规范用于降低编译耦合、提高可维护性，并与当前代码重构方向一致。

### 4.1 分层与依赖方向
- 依赖方向固定为：`core <- grpcService <- grpcNode <- tests/app`。
- `core` 不依赖 `grpcNode`。
- `include` 中的公共头禁止包含与本层无关的实现细节。

### 4.2 头文件与实现文件
- 新增类默认采用 `.hpp` 声明 + `.cpp` 实现。
- 头文件只保留必要声明，重逻辑下沉到 `.cpp`。
- 优先使用前置声明与 PImpl，减少 gRPC/protobuf/OpenPnI 重头传播。
- 不在头文件放复杂业务逻辑（模板必要逻辑除外）。

### 4.3 Include 规则
- 使用项目内统一路径：`core/...`、`grpcNode/...`、`grpcService/...`。
- include 顺序建议：
  1) 本文件对应头
  2) 第三方库头
  3) 标准库头
  4) 项目内其他头
- `.cpp` 必须显式包含自己依赖的标准库头，禁止依赖“传递包含”。

### 4.4 命名与风格
- 类型名使用 `UpperCamelCase`。
- 函数名使用 `lowerCamelCase`。
- 变量名使用 `snake_case` 或 `lowerCamelCase`，以模块内现有风格为准，避免混用。
- 常量使用 `kName` 或全大写宏；优先 `constexpr`，少用宏。

### 4.5 并发与资源管理
- 线程、锁、流对象必须 RAII 管理。
- 跨线程共享状态使用 `std::atomic` 或明确的互斥保护。
- 停止逻辑必须可重入（重复 stop 不崩溃）。
- 禁止裸 `new/delete`（除第三方 API 强制场景），优先智能指针。

### 4.6 错误处理与日志
- 运行时错误优先返回 `bool/Status` + 清晰日志。
- 不吞异常；catch 后至少记录上下文。
- 日志必须包含模块前缀与关键标识（node id、chunk id、segment id）。

### 4.7 Protobuf/gRPC 变更
- 修改 `.proto` 后必须同步检查生成文件与调用方。
- 协议字段新增优先向后兼容，不随意复用字段语义。
- 节点控制命令优先使用结构化字段，不依赖字符串参数。

### 4.8 测试要求
- 新增或修改链路逻辑时，至少补 1 个对应测试。
- 优先覆盖：
  - 编译/链接完整性
  - 状态机切换
  - 超时/异常分支
  - 端到端最小可运行路径
- 提交前至少执行受影响目标的构建与测试（`./build.sh --tests` + 对应 `make test-*`）。

## 5. 文档说明
- 本 README 只保留项目总览与开发约定。
- 模块细节、目录映射见 `PROJECT_STRUCTURE.md`。
- 设计与协议细节见 `docs/` 下专题文档。
