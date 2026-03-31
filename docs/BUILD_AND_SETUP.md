# 编译与构建

## 文档范围
本文档仅说明 `r2c` 项目自身的编译与构建流程。

OpenPnI 本体的环境准备、`config.py` 配置和 `build.py` 安装流程请参考 OpenPnI 文档（`pni-standard-project/docs/项目介绍/编译与构建.md`）。

## 前置条件（本项目增量）
- 已完成 OpenPnI 安装，并且 `pkg-config` 可发现 `libpni`。
- 已安装本项目需要的 gRPC/Protobuf 开发包。
- 当前仓库根目录为 `r2c/`。

### Ubuntu 24.04：安装 gRPC/Protobuf 开发包

```bash
sudo apt update
sudo apt install -y \
	build-essential \
	pkg-config \
	libprotobuf-dev \
	protobuf-compiler \
	protobuf-compiler-grpc \
	libgrpc-dev \
	libgrpc++-dev
```
**glog 日志库（0.4.0+）：**
```bash
sudo apt-get install libgoogle-glog-dev libgflags-dev
```

安装后建议执行以下检查：

```bash
protoc --version
which grpc_cpp_plugin
pkg-config --modversion protobuf
pkg-config --modversion grpc++
pkg-config --cflags --libs grpc++ protobuf
pkg-config --modversion libglog
```

说明：
- 本项目 `Makefile` 默认使用 `PKG_CONFIG_PATH_OVERRIDE=/usr/lib/x86_64-linux-gnu/pkgconfig` 获取 gRPC/Protobuf 配置。
- 如你使用非标准库路径，可在构建时覆盖：`make all-full SYSTEM_LIB_DIR=/your/system/lib/dir`。

建议先做一次环境自检：

```bash
pkg-config --cflags --libs libpni
pkg-config --cflags --libs grpc++ protobuf
which protoc
```

## 快速开始

```bash
# 查看全部目标
make help

# 基础构建（不依赖 PNI/CUDA）
make

# 完整构建（含 PNI/CUDA 相关目标）
make all-full
```

## 常用构建目标
- `make`：构建基础测试程序（无 PNI/CUDA 依赖）。
- `make all-full`：构建完整目标集（含 PNI/CUDA 相关程序）。
- `make test`：运行基础模拟测试。
- `make test-grpc`：运行 gRPC 时钟同步测试。
- `make test-streaming`：运行流式符合测试。
- `make test-pni-r2c`：运行 PNI R2C 测试。
- `make test-local-grpc-r2s`：运行本地 gRPC R2S 测试。
- `make test-local-grpc-coin`：运行本地 gRPC Coin 接收测试。
- `make clean`：清理构建产物。
- `make clean-proto`：清理 proto 生成文件。

## Makefile 关键配置

### 1. PNI 依赖解析策略
本项目采用“`pkg-config libpni` 优先，`PNI_PROJECT_PATH` 回退”的策略：
- 默认：通过 `pkg-config --cflags/--libs libpni` 获取编译和链接参数。
- 回退：当 `pkg-config` 不可用时，使用 `PNI_PROJECT_PATH` 下的 `include` 与 `build`。

相关变量：
- `PNI_PKG_NAME`（默认 `libpni`）
- `PNI_PROJECT_PATH`（仅回退模式使用）

### 2. gRPC/Protobuf 链接优先级
为避免系统环境混装导致 `-lprotobuf` 误选到 `/usr/local/lib/libprotobuf.a`（进而触发 `absl` 未定义符号），Makefile 在链接阶段会优先加入系统库目录：
- `SYSTEM_LIB_DIR`（默认 `/usr/lib/x86_64-linux-gnu`）

如你的发行版路径不同，可覆盖：

```bash
make all-full SYSTEM_LIB_DIR=/your/system/lib/dir
```

### 3. CUDA 路径
- `CUDA_PATH` 默认 `/usr/local/cuda`。
- 如 CUDA 安装在其他位置，可覆盖：

```bash
make all-full CUDA_PATH=/opt/cuda
```

## 典型构建流程

```bash
# 1) 清理
make clean

# 2) 必要时清理并重新生成 proto
make clean-proto

# 3) 完整构建
make all-full
```

## 常见问题

### 1) `undefined reference to absl::...`（链接阶段）
通常是 `libprotobuf` 选到了 `/usr/local/lib/libprotobuf.a`，但未链接对应 absl 静态库。

处理方式：
- 优先使用系统动态库路径（默认已配置 `SYSTEM_LIB_DIR`）。
- 若仍异常，显式指定：

```bash
make all-full SYSTEM_LIB_DIR=/usr/lib/x86_64-linux-gnu
```

### 2) Protobuf 版本/头文件不一致
若出现 `.pb.cc` 相关命名空间或类型错误，建议先清理并重生 proto：

```bash
make clean-proto
make all-full
```

### 3) NVCC 标准支持问题
OpenPnI 相关代码是 C++23 + CUDA 混合链路，`nvcc` 侧应保持 C++20（Makefile 已按此配置）。

## 说明
`make all-full` 只保证“编译与链接”通过。部分测试目标在运行阶段仍可能因数据文件内容、路径或格式不匹配而失败，这属于运行时数据问题，不属于构建问题。
