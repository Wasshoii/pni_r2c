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

## 快速开始（makefile计划后续移除，转为cmake）

```bash
# 查看全部目标
make help

# 基础构建（不依赖 PNI/CUDA）
make

# 完整构建（含 PNI/CUDA 相关目标）
make all-full
```

## CMake 迁移（tests/apps 分离架构）

当前仓库 CMake 预设已按“目标类型（tests/apps）+ 依赖层（core/pni/cuda）”分离，避免应用与测试混编。

如需手动指定 protoc / grpc plugin（跨环境常用）：

```bash
cmake -S . -B build/cmake \
-DCMAKE_BUILD_TYPE=Release \
-DR2C_PROTOC_EXECUTABLE=$(command -v protoc) \
-DR2C_GRPC_CPP_PLUGIN_EXECUTABLE=$(command -v grpc_cpp_plugin)
```

说明：
- 默认优先使用 `/usr/bin/protoc` 与 `/usr/bin/grpc_cpp_plugin`（与 Makefile 行为一致）。
- 默认 `R2C_PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig`（避免 protobuf/grpc 版本漂移）。
- 预设默认使用 `/usr/bin/g++-13`（与 Makefile 对齐）。

### Tests 预设（建议 CI 使用）

1. Core Tests（仅基础 gRPC/Protobuf）
- 目标：`test_distributed_clock_sync`、`test_distributed_clock_sync_grpc`

```bash
cmake --preset linux-release-tests-core
cmake --build --preset build-tests-core -j
ctest --test-dir build/tests/core -L core --output-on-failure
```

2. PNI Tests（libpni + tbb + openmp，无 CUDA）
- 目标：`test_streaming_coincidence`、`test_local_grpc_coin`、`test_acquisition_control_init`、`test_acquisition_datapath_udp`

```bash
cmake --preset linux-release-tests-pni
cmake --build --preset build-tests-pni -j
ctest --test-dir build/tests/pni -L pni -LE integration --output-on-failure
```

说明：`test_local_grpc_coin` 标记为 `integration`，如需运行请显式执行：

```bash
ctest --test-dir build/tests/pni -R test_local_grpc_coin --output-on-failure
```

3. CUDA Tests（nvcc + libpni + tbb + openmp）
- 目标：`test_pni_r2c`、`test_local_grpc_r2s`、`test_pni_coin`、`test_acquisition_r2s_pipeline`

```bash
cmake --preset linux-release-tests-cuda
cmake --build --preset build-tests-cuda -j
ctest --test-dir build/tests/cuda -L cuda -LE integration --output-on-failure
```

说明：`test_local_grpc_r2s` 标记为 `integration`，如需运行请显式执行：

```bash
ctest --test-dir build/tests/cuda -R test_local_grpc_r2s --output-on-failure
```

### Apps 预设（部署构建建议）

1. Basic Apps（无 CUDA）
- 目标：`app_coin_master`、`app_udp_raw_replayer`

```bash
cmake --preset linux-release-apps-basic
cmake --build --preset build-apps-basic -j
```

2. CUDA Apps
- 目标：`app_acq_r2s_node`

```bash
cmake --preset linux-release-apps-cuda
cmake --build --preset build-apps-cuda -j
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
