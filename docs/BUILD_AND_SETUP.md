# 编译与构建

## 文档范围
本文档仅说明 `r2c` 项目自身的编译与构建流程。

OpenPnI 本体的环境准备、`config.py` 配置和 `build.py` 安装流程请参考 OpenPnI 文档（`pni-standard-project/docs/项目介绍/编译与构建.md`）。

## 前置条件
- 已完成 OpenPnI 安装，并且 `pkg-config` 可发现 `libpni`。
- 已安装本项目需要的 gRPC/Protobuf 开发包。
- 当前仓库根目录为 `pni_r2c/`。

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

建议先做一次环境自检：

```bash
pkg-config --cflags --libs libpni
pkg-config --cflags --libs grpc++ protobuf
which protoc
```

## 快速开始
若无其他测试需求，建议使用 build.sh 作为唯一的构建方式，若想要单独编译某些可执行程序，请参考下一章节

```bash
# 一键构建 app/test/tools
./build.sh --all

# 仅构建app
./build.sh --apps

# 仅构建测试
./build.sh --tests

# 仅构建工具
./build.sh --tools

# 若使用gdb等工具时,需要进行调试编译，加入以下参数
./build.sh --debug
```
构建完成后，可执行的二进制文件会输出到`bin/`目录下

## CMake 编译 (建议仅在需要单独编译时使用)

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
- 目标：`test_streaming_coincidence`、`test_local_grpc_coin`、`test_acquisition_control_init`

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
- 目标：`test_pni_r2c`、`test_local_grpc_r2s`、`test_pni_coin`、`test_bdm50100_online_pipeline`

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
- 目标：`app_coin_master`、`app_udp_raw_replayer`、`tool_sharded_raw_merge`

```bash
cmake --preset linux-release-apps-basic
cmake --build --preset build-apps-basic -j
```

2. CUDA Apps
- 目标：`app_acq_r2s_node`、`tool_sharded_raw_merge`

```bash
cmake --preset linux-release-apps-cuda
cmake --build --preset build-apps-cuda -j
```

3. 单独构建分盘合并工具

```bash
cmake -S . -B build/tools -DCMAKE_BUILD_TYPE=Release \
	-DR2C_PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig \
	-DR2C_BUILD_TOOL_SHARDED_RAW_MERGE=ON
cmake --build build/tools -j --target tool_sharded_raw_merge
```

## 测试运行（Makefile 仅用于运行测试）

Makefile 不再负责编译，仅保留测试运行入口。

```bash
# 查看测试入口
make help

# 运行 core 测试
make test
make test-grpc

# 运行 PNI/CUDA 测试（非 integration）
make all-full

# 单项测试（示例）
make test-streaming
make test-pni-r2c
make test-acq-control-init
```

说明：以上 make 命令会转发到 tests/Makefile，运行前请确保已执行 `./build.sh --tests` 完成构建。

## IO 配置

项目的 IO 层已完全收敛到 pni 的 Latest 实现（不再有 V1 分支，无需/不再支持通过
`PNI_R2C_IO_BACKEND` 环境变量切换）。

RawData / Singles（R2S）/ Listmode（Coincidence）三类输出统一支持"文件写盘策略"配置，
包含两项：

- **分卷阈值（maxFileSizeMb）**：单个输出文件累计写入超过该大小（MB）后自动滚动到下一个
  序号文件（文件名形如 `{prefix}_{seq:04d}.{ext}`）。默认值 `0` 表示不分卷，行为与历史
  单文件写入完全一致。
- **是否覆盖已存在文件（overwriteExisting）**：默认 `true`。

对应的配置字段：

| 配置节 | 字段 | 作用 |
| --- | --- | --- |
| `acqNode` | `maxFileSizeMb` / `overwriteExisting` | 采集节点 RawData 输出分卷/覆盖策略 |
| `r2s` | `maxFileSizeMb` / `overwriteExisting` | R2S Singles（`.lsingle`）输出分卷/覆盖策略 |
| `aligner` | `maxFileSizeMb` / `overwriteExisting` | Coincidence Listmode（`prompt`/`delay`）输出分卷/覆盖策略 |

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


