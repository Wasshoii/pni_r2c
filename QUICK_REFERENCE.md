# PNI R2C 快速参考卡片

## 🚀 启动命令

```bash
# 最快方式（推荐）
./quick-start.sh

# 手动步骤
chmod +x setup-environment.sh
./setup-environment.sh --verify    # 检查
make test                           # 编译
./bin/test_distributed_clock_sync   # 运行
```

## 📋 关键文件

| 文件 | 用途 |
|------|------|
| `setup-environment.sh` | 自动配置脚本 |
| `quick-start.sh` | 快速启动脚本 |
| `.env.example` | 环境变量配置模板 |
| `SETUP_GUIDE.md` | 详细安装指南 |
| `README.md` | 项目概览 |

## 🔧 常用命令

```bash
# 编译相关
make clean                          # 清理编译
make test                           # 编译测试
make test CXXFLAGS="-std=c++17 -O3" # 性能编译

# 测试相关
./bin/test_distributed_clock_sync   # 运行所有测试
./bin/test_distributed_clock_sync 2>&1 | grep "✓"  # 查看通过的测试

# 诊断相关
./setup-environment.sh --verify     # 验证配置
./setup-environment.sh --verbose    # 详细诊断
cat .setup-report.txt              # 查看诊断报告
```

## 📚 文档导航

```
快速参考
├── 本文件：快速命令和链接
└── 

新手入门
├── SETUP_GUIDE.md      → 详细安装步骤
├── README.md           → 项目概览
└── docs/README_CLOCK_SYNC.md  → 时钟同步快速参考

集成开发
├── docs/DISTRIBUTED_CLOCK_SYNC_INTEGRATION.md  → 详细集成指南
├── docs/IMPLEMENTATION_SUMMARY.md              → 实现细节
└── tests/QUICK_INTEGRATION_GUIDE.cpp           → 集成代码示例

维护和调试
├── PROJECT_STRUCTURE.md  → 目录结构说明
├── .setup-report.txt     → 系统诊断报告
└── Makefile              → 构建配置
```

## ✅ 验证清单

### 系统检查
- [ ] GCC 版本 ≥ 9.0
- [ ] C++17 支持可用
- [ ] Make 版本 ≥ 4.0
- [ ] 磁盘空间 ≥ 50MB
- [ ] 内存 ≥ 2GB

### 依赖检查
- [ ] gRPC 头文件存在（include/grpc**）
- [ ] gRPC 库文件存在（lib/*.a，≥100个）
- [ ] OpenSSL 已安装
- [ ] 项目目录结构完整

### 编译检查
- [ ] `make clean` 成功
- [ ] `make test` 完成无错误
- [ ] bin/test_distributed_clock_sync 可执行

### 测试检查
- [ ] Test 1: Basic Clock Sync ✓
- [ ] Test 2: Multi-Client Alignment ✓
- [ ] Test 3: Segment Merging ✓
- [ ] Test 4: Event Calibration ✓

## 🐛 常见问题速解

| 问题 | 快速解决 |
|------|---------|
| `g++: command not found` | `sudo apt install g++` |
| `grpcpp/grpcpp.h not found` | `./setup-environment.sh --verbose` |
| `undefined reference to libgrpc` | 检查 `lib/` 目录，运行 `make clean && make test` |
| `C++17 not supported` | 升级 GCC：`gcc-9` 或更高版本 |
| 编译缓慢 | 使用 `-O3`：`make CXXFLAGS="-std=c++17 -O3"` |
| 测试输出乱码 | 设置 locale：`export LANG=zh_CN.UTF-8` |

## 📊 文件大小参考

```
configure scripts:      ~2-3 MB
include/                ~50-100 MB (gRPC headers)
lib/                    ~2-3 GB (gRPC static libs)
src/                    ~10-20 KB
tests/                  ~10-20 KB
docs/                   ~50-100 KB
build/                  ~50-100 MB (after make)
bin/test_distributed_clock_sync  ~2-5 MB (after make)
```

## 🔗 重要路径

```
项目路径
└── /media/ustc-pni/5282FE19AB6D5297/pni_grpc/r2c

gRPC 源代码
└── /media/ustc-pni/5282FE19AB6D5297/pni_grpc/grpc

gRPC 编译输出
└── /media/ustc-pni/5282FE19AB6D5297/pni_grpc/grpc/cmake/build

PNI 核心库（如需）
└── /media/ustc-pni/5282FE19AB6D5297/...
```

## 💡 性能优化

```bash
# 启用全部优化
./setup-environment.sh --verify
make clean
make test \
  CXXFLAGS="-std=c++17 -Wall -Wextra -O3 -march=native -flto" \
  LDFLAGS="-fuse-ld=gold $LDFLAGS"

# 快速编译（调试）
make test CXXFLAGS="-std=c++17 -g -O0"

# 并行编译（CMake）
cmake --build . --parallel $(nproc)
```

## 🔐 安全性检查

```bash
# 检查依赖项是否最新
openssl version
ldd bin/test_distributed_clock_sync | grep -E "ssl|crypto"

# 验证编译选项
file bin/test_distributed_clock_sync
strings bin/test_distributed_clock_sync | grep -i "optimization"
```

## 🎯 核心概念速查

```cpp
// 客户端使用
#include "include/timesync/TimeSyncClient.hpp"
auto client = TimeSyncClient(node_id, "server:50051");
client.StartSync();
uint64_t corrected_time = client.GetCorrectedTimeNs();

// 服务器使用
#include "include/timesync/TimeSyncServer.hpp"
TimeSyncServer server;
uint64_t server_time;
int64_t offset;
server.SyncClock(client_id, hostname, client_time, 
                 server_time, offset, network_delay);

// 合并应用
#include "include/timesync/DistributedClockSyncManager.hpp"
auto calib = ClockCalibrationUtil::CalibrateSegmentTime(
    start, end, client_id, offset_ns);
bool merge = ClockCalibrationUtil::ShouldMerge(seg1, seg2, tolerance);
```

## 📝 环境变量

```bash
# 查看示例
cat .env.example

# 加载配置
source .env

# 常用变量
export CXX=g++
export CXXFLAGS="-std=c++17 -Wall -Wextra -O2"
export GRPC_PATH="/media/ustc-pni/5282FE19AB6D5297/pni_grpc/grpc"
```

## 🆘 获取帮助

```bash
# 脚本帮助
./setup-environment.sh --help
./quick-start.sh --help

# 查看完整指南
cat SETUP_GUIDE.md
cat README.md
cat docs/DISTRIBUTED_CLOCK_SYNC_INTEGRATION.md

# 生成诊断报告
./setup-environment.sh --verbose
cat .setup-report.txt

# 查看编译日志
make test 2>&1 | head -50
cat /tmp/compilation.log
```

## 📅 版本信息

- **项目版本**: 1.0
- **脚本版本**: 1.0
- **更新日期**: 2026年1月4日
- **GCC 要求**: 9.0+
- **C++ 标准**: C++17

---

**💬 最常用的三个命令：**

```bash
./quick-start.sh          # 新手：一键启动
./setup-environment.sh    # 诊断和修复问题
make test                 # 快速编译测试
```

**🎓 建议学习顺序：**

1. 运行 `./quick-start.sh` - 了解整体流程
2. 阅读 `README.md` - 理解项目架构
3. 查看 `SETUP_GUIDE.md` - 深入了解配置
4. 学习 `docs/` 中的集成指南 - 开始集成代码

**✨ 预期效果：**

```
✓ 编译成功
✓ 4个测试全部通过
✓ 输出: "All tests completed successfully!"
✓ 准备好进行集成开发
```
