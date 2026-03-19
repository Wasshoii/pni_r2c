# 项目文件结构说明

## 整体目录树

```
r2c/
├── protos/                           # Protocol Buffers 定义
│   └── timesync.proto                # gRPC 协议定义
├── include/                          # 头文件目录
│   └── timesync/                     # 分布式时钟同步模块
│       ├── TimeSyncCommon.hpp        # 公共工具和数据结构
│       ├── TimeSyncServer.hpp        # 同步服务器实现
│       ├── TimeSyncClient.hpp        # 同步客户端实现
│       └── DistributedClockSyncManager.hpp  # 集成接口
│
├── src/                              # 源代码目录
│   ├── core/                         # 核心处理模块
│   │   ├── raw2coin.cpp              # Raw转Single转换
│   │   ├── raw2coin01.cpp            # 替代实现版本
│   │   ├── MergeAndCoin.hpp          # 文件合并和符合处理
│   │   ├── R2S.hpp                   # Raw转Single接口
│   │   └── testTool.hpp              # 测试工具类
│   └── timesync/                     # 时钟同步实现源码（可选）
│       └── (后续可添加 .cpp 实现文件)
│
├── tests/                            # 测试文件
│   ├── test_distributed_clock_sync.cpp    # 完整测试套件
│   │   ├── TestBasicClockSync             # 基础同步测试
│   │   ├── TestMultiClientAlignment      # 多客户端对齐测试
│   │   ├── TestSegmentMergingWithCalibration  # 段合并测试
│   │   └── TestEventTimeCalibration      # 事件时间校正测试
│   └── QUICK_INTEGRATION_GUIDE.cpp   # 快速集成代码示例
│
├── docs/                             # 文档目录
│   ├── DISTRIBUTED_CLOCK_SYNC_INTEGRATION.md   # 详细集成指南
│   ├── IMPLEMENTATION_SUMMARY.md              # 实现总结
│   └── README_CLOCK_SYNC.md                   # 快速参考
│
├── Data/                             # 数据目录（数据文件）
│   ├── bdm2/                         # BDM2 数据
│   ├── bdmbid/                       # BDMBiD 数据
│   └── result/                       # 处理结果输出
│
├── .vscode/                          # VS Code 配置
│
├── build/                            # 编译输出目录（生成）
│   └── bin/                          # 可执行文件
│
├── Makefile                          # 构建脚本
│
└── README.md                         # 项目总体说明（建议新增）
```

## 文件分类详解

### 📦 include/grpcService/timesync/ - 时钟同步头文件
用于分布式采集系统的时钟同步功能。

| 文件 | 大小 | 用途 |
|-----|------|------|
| `TimeSyncCommon.hpp` | 2.3KB | 公共工具类和数据结构 |
| `TimeSyncServer.hpp` | 9.7KB | 同步服务器（服务端部署） |
| `TimeSyncClient.hpp` | 9.0KB | 同步客户端（采集端部署） |
| `DistributedClockSyncManager.hpp` | 7.2KB | 集成接口和工具函数 |
| `timesync.proto` | 1.6KB | gRPC 服务定义 |

**关键接口**:
```cpp
// 在采集代码中使用
TimeSyncClient client(node_id, "server:50051");
client->StartSync();
uint64_t corrected_time = client->GetCorrectedTimeNs();

// 在合并代码中使用
auto calib = ClockCalibrationUtil::CalibrateSegmentTime(...);
bool should_merge = ClockCalibrationUtil::ShouldMerge(seg1, seg2);
```

---

### 📁 src/core/ - 核心处理模块
原始数据处理、单事件转换和符合计算逻辑。

| 文件 | 用途 |
|-----|------|
| `raw2coin.cpp` | 主要的 Raw 转 Coin 处理程序 |
| `raw2coin01.cpp` | 替代实现版本 |
| `MergeAndCoin.hpp` | 多文件合并和符合计算（**关键**） |
| `R2S.hpp` | Raw 转 Single 转换接口 |
| `testTool.hpp` | 测试和调试工具 |

**集成点**:
- `raw2coin.cpp` 中调用 `TimeSyncClient::GetCorrectedTimeNs()` 获取同步时间
- `MergeAndCoin.hpp` 中应用 `ClockCalibrationUtil` 进行段级校正

---

### 🧪 tests/ - 测试文件
单机模拟和集成示例代码。

| 文件 | 包含 | 用途 |
|-----|------|------|
| `test_distributed_clock_sync.cpp` | 4个测试用例 | 验证时钟同步功能 |
| `QUICK_INTEGRATION_GUIDE.cpp` | 代码示例 | 快速集成参考 |

**运行方式**:
```bash
make test  # 编译并运行所有测试
./bin/test_distributed_clock_sync  # 直接运行
```

---

### 📖 docs/ - 文档
详细的集成指南和实现说明。

| 文件 | 读者 | 内容 |
|-----|------|------|
| `DISTRIBUTED_CLOCK_SYNC_INTEGRATION.md` | 集成工程师 | 系统架构、分步指南、配置说明 |
| `IMPLEMENTATION_SUMMARY.md` | 项目管理者 | 实现完成总结、性能指标、下一步 |
| `README_CLOCK_SYNC.md` | 所有用户 | 快速开始、常见问题、文件说明 |

**阅读建议**:
1. 新用户：先读 `README_CLOCK_SYNC.md`
2. 集成：参考 `DISTRIBUTED_CLOCK_SYNC_INTEGRATION.md`
3. 了解：查看 `IMPLEMENTATION_SUMMARY.md`

---

### 📊 Data/ - 数据目录
原始数据、校准数据和输出结果。

```
Data/
├── bdm2/                 # BDM2 扫描仪数据
│   ├── BDM2_12.18/      # 特定日期的数据
│   ├── calibration/      # 校准参数文件
│   └── raw_data/         # 原始扫描数据
│
└── bdmbid/               # BDMBiD 扫描仪数据
    ├── BDMBiD/          # 应用程序
    ├── calibration/      # 校准参数文件
    ├── raw_data/         # 原始数据
    └── result/           # 处理结果
        ├── singles/      # Single 格式输出
        └── coincidence/  # Coincidence 格式输出（可选）
```

---

## 编译和构建

### 目录结构
```
编译时生成:
build/          # 临时编译文件
bin/            # 最终可执行文件
```

### 编译命令

```bash
# 显示帮助
make help

# 编译所有（默认）
make

# 编译并运行测试
make test

# 清理编译文件
make clean
```

### 编译器要求
- C++17 或更高版本
- `<thread>` 支持（多线程）
- `<chrono>` 支持（高精度时间）

---

## 使用场景映射

### 场景1：采集节点集成

```cpp
#include "include/grpcService/timesync/TimeSyncClient.hpp"

// 在采集模块中（例如 src/core/raw2coin.cpp）
auto time_sync = TimeSyncClient(node_id, "server:50051");
time_sync->StartSync();

while (collecting) {
    // 获取时钟校正时间
    event.timestamp = time_sync->GetCorrectedTimeNs();
}
```

**相关文件**:
- `include/grpcService/timesync/TimeSyncClient.hpp` - 客户端
- `src/core/raw2coin.cpp` - 集成位置

---

### 场景2：合并服务器处理

```cpp
#include "include/grpcService/timesync/DistributedClockSyncManager.hpp"
#include "src/core/MergeAndCoin.hpp"

// 在合并模块中
auto calibrations = GetClientOffsets();  // 从服务器获取

for (auto& seg : segments) {
    auto calib_seg = ClockCalibrationUtil::CalibrateSegmentTime(...);
    // 使用校正时间进行排序和分组
}
```

**相关文件**:
- `include/grpcService/timesync/DistributedClockSyncManager.hpp` - 校正工具
- `src/core/MergeAndCoin.hpp` - 合并逻辑

---

### 场景3：符合计算

```cpp
#include "src/core/MergeAndCoin.hpp"

// 处理已校正和排序的数据
coincidence_processor(merged_file);
```

**相关文件**:
- `src/core/MergeAndCoin.hpp` - 已集成校正后的合并数据

---

## 文件依赖关系

```
采集端                 合并端               计算端
    │                    │                     │
    ├─ TimeSyncClient    ├─ TimeSyncServer   │
    │                    ├─ ClockCalibrationUtil
    │                    ├─ MergeAndCoin.hpp │
    │                    │                     │
    └────> (events) ────>└────> (merged) ───>─┘
         (corrected)          (calibrated)
```

---

## 迁移检查清单

如果您正在从旧的扁平结构迁移，请确保：

- [ ] 头文件包含路径已更新（添加 `-Iinclude` 编译标志）
- [ ] `#include "TimeSyncClient.hpp"` 改为 `#include "timesync/TimeSyncClient.hpp"`
- [ ] Makefile 已更新（已自动处理）
- [ ] 测试程序可正常编译（`make test`）
- [ ] Data 文件夹路径未改变（仍在项目根）

---

## 后续扩展建议

### 新增模块时的目录规划

```
新增时钟同步 gRPC 实现：
  src/timesync/
    ├── timesync_server_impl.cpp
    ├── timesync_client_impl.cpp
    └── Makefile (子目录)

新增数据转换器：
  src/converters/
    ├── raw_converter.cpp
    ├── single_converter.cpp
    └── ...

新增数据格式：
  include/formats/
    ├── single_format.hpp
    ├── coin_format.hpp
    └── ...
```

---

## 快速参考

### 重要文件位置

| 需求 | 文件位置 |
|-----|---------|
| 编译测试 | `make test` |
| 查看集成例子 | `tests/QUICK_INTEGRATION_GUIDE.cpp` |
| 了解系统架构 | `docs/DISTRIBUTED_CLOCK_SYNC_INTEGRATION.md` |
| 学习头文件 API | `docs/README_CLOCK_SYNC.md` |
| 修改合并逻辑 | `src/core/MergeAndCoin.hpp` |
| 修改采集时间 | `src/core/raw2coin.cpp` |

---

## 总结

新的目录结构遵循标准的 C++ 项目布局：

- **include/** - 公共头文件（用于被其他模块引用）
- **src/** - 实现文件（特定功能的具体代码）
- **tests/** - 测试和示例代码
- **docs/** - 文档和指南
- **Data/** - 数据文件（保持原样）

这样的组织方式：
✓ 易于维护和扩展
✓ 清晰的模块边界
✓ 遵循业界最佳实践
✓ 便于团队协作
