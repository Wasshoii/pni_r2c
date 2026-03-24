# Makefile for Distributed PET Data Processing System

# ==================== 编译器配置 ====================
# 使用 g++-13 以支持 C++20 <format> 头文件
CXX = g++-13
NVCC = nvcc

# C++ 标准：PNI 库需要 C++23 (使用 std::expected 等特性)
CXXFLAGS = -std=c++23 -Wall -Wextra -O2 -pthread
NVCCFLAGS = -std=c++20 -O2 --expt-relaxed-constexpr

# 调试开关：使用 `make DEBUG=1 <target>` 启用
DEBUG ?= 0
ifeq ($(DEBUG),1)
CXXFLAGS += -DDEBUG -g
NVCCFLAGS += -DDEBUG -lineinfo
endif

# 链接标志
LDFLAGS =

# ==================== CUDA 配置 ====================
CUDA_PATH ?= /usr/local/cuda
CUDA_INCLUDE = -I$(CUDA_PATH)/include
CUDA_LIBS = -L$(CUDA_PATH)/lib64 -lcudart -lcuda

# ==================== pkg-config 配置 ====================
PKG_CONFIG = pkg-config
PKG_CONFIG_PATH_OVERRIDE = /usr/lib/x86_64-linux-gnu/pkgconfig

# ==================== PNI 库配置 ====================
# 优先使用系统安装的 libpni pkg-config；若不可用则回退到源码目录路径
PNI_PKG_NAME ?= libpni
PNI_PKG_CFLAGS = $(shell $(PKG_CONFIG) --cflags $(PNI_PKG_NAME) 2>/dev/null)
PNI_PKG_CFLAGS_I = $(shell $(PKG_CONFIG) --cflags-only-I $(PNI_PKG_NAME) 2>/dev/null)
PNI_PKG_LIBS = $(shell $(PKG_CONFIG) --libs $(PNI_PKG_NAME) 2>/dev/null)

ifeq ($(strip $(PNI_PKG_LIBS)),)
# 回退模式：使用源码树中的 include/build 目录
PNI_PROJECT_PATH ?= /media/ustc-pni/5282FE19AB6D5297/pni_grpc/pni-standard-project
PNI_CFLAGS = -I$(PNI_PROJECT_PATH)/include
PNI_NVCC_CFLAGS = $(PNI_CFLAGS)
PNI_LIB_PATH = $(PNI_PROJECT_PATH)/build
PNI_LIBS = -L$(PNI_LIB_PATH) -lpni -lpni_cu -Wl,-rpath,$(PNI_LIB_PATH)
PNI_EXTRA_CXXFLAGS = $(CUDA_INCLUDE)
PNI_EXTRA_LDFLAGS = $(CUDA_LIBS) -levent
else
# 系统安装模式：使用 pkg-config 提供的完整编译/链接参数
PNI_CFLAGS = $(PNI_PKG_CFLAGS)
PNI_NVCC_CFLAGS = $(PNI_PKG_CFLAGS_I)
PNI_LIBS = $(PNI_PKG_LIBS)
PNI_EXTRA_CXXFLAGS =
PNI_EXTRA_LDFLAGS = -lcuda
endif

# ==================== gRPC 配置 ====================
GRPC_CFLAGS_RAW = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH_OVERRIDE) $(PKG_CONFIG) --cflags grpc++ protobuf 2>/dev/null || echo "")
GRPC_LIBS_RAW = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH_OVERRIDE) $(PKG_CONFIG) --libs grpc++ protobuf 2>/dev/null || echo "-lgrpc++ -lprotobuf")
GRPC_CFLAGS = $(filter-out -pthread,$(GRPC_CFLAGS_RAW))
GRPC_LIBS = $(filter-out -pthread,$(GRPC_LIBS_RAW))

# 优先使用系统 protobuf/grpc 动态库，避免误链接到 /usr/local 下的静态 libprotobuf.a
SYSTEM_LIB_DIR ?= /usr/lib/x86_64-linux-gnu
SYSTEM_LIB_HINT = -L$(SYSTEM_LIB_DIR)

# ==================== 汇总编译选项 ====================
# 基础编译选项（不含 PNI/CUDA）
CXXFLAGS_BASE = $(CXXFLAGS) -I. -Iinclude_override $(GRPC_CFLAGS) -Iinclude -Iprotos -Isrc

# 完整编译选项（含 PNI/CUDA）
CXXFLAGS_FULL = $(CXXFLAGS_BASE) $(PNI_CFLAGS) $(PNI_EXTRA_CXXFLAGS)

# 基础链接选项
LDFLAGS_BASE = $(LDFLAGS) $(SYSTEM_LIB_HINT) $(GRPC_LIBS) -ldl

# 完整链接选项（含 PNI/CUDA）
LDFLAGS_FULL = $(LDFLAGS_BASE) $(PNI_LIBS) $(PNI_EXTRA_LDFLAGS)

# Tools - Force system versions
PROTOC = /usr/bin/protoc
GRPC_CPP_PLUGIN = /usr/bin/grpc_cpp_plugin

# Build directories
BUILD_DIR = build
BIN_DIR = bin

# Source files
TEST_SRC = tests/test_distributed_clock_sync.cpp
TEST_TARGET = $(BIN_DIR)/test_distributed_clock_sync

TEST_GRPC_SRC = tests/test_distributed_clock_sync_grpc.cpp
TEST_GRPC_TARGET = $(BIN_DIR)/test_distributed_clock_sync_grpc

# Streaming Coincidence test
TEST_STREAMING_SRC = tests/test_streaming_coincidence.cpp
TEST_STREAMING_TARGET = $(BIN_DIR)/test_streaming_coincidence

# PNI R2C test
TEST_PNI_R2C_SRC = tests/test_pni_r2c.cpp
TEST_PNI_R2C_TARGET = $(BIN_DIR)/test_pni_r2c

# PNI standalone coincidence test
TEST_PNI_COIN_SRC = tests/test_pni_coin.cpp
TEST_PNI_COIN_TARGET = $(BIN_DIR)/test_pni_coin

# Local gRPC R2S BDM2 test (receiver only)
TEST_LOCAL_GRPC_R2S_SRC = tests/test_local_grpc_r2s.cpp
TEST_LOCAL_GRPC_R2S_TARGET = $(BIN_DIR)/test_local_grpc_r2s

# Local gRPC coin receiver-only host test
TEST_LOCAL_GRPC_COIN_SRC = tests/test_local_grpc_coin.cpp
TEST_LOCAL_GRPC_COIN_TARGET = $(BIN_DIR)/test_local_grpc_coin

# Acquisition control protocol/state-machine smoke compile
TEST_ACQ_CONTROL_SMOKE_SRC = tests/test_acquisition_control_smoke.cpp
TEST_ACQ_CONTROL_SMOKE_OBJ = $(BUILD_DIR)/test_acquisition_control_smoke.o

# Acquisition control init integration test (1 master + 2 nodes, init only)
TEST_ACQ_CONTROL_INIT_SRC = tests/test_acquisition_control_init.cpp
TEST_ACQ_CONTROL_INIT_TARGET = $(BIN_DIR)/test_acquisition_control_init

# Acquisition data-path test with UDP packet simulation (scheme-1)
TEST_ACQ_DATAPATH_UDP_SRC = tests/test_acquisition_datapath_udp.cpp
TEST_ACQ_DATAPATH_UDP_TARGET = $(BIN_DIR)/test_acquisition_datapath_udp

# Acquisition -> R2S end-to-end pipeline test (UDP replay + lock-free raw queue)
TEST_ACQ_R2S_PIPELINE_SRC = tests/test_acquisition_r2s_pipeline.cpp
TEST_ACQ_R2S_PIPELINE_TARGET = $(BIN_DIR)/test_acquisition_r2s_pipeline

# Deployable app entrypoints
APP_ACQ_R2S_NODE_SRC = app/acq_r2s_node_main.cpp
APP_ACQ_R2S_NODE_TARGET = $(BIN_DIR)/app_acq_r2s_node
APP_COIN_MASTER_SRC = app/coin_master_main.cpp
APP_COIN_MASTER_TARGET = $(BIN_DIR)/app_coin_master
APP_COMMON_CONFIG_SRC = app/common/AppConfig.cpp
APP_COMMON_CONFIG_OBJ = $(BUILD_DIR)/app_common_AppConfig.o

# CUDA source files for PNI R2C
CUDA_SINGLES_PROCESS_SRC = src/tools/SinglesProcess.cu
CUDA_SINGLES_PROCESS_OBJ = $(BUILD_DIR)/SinglesProcess.o

# grpcService split source files
GRPC_SERVICE_DIR = src/grpcService
GRPC_SERVICE_SRCS = $(GRPC_SERVICE_DIR)/AcquisitionMaster.cpp \
					$(GRPC_SERVICE_DIR)/CoincidenceClient.cpp \
					$(GRPC_SERVICE_DIR)/CoincidenceServiceImpl.cpp
GRPC_SERVICE_OBJS = $(patsubst $(GRPC_SERVICE_DIR)/%.cpp,$(BUILD_DIR)/grpc_service_%.o,$(GRPC_SERVICE_SRCS))
GRPC_ACQ_MASTER_OBJ = $(BUILD_DIR)/grpc_service_AcquisitionMaster.o
GRPC_COIN_CLIENT_OBJ = $(BUILD_DIR)/grpc_service_CoincidenceClient.o
GRPC_COIN_SERVICE_OBJ = $(BUILD_DIR)/grpc_service_CoincidenceServiceImpl.o

# grpcNode split source files
GRPC_NODE_ACQ_SRC = src/grpcNode/acquisitionNode.cpp
GRPC_NODE_R2S_SRC = src/grpcNode/r2sNode.cpp
GRPC_NODE_COIN_SRC = src/grpcNode/coinNode.cpp
GRPC_NODE_ACQ_OBJ = $(BUILD_DIR)/grpc_node_acquisitionNode.o
GRPC_NODE_R2S_OBJ = $(BUILD_DIR)/grpc_node_r2sNode.o
GRPC_NODE_COIN_OBJ = $(BUILD_DIR)/grpc_node_coinNode.o

# core split source files
CORE_ACQ_SRC = src/core/acquisition/AcquisitionServer.cpp
CORE_STREAMING_SRC = src/core/streaming/StreamingCoincidence.cpp
CORE_R2S_SRC = src/core/r2s/R2S.cpp
CORE_ACQ_OBJ = $(BUILD_DIR)/core_acquisition_AcquisitionServer.o
CORE_STREAMING_OBJ = $(BUILD_DIR)/core_streaming_StreamingCoincidence.o
CORE_R2S_OBJ = $(BUILD_DIR)/core_r2s_R2S.o

# Proto files
PROTO_DIR = protos
PROTO_SRCS = $(wildcard $(PROTO_DIR)/*.proto)
PROTO_PB_CCS = $(PROTO_SRCS:.proto=.pb.cc)
PROTO_PB_HS = $(PROTO_SRCS:.proto=.pb.h)
PROTO_GRPC_CCS = $(PROTO_SRCS:.proto=.grpc.pb.cc)
PROTO_GRPC_HS = $(PROTO_SRCS:.proto=.grpc.pb.h)

# Object files for protos
PROTO_OBJS = $(patsubst $(PROTO_DIR)/%.proto,$(BUILD_DIR)/%.pb.o,$(PROTO_SRCS)) \
             $(patsubst $(PROTO_DIR)/%.proto,$(BUILD_DIR)/%.grpc.pb.o,$(PROTO_SRCS))

# Targets
all: directories $(TEST_TARGET) $(TEST_GRPC_TARGET)

# 完整构建（包含 PNI 依赖的测试）
all-full: directories $(TEST_TARGET) $(TEST_GRPC_TARGET) $(TEST_STREAMING_TARGET) $(TEST_PNI_R2C_TARGET) $(TEST_LOCAL_GRPC_R2S_TARGET)

# Create build directories
directories:
	@mkdir -p $(BUILD_DIR) $(BIN_DIR)

# 编译原始测试程序（模拟，无 PNI 依赖）
$(TEST_TARGET): $(TEST_SRC) | directories
	$(CXX) $(CXXFLAGS_BASE) -o $(TEST_TARGET) $(TEST_SRC)
	@echo "✓ Test program compiled successfully"
	@echo "Run: $(TEST_TARGET)"

# 生成 Proto 和 gRPC 代码
.PRECIOUS: $(PROTO_PB_CCS) $(PROTO_PB_HS) $(PROTO_GRPC_CCS) $(PROTO_GRPC_HS)

$(PROTO_DIR)/%.pb.cc $(PROTO_DIR)/%.pb.h $(PROTO_DIR)/%.grpc.pb.cc $(PROTO_DIR)/%.grpc.pb.h: $(PROTO_DIR)/%.proto
	$(PROTOC) -I $(PROTO_DIR) --cpp_out=$(PROTO_DIR) $<
	$(PROTOC) -I $(PROTO_DIR) --grpc_out=$(PROTO_DIR) --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN) $<
	@echo "✓ Proto files generated for $<"

# 编译 Proto 对象文件
$(BUILD_DIR)/%.pb.o: $(PROTO_DIR)/%.pb.cc | directories
	$(CXX) $(CXXFLAGS_BASE) -c $< -o $@

$(BUILD_DIR)/%.grpc.pb.o: $(PROTO_DIR)/%.grpc.pb.cc | directories
	$(CXX) $(CXXFLAGS_BASE) -c $< -o $@

# 编译 grpcService 拆分后的实现文件
$(BUILD_DIR)/grpc_service_%.o: $(GRPC_SERVICE_DIR)/%.cpp | directories
	$(CXX) $(CXXFLAGS_FULL) -c $< -o $@

$(GRPC_NODE_ACQ_OBJ): $(GRPC_NODE_ACQ_SRC) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(GRPC_NODE_ACQ_SRC) -o $(GRPC_NODE_ACQ_OBJ)

$(GRPC_NODE_R2S_OBJ): $(GRPC_NODE_R2S_SRC) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(GRPC_NODE_R2S_SRC) -o $(GRPC_NODE_R2S_OBJ)

$(GRPC_NODE_COIN_OBJ): $(GRPC_NODE_COIN_SRC) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(GRPC_NODE_COIN_SRC) -o $(GRPC_NODE_COIN_OBJ)

# 编译 core 拆分后的实现文件
$(CORE_ACQ_OBJ): $(CORE_ACQ_SRC) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(CORE_ACQ_SRC) -o $(CORE_ACQ_OBJ)

$(CORE_STREAMING_OBJ): $(CORE_STREAMING_SRC) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(CORE_STREAMING_SRC) -o $(CORE_STREAMING_OBJ)

$(CORE_R2S_OBJ): $(CORE_R2S_SRC) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(CORE_R2S_SRC) -o $(CORE_R2S_OBJ)

$(APP_COMMON_CONFIG_OBJ): $(APP_COMMON_CONFIG_SRC) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(APP_COMMON_CONFIG_SRC) -o $(APP_COMMON_CONFIG_OBJ)

# 编译 gRPC 测试程序
$(TEST_GRPC_TARGET): $(TEST_GRPC_SRC) $(PROTO_OBJS) | directories
	$(CXX) $(CXXFLAGS_BASE) -c $(TEST_GRPC_SRC) -o build/test_grpc.o
	$(CXX) $(CXXFLAGS_BASE) -o $(TEST_GRPC_TARGET) build/test_grpc.o $(PROTO_OBJS) $(LDFLAGS_BASE)
	@echo "✓ gRPC test program compiled successfully"
	@echo "Run: $(TEST_GRPC_TARGET)"

# 编译流式符合测试程序（需要 PNI 库和 CUDA）
$(TEST_STREAMING_TARGET): $(TEST_STREAMING_SRC) $(PROTO_OBJS) $(CORE_STREAMING_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(TEST_STREAMING_SRC) -o build/test_streaming.o
	$(CXX) $(CXXFLAGS_FULL) -o $(TEST_STREAMING_TARGET) build/test_streaming.o $(CORE_STREAMING_OBJ) $(PROTO_OBJS) $(LDFLAGS_FULL)
	@echo "✓ Streaming coincidence test program compiled successfully"
	@echo "Run: $(TEST_STREAMING_TARGET)"

# 编译 CUDA 源文件（使用 g++-13 作为 host 编译器以支持 <format>）
$(CUDA_SINGLES_PROCESS_OBJ): $(CUDA_SINGLES_PROCESS_SRC) | directories
	$(NVCC) $(NVCCFLAGS) -ccbin g++-13 $(PNI_NVCC_CFLAGS) $(PNI_EXTRA_CXXFLAGS) -Iinclude -Iprotos -Isrc -c $(CUDA_SINGLES_PROCESS_SRC) -o $(CUDA_SINGLES_PROCESS_OBJ)
	@echo "✓ CUDA SinglesProcess compiled"

# 编译 PNI R2C 测试程序（需要 PNI 库、CUDA 和 TBB）
$(TEST_PNI_R2C_TARGET): $(TEST_PNI_R2C_SRC) $(CUDA_SINGLES_PROCESS_OBJ) $(CORE_R2S_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -c $(TEST_PNI_R2C_SRC) -o build/test_pni_r2c.o
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -o $(TEST_PNI_R2C_TARGET) build/test_pni_r2c.o $(CUDA_SINGLES_PROCESS_OBJ) $(CORE_R2S_OBJ) $(LDFLAGS_FULL) -ltbb
	@echo "✓ PNI R2C test program compiled successfully"
	@echo "Run: $(TEST_PNI_R2C_TARGET)"

# 编译 PNI 独立符合测试程序（读取 singles.single -> 输出 prompt/delay lmf）
$(TEST_PNI_COIN_TARGET): $(TEST_PNI_COIN_SRC) $(CUDA_SINGLES_PROCESS_OBJ) $(CORE_R2S_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -c $(TEST_PNI_COIN_SRC) -o build/test_pni_coin.o
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -o $(TEST_PNI_COIN_TARGET) build/test_pni_coin.o $(CUDA_SINGLES_PROCESS_OBJ) $(CORE_R2S_OBJ) $(LDFLAGS_FULL) -ltbb
	@echo "✓ PNI standalone coincidence test program compiled successfully"
	@echo "Run: $(TEST_PNI_COIN_TARGET)"

# 编译本地 gRPC + R2S(BDM2) 测试程序（符合端仅接收）
$(TEST_LOCAL_GRPC_R2S_TARGET): $(TEST_LOCAL_GRPC_R2S_SRC) $(PROTO_OBJS) $(CUDA_SINGLES_PROCESS_OBJ) $(CORE_R2S_OBJ) $(GRPC_NODE_R2S_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -c $(TEST_LOCAL_GRPC_R2S_SRC) -o build/test_local_grpc_r2s_bdm2.o
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -o $(TEST_LOCAL_GRPC_R2S_TARGET) build/test_local_grpc_r2s_bdm2.o $(CUDA_SINGLES_PROCESS_OBJ) $(CORE_R2S_OBJ) $(GRPC_NODE_R2S_OBJ) $(PROTO_OBJS) $(LDFLAGS_FULL) -ltbb
	@echo "✓ Local gRPC R2S BDM2 test program compiled successfully"
	@echo "Run: $(TEST_LOCAL_GRPC_R2S_TARGET)"

# 编译本地 gRPC Coin 接收测试程序（仅接收 + 发开始信号）
$(TEST_LOCAL_GRPC_COIN_TARGET): $(TEST_LOCAL_GRPC_COIN_SRC) $(PROTO_OBJS) $(GRPC_COIN_SERVICE_OBJ) $(GRPC_COIN_CLIENT_OBJ) $(CORE_STREAMING_OBJ) $(GRPC_NODE_COIN_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(TEST_LOCAL_GRPC_COIN_SRC) -o build/test_local_grpc_coin.o
	$(CXX) $(CXXFLAGS_FULL) -o $(TEST_LOCAL_GRPC_COIN_TARGET) build/test_local_grpc_coin.o $(GRPC_COIN_SERVICE_OBJ) $(GRPC_COIN_CLIENT_OBJ) $(CORE_STREAMING_OBJ) $(GRPC_NODE_COIN_OBJ) $(PROTO_OBJS) $(LDFLAGS_FULL)
	@echo "✓ Local gRPC coin receiver test program compiled successfully"
	@echo "Run: $(TEST_LOCAL_GRPC_COIN_TARGET)"

# 采集控制路径冒烟编译（仅编译，不链接运行）
$(TEST_ACQ_CONTROL_SMOKE_OBJ): $(TEST_ACQ_CONTROL_SMOKE_SRC) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(TEST_ACQ_CONTROL_SMOKE_SRC) -o $(TEST_ACQ_CONTROL_SMOKE_OBJ)
	@echo "✓ Acquisition control smoke compile succeeded"

# 编译采集控制初始化测试（1 主 + 2 采集节点，仅初始化）
$(TEST_ACQ_CONTROL_INIT_TARGET): $(TEST_ACQ_CONTROL_INIT_SRC) $(PROTO_OBJS) $(GRPC_ACQ_MASTER_OBJ) $(CORE_ACQ_OBJ) $(GRPC_NODE_ACQ_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(TEST_ACQ_CONTROL_INIT_SRC) -o build/test_acquisition_control_init.o
	$(CXX) $(CXXFLAGS_FULL) -o $(TEST_ACQ_CONTROL_INIT_TARGET) build/test_acquisition_control_init.o $(GRPC_ACQ_MASTER_OBJ) $(CORE_ACQ_OBJ) $(GRPC_NODE_ACQ_OBJ) $(PROTO_OBJS) $(LDFLAGS_FULL)
	@echo "✓ Acquisition control init test program compiled successfully"
	@echo "Run: $(TEST_ACQ_CONTROL_INIT_TARGET)"

# 编译采集通路 UDP 模拟测试（仅验证采集链路，不含单事件转换）
$(TEST_ACQ_DATAPATH_UDP_TARGET): $(TEST_ACQ_DATAPATH_UDP_SRC) $(PROTO_OBJS) $(GRPC_ACQ_MASTER_OBJ) $(CORE_ACQ_OBJ) $(GRPC_NODE_ACQ_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(TEST_ACQ_DATAPATH_UDP_SRC) -o build/test_acquisition_datapath_udp.o
	$(CXX) $(CXXFLAGS_FULL) -o $(TEST_ACQ_DATAPATH_UDP_TARGET) build/test_acquisition_datapath_udp.o $(GRPC_ACQ_MASTER_OBJ) $(CORE_ACQ_OBJ) $(GRPC_NODE_ACQ_OBJ) $(PROTO_OBJS) $(LDFLAGS_FULL)
	@echo "✓ Acquisition datapath UDP test program compiled successfully"
	@echo "Run: $(TEST_ACQ_DATAPATH_UDP_TARGET)"

# 编译采集->单事件转换端到端测试（UDP回放 + 无锁 raw 队列 + R2S）
$(TEST_ACQ_R2S_PIPELINE_TARGET): $(TEST_ACQ_R2S_PIPELINE_SRC) $(PROTO_OBJS) $(CUDA_SINGLES_PROCESS_OBJ) $(GRPC_ACQ_MASTER_OBJ) $(CORE_ACQ_OBJ) $(CORE_R2S_OBJ) $(GRPC_NODE_ACQ_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -c $(TEST_ACQ_R2S_PIPELINE_SRC) -o build/test_acquisition_r2s_pipeline.o
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -o $(TEST_ACQ_R2S_PIPELINE_TARGET) build/test_acquisition_r2s_pipeline.o $(CUDA_SINGLES_PROCESS_OBJ) $(GRPC_ACQ_MASTER_OBJ) $(CORE_ACQ_OBJ) $(CORE_R2S_OBJ) $(GRPC_NODE_ACQ_OBJ) $(PROTO_OBJS) $(LDFLAGS_FULL) -ltbb
	@echo "✓ Acquisition -> R2S pipeline test program compiled successfully"
	@echo "Run: $(TEST_ACQ_R2S_PIPELINE_TARGET)"

# 编译应用：采集-单事件转换子节点
$(APP_ACQ_R2S_NODE_TARGET): $(APP_ACQ_R2S_NODE_SRC) $(APP_COMMON_CONFIG_OBJ) $(PROTO_OBJS) $(CUDA_SINGLES_PROCESS_OBJ) $(CORE_R2S_OBJ) $(CORE_ACQ_OBJ) $(GRPC_NODE_ACQ_OBJ) $(GRPC_COIN_CLIENT_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -c $(APP_ACQ_R2S_NODE_SRC) -o build/app_acq_r2s_node_main.o
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -o $(APP_ACQ_R2S_NODE_TARGET) build/app_acq_r2s_node_main.o $(APP_COMMON_CONFIG_OBJ) $(CUDA_SINGLES_PROCESS_OBJ) $(CORE_R2S_OBJ) $(CORE_ACQ_OBJ) $(GRPC_NODE_ACQ_OBJ) $(GRPC_COIN_CLIENT_OBJ) $(PROTO_OBJS) $(LDFLAGS_FULL) -ltbb
	@echo "✓ App acquisition + R2S node compiled successfully"
	@echo "Run: $(APP_ACQ_R2S_NODE_TARGET) --config app/config/examples/acq_r2s_node.example.json"

# 编译应用：符合主控节点（可选采集主控）
$(APP_COIN_MASTER_TARGET): $(APP_COIN_MASTER_SRC) $(APP_COMMON_CONFIG_OBJ) $(PROTO_OBJS) $(GRPC_ACQ_MASTER_OBJ) $(CORE_STREAMING_OBJ) $(GRPC_COIN_SERVICE_OBJ) $(GRPC_NODE_COIN_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -c $(APP_COIN_MASTER_SRC) -o build/app_coin_master_main.o
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -o $(APP_COIN_MASTER_TARGET) build/app_coin_master_main.o $(APP_COMMON_CONFIG_OBJ) $(GRPC_ACQ_MASTER_OBJ) $(CORE_STREAMING_OBJ) $(GRPC_COIN_SERVICE_OBJ) $(GRPC_NODE_COIN_OBJ) $(PROTO_OBJS) $(LDFLAGS_FULL) -ltbb
	@echo "✓ App coin master node compiled successfully"
	@echo "Run: $(APP_COIN_MASTER_TARGET) --config app/config/examples/coin_master.example.json"

# 清理
clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)
	@echo "✓ Cleaned"

# 清理 proto 生成的文件
clean-proto:
	rm -f $(PROTO_DIR)/*.pb.cc $(PROTO_DIR)/*.pb.h $(PROTO_DIR)/*.grpc.pb.cc $(PROTO_DIR)/*.grpc.pb.h
	@echo "✓ Proto files cleaned"

# 运行原始测试
test: $(TEST_TARGET)
	@echo "Running distributed clock sync tests (mock)..."
	@echo "==========================================="
	@$(TEST_TARGET)
	@echo "==========================================="
	@echo "✓ Tests completed"

# 运行 gRPC 测试
test-grpc: $(TEST_GRPC_TARGET)
	@echo "Running distributed clock sync tests (gRPC)..."
	@echo "============================================="
	@$(TEST_GRPC_TARGET)
	@echo "============================================="
	@echo "✓ Tests completed"

# 显示帮助
help:
	@echo "Distributed PET Data Processing System - Build Commands"
	@echo "========================================================="
	@echo ""
	@echo "基础构建（无 PNI/CUDA 依赖）:"
	@echo "  make              - 编译基础测试程序"
	@echo "  make test         - 编译并运行模拟测试"
	@echo "  make test-grpc    - 编译并运行 gRPC 测试"
	@echo ""
	@echo "完整构建（需要 PNI 库和 CUDA）:"
	@echo "  make all-full         - 编译所有程序（含 PNI 依赖）"
	@echo "  make test-streaming   - 编译并运行流式符合测试"
	@echo "  make test-pni-r2c     - 编译并运行 PNI R2C 测试"
	@echo "  make test-pni-coin    - 编译并运行 PNI 独立符合测试"
	@echo "  make test-local-grpc-coin - 编译并运行本地 gRPC 符合主机接收测试"
	@echo "  make test-local-grpc-r2s - 编译并运行本地 gRPC 单事件转换测试(BDM2)"
	@echo "  make test-acq-control-smoke - 仅编译采集控制协议/状态机路径"
	@echo "  make test-acq-control-init - 编译并运行采集控制初始化测试(1主2节点)"
	@echo "  make test-acq-datapath-udp - 编译并运行采集通路UDP模拟测试(不含单事件转换)"
	@echo "  make test-acq-r2s-pipeline - 编译并运行采集->单事件转换端到端测试"
	@echo "  make app-acq-r2s-node      - 编译可部署采集-单事件转换子节点程序"
	@echo "  make app-coin-master       - 编译可部署符合主控节点程序"
	@echo ""
	@echo "清理:"
	@echo "  make clean        - 删除构建文件"
	@echo "  make clean-proto  - 删除 proto 生成文件"
	@echo ""
	@echo "环境变量:"
	@echo "  CUDA_PATH         - CUDA 安装路径 (默认: /usr/local/cuda)"
	@echo "  SYSTEM_LIB_DIR    - 系统 grpc/protobuf 库路径 (默认: /usr/lib/x86_64-linux-gnu)"
	@echo "  PNI_PKG_NAME      - OpenPnI pkg-config 名称 (默认: libpni)"
	@echo "  PNI_PROJECT_PATH  - 当 pkg-config 不可用时的回退源码路径"
	@echo "  DEBUG             - 调试开关，1 启用 -DDEBUG -g (示例: make DEBUG=1 test-local-grpc-r2s)"
	@echo ""
	@echo "依赖:"
	@echo "  基础: libgrpc++-dev, protobuf-compiler-grpc"
	@echo "  完整: + libopenpni, CUDA Toolkit, TBB"

# 运行流式符合测试
test-streaming: $(TEST_STREAMING_TARGET)
	@echo "Running streaming coincidence tests..."
	@echo "======================================"
	@$(TEST_STREAMING_TARGET)
	@echo "======================================"
	@echo "✓ Tests completed"

# 运行 PNI R2C 测试
test-pni-r2c: $(TEST_PNI_R2C_TARGET)
	@echo "Running PNI R2C tests..."
	@echo "========================"
	@$(TEST_PNI_R2C_TARGET)
	@echo "========================"
	@echo "✓ Tests completed"

# 运行 PNI 独立符合测试
test-pni-coin: $(TEST_PNI_COIN_TARGET)
	@echo "Running PNI standalone coincidence test..."
	@echo "==========================================="
	@$(TEST_PNI_COIN_TARGET)
	@echo "==========================================="
	@echo "✓ Tests completed"

# 运行本地 gRPC + R2S(BDM2) 测试
test-local-grpc-r2s: $(TEST_LOCAL_GRPC_R2S_TARGET)
	@echo "Running local gRPC R2S BDM2 test..."
	@echo "===================================="
	@$(TEST_LOCAL_GRPC_R2S_TARGET)
	@echo "===================================="
	@echo "✓ Tests completed"

# 运行本地 gRPC Coin 接收测试
test-local-grpc-coin: $(TEST_LOCAL_GRPC_COIN_TARGET)
	@echo "Running local gRPC coin receiver test..."
	@echo "========================================="
	@$(TEST_LOCAL_GRPC_COIN_TARGET)
	@echo "========================================="
	@echo "✓ Tests completed"

# 运行采集控制路径冒烟编译
test-acq-control-smoke: $(TEST_ACQ_CONTROL_SMOKE_OBJ)
	@echo "Acquisition control smoke compile completed"

# 运行采集控制初始化测试（不执行采集）
test-acq-control-init: $(TEST_ACQ_CONTROL_INIT_TARGET)
	@echo "Running acquisition control init test (1 master + 2 nodes)..."
	@echo "============================================================"
	@$(TEST_ACQ_CONTROL_INIT_TARGET)
	@echo "============================================================"
	@echo "✓ Init test completed"

# 运行采集通路 UDP 模拟测试（不执行单事件转换）
test-acq-datapath-udp: $(TEST_ACQ_DATAPATH_UDP_TARGET)
	@echo "Running acquisition datapath UDP simulation test..."
	@echo "===================================================="
	@$(TEST_ACQ_DATAPATH_UDP_TARGET)
	@echo "===================================================="
	@echo "✓ Datapath UDP test completed"

# 运行采集->单事件转换端到端测试
test-acq-r2s-pipeline: $(TEST_ACQ_R2S_PIPELINE_TARGET)
	@echo "Running acquisition -> R2S end-to-end pipeline test..."
	@echo "========================================================"
	@$(TEST_ACQ_R2S_PIPELINE_TARGET)
	@echo "========================================================"
	@echo "✓ Acquisition -> R2S pipeline test completed"

app-acq-r2s-node: $(APP_ACQ_R2S_NODE_TARGET)

app-coin-master: $(APP_COIN_MASTER_TARGET)

.PHONY: all all-full test test-grpc test-streaming test-pni-r2c test-pni-coin test-local-grpc-coin test-local-grpc-r2s test-acq-control-smoke test-acq-control-init test-acq-datapath-udp test-acq-r2s-pipeline app-acq-r2s-node app-coin-master clean clean-proto help directories
