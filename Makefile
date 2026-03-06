# Makefile for Distributed PET Data Processing System

# ==================== 编译器配置 ====================
# 使用 g++-13 以支持 C++20 <format> 头文件
CXX = g++-13
NVCC = nvcc

# C++ 标准：PNI 库需要 C++23 (使用 std::expected 等特性)
CXXFLAGS = -std=c++23 -Wall -Wextra -O2 -pthread
NVCCFLAGS = -std=c++20 -O2 --expt-relaxed-constexpr

# 链接标志
LDFLAGS = -pthread

# ==================== CUDA 配置 ====================
CUDA_PATH ?= /usr/local/cuda
CUDA_INCLUDE = -I$(CUDA_PATH)/include
CUDA_LIBS = -L$(CUDA_PATH)/lib64 -lcudart -lcuda

# ==================== PNI 库配置 ====================
# PNI 项目路径（可通过环境变量覆盖）
PNI_PROJECT_PATH ?= /media/ustc-pni/5282FE19AB6D5297/pni_grpc/pni-standard-project
PNI_INCLUDE = -I$(PNI_PROJECT_PATH)/include
PNI_LIB_PATH = $(PNI_PROJECT_PATH)/build
PNI_LIBS = -L$(PNI_LIB_PATH) -lpni -lpni_cu -Wl,-rpath,$(PNI_LIB_PATH)

# ==================== gRPC 配置 ====================
PKG_CONFIG_PATH_OVERRIDE = /usr/lib/x86_64-linux-gnu/pkgconfig
PKG_CONFIG = pkg-config

GRPC_CFLAGS = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH_OVERRIDE) $(PKG_CONFIG) --cflags grpc++ protobuf 2>/dev/null || echo "")
GRPC_LIBS = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH_OVERRIDE) $(PKG_CONFIG) --libs grpc++ protobuf 2>/dev/null || echo "-lgrpc++ -lprotobuf")

# ==================== 汇总编译选项 ====================
# 基础编译选项（不含 PNI/CUDA）
CXXFLAGS_BASE = $(CXXFLAGS) -Iinclude_override $(GRPC_CFLAGS) -Iinclude -Iprotos -Isrc

# 完整编译选项（含 PNI/CUDA）
CXXFLAGS_FULL = $(CXXFLAGS_BASE) $(PNI_INCLUDE) $(CUDA_INCLUDE)

# 基础链接选项
LDFLAGS_BASE = $(LDFLAGS) $(GRPC_LIBS) -ldl

# 完整链接选项（含 PNI/CUDA）
LDFLAGS_FULL = $(LDFLAGS_BASE) $(PNI_LIBS) $(CUDA_LIBS) -levent

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

# Local gRPC R2S BDM2 test (receiver only)
TEST_LOCAL_GRPC_R2S_SRC = tests/test_local_grpc_r2s_bdm2.cpp
TEST_LOCAL_GRPC_R2S_TARGET = $(BIN_DIR)/test_local_grpc_r2s_bdm2

# CUDA source files for PNI R2C
CUDA_SINGLES_PROCESS_SRC = src/tools/SinglesProcess.cu
CUDA_SINGLES_PROCESS_OBJ = $(BUILD_DIR)/SinglesProcess.o

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
	$(CXX) $(CXXFLAGS_BASE) -o $(TEST_TARGET) $(TEST_SRC) -pthread
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

# 编译 gRPC 测试程序
$(TEST_GRPC_TARGET): $(TEST_GRPC_SRC) $(PROTO_OBJS) | directories
	$(CXX) $(CXXFLAGS_BASE) -c $(TEST_GRPC_SRC) -o build/test_grpc.o
	$(CXX) $(CXXFLAGS_BASE) -o $(TEST_GRPC_TARGET) build/test_grpc.o $(PROTO_OBJS) $(LDFLAGS_BASE)
	@echo "✓ gRPC test program compiled successfully"
	@echo "Run: $(TEST_GRPC_TARGET)"

# 编译流式符合测试程序（需要 PNI 库和 CUDA）
$(TEST_STREAMING_TARGET): $(TEST_STREAMING_SRC) $(PROTO_OBJS) | directories
	$(CXX) $(CXXFLAGS_FULL) -c $(TEST_STREAMING_SRC) -o build/test_streaming.o
	$(CXX) $(CXXFLAGS_FULL) -o $(TEST_STREAMING_TARGET) build/test_streaming.o $(PROTO_OBJS) $(LDFLAGS_FULL)
	@echo "✓ Streaming coincidence test program compiled successfully"
	@echo "Run: $(TEST_STREAMING_TARGET)"

# 编译 CUDA 源文件（使用 g++-13 作为 host 编译器以支持 <format>）
$(CUDA_SINGLES_PROCESS_OBJ): $(CUDA_SINGLES_PROCESS_SRC) | directories
	$(NVCC) $(NVCCFLAGS) -ccbin g++-13 $(PNI_INCLUDE) $(CUDA_INCLUDE) -Isrc -c $(CUDA_SINGLES_PROCESS_SRC) -o $(CUDA_SINGLES_PROCESS_OBJ)
	@echo "✓ CUDA SinglesProcess compiled"

# 编译 PNI R2C 测试程序（需要 PNI 库、CUDA 和 TBB）
$(TEST_PNI_R2C_TARGET): $(TEST_PNI_R2C_SRC) $(CUDA_SINGLES_PROCESS_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -c $(TEST_PNI_R2C_SRC) -o build/test_pni_r2c.o
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -o $(TEST_PNI_R2C_TARGET) build/test_pni_r2c.o $(CUDA_SINGLES_PROCESS_OBJ) $(LDFLAGS_FULL) -ltbb
	@echo "✓ PNI R2C test program compiled successfully"
	@echo "Run: $(TEST_PNI_R2C_TARGET)"

# 编译本地 gRPC + R2S(BDM2) 测试程序（符合端仅接收）
$(TEST_LOCAL_GRPC_R2S_TARGET): $(TEST_LOCAL_GRPC_R2S_SRC) $(PROTO_OBJS) $(CUDA_SINGLES_PROCESS_OBJ) | directories
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -c $(TEST_LOCAL_GRPC_R2S_SRC) -o build/test_local_grpc_r2s_bdm2.o
	$(CXX) $(CXXFLAGS_FULL) -O3 -march=native -fopenmp -o $(TEST_LOCAL_GRPC_R2S_TARGET) build/test_local_grpc_r2s_bdm2.o $(CUDA_SINGLES_PROCESS_OBJ) $(PROTO_OBJS) $(LDFLAGS_FULL) -ltbb
	@echo "✓ Local gRPC R2S BDM2 test program compiled successfully"
	@echo "Run: $(TEST_LOCAL_GRPC_R2S_TARGET)"

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
	@echo "  make test-local-grpc-r2s - 编译并运行本地 gRPC 单事件转换测试(BDM2)"
	@echo ""
	@echo "清理:"
	@echo "  make clean        - 删除构建文件"
	@echo "  make clean-proto  - 删除 proto 生成文件"
	@echo ""
	@echo "环境变量:"
	@echo "  CUDA_PATH         - CUDA 安装路径 (默认: /usr/local/cuda)"
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

# 运行本地 gRPC + R2S(BDM2) 测试
test-local-grpc-r2s: $(TEST_LOCAL_GRPC_R2S_TARGET)
	@echo "Running local gRPC R2S BDM2 test..."
	@echo "===================================="
	@$(TEST_LOCAL_GRPC_R2S_TARGET)
	@echo "===================================="
	@echo "✓ Tests completed"

.PHONY: all all-full test test-grpc test-streaming test-pni-r2c test-local-grpc-r2s clean clean-proto help directories
