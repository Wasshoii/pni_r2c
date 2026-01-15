# Makefile for Distributed PET Data Processing System

# ==================== 编译器配置 ====================
CXX = g++
NVCC = nvcc

# C++ 标准：PNI 库需要 C++20
CXXFLAGS = -std=c++20 -Wall -Wextra -O2 -pthread
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
all-full: directories $(TEST_TARGET) $(TEST_GRPC_TARGET) $(TEST_STREAMING_TARGET)

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
	@echo "  完整: + libopenpni, CUDA Toolkit"

# 运行流式符合测试
test-streaming: $(TEST_STREAMING_TARGET)
	@echo "Running streaming coincidence tests..."
	@echo "======================================"
	@$(TEST_STREAMING_TARGET)
	@echo "======================================"
	@echo "✓ Tests completed"

.PHONY: all all-full test test-grpc test-streaming clean clean-proto help directories
