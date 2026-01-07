# Makefile for Distributed Clock Synchronization System

CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -pthread
LDFLAGS = -pthread

# Force use of system libraries by pointing pkg-config to the correct directory
PKG_CONFIG_PATH_OVERRIDE = /usr/lib/x86_64-linux-gnu/pkgconfig
PKG_CONFIG = pkg-config

# Use shell command with environment variable to get flags
GRPC_CFLAGS = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH_OVERRIDE) $(PKG_CONFIG) --cflags grpc++ protobuf)
GRPC_LIBS = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH_OVERRIDE) $(PKG_CONFIG) --libs grpc++ protobuf)

CXXFLAGS += -Iinclude_override $(GRPC_CFLAGS) -Iinclude -Iprotos
LDFLAGS += $(GRPC_LIBS) -ldl

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

# Create build directories
directories:
	@mkdir -p $(BUILD_DIR) $(BIN_DIR)

# 编译原始测试程序（模拟）
$(TEST_TARGET): $(TEST_SRC) | directories
	$(CXX) $(CXXFLAGS) -o $(TEST_TARGET) $(TEST_SRC) -pthread
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
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/%.grpc.pb.o: $(PROTO_DIR)/%.grpc.pb.cc | directories
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 编译 gRPC 测试程序
$(TEST_GRPC_TARGET): $(TEST_GRPC_SRC) $(PROTO_OBJS) | directories
	$(CXX) $(CXXFLAGS) -c $(TEST_GRPC_SRC) -o build/test_grpc.o
	$(CXX) $(CXXFLAGS) -o $(TEST_GRPC_TARGET) build/test_grpc.o $(PROTO_OBJS) $(LDFLAGS)
	@echo "✓ gRPC test program compiled successfully"
	@echo "Run: $(TEST_GRPC_TARGET)"

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
	@echo "Distributed Clock Synchronization - Build Commands"
	@echo "====================================================="
	@echo "make              - Compile all programs"
	@echo "make test         - Compile and run mock tests"
	@echo "make test-grpc    - Compile and run gRPC tests"
	@echo "make clean        - Remove built files"
	@echo "make clean-proto  - Remove proto generated files"
	@echo "make help         - Show this help message"
	@echo ""
	@echo "Test Modes:"
	@echo "  • test      : Mock/simulated testing (no network)"
	@echo "  • test-grpc : Real gRPC client-server testing"
	@echo ""
	@echo "Requires: System installed gRPC and Protobuf (libgrpc++-dev, protobuf-compiler-grpc)"

.PHONY: all test test-grpc clean clean-proto help directories
