# Makefile for running tests (CMake build required)

ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
CORE_DIR := $(ROOT_DIR)/build/tests/core
PNI_DIR := $(ROOT_DIR)/build/tests/pni
CUDA_DIR := $(ROOT_DIR)/build/tests/cuda

CTEST = ctest --output-on-failure

.PHONY: all all-full help \
	test test-grpc test-streaming test-pni-r2c test-pni-coin \
	test-local-grpc-coin test-local-grpc-r2s test-acq-control-smoke \
	test-acq-control-init test-acq-datapath-udp test-acq-r2s-pipeline

all: test test-grpc

all-full: test test-grpc test-streaming test-acq-control-init test-acq-datapath-udp \
	test-pni-r2c test-pni-coin test-acq-r2s-pipeline

help:
	@echo "Test runner (CMake build required)"
	@echo "=================================="
	@echo "Basic:"
	@echo "  make test                 - run mock clock sync test"
	@echo "  make test-grpc            - run gRPC clock sync test"
	@echo ""
	@echo "PNI/CUDA (non-integration):"
	@echo "  make test-streaming"
	@echo "  make test-acq-control-init"
	@echo "  make test-acq-datapath-udp"
	@echo "  make test-pni-r2c"
	@echo "  make test-pni-coin"
	@echo "  make test-acq-r2s-pipeline"
	@echo ""
	@echo "Integration:"
	@echo "  make test-local-grpc-coin"
	@echo "  make test-local-grpc-r2s"
	@echo ""
	@echo "Meta:"
	@echo "  make all                  - run core tests"
	@echo "  make all-full             - run non-integration tests"
	@echo ""
	@echo "Note: Build tests first via ./build.sh --tests"

check-dir:
	@if [ ! -d "$(CORE_DIR)" ]; then echo "Build dir missing: $(CORE_DIR). Run ./build.sh --tests"; exit 1; fi

check-dir-pni:
	@if [ ! -d "$(PNI_DIR)" ]; then echo "Build dir missing: $(PNI_DIR). Run ./build.sh --tests"; exit 1; fi

check-dir-cuda:
	@if [ ! -d "$(CUDA_DIR)" ]; then echo "Build dir missing: $(CUDA_DIR). Run ./build.sh --tests"; exit 1; fi

# Core tests

test: check-dir
	@$(CTEST) --test-dir "$(CORE_DIR)" -R test_distributed_clock_sync


test-grpc: check-dir
	@$(CTEST) --test-dir "$(CORE_DIR)" -R test_distributed_clock_sync_grpc

# PNI tests

test-streaming: check-dir-pni
	@$(CTEST) --test-dir "$(PNI_DIR)" -R test_streaming_coincidence


test-acq-control-init: check-dir-pni
	@$(CTEST) --test-dir "$(PNI_DIR)" -R test_acquisition_control_init


test-acq-datapath-udp: check-dir-pni
	@$(CTEST) --test-dir "$(PNI_DIR)" -R test_acquisition_datapath_udp


test-local-grpc-coin: check-dir-pni
	@$(CTEST) --test-dir "$(PNI_DIR)" -R test_local_grpc_coin

# CUDA tests

test-pni-r2c: check-dir-cuda
	@$(CTEST) --test-dir "$(CUDA_DIR)" -R test_pni_r2c


test-pni-coin: check-dir-cuda
	@$(CTEST) --test-dir "$(CUDA_DIR)" -R test_pni_coin


test-local-grpc-r2s: check-dir-cuda
	@$(CTEST) --test-dir "$(CUDA_DIR)" -R test_local_grpc_r2s


test-acq-r2s-pipeline: check-dir-cuda
	@$(CTEST) --test-dir "$(CUDA_DIR)" -R test_acquisition_r2s_pipeline

# Legacy target (compile-only in old Makefile). Keep for interface compatibility.

test-acq-control-smoke:
	@echo "test-acq-control-smoke is compile-only in legacy Makefile."
	@echo "Please run ./build.sh --tests to compile, then run relevant tests."
