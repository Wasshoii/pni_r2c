cmake_minimum_required(VERSION 3.20)

project(r2c LANGUAGES CXX)

include(CTest)

# Build options: Tests
# - core (grpc/protobuf only)
option(R2C_BUILD_TEST_GRPC "Build gRPC clock-sync test" ON)
option(R2C_BUILD_TEST_BASIC "Build mock clock-sync test" ON)

# - pni (libpni + tbb + openmp; streaming coincidence now also needs nvcc for multi-GPU)
option(R2C_BUILD_TEST_STREAMING "Build streaming coincidence test (requires libpni, nvcc)" OFF)
option(R2C_BUILD_TEST_LOCAL_GRPC_COIN "Build local gRPC coin receiver test (requires libpni, tbb, nvcc)" OFF)
option(R2C_BUILD_TEST_ACQ_CONTROL_INIT "Build acquisition control init test (requires libpni, tbb)" OFF)

# - cuda (nvcc + libpni + tbb + openmp)
option(R2C_BUILD_TEST_PNI_R2C "Build PNI R2C test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_PNI_COIN "Build PNI standalone coincidence test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_LOCAL_GRPC_R2S "Build local gRPC R2S test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_LOCAL_GRPC_R2S_COIN "Build local gRPC R2S→coincidence E2E test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_LOCAL_GRPC_SINGLES_INGRESS "Build L2 gRPC singles ingress test (requires libpni)" OFF)
option(R2C_BUILD_TEST_LOCAL_GRPC_COIN_STREAM "Build L3 gRPC coincidence stream test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_BDM50100_ONLINE_PIPELINE "Build BDM50100 NECR1 online Acq→R2S→Coin pipeline test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_R2S_MULTI_GPU "Build BDM50100 multi-GPU R2S test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_R2S_50100_PERF "Build BDM50100 single-ring R2S performance test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_COIN_PERF "Build 9120 StreamingTimeAligner coincidence performance test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_COIN_MULTI_GPU "Build coincidence multi-GPU consistency test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_COIN_CARRY_BOUNDARY "Build coincidence carry-boundary 3-mode comparison test (requires libpni)" OFF)

# Build options: Apps
# - basic (libpni + tbb + openmp; coin master needs nvcc for multi-GPU coincidence)
option(R2C_BUILD_APP_COIN_MASTER "Build app coin master executable (requires libpni, tbb, nvcc)" OFF)
option(R2C_BUILD_TOOL_UDP_RAW_REPLAYER "Build tool udp raw replayer executable (requires libpni)" OFF)
option(R2C_BUILD_TOOL_DPDK_TX_REPLAYER "Build tool DPDK tx replayer executable (requires libdpdk)" OFF)

# - cuda (nvcc + libpni + tbb + openmp)
option(R2C_BUILD_APP_ACQ_R2S_NODE "Build app acquisition+r2s node executable (requires libpni, nvcc, tbb)" OFF)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(EXISTS "/usr/bin/protoc")
  set(R2C_PROTOC_DEFAULT "/usr/bin/protoc")
else()
  find_program(R2C_PROTOC_DEFAULT NAMES protoc)
endif()

if(EXISTS "/usr/bin/grpc_cpp_plugin")
  set(R2C_GRPC_CPP_PLUGIN_DEFAULT "/usr/bin/grpc_cpp_plugin")
else()
  find_program(R2C_GRPC_CPP_PLUGIN_DEFAULT NAMES grpc_cpp_plugin)
endif()

include(GNUInstallDirs)
find_package(PkgConfig REQUIRED)

set(R2C_PKG_CONFIG_PATH "/usr/lib/x86_64-linux-gnu/pkgconfig:/usr/local/lib/x86_64-linux-gnu/pkgconfig" CACHE STRING "pkg-config search path override; set empty to use environment default")
if(NOT R2C_PKG_CONFIG_PATH STREQUAL "")
  set(ENV{PKG_CONFIG_PATH} "${R2C_PKG_CONFIG_PATH}")
endif()

# Keep migration low-risk: use pkg-config (same dependency source as Makefile).
pkg_check_modules(R2C_GRPC REQUIRED IMPORTED_TARGET grpc++ protobuf)
pkg_check_modules(R2C_GLOG REQUIRED IMPORTED_TARGET libglog gflags)
pkg_check_modules(R2C_IBVERBS REQUIRED IMPORTED_TARGET libibverbs)

option(OPENPNI_ENABLE_RDMA "Build RDMA (RoCE) singles data plane (required)" ON)
if(NOT OPENPNI_ENABLE_RDMA)
  message(FATAL_ERROR "OPENPNI_ENABLE_RDMA is required; gRPC StreamSingles data plane has been removed")
endif()

set(R2C_PROTOC_EXECUTABLE "${R2C_PROTOC_DEFAULT}" CACHE FILEPATH "Path to protoc")
set(R2C_GRPC_CPP_PLUGIN_EXECUTABLE "${R2C_GRPC_CPP_PLUGIN_DEFAULT}" CACHE FILEPATH "Path to grpc_cpp_plugin")

if(NOT R2C_PROTOC_EXECUTABLE)
  message(FATAL_ERROR "protoc not found. Set R2C_PROTOC_EXECUTABLE manually.")
endif()

if(NOT R2C_GRPC_CPP_PLUGIN_EXECUTABLE)
  message(FATAL_ERROR "grpc_cpp_plugin not found. Set R2C_GRPC_CPP_PLUGIN_EXECUTABLE manually.")
endif()

message(STATUS "Using protoc: ${R2C_PROTOC_EXECUTABLE}")
message(STATUS "Using grpc_cpp_plugin: ${R2C_GRPC_CPP_PLUGIN_EXECUTABLE}")

set(R2C_BIN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/bin")
file(MAKE_DIRECTORY "${R2C_BIN_DIR}/app" "${R2C_BIN_DIR}/test" "${R2C_BIN_DIR}/tools")

set(R2C_PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/protos")
set(R2C_PROTO_FILES
  "${R2C_PROTO_DIR}/acquisition.proto"
  "${R2C_PROTO_DIR}/coincidence.proto"
  "${R2C_PROTO_DIR}/timesync.proto"
)

set(R2C_PROTO_GEN_ROOT "${CMAKE_CURRENT_SOURCE_DIR}")
set(R2C_PROTO_GEN_DIR "${R2C_PROTO_GEN_ROOT}/protos")

set(R2C_PROTO_GEN_SRCS "")
set(R2C_PROTO_GEN_HDRS "")

foreach(proto_file IN LISTS R2C_PROTO_FILES)
  get_filename_component(proto_name "${proto_file}" NAME_WE)

  set(pb_cc "${R2C_PROTO_GEN_DIR}/${proto_name}.pb.cc")
  set(pb_h "${R2C_PROTO_GEN_DIR}/${proto_name}.pb.h")
  set(grpc_cc "${R2C_PROTO_GEN_DIR}/${proto_name}.grpc.pb.cc")
  set(grpc_h "${R2C_PROTO_GEN_DIR}/${proto_name}.grpc.pb.h")

  add_custom_command(
    OUTPUT "${pb_cc}" "${pb_h}" "${grpc_cc}" "${grpc_h}"
    COMMAND ${R2C_PROTOC_EXECUTABLE} -I ${R2C_PROTO_DIR} --cpp_out=${R2C_PROTO_GEN_DIR} ${proto_file}
    COMMAND ${R2C_PROTOC_EXECUTABLE} -I ${R2C_PROTO_DIR} --grpc_out=${R2C_PROTO_GEN_DIR} --plugin=protoc-gen-grpc=${R2C_GRPC_CPP_PLUGIN_EXECUTABLE} ${proto_file}
    DEPENDS "${proto_file}"
    COMMENT "Generating protobuf/grpc sources for ${proto_name}"
    VERBATIM
  )

  list(APPEND R2C_PROTO_GEN_SRCS "${pb_cc}" "${grpc_cc}")
  list(APPEND R2C_PROTO_GEN_HDRS "${pb_h}" "${grpc_h}")
endforeach()

add_library(r2c_proto_objs OBJECT ${R2C_PROTO_GEN_SRCS} ${R2C_PROTO_GEN_HDRS})
target_include_directories(r2c_proto_objs
  PUBLIC
    "${R2C_PROTO_GEN_ROOT}"
    "${R2C_PROTO_GEN_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_link_libraries(r2c_proto_objs
  PUBLIC
    PkgConfig::R2C_GRPC
)
target_compile_options(r2c_proto_objs
  PRIVATE
    -Wall
    -Wextra
    -O2
    -pthread
)

add_library(r2c_dataplane_rdma_objs OBJECT
  src/dataplane/rdma/HugepageArena.cpp
  src/dataplane/rdma/SlotRing.cpp
  src/dataplane/rdma/RdmaContext.cpp
  src/dataplane/rdma/RdmaRecvServer.cpp
  src/dataplane/rdma/RdmaWriteSender.cpp
)
target_include_directories(r2c_dataplane_rdma_objs
  PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/src"
    "${R2C_PROTO_GEN_ROOT}"
    "${R2C_PROTO_GEN_DIR}"
)
target_compile_options(r2c_dataplane_rdma_objs PRIVATE -Wall -Wextra -O3 -pthread)
target_compile_definitions(r2c_dataplane_rdma_objs PRIVATE GLOG_USE_GLOG_EXPORT)
target_link_libraries(r2c_dataplane_rdma_objs
  PUBLIC
    PkgConfig::R2C_IBVERBS
    PkgConfig::R2C_GLOG
    PkgConfig::R2C_GRPC
)

# Optional explicit target to (re)generate proto/grpc sources.
add_custom_target(r2c_proto
  DEPENDS ${R2C_PROTO_GEN_SRCS} ${R2C_PROTO_GEN_HDRS}
)
add_dependencies(r2c_dataplane_rdma_objs r2c_proto)

if(R2C_BUILD_TEST_BASIC)
  add_executable(test_timesync_algorithm tests/correctness/test_timesync_algorithm.cpp)
  target_include_directories(test_timesync_algorithm
    PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
      "${CMAKE_CURRENT_SOURCE_DIR}/protos"
  )
  target_compile_options(test_timesync_algorithm PRIVATE -Wall -Wextra -O2 -pthread)
endif()

if(R2C_BUILD_TEST_GRPC)
  add_executable(test_timesync_grpc
    tests/correctness/test_timesync_grpc.cpp
    $<TARGET_OBJECTS:r2c_proto_objs>
  )
  target_include_directories(test_timesync_grpc
    PRIVATE
      "${R2C_PROTO_GEN_ROOT}"
      "${R2C_PROTO_GEN_DIR}"
      "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(test_timesync_grpc PRIVATE -Wall -Wextra -O2 -pthread)
  target_link_libraries(test_timesync_grpc
    PRIVATE
      PkgConfig::R2C_GRPC
      PkgConfig::R2C_IBVERBS
      PkgConfig::R2C_GLOG
      dl
  )
endif()

if(R2C_BUILD_TEST_STREAMING OR R2C_BUILD_TEST_LOCAL_GRPC_COIN OR R2C_BUILD_APP_COIN_MASTER OR R2C_BUILD_TEST_COIN_MULTI_GPU OR R2C_BUILD_TEST_COIN_PERF OR R2C_BUILD_TEST_COIN_CARRY_BOUNDARY OR R2C_BUILD_TEST_LOCAL_GRPC_R2S_COIN OR R2C_BUILD_TEST_LOCAL_GRPC_SINGLES_INGRESS OR R2C_BUILD_TEST_LOCAL_GRPC_COIN_STREAM OR R2C_BUILD_TEST_BDM50100_ONLINE_PIPELINE)
  pkg_check_modules(R2C_PNI REQUIRED IMPORTED_TARGET libpni)
  pkg_check_modules(R2C_DPDK QUIET libdpdk)

  if(EXISTS "/usr/bin/nvcc")
    set(R2C_NVCC_EXECUTABLE "/usr/bin/nvcc")
  else()
    find_program(R2C_NVCC_EXECUTABLE NAMES nvcc REQUIRED)
  endif()

  set(R2C_CUDA_OBJ_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated/cuda")
  file(MAKE_DIRECTORY "${R2C_CUDA_OBJ_DIR}")
  set(R2C_S2C_COMPUTE_OBJ "${R2C_CUDA_OBJ_DIR}/S2CCompute.o")

  set(R2C_S2C_NVCC_INCLUDE_FLAGS "")
  foreach(include_dir IN LISTS R2C_PNI_INCLUDE_DIRS)
    list(APPEND R2C_S2C_NVCC_INCLUDE_FLAGS "-I${include_dir}")
  endforeach()

  set(R2C_S2C_NVCC_COMMON_FLAGS
    -std=c++20
    -O2
    --expt-relaxed-constexpr
    -ccbin ${CMAKE_CXX_COMPILER}
    ${R2C_S2C_NVCC_INCLUDE_FLAGS}
    -I${CMAKE_CURRENT_SOURCE_DIR}/include
    -I${CMAKE_CURRENT_SOURCE_DIR}/protos
    -I${CMAKE_CURRENT_SOURCE_DIR}/src
  )

  add_custom_command(
    OUTPUT "${R2C_S2C_COMPUTE_OBJ}"
    COMMAND ${R2C_NVCC_EXECUTABLE}
      ${R2C_S2C_NVCC_COMMON_FLAGS}
      -c ${CMAKE_CURRENT_SOURCE_DIR}/src/core/streaming/multi_gpu/S2CCompute.cu
      -o ${R2C_S2C_COMPUTE_OBJ}
    DEPENDS
      ${CMAKE_CURRENT_SOURCE_DIR}/src/core/streaming/multi_gpu/S2CCompute.cu
      ${CMAKE_CURRENT_SOURCE_DIR}/include/core/streaming/multi_gpu/S2CCompute.cuh
      ${CMAKE_CURRENT_SOURCE_DIR}/include/core/r2s/multi_gpu/PinnedHostCopy.cuh
      ${CMAKE_CURRENT_SOURCE_DIR}/include/core/r2s/multi_gpu/SPSCProcessor.hpp
    COMMENT "Building CUDA object S2CCompute.o"
    VERBATIM
  )
  set_source_files_properties(${R2C_S2C_COMPUTE_OBJ} PROPERTIES EXTERNAL_OBJECT TRUE GENERATED TRUE)
  # One owner for the nvcc output. Listing the same .o as a source of many
  # executables races under make -j (ld: "file format not recognized").
  add_custom_target(r2c_cuda_s2c_gen DEPENDS "${R2C_S2C_COMPUTE_OBJ}")
  function(r2c_target_link_cuda_s2c target_name)
    target_link_libraries(${target_name} PRIVATE "${R2C_S2C_COMPUTE_OBJ}")
    add_dependencies(${target_name} r2c_cuda_s2c_gen)
  endfunction()

  if(NOT TARGET r2c_core_streaming_objs)
    add_library(r2c_core_streaming_objs OBJECT
      src/core/streaming/StreamingCoincidence.cpp
      src/core/streaming/multi_gpu/CoincidenceMultiGpuEngine.cpp
    )
    target_include_directories(r2c_core_streaming_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(r2c_core_streaming_objs
      PRIVATE
        -Wall
        -Wextra
        -O2
        -pthread
    )
    target_link_libraries(r2c_core_streaming_objs
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GLOG
    )
  endif()

  if(R2C_BUILD_TEST_STREAMING)
  add_executable(test_coin_streaming_aligner
    tests/correctness/test_coin_streaming_aligner.cpp
    $<TARGET_OBJECTS:r2c_core_streaming_objs>
    $<TARGET_OBJECTS:r2c_proto_objs>
  )
  target_include_directories(test_coin_streaming_aligner
    PRIVATE
      "${R2C_PROTO_GEN_ROOT}"
      "${R2C_PROTO_GEN_DIR}"
      "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(test_coin_streaming_aligner PRIVATE -Wall -Wextra -O2 -pthread)
  target_link_libraries(test_coin_streaming_aligner
    PRIVATE
      PkgConfig::R2C_PNI
      PkgConfig::R2C_GRPC
      PkgConfig::R2C_IBVERBS
      PkgConfig::R2C_GLOG
      dl
  )
  r2c_target_link_cuda_s2c(test_coin_streaming_aligner)
  endif()

  if(R2C_BUILD_TEST_STREAMING OR R2C_BUILD_TEST_LOCAL_GRPC_COIN)
    add_executable(test_coin_synthetic_singles
      tests/correctness/test_coin_synthetic_singles.cpp
    )
    target_include_directories(test_coin_synthetic_singles
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
    )
    target_compile_options(test_coin_synthetic_singles PRIVATE -Wall -Wextra -O2)
    target_link_libraries(test_coin_synthetic_singles
      PRIVATE
        PkgConfig::R2C_PNI
    )
  endif()

  if(R2C_BUILD_TEST_COIN_MULTI_GPU)
    add_executable(test_coin_multi_gpu
      tests/correctness/test_coin_multi_gpu.cpp
      $<TARGET_OBJECTS:r2c_core_streaming_objs>
    )
    target_include_directories(test_coin_multi_gpu
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_coin_multi_gpu PRIVATE -O3 -march=native -pthread)
    target_link_libraries(test_coin_multi_gpu
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GLOG
        dl
    )
    r2c_target_link_cuda_s2c(test_coin_multi_gpu)
  endif()

  if(R2C_BUILD_TEST_COIN_PERF)
    add_executable(test_coin_9120_aligner
      tests/performance/test_coin_9120_aligner.cpp
      $<TARGET_OBJECTS:r2c_core_streaming_objs>
    )
    target_include_directories(test_coin_9120_aligner
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_coin_9120_aligner PRIVATE -O3 -march=native -pthread)
    target_link_libraries(test_coin_9120_aligner
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GLOG
        dl
    )
    r2c_target_link_cuda_s2c(test_coin_9120_aligner)
  endif()

  if(R2C_BUILD_TEST_COIN_CARRY_BOUNDARY)
    add_executable(test_coin_carry_boundary
      tests/correctness/test_coin_carry_boundary.cpp
    )
    target_include_directories(test_coin_carry_boundary
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_coin_carry_boundary PRIVATE -O3 -march=native -pthread)
    target_link_libraries(test_coin_carry_boundary
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GLOG
        dl
    )
  endif()

  if(R2C_BUILD_TEST_LOCAL_GRPC_COIN OR R2C_BUILD_APP_COIN_MASTER OR R2C_BUILD_TEST_LOCAL_GRPC_R2S_COIN OR R2C_BUILD_TEST_LOCAL_GRPC_COIN_STREAM)
    find_library(R2C_TBB_LIBRARY NAMES tbb REQUIRED)
    find_package(OpenMP REQUIRED)

    if(NOT TARGET r2c_app_common_objs)
      add_library(r2c_app_common_objs OBJECT
        app/common/AppConfig.cpp
      )
      target_include_directories(r2c_app_common_objs
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(r2c_app_common_objs
        PRIVATE
          -Wall
          -Wextra
          -O3
          -march=native
          -fopenmp
      )
      target_link_libraries(r2c_app_common_objs
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_IBVERBS
          PkgConfig::R2C_GLOG
          OpenMP::OpenMP_CXX
          ${R2C_TBB_LIBRARY}
      )
    endif()

    if(NOT TARGET r2c_grpc_service_acq_master_objs)
      add_library(r2c_grpc_service_acq_master_objs OBJECT
        src/grpcService/AcquisitionMaster.cpp
      )
      target_include_directories(r2c_grpc_service_acq_master_objs
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(r2c_grpc_service_acq_master_objs
        PRIVATE
          -Wall
          -Wextra
          -O3
          -march=native
          -fopenmp
      )
      target_link_libraries(r2c_grpc_service_acq_master_objs
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_IBVERBS
          PkgConfig::R2C_GLOG
          OpenMP::OpenMP_CXX
          ${R2C_TBB_LIBRARY}
      )
    endif()

    if(NOT TARGET r2c_grpc_service_coin_objs)
      add_library(r2c_grpc_service_coin_objs OBJECT
        src/grpcService/CoincidenceServiceImpl.cpp
        src/grpcService/CoincidenceClient.cpp
      )
      target_include_directories(r2c_grpc_service_coin_objs
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(r2c_grpc_service_coin_objs
        PRIVATE
          -Wall
          -Wextra
          -O3
          -march=native
          -fopenmp
      )
      target_link_libraries(r2c_grpc_service_coin_objs
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_GLOG
          PkgConfig::R2C_IBVERBS
          OpenMP::OpenMP_CXX
          ${R2C_TBB_LIBRARY}
      )
    endif()

    if(NOT TARGET r2c_grpc_node_coin_objs)
      add_library(r2c_grpc_node_coin_objs OBJECT
        src/grpcNode/coinNode.cpp
      )
      target_include_directories(r2c_grpc_node_coin_objs
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(r2c_grpc_node_coin_objs
        PRIVATE
          -Wall
          -Wextra
          -O3
          -march=native
          -fopenmp
      )
      target_link_libraries(r2c_grpc_node_coin_objs
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_IBVERBS
          PkgConfig::R2C_GLOG
          OpenMP::OpenMP_CXX
          ${R2C_TBB_LIBRARY}
      )
    endif()

    if(R2C_BUILD_TEST_LOCAL_GRPC_COIN)
      add_executable(test_rdma_recv_stub
        tests/correctness/test_rdma_recv_stub.cpp
        $<TARGET_OBJECTS:r2c_grpc_service_coin_objs>
        $<TARGET_OBJECTS:r2c_dataplane_rdma_objs>
        $<TARGET_OBJECTS:r2c_grpc_node_coin_objs>
        $<TARGET_OBJECTS:r2c_core_streaming_objs>
        $<TARGET_OBJECTS:r2c_proto_objs>
      )
      target_include_directories(test_rdma_recv_stub
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(test_rdma_recv_stub PRIVATE -O3 -march=native -fopenmp)
      target_link_libraries(test_rdma_recv_stub
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_IBVERBS
          PkgConfig::R2C_GLOG
          OpenMP::OpenMP_CXX
          ${R2C_TBB_LIBRARY}
          dl
      )
      r2c_target_link_cuda_s2c(test_rdma_recv_stub)

      add_executable(test_rdma_orchestration
        tests/correctness/test_rdma_orchestration.cpp
        $<TARGET_OBJECTS:r2c_grpc_service_coin_objs>
        $<TARGET_OBJECTS:r2c_dataplane_rdma_objs>
        $<TARGET_OBJECTS:r2c_grpc_node_coin_objs>
        $<TARGET_OBJECTS:r2c_core_streaming_objs>
        $<TARGET_OBJECTS:r2c_proto_objs>
      )
      target_include_directories(test_rdma_orchestration
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(test_rdma_orchestration PRIVATE -O2 -pthread -fopenmp)
      target_link_libraries(test_rdma_orchestration
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_IBVERBS
          PkgConfig::R2C_GLOG
          OpenMP::OpenMP_CXX
          ${R2C_TBB_LIBRARY}
          dl
      )
      r2c_target_link_cuda_s2c(test_rdma_orchestration)

      add_executable(test_rdma_dataplane_loopback
        tests/correctness/test_rdma_dataplane_loopback.cpp
        $<TARGET_OBJECTS:r2c_dataplane_rdma_objs>
        $<TARGET_OBJECTS:r2c_proto_objs>
      )
      target_include_directories(test_rdma_dataplane_loopback
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(test_rdma_dataplane_loopback PRIVATE -O2 -pthread)
      target_link_libraries(test_rdma_dataplane_loopback
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_GLOG
          PkgConfig::R2C_IBVERBS
          dl
          pthread
      )

      add_executable(test_app_config_parse
        tests/correctness/test_app_config_parse.cpp
        $<TARGET_OBJECTS:r2c_app_common_objs>
      )
      target_include_directories(test_app_config_parse
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(test_app_config_parse PRIVATE -Wall -Wextra -O2)
      target_link_libraries(test_app_config_parse
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_GLOG
          OpenMP::OpenMP_CXX
      )

      add_executable(test_app_inprocess_smoke
        tests/correctness/test_app_inprocess_smoke.cpp
      )
      target_compile_options(test_app_inprocess_smoke PRIVATE -Wall -Wextra -O2)
    endif()

    if(R2C_BUILD_TEST_LOCAL_GRPC_SINGLES_INGRESS)
      add_executable(test_rdma_singles_ingress
        tests/correctness/test_rdma_singles_ingress.cpp
        tests/correctness/lsingle_replay.cpp
        src/grpcService/CoincidenceClient.cpp
        $<TARGET_OBJECTS:r2c_dataplane_rdma_objs>
        $<TARGET_OBJECTS:r2c_proto_objs>
      )
      target_include_directories(test_rdma_singles_ingress
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(test_rdma_singles_ingress PRIVATE -O3 -march=native)
      target_link_libraries(test_rdma_singles_ingress
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_GLOG
          PkgConfig::R2C_IBVERBS
          dl
      )
    endif()

    if(R2C_BUILD_TEST_LOCAL_GRPC_COIN_STREAM)
      add_executable(test_coin_lsingle_stream
        tests/correctness/test_coin_lsingle_stream.cpp
        tests/correctness/lsingle_replay.cpp
        $<TARGET_OBJECTS:r2c_grpc_service_coin_objs>
        $<TARGET_OBJECTS:r2c_dataplane_rdma_objs>
        $<TARGET_OBJECTS:r2c_grpc_node_coin_objs>
        $<TARGET_OBJECTS:r2c_core_streaming_objs>
        $<TARGET_OBJECTS:r2c_proto_objs>
      )
      target_include_directories(test_coin_lsingle_stream
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(test_coin_lsingle_stream PRIVATE -O3 -march=native -fopenmp)
      target_link_libraries(test_coin_lsingle_stream
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_IBVERBS
          PkgConfig::R2C_GLOG
          OpenMP::OpenMP_CXX
          ${R2C_TBB_LIBRARY}
          dl
      )
      r2c_target_link_cuda_s2c(test_coin_lsingle_stream)
    endif()

    if(R2C_BUILD_APP_COIN_MASTER)
      add_executable(app_coin_master
        app/coin_master_main.cpp
        $<TARGET_OBJECTS:r2c_app_common_objs>
        $<TARGET_OBJECTS:r2c_grpc_service_acq_master_objs>
        $<TARGET_OBJECTS:r2c_grpc_service_coin_objs>
        $<TARGET_OBJECTS:r2c_dataplane_rdma_objs>
        $<TARGET_OBJECTS:r2c_grpc_node_coin_objs>
        $<TARGET_OBJECTS:r2c_core_streaming_objs>
        $<TARGET_OBJECTS:r2c_proto_objs>
      )
      target_include_directories(app_coin_master
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${R2C_PROTO_GEN_ROOT}"
          "${R2C_PROTO_GEN_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_SOURCE_DIR}/src"
      )
      target_compile_options(app_coin_master PRIVATE -O3 -march=native -fopenmp)
      target_link_libraries(app_coin_master
        PRIVATE
          PkgConfig::R2C_PNI
          PkgConfig::R2C_GRPC
          PkgConfig::R2C_IBVERBS
          PkgConfig::R2C_GLOG
          OpenMP::OpenMP_CXX
          ${R2C_TBB_LIBRARY}
          dl
      )
      r2c_target_link_cuda_s2c(app_coin_master)
      if(R2C_DPDK_FOUND)
        target_link_directories(app_coin_master PRIVATE ${R2C_DPDK_LIBRARY_DIRS})
        target_link_options(app_coin_master PRIVATE -Wl,--no-as-needed)
        target_link_libraries(app_coin_master
          PRIVATE
            PkgConfig::R2C_PNI
            ${R2C_DPDK_LINK_LIBRARIES}
        )
      endif()
    endif()
  endif()
endif()

if(R2C_BUILD_TEST_ACQ_CONTROL_INIT OR R2C_BUILD_TEST_BDM50100_ONLINE_PIPELINE)
  pkg_check_modules(R2C_PNI REQUIRED IMPORTED_TARGET libpni)
  find_library(R2C_TBB_LIBRARY NAMES tbb REQUIRED)
  find_package(OpenMP REQUIRED)

  if(NOT TARGET r2c_grpc_service_acq_master_objs)
    add_library(r2c_grpc_service_acq_master_objs OBJECT
      src/grpcService/AcquisitionMaster.cpp
    )
    target_include_directories(r2c_grpc_service_acq_master_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(r2c_grpc_service_acq_master_objs
      PRIVATE
        -Wall
        -Wextra
        -O3
        -march=native
        -fopenmp
    )
    target_link_libraries(r2c_grpc_service_acq_master_objs
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )
  endif()

  if(NOT TARGET r2c_grpc_node_acq_objs)
    add_library(r2c_grpc_node_acq_objs OBJECT
      src/grpcNode/acquisitionNode.cpp
    )
    target_include_directories(r2c_grpc_node_acq_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(r2c_grpc_node_acq_objs
      PRIVATE
        -Wall
        -Wextra
        -O3
        -march=native
        -fopenmp
    )
    target_link_libraries(r2c_grpc_node_acq_objs
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )
  endif()

  if(NOT TARGET r2c_core_acq_objs)
    add_library(r2c_core_acq_objs OBJECT
      src/core/acquisition/AcquisitionServer.cpp
      src/core/io/RawFileOutputFactory.cpp
      src/core/io/ShardedRawFileOutput.cpp
    )
    target_include_directories(r2c_core_acq_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(r2c_core_acq_objs
      PRIVATE
        -Wall
        -Wextra
        -O3
        -march=native
        -fopenmp
    )
    target_link_libraries(r2c_core_acq_objs
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )
  endif()

  if(R2C_BUILD_TEST_ACQ_CONTROL_INIT)
    add_executable(test_acq_control_init
      tests/correctness/test_acq_control_init.cpp
      $<TARGET_OBJECTS:r2c_grpc_service_acq_master_objs>
      $<TARGET_OBJECTS:r2c_core_acq_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_acq_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(test_acq_control_init
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_acq_control_init PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_acq_control_init
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
  endif()

endif()

if(R2C_BUILD_TEST_PNI_R2C OR R2C_BUILD_TEST_PNI_COIN OR R2C_BUILD_TEST_LOCAL_GRPC_R2S OR R2C_BUILD_TEST_LOCAL_GRPC_R2S_COIN OR R2C_BUILD_TEST_BDM50100_ONLINE_PIPELINE OR R2C_BUILD_APP_ACQ_R2S_NODE OR R2C_BUILD_TEST_R2S_MULTI_GPU OR R2C_BUILD_TEST_R2S_50100_PERF)
  pkg_check_modules(R2C_PNI REQUIRED IMPORTED_TARGET libpni)
  pkg_check_modules(R2C_DPDK QUIET libdpdk)
  find_library(R2C_TBB_LIBRARY NAMES tbb REQUIRED)
  find_package(OpenMP REQUIRED)

  if(EXISTS "/usr/bin/nvcc")
    set(R2C_NVCC_EXECUTABLE "/usr/bin/nvcc")
  else()
    find_program(R2C_NVCC_EXECUTABLE NAMES nvcc REQUIRED)
  endif()

  set(R2C_CUDA_OBJ_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated/cuda")
  file(MAKE_DIRECTORY "${R2C_CUDA_OBJ_DIR}")
  set(R2C_SINGLES_PROCESS_OBJ "${R2C_CUDA_OBJ_DIR}/SinglesProcess.o")
  set(R2C_DPACKETS_ASYNC_OBJ "${R2C_CUDA_OBJ_DIR}/DPacketsAsync.o")
  set(R2C_R2S50100_COMPUTE_OBJ "${R2C_CUDA_OBJ_DIR}/R2S50100Compute.o")
  set(R2C_R2S_CUDA_OBJS
    ${R2C_SINGLES_PROCESS_OBJ}
    ${R2C_DPACKETS_ASYNC_OBJ}
    ${R2C_R2S50100_COMPUTE_OBJ}
  )

  set(R2C_NVCC_INCLUDE_FLAGS "")
  foreach(include_dir IN LISTS R2C_PNI_INCLUDE_DIRS)
    list(APPEND R2C_NVCC_INCLUDE_FLAGS "-I${include_dir}")
  endforeach()

  set(R2C_NVCC_COMMON_FLAGS
    -std=c++20
    -O2
    --expt-relaxed-constexpr
    -ccbin ${CMAKE_CXX_COMPILER}
    ${R2C_NVCC_INCLUDE_FLAGS}
    -I${CMAKE_CURRENT_SOURCE_DIR}/include
    -I${CMAKE_CURRENT_SOURCE_DIR}/protos
    -I${CMAKE_CURRENT_SOURCE_DIR}/src
  )

  add_custom_command(
    OUTPUT "${R2C_SINGLES_PROCESS_OBJ}"
    COMMAND ${R2C_NVCC_EXECUTABLE}
      ${R2C_NVCC_COMMON_FLAGS}
      -c ${CMAKE_CURRENT_SOURCE_DIR}/src/tools/SinglesProcess.cu
      -o ${R2C_SINGLES_PROCESS_OBJ}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/tools/SinglesProcess.cu
    COMMENT "Building CUDA object SinglesProcess.o"
    VERBATIM
  )

  add_custom_command(
    OUTPUT "${R2C_DPACKETS_ASYNC_OBJ}"
    COMMAND ${R2C_NVCC_EXECUTABLE}
      ${R2C_NVCC_COMMON_FLAGS}
      -c ${CMAKE_CURRENT_SOURCE_DIR}/src/core/r2s/multi_gpu/DPacketsAsync.cu
      -o ${R2C_DPACKETS_ASYNC_OBJ}
    DEPENDS
      ${CMAKE_CURRENT_SOURCE_DIR}/src/core/r2s/multi_gpu/DPacketsAsync.cu
      ${CMAKE_CURRENT_SOURCE_DIR}/include/core/r2s/multi_gpu/DPacketsAsync.cuh
    COMMENT "Building CUDA object DPacketsAsync.o"
    VERBATIM
  )

  add_custom_command(
    OUTPUT "${R2C_R2S50100_COMPUTE_OBJ}"
    COMMAND ${R2C_NVCC_EXECUTABLE}
      ${R2C_NVCC_COMMON_FLAGS}
      -c ${CMAKE_CURRENT_SOURCE_DIR}/src/core/r2s/multi_gpu/R2S50100Compute.cu
      -o ${R2C_R2S50100_COMPUTE_OBJ}
    DEPENDS
      ${CMAKE_CURRENT_SOURCE_DIR}/src/core/r2s/multi_gpu/R2S50100Compute.cu
      ${CMAKE_CURRENT_SOURCE_DIR}/include/core/r2s/multi_gpu/R2S50100Compute.cuh
      ${CMAKE_CURRENT_SOURCE_DIR}/include/core/r2s/multi_gpu/PinnedHostCopy.cuh
      ${CMAKE_CURRENT_SOURCE_DIR}/include/core/r2s/multi_gpu/DPacketsAsync.cuh
    COMMENT "Building CUDA object R2S50100Compute.o"
    VERBATIM
  )
  add_custom_target(r2c_cuda_r2s_gen DEPENDS ${R2C_R2S_CUDA_OBJS})
  function(r2c_target_link_cuda_r2s target_name)
    target_link_libraries(${target_name} PRIVATE ${R2C_R2S_CUDA_OBJS})
    add_dependencies(${target_name} r2c_cuda_r2s_gen)
  endfunction()

  add_library(r2c_core_r2s_objs OBJECT
    src/core/r2s/R2S.cpp
    src/core/r2s/multi_gpu/R2S50100MultiGpuEngine.cpp
  )
  target_include_directories(r2c_core_r2s_objs
    PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(r2c_core_r2s_objs
    PRIVATE
      -Wall
      -Wextra
      -O3
      -march=native
      -fopenmp
  )
  target_link_libraries(r2c_core_r2s_objs
    PRIVATE
      PkgConfig::R2C_PNI
      PkgConfig::R2C_GLOG
      OpenMP::OpenMP_CXX
  )

  add_library(r2c_grpc_node_r2s_objs OBJECT
    src/grpcNode/r2sNode.cpp
  )
  target_include_directories(r2c_grpc_node_r2s_objs
    PRIVATE
      "${R2C_PROTO_GEN_ROOT}"
      "${R2C_PROTO_GEN_DIR}"
      "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(r2c_grpc_node_r2s_objs
    PRIVATE
      -Wall
      -Wextra
      -O3
      -march=native
      -fopenmp
  )
  target_link_libraries(r2c_grpc_node_r2s_objs
    PRIVATE
      PkgConfig::R2C_PNI
      PkgConfig::R2C_GRPC
      PkgConfig::R2C_IBVERBS
      PkgConfig::R2C_GLOG
      OpenMP::OpenMP_CXX
      ${R2C_TBB_LIBRARY}
  )

  if(NOT TARGET r2c_grpc_node_acq_objs)
    add_library(r2c_grpc_node_acq_objs OBJECT
      src/grpcNode/acquisitionNode.cpp
    )
    target_include_directories(r2c_grpc_node_acq_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(r2c_grpc_node_acq_objs
      PRIVATE
        -Wall
        -Wextra
        -O3
        -march=native
        -fopenmp
    )
    target_link_libraries(r2c_grpc_node_acq_objs
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )
  endif()

  if(NOT TARGET r2c_core_acq_objs)
    add_library(r2c_core_acq_objs OBJECT
      src/core/acquisition/AcquisitionServer.cpp
      src/core/io/RawFileOutputFactory.cpp
      src/core/io/ShardedRawFileOutput.cpp
    )
    target_include_directories(r2c_core_acq_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(r2c_core_acq_objs
      PRIVATE
        -Wall
        -Wextra
        -O3
        -march=native
        -fopenmp
    )
    target_link_libraries(r2c_core_acq_objs
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )
  endif()

  add_library(r2c_grpc_service_coin_client_objs OBJECT
    src/grpcService/CoincidenceClient.cpp
  )
  target_include_directories(r2c_grpc_service_coin_client_objs
    PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}"
      "${R2C_PROTO_GEN_ROOT}"
      "${R2C_PROTO_GEN_DIR}"
      "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(r2c_grpc_service_coin_client_objs
    PRIVATE
      -Wall
      -Wextra
      -O3
      -march=native
      -fopenmp
  )
  target_link_libraries(r2c_grpc_service_coin_client_objs
    PRIVATE
      PkgConfig::R2C_PNI
      PkgConfig::R2C_GRPC
      PkgConfig::R2C_IBVERBS
      PkgConfig::R2C_GLOG
      OpenMP::OpenMP_CXX
      ${R2C_TBB_LIBRARY}
  )

  if(NOT TARGET r2c_app_common_objs)
    add_library(r2c_app_common_objs OBJECT
      app/common/AppConfig.cpp
    )
    target_include_directories(r2c_app_common_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(r2c_app_common_objs
      PRIVATE
        -Wall
        -Wextra
        -O3
        -march=native
        -fopenmp
    )
    target_link_libraries(r2c_app_common_objs
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )
  endif()

  set_source_files_properties(${R2C_R2S_CUDA_OBJS} PROPERTIES EXTERNAL_OBJECT TRUE GENERATED TRUE)

  if(R2C_BUILD_TEST_PNI_R2C)
    add_executable(test_pni_r2s_offline
      tests/correctness/test_pni_r2s_offline.cpp
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
    )
    target_include_directories(test_pni_r2s_offline
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}/protos"
    )
    target_compile_options(test_pni_r2s_offline PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_pni_r2s_offline
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
    r2c_target_link_cuda_r2s(test_pni_r2s_offline)
  endif()

  if(R2C_BUILD_TEST_PNI_COIN)
    add_executable(test_pni_coin_offline
      tests/correctness/test_pni_coin_offline.cpp
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
    )
    target_include_directories(test_pni_coin_offline
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}/protos"
    )
    target_compile_options(test_pni_coin_offline PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_pni_coin_offline
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
    r2c_target_link_cuda_r2s(test_pni_coin_offline)
  endif()

  if(R2C_BUILD_TEST_LOCAL_GRPC_R2S)
    add_executable(test_r2s_rdma_send
      tests/correctness/test_r2s_rdma_send.cpp
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_r2s_objs>
      $<TARGET_OBJECTS:r2c_dataplane_rdma_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(test_r2s_rdma_send
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_r2s_rdma_send PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_r2s_rdma_send
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
    r2c_target_link_cuda_r2s(test_r2s_rdma_send)
  endif()

  if(R2C_BUILD_TEST_LOCAL_GRPC_R2S_COIN)
    if(NOT TARGET r2c_core_streaming_objs OR NOT TARGET r2c_grpc_service_coin_objs OR NOT TARGET r2c_grpc_node_coin_objs)
      message(FATAL_ERROR
        "R2C_BUILD_TEST_LOCAL_GRPC_R2S_COIN requires streaming/coin objects. "
        "Ensure the streaming block is enabled (option itself pulls it in).")
    endif()
    if(NOT DEFINED R2C_S2C_COMPUTE_OBJ)
      message(FATAL_ERROR "R2C_BUILD_TEST_LOCAL_GRPC_R2S_COIN requires R2C_S2C_COMPUTE_OBJ from streaming block")
    endif()

    add_executable(test_r2s_rdma_coin
      tests/correctness/test_r2s_rdma_coin.cpp
      tests/correctness/test_r2s_rdma_coin_runner.cpp
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_r2s_objs>
      $<TARGET_OBJECTS:r2c_grpc_service_coin_objs>
        $<TARGET_OBJECTS:r2c_dataplane_rdma_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_coin_objs>
      $<TARGET_OBJECTS:r2c_core_streaming_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(test_r2s_rdma_coin
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_r2s_rdma_coin PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_r2s_rdma_coin
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
    r2c_target_link_cuda_r2s(test_r2s_rdma_coin)
    r2c_target_link_cuda_s2c(test_r2s_rdma_coin)
  endif()

  if(R2C_BUILD_APP_ACQ_R2S_NODE)
    add_executable(app_acq_r2s_node
      app/acq_r2s_node_main.cpp
      $<TARGET_OBJECTS:r2c_app_common_objs>
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
      $<TARGET_OBJECTS:r2c_core_acq_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_acq_objs>
      $<TARGET_OBJECTS:r2c_grpc_service_coin_client_objs>
      $<TARGET_OBJECTS:r2c_dataplane_rdma_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(app_acq_r2s_node
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(app_acq_r2s_node PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(app_acq_r2s_node
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
    r2c_target_link_cuda_r2s(app_acq_r2s_node)
    if(R2C_DPDK_FOUND)
      target_link_directories(app_acq_r2s_node PRIVATE ${R2C_DPDK_LIBRARY_DIRS})
      target_link_options(app_acq_r2s_node PRIVATE -Wl,--no-as-needed)
      target_link_libraries(app_acq_r2s_node
        PRIVATE
          PkgConfig::R2C_PNI
          ${R2C_DPDK_LINK_LIBRARIES}
      )
    endif()
  endif()

  if(R2C_BUILD_TEST_BDM50100_ONLINE_PIPELINE)
    if(NOT TARGET r2c_core_streaming_objs OR NOT TARGET r2c_grpc_service_coin_objs OR NOT TARGET r2c_grpc_node_coin_objs)
      message(FATAL_ERROR
        "R2C_BUILD_TEST_BDM50100_ONLINE_PIPELINE requires streaming/coin objects. "
        "Ensure the streaming block is enabled (option itself pulls it in).")
    endif()
    if(NOT DEFINED R2C_S2C_COMPUTE_OBJ)
      message(FATAL_ERROR "R2C_BUILD_TEST_BDM50100_ONLINE_PIPELINE requires R2C_S2C_COMPUTE_OBJ from streaming block")
    endif()
    if(NOT TARGET r2c_grpc_service_acq_master_objs)
      message(FATAL_ERROR
        "R2C_BUILD_TEST_BDM50100_ONLINE_PIPELINE requires AcquisitionMaster objects. "
        "Ensure the acquisition or streaming block is enabled.")
    endif()

    add_executable(test_r2s_online_pipeline
      tests/correctness/test_r2s_online_pipeline.cpp
      tests/correctness/test_r2s_online_pipeline_r2s.cpp
      $<TARGET_OBJECTS:r2c_grpc_service_acq_master_objs>
      $<TARGET_OBJECTS:r2c_core_acq_objs>
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_acq_objs>
      $<TARGET_OBJECTS:r2c_grpc_service_coin_objs>
      $<TARGET_OBJECTS:r2c_dataplane_rdma_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_coin_objs>
      $<TARGET_OBJECTS:r2c_core_streaming_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(test_r2s_online_pipeline
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_ROOT}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_r2s_online_pipeline PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_r2s_online_pipeline
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_IBVERBS
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
    r2c_target_link_cuda_r2s(test_r2s_online_pipeline)
    r2c_target_link_cuda_s2c(test_r2s_online_pipeline)
  endif()

  if(R2C_BUILD_TEST_R2S_MULTI_GPU)
    add_executable(test_r2s_50100_multi_gpu
      tests/correctness/test_r2s_50100_multi_gpu.cpp
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
    )
    target_include_directories(test_r2s_50100_multi_gpu
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}/protos"
    )
    target_compile_options(test_r2s_50100_multi_gpu PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_r2s_50100_multi_gpu
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
    r2c_target_link_cuda_r2s(test_r2s_50100_multi_gpu)
  endif()

  if(R2C_BUILD_TEST_R2S_50100_PERF)
    add_executable(test_r2s_50100_single_ring
      tests/performance/test_r2s_50100_single_ring.cpp
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
    )
    target_include_directories(test_r2s_50100_single_ring
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}/protos"
    )
    target_compile_options(test_r2s_50100_single_ring PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_r2s_50100_single_ring
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
    r2c_target_link_cuda_r2s(test_r2s_50100_single_ring)
  endif()
endif()

if(R2C_BUILD_TOOL_UDP_RAW_REPLAYER)
  pkg_check_modules(R2C_PNI REQUIRED IMPORTED_TARGET libpni)
  find_package(OpenMP REQUIRED)

  add_executable(tool_udp_raw_replayer
    src/tools/udp_raw_replayer_main.cpp
  )
  target_include_directories(tool_udp_raw_replayer
    PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(tool_udp_raw_replayer
    PRIVATE
      -Wall
      -Wextra
      -O3
      -march=native
      -fopenmp
  )
  target_link_libraries(tool_udp_raw_replayer
    PRIVATE
      PkgConfig::R2C_PNI
      PkgConfig::R2C_GRPC
      PkgConfig::R2C_IBVERBS
      PkgConfig::R2C_GLOG
      OpenMP::OpenMP_CXX
      dl
  )
endif()

if(R2C_BUILD_TOOL_SHARDED_RAW_MERGE)
  pkg_check_modules(R2C_PNI REQUIRED IMPORTED_TARGET libpni)

  add_executable(tool_sharded_raw_merge
    src/tools/sharded_raw_merge_main.cpp
    src/core/io/ShardedRawFileOutput.cpp
  )
  add_dependencies(tool_sharded_raw_merge r2c_proto_objs)
  target_include_directories(tool_sharded_raw_merge
    PRIVATE
      "${CMAKE_CURRENT_BINARY_DIR}/generated"
      "${R2C_PROTO_GEN_ROOT}"
      "${R2C_PROTO_GEN_DIR}"
      "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(tool_sharded_raw_merge
    PRIVATE
      -Wall
      -Wextra
      -O3
      -march=native
  )
  target_link_libraries(tool_sharded_raw_merge
    PRIVATE
      PkgConfig::R2C_PNI
      dl
  )
endif()

if(R2C_BUILD_TOOL_DPDK_TX_REPLAYER)
  pkg_check_modules(R2C_DPDK REQUIRED IMPORTED_TARGET libdpdk)

  add_executable(tool_dpdk_tx_replayer
    src/tools/dpdk_tx_replayer_main.cpp
  )
  target_include_directories(tool_dpdk_tx_replayer
    PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(tool_dpdk_tx_replayer
    PRIVATE
      -Wall
      -Wextra
      -O3
      -march=native
  )
  target_link_libraries(tool_dpdk_tx_replayer
    PRIVATE
      PkgConfig::R2C_DPDK
      dl
  )
endif()

# Output directories
function(r2c_set_output_dir target_name output_subdir)
  if(TARGET ${target_name})
    set_property(TARGET ${target_name} PROPERTY RUNTIME_OUTPUT_DIRECTORY "${R2C_BIN_DIR}/${output_subdir}")
  endif()
endfunction()

r2c_set_output_dir(app_acq_r2s_node app)
r2c_set_output_dir(app_coin_master app)

r2c_set_output_dir(tool_sharded_raw_merge tools)
r2c_set_output_dir(tool_udp_raw_replayer tools)
r2c_set_output_dir(tool_dpdk_tx_replayer tools)

r2c_set_output_dir(test_timesync_algorithm test)
r2c_set_output_dir(test_timesync_grpc test)
r2c_set_output_dir(test_coin_streaming_aligner test)
r2c_set_output_dir(test_coin_synthetic_singles test)
r2c_set_output_dir(test_coin_multi_gpu test)
r2c_set_output_dir(test_rdma_recv_stub test)
r2c_set_output_dir(test_rdma_orchestration test)
r2c_set_output_dir(test_app_config_parse test)
r2c_set_output_dir(test_app_inprocess_smoke test)
r2c_set_output_dir(test_acq_control_init test)
r2c_set_output_dir(test_pni_r2s_offline test)
r2c_set_output_dir(test_pni_coin_offline test)
r2c_set_output_dir(test_coin_carry_boundary test)
r2c_set_output_dir(test_r2s_rdma_send test)
r2c_set_output_dir(test_r2s_rdma_coin test)
r2c_set_output_dir(test_rdma_singles_ingress test)
r2c_set_output_dir(test_rdma_dataplane_loopback test)
r2c_set_output_dir(test_coin_lsingle_stream test)
r2c_set_output_dir(test_r2s_online_pipeline test)
r2c_set_output_dir(test_r2s_50100_multi_gpu test)
r2c_set_output_dir(test_r2s_50100_single_ring test)
r2c_set_output_dir(test_coin_9120_aligner test)

# CTest registration: only register targets that are enabled and built.
function(r2c_register_test target_name tier_label)
  if(BUILD_TESTING AND TARGET ${target_name})
    set(r2c_test_labels "${tier_label}")
    if(ARGN)
      list(APPEND r2c_test_labels ${ARGN})
    endif()

    add_test(NAME ${target_name} COMMAND $<TARGET_FILE:${target_name}>)
    set_tests_properties(${target_name} PROPERTIES
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
      LABELS "${r2c_test_labels}"
      TIMEOUT 120
    )
  endif()
endfunction()

r2c_register_test(test_timesync_algorithm core correctness)
r2c_register_test(test_timesync_grpc core correctness)

r2c_register_test(test_coin_streaming_aligner pni integration manual correctness)
r2c_register_test(test_coin_synthetic_singles pni correctness)
r2c_register_test(test_coin_multi_gpu cuda integration correctness)
r2c_register_test(test_rdma_recv_stub pni integration manual correctness)
r2c_register_test(test_rdma_orchestration pni integration correctness)
r2c_register_test(test_app_config_parse pni correctness)
r2c_register_test(test_app_inprocess_smoke pni integration correctness)
r2c_register_test(test_acq_control_init pni correctness)

r2c_register_test(test_pni_r2s_offline cuda manual correctness)
r2c_register_test(test_pni_coin_offline cuda manual correctness)
r2c_register_test(test_coin_carry_boundary cuda integration correctness)
r2c_register_test(test_r2s_rdma_send cuda integration correctness)
r2c_register_test(test_r2s_rdma_coin cuda integration correctness)
if(BUILD_TESTING AND TARGET test_r2s_rdma_coin)
  set_tests_properties(test_r2s_rdma_coin PROPERTIES TIMEOUT 7200)
endif()
r2c_register_test(test_rdma_singles_ingress cuda integration correctness)
r2c_register_test(test_rdma_dataplane_loopback pni correctness)
r2c_register_test(test_coin_lsingle_stream cuda integration correctness)
if(BUILD_TESTING AND TARGET test_coin_lsingle_stream)
  set_tests_properties(test_coin_lsingle_stream PROPERTIES TIMEOUT 7200)
endif()
r2c_register_test(test_r2s_online_pipeline cuda integration manual correctness)
if(BUILD_TESTING AND TARGET test_r2s_online_pipeline)
  set_tests_properties(test_r2s_online_pipeline PROPERTIES TIMEOUT 7200)
endif()
r2c_register_test(test_r2s_50100_multi_gpu cuda integration correctness)
r2c_register_test(test_r2s_50100_single_ring cuda performance manual)
if(BUILD_TESTING AND TARGET test_r2s_50100_single_ring)
  set_tests_properties(test_r2s_50100_single_ring PROPERTIES TIMEOUT 7200)
endif()
r2c_register_test(test_coin_9120_aligner cuda performance manual)
if(BUILD_TESTING AND TARGET test_coin_9120_aligner)
  set_tests_properties(test_coin_9120_aligner PROPERTIES TIMEOUT 7200)
endif()
