cmake_minimum_required(VERSION 3.20)

project(r2c LANGUAGES CXX)

include(CTest)

# Build options: Tests
# - core (grpc/protobuf only)
option(R2C_BUILD_TEST_GRPC "Build gRPC clock-sync test" ON)
option(R2C_BUILD_TEST_BASIC "Build mock clock-sync test" ON)

# - pni (libpni + tbb + openmp, no CUDA)
option(R2C_BUILD_TEST_STREAMING "Build streaming coincidence test (requires libpni)" OFF)
option(R2C_BUILD_TEST_LOCAL_GRPC_COIN "Build local gRPC coin receiver test (requires libpni, tbb)" OFF)
option(R2C_BUILD_TEST_ACQ_CONTROL_INIT "Build acquisition control init test (requires libpni, tbb)" OFF)
option(R2C_BUILD_TEST_ACQ_DATAPATH_UDP "Build acquisition datapath UDP test (requires libpni, tbb)" OFF)

# - cuda (nvcc + libpni + tbb + openmp)
option(R2C_BUILD_TEST_PNI_R2C "Build PNI R2C test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_PNI_COIN "Build PNI standalone coincidence test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_LOCAL_GRPC_R2S "Build local gRPC R2S test (requires libpni, nvcc, tbb)" OFF)
option(R2C_BUILD_TEST_ACQ_R2S_PIPELINE "Build acquisition to R2S pipeline test (requires libpni, nvcc, tbb)" OFF)

# Build options: Apps
# - basic (libpni + tbb + openmp, no CUDA)
option(R2C_BUILD_APP_COIN_MASTER "Build app coin master executable (requires libpni, tbb)" OFF)
option(R2C_BUILD_APP_UDP_RAW_REPLAYER "Build app udp raw replayer executable (requires libpni)" OFF)

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

set(R2C_PKG_CONFIG_PATH "/usr/lib/x86_64-linux-gnu/pkgconfig" CACHE STRING "pkg-config search path override; set empty to use environment default")
if(NOT R2C_PKG_CONFIG_PATH STREQUAL "")
  set(ENV{PKG_CONFIG_PATH} "${R2C_PKG_CONFIG_PATH}")
endif()

# Keep migration low-risk: use pkg-config (same dependency source as Makefile).
pkg_check_modules(R2C_GRPC REQUIRED IMPORTED_TARGET grpc++ protobuf)
pkg_check_modules(R2C_GLOG REQUIRED IMPORTED_TARGET libglog gflags)

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

set(R2C_PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/protos")
set(R2C_PROTO_FILES
  "${R2C_PROTO_DIR}/acquisition.proto"
  "${R2C_PROTO_DIR}/coincidence.proto"
  "${R2C_PROTO_DIR}/timesync.proto"
)

set(R2C_PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated/protos")
file(MAKE_DIRECTORY "${R2C_PROTO_GEN_DIR}")

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

if(R2C_BUILD_TEST_BASIC)
  add_executable(test_distributed_clock_sync tests/test_distributed_clock_sync.cpp)
  target_include_directories(test_distributed_clock_sync
    PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
      "${CMAKE_CURRENT_SOURCE_DIR}/protos"
  )
  target_compile_options(test_distributed_clock_sync PRIVATE -Wall -Wextra -O2 -pthread)
endif()

if(R2C_BUILD_TEST_GRPC)
  add_executable(test_distributed_clock_sync_grpc
    tests/test_distributed_clock_sync_grpc.cpp
    $<TARGET_OBJECTS:r2c_proto_objs>
  )
  target_include_directories(test_distributed_clock_sync_grpc
    PRIVATE
      "${R2C_PROTO_GEN_DIR}"
      "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(test_distributed_clock_sync_grpc PRIVATE -Wall -Wextra -O2 -pthread)
  target_link_libraries(test_distributed_clock_sync_grpc
    PRIVATE
      PkgConfig::R2C_GRPC
      PkgConfig::R2C_GLOG
      dl
  )
endif()

if(R2C_BUILD_TEST_STREAMING OR R2C_BUILD_TEST_LOCAL_GRPC_COIN OR R2C_BUILD_APP_COIN_MASTER)
  pkg_check_modules(R2C_PNI REQUIRED IMPORTED_TARGET libpni)

  if(NOT TARGET r2c_core_streaming_objs)
    add_library(r2c_core_streaming_objs OBJECT
      src/core/streaming/StreamingCoincidence.cpp
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

  add_executable(test_streaming_coincidence
    tests/test_streaming_coincidence.cpp
    $<TARGET_OBJECTS:r2c_core_streaming_objs>
    $<TARGET_OBJECTS:r2c_proto_objs>
  )
  target_include_directories(test_streaming_coincidence
    PRIVATE
      "${R2C_PROTO_GEN_DIR}"
      "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(test_streaming_coincidence PRIVATE -Wall -Wextra -O2 -pthread)
  target_link_libraries(test_streaming_coincidence
    PRIVATE
      PkgConfig::R2C_PNI
      PkgConfig::R2C_GRPC
      PkgConfig::R2C_GLOG
      dl
  )

  if(R2C_BUILD_TEST_LOCAL_GRPC_COIN OR R2C_BUILD_APP_COIN_MASTER)
    find_library(R2C_TBB_LIBRARY NAMES tbb REQUIRED)
    find_package(OpenMP REQUIRED)

    if(NOT TARGET r2c_app_common_objs)
      add_library(r2c_app_common_objs OBJECT
        app/common/AppConfig.cpp
      )
      target_include_directories(r2c_app_common_objs
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
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
          PkgConfig::R2C_GLOG
          OpenMP::OpenMP_CXX
          ${R2C_TBB_LIBRARY}
      )
    endif()

    add_library(r2c_grpc_service_coin_objs OBJECT
      src/grpcService/CoincidenceServiceImpl.cpp
      src/grpcService/CoincidenceClient.cpp
    )
    target_include_directories(r2c_grpc_service_coin_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
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
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )

    add_library(r2c_grpc_node_coin_objs OBJECT
      src/grpcNode/coinNode.cpp
    )
    target_include_directories(r2c_grpc_node_coin_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
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
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )

    add_executable(test_local_grpc_coin
      tests/test_local_grpc_coin.cpp
      $<TARGET_OBJECTS:r2c_grpc_service_coin_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_coin_objs>
      $<TARGET_OBJECTS:r2c_core_streaming_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(test_local_grpc_coin
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_local_grpc_coin PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_local_grpc_coin
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )

    if(R2C_BUILD_APP_COIN_MASTER)
      add_executable(app_coin_master
        app/coin_master_main.cpp
        $<TARGET_OBJECTS:r2c_app_common_objs>
        $<TARGET_OBJECTS:r2c_grpc_service_acq_master_objs>
        $<TARGET_OBJECTS:r2c_grpc_service_coin_objs>
        $<TARGET_OBJECTS:r2c_grpc_node_coin_objs>
        $<TARGET_OBJECTS:r2c_core_streaming_objs>
        $<TARGET_OBJECTS:r2c_proto_objs>
      )
      target_include_directories(app_coin_master
        PRIVATE
          "${CMAKE_CURRENT_SOURCE_DIR}"
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
          PkgConfig::R2C_GLOG
          OpenMP::OpenMP_CXX
          ${R2C_TBB_LIBRARY}
          dl
      )
    endif()
  endif()
endif()

if(R2C_BUILD_TEST_ACQ_CONTROL_INIT OR R2C_BUILD_TEST_ACQ_DATAPATH_UDP OR R2C_BUILD_TEST_ACQ_R2S_PIPELINE)
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
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )
  endif()

  if(NOT TARGET r2c_core_acq_objs)
    add_library(r2c_core_acq_objs OBJECT
      src/core/acquisition/AcquisitionServer.cpp
    )
    target_include_directories(r2c_core_acq_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
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
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )
  endif()

  if(R2C_BUILD_TEST_ACQ_CONTROL_INIT)
    add_executable(test_acquisition_control_init
      tests/test_acquisition_control_init.cpp
      $<TARGET_OBJECTS:r2c_grpc_service_acq_master_objs>
      $<TARGET_OBJECTS:r2c_core_acq_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_acq_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(test_acquisition_control_init
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_acquisition_control_init PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_acquisition_control_init
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
  endif()

  if(R2C_BUILD_TEST_ACQ_DATAPATH_UDP)
    add_executable(test_acquisition_datapath_udp
      tests/test_acquisition_datapath_udp.cpp
      $<TARGET_OBJECTS:r2c_grpc_service_acq_master_objs>
      $<TARGET_OBJECTS:r2c_core_acq_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_acq_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(test_acquisition_datapath_udp
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_acquisition_datapath_udp PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_acquisition_datapath_udp
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
  endif()
endif()

if(R2C_BUILD_TEST_PNI_R2C OR R2C_BUILD_TEST_PNI_COIN OR R2C_BUILD_TEST_LOCAL_GRPC_R2S OR R2C_BUILD_TEST_ACQ_R2S_PIPELINE OR R2C_BUILD_APP_ACQ_R2S_NODE)
  pkg_check_modules(R2C_PNI REQUIRED IMPORTED_TARGET libpni)
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

  set(R2C_NVCC_INCLUDE_FLAGS "")
  foreach(include_dir IN LISTS R2C_PNI_INCLUDE_DIRS)
    list(APPEND R2C_NVCC_INCLUDE_FLAGS "-I${include_dir}")
  endforeach()

  add_custom_command(
    OUTPUT "${R2C_SINGLES_PROCESS_OBJ}"
    COMMAND ${R2C_NVCC_EXECUTABLE}
      -std=c++20
      -O2
      --expt-relaxed-constexpr
      -ccbin ${CMAKE_CXX_COMPILER}
      ${R2C_NVCC_INCLUDE_FLAGS}
      -I${CMAKE_CURRENT_SOURCE_DIR}/include
      -I${CMAKE_CURRENT_SOURCE_DIR}/protos
      -I${CMAKE_CURRENT_SOURCE_DIR}/src
      -c ${CMAKE_CURRENT_SOURCE_DIR}/src/tools/SinglesProcess.cu
      -o ${R2C_SINGLES_PROCESS_OBJ}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/tools/SinglesProcess.cu
    COMMENT "Building CUDA object SinglesProcess.o"
    VERBATIM
  )

  add_library(r2c_core_r2s_objs OBJECT
    src/core/r2s/R2S.cpp
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
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )
  endif()

  if(NOT TARGET r2c_core_acq_objs)
    add_library(r2c_core_acq_objs OBJECT
      src/core/acquisition/AcquisitionServer.cpp
    )
    target_include_directories(r2c_core_acq_objs
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
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
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
    )
  endif()

  set_source_files_properties(${R2C_SINGLES_PROCESS_OBJ} PROPERTIES EXTERNAL_OBJECT TRUE GENERATED TRUE)

  if(R2C_BUILD_TEST_PNI_R2C)
    add_executable(test_pni_r2c
      tests/test_pni_r2c.cpp
      ${R2C_SINGLES_PROCESS_OBJ}
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
    )
    target_include_directories(test_pni_r2c
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}/protos"
    )
    target_compile_options(test_pni_r2c PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_pni_r2c
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
  endif()

  if(R2C_BUILD_TEST_PNI_COIN)
    add_executable(test_pni_coin
      tests/test_pni_coin.cpp
      ${R2C_SINGLES_PROCESS_OBJ}
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
    )
    target_include_directories(test_pni_coin
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}/protos"
    )
    target_compile_options(test_pni_coin PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_pni_coin
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
  endif()

  if(R2C_BUILD_TEST_LOCAL_GRPC_R2S)
    add_executable(test_local_grpc_r2s
      tests/test_local_grpc_r2s.cpp
      ${R2C_SINGLES_PROCESS_OBJ}
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_r2s_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(test_local_grpc_r2s
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_local_grpc_r2s PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_local_grpc_r2s
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
  endif()

  if(R2C_BUILD_APP_ACQ_R2S_NODE)
    add_executable(app_acq_r2s_node
      app/acq_r2s_node_main.cpp
      ${R2C_SINGLES_PROCESS_OBJ}
      $<TARGET_OBJECTS:r2c_app_common_objs>
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
      $<TARGET_OBJECTS:r2c_core_acq_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_acq_objs>
      $<TARGET_OBJECTS:r2c_grpc_service_coin_client_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(app_acq_r2s_node
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
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
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
  endif()

  if(R2C_BUILD_TEST_ACQ_R2S_PIPELINE)
    add_executable(test_acquisition_r2s_pipeline
      tests/test_acquisition_r2s_pipeline.cpp
      ${R2C_SINGLES_PROCESS_OBJ}
      $<TARGET_OBJECTS:r2c_grpc_service_acq_master_objs>
      $<TARGET_OBJECTS:r2c_core_acq_objs>
      $<TARGET_OBJECTS:r2c_core_r2s_objs>
      $<TARGET_OBJECTS:r2c_grpc_node_acq_objs>
      $<TARGET_OBJECTS:r2c_proto_objs>
    )
    target_include_directories(test_acquisition_r2s_pipeline
      PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${R2C_PROTO_GEN_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/include_override"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_options(test_acquisition_r2s_pipeline PRIVATE -O3 -march=native -fopenmp)
    target_link_libraries(test_acquisition_r2s_pipeline
      PRIVATE
        PkgConfig::R2C_PNI
        PkgConfig::R2C_GRPC
        PkgConfig::R2C_GLOG
        OpenMP::OpenMP_CXX
        ${R2C_TBB_LIBRARY}
        dl
    )
  endif()
endif()

if(R2C_BUILD_APP_UDP_RAW_REPLAYER)
  pkg_check_modules(R2C_PNI REQUIRED IMPORTED_TARGET libpni)
  find_package(OpenMP REQUIRED)

  add_executable(app_udp_raw_replayer
    app/udp_raw_replayer_main.cpp
  )
  target_include_directories(app_udp_raw_replayer
    PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_compile_options(app_udp_raw_replayer
    PRIVATE
      -Wall
      -Wextra
      -O3
      -march=native
      -fopenmp
  )
  target_link_libraries(app_udp_raw_replayer
    PRIVATE
      PkgConfig::R2C_PNI
      PkgConfig::R2C_GRPC
      PkgConfig::R2C_GLOG
      OpenMP::OpenMP_CXX
      dl
  )
endif()

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

r2c_register_test(test_distributed_clock_sync core)
r2c_register_test(test_distributed_clock_sync_grpc core)

r2c_register_test(test_streaming_coincidence pni)
r2c_register_test(test_local_grpc_coin pni integration)
r2c_register_test(test_acquisition_control_init pni)
r2c_register_test(test_acquisition_datapath_udp pni)

r2c_register_test(test_pni_r2c cuda)
r2c_register_test(test_pni_coin cuda)
r2c_register_test(test_local_grpc_r2s cuda integration)
r2c_register_test(test_acquisition_r2s_pipeline cuda)
