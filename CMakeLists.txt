cmake_minimum_required(VERSION 3.20)
project(ReclaimNet VERSION 0.1.0 LANGUAGES CXX C)



set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")



include(ReclaimNetUtils)



find_package(Protobuf QUIET)
find_package(gRPC     QUIET)
if(Protobuf_FOUND AND gRPC_FOUND)
  add_subdirectory(proto)
  message(STATUS "ReclaimNet: protobuf + gRPC found – building proto library")
else()
  message(STATUS "ReclaimNet: protobuf/gRPC not found – skipping proto codegen")

  add_library(reclaimnet_proto INTERFACE)
endif()



add_subdirectory(common)



add_subdirectory(control_plane)



add_subdirectory(node_agent)



option(RECLAIMNET_BUILD_TESTS "Build unit tests" ON)
if(RECLAIMNET_BUILD_TESTS)
  add_subdirectory(tests)
endif()



if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  find_package(Libbpf QUIET)
  if(Libbpf_FOUND OR libbpf_FOUND)
    message(STATUS "ReclaimNet: libbpf found – eBPF data plane enabled")
  else()
    message(STATUS "ReclaimNet: libbpf NOT found – use 'make -C data_plane' separately")
  endif()
else()
  message(STATUS "ReclaimNet: non-Linux build – eBPF data plane skipped")
endif()



install(TARGETS coordinator node_agent
  RUNTIME DESTINATION bin
)
install(DIRECTORY configs/ DESTINATION etc/reclaimnet)
