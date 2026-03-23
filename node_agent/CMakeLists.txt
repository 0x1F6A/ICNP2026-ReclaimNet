cmake_minimum_required(VERSION 3.20)



set(AGENT_SOURCES
  src/checkpoint_manager.cc
  src/migration_client.cc
  src/flow_table.cc
  src/telemetry.cc
  src/reclaim_handler.cc
  src/grpc_client.cc
  src/agent.cc
)


if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND AGENT_SOURCES src/ebpf_loader.cc)
endif()

add_library(reclaimnet_agent STATIC ${AGENT_SOURCES})

target_include_directories(reclaimnet_agent
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(reclaimnet_agent PUBLIC cxx_std_17)
target_link_libraries(reclaimnet_agent PUBLIC reclaimnet_common)


find_package(gRPC QUIET)
if(gRPC_FOUND)
  target_link_libraries(reclaimnet_agent PUBLIC gRPC::grpc++ reclaimnet_proto)
endif()


if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  find_package(Libbpf QUIET)
  if(Libbpf_FOUND OR libbpf_FOUND)
    target_link_libraries(reclaimnet_agent PUBLIC libbpf::libbpf)
    message(STATUS "reclaimnet_agent: libbpf found")
  else()
    message(STATUS "reclaimnet_agent: libbpf NOT found – eBPF disabled")
  endif()
endif()


add_executable(node_agent src/agent_main.cc)
target_link_libraries(node_agent PRIVATE reclaimnet_agent)
