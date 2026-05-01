cmake_minimum_required(VERSION 3.20)



set(AGENT_SOURCES
  src/runtime.cc
  src/runtime_python.cc
  src/gpu_isolation.cc
  src/ebpf_loader.cc
  src/agent_service.cc
  src/checkpoint_manager.cc
  src/migration_client.cc
  src/flow_table.cc
  src/telemetry.cc
  src/reclaim_handler.cc
  src/grpc_client.cc
  src/agent.cc
)



add_library(reclaimnet_agent STATIC ${AGENT_SOURCES})

target_include_directories(reclaimnet_agent
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(reclaimnet_agent PUBLIC cxx_std_17)
target_link_libraries(reclaimnet_agent PUBLIC reclaimnet_common)
target_link_libraries(reclaimnet_agent PUBLIC ${CMAKE_DL_LIBS})
find_package(Python3 COMPONENTS Interpreter Development QUIET)
find_package(pybind11 CONFIG QUIET)
set(RECLAIMNET_APP_BRIDGE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/python" CACHE PATH "Application checkpoint bridge module directory")
if(Python3_FOUND AND pybind11_FOUND)
  target_link_libraries(reclaimnet_agent PUBLIC pybind11::embed)
  target_compile_definitions(reclaimnet_agent PRIVATE RECLAIMNET_HAVE_PYBIND=1
    RECLAIMNET_APP_BRIDGE_DIR="${RECLAIMNET_APP_BRIDGE_DIR}"
    RECLAIMNET_PYTHON_EXECUTABLE="${Python3_EXECUTABLE}")
else()
  target_compile_definitions(reclaimnet_agent PRIVATE RECLAIMNET_HAVE_PYBIND=0)
endif()


target_link_libraries(reclaimnet_agent PUBLIC reclaimnet_proto)


if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  find_package(Libbpf QUIET)
  if(Libbpf_FOUND)
    target_compile_definitions(reclaimnet_agent PUBLIC RECLAIMNET_HAVE_LIBBPF=1)
    target_link_libraries(reclaimnet_agent PUBLIC libbpf::libbpf)
    message(STATUS "reclaimnet_agent: libbpf found")
  else()
    target_compile_definitions(reclaimnet_agent PUBLIC RECLAIMNET_HAVE_LIBBPF=0)
    message(STATUS "reclaimnet_agent: libbpf NOT found – eBPF disabled")
  endif()
else()
  target_compile_definitions(reclaimnet_agent PUBLIC RECLAIMNET_HAVE_LIBBPF=0)
endif()


add_executable(node_agent src/agent_main.cc)
target_link_libraries(node_agent PRIVATE reclaimnet_agent)
