cmake_minimum_required(VERSION 3.20)



add_library(reclaimnet_control STATIC
  src/topology.cc
  src/hazard_estimator.cc
  src/job_registry.cc
  src/node_registry.cc
  src/p1_allocator.cc
  src/p2_selector.cc
  src/p3_admission.cc
  src/persistence.cc
  src/coordinator.cc
  src/grpc_service.cc
)

target_include_directories(reclaimnet_control
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(reclaimnet_control PUBLIC cxx_std_17)
target_link_libraries(reclaimnet_control PUBLIC reclaimnet_common)


find_package(SQLite3 QUIET)
if(SQLite3_FOUND)
  target_link_libraries(reclaimnet_control PUBLIC SQLite::SQLite3)
  message(STATUS "reclaimnet_control: SQLite3 found")
else()
  message(STATUS "reclaimnet_control: SQLite3 NOT found – persistence disabled")
endif()


find_package(nlohmann_json QUIET)
if(nlohmann_json_FOUND)
  target_link_libraries(reclaimnet_control PUBLIC nlohmann_json::nlohmann_json)
  message(STATUS "reclaimnet_control: nlohmann_json found")
else()
  message(STATUS "reclaimnet_control: nlohmann/json NOT found – topology file load disabled")
endif()


find_package(gRPC QUIET)
if(gRPC_FOUND)
  target_link_libraries(reclaimnet_control PUBLIC gRPC::grpc++ reclaimnet_proto)
  message(STATUS "reclaimnet_control: gRPC found")
endif()


add_executable(coordinator src/coordinator_main.cc)
target_link_libraries(coordinator PRIVATE reclaimnet_control)
