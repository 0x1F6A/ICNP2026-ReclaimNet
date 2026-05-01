add_library(reclaimnet_control STATIC
  src/topology.cc src/hazard_estimator.cc src/job_registry.cc src/node_registry.cc
  src/p1_allocator.cc src/p2_selector.cc src/p3_admission.cc src/persistence.cc
  src/coordinator.cc src/grpc_service.cc src/capacity_estimator.cc src/checkpoint_catalog.cc)
target_include_directories(reclaimnet_control PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_compile_features(reclaimnet_control PUBLIC cxx_std_17)
target_link_libraries(reclaimnet_control PUBLIC reclaimnet_common reclaimnet_proto)
if(RECLAIMNET_ENABLE_SQLITE AND SQLite3_FOUND)
  if(TARGET SQLite3::SQLite3)
    target_link_libraries(reclaimnet_control PUBLIC SQLite3::SQLite3)
  else()
    target_link_libraries(reclaimnet_control PUBLIC SQLite::SQLite3)
  endif()
  target_compile_definitions(reclaimnet_control PUBLIC RECLAIMNET_HAVE_SQLITE=1)
else()
  target_compile_definitions(reclaimnet_control PUBLIC RECLAIMNET_HAVE_SQLITE=0)
endif()
add_executable(coordinator src/coordinator_main.cc)
target_link_libraries(coordinator PRIVATE reclaimnet_control)
