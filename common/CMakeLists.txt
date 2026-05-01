add_library(reclaimnet_common STATIC
  src/config.cc src/log.cc src/time.cc src/mtls.cc src/prometheus.cc src/trace.cc
  src/trace_writer.cc src/security.cc src/metrics_server.cc)
target_include_directories(reclaimnet_common PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_compile_features(reclaimnet_common PUBLIC cxx_std_17)
target_compile_definitions(reclaimnet_common PUBLIC
  RECLAIMNET_HAVE_GRPC=$<BOOL:${RECLAIMNET_WITH_GRPC}>
  RECLAIMNET_HAVE_PROTO=$<BOOL:${RECLAIMNET_WITH_GRPC}>)
if(RECLAIMNET_WITH_GRPC)
  target_link_libraries(reclaimnet_common PUBLIC gRPC::grpc++)
endif()
if(RECLAIMNET_ENABLE_YAML AND yaml-cpp_FOUND)
  if(TARGET yaml-cpp::yaml-cpp)
    target_link_libraries(reclaimnet_common PUBLIC yaml-cpp::yaml-cpp)
  else()
    target_link_libraries(reclaimnet_common PUBLIC yaml-cpp)
  endif()
  target_compile_definitions(reclaimnet_common PUBLIC RECLAIMNET_HAVE_YAML=1)
else()
  target_compile_definitions(reclaimnet_common PUBLIC RECLAIMNET_HAVE_YAML=0)
endif()
if(RECLAIMNET_ENABLE_JSON AND nlohmann_json_FOUND)
  target_link_libraries(reclaimnet_common PUBLIC nlohmann_json::nlohmann_json)
  target_compile_definitions(reclaimnet_common PUBLIC RECLAIMNET_HAVE_JSON=1)
else()
  target_compile_definitions(reclaimnet_common PUBLIC RECLAIMNET_HAVE_JSON=0)
endif()
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
target_link_libraries(reclaimnet_common PUBLIC Threads::Threads OpenSSL::SSL OpenSSL::Crypto)
