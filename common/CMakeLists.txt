cmake_minimum_required(VERSION 3.20)



add_library(reclaimnet_common STATIC
  src/config.cc
  src/log.cc
  src/time.cc
  src/mtls.cc
  src/prometheus.cc
)

target_include_directories(reclaimnet_common
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(reclaimnet_common PUBLIC cxx_std_17)


find_package(yaml-cpp QUIET)
if(yaml-cpp_FOUND)
  target_link_libraries(reclaimnet_common PUBLIC yaml-cpp)
  target_compile_definitions(reclaimnet_common PUBLIC RECLAIMNET_HAVE_YAML=1)
  message(STATUS "reclaimnet_common: yaml-cpp found")
else()
  message(STATUS "reclaimnet_common: yaml-cpp NOT found – config will use defaults")
endif()


find_package(gRPC QUIET)
if(gRPC_FOUND)
  target_link_libraries(reclaimnet_common PUBLIC gRPC::grpc++)
  message(STATUS "reclaimnet_common: gRPC found")
else()
  message(STATUS "reclaimnet_common: gRPC NOT found – mTLS helpers will be stubs")
endif()


find_package(Threads REQUIRED)
target_link_libraries(reclaimnet_common PUBLIC Threads::Threads)
