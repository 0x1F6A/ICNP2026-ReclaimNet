cmake_minimum_required(VERSION 3.20)



find_package(GTest QUIET)
if(NOT GTest_FOUND)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
  )
  FetchContent_MakeAvailable(googletest)
endif()

set(TEST_SOURCES
  test_p1_allocator.cc
  test_p2_selector.cc
  test_p3_admission.cc
  test_hazard_estimator.cc
  test_coordinator.cc
  test_trace.cc
  test_topology.cc
  test_node_runtime.cc
  test_security_gpu_bpf.cc
  test_catalog_metrics.cc
  test_bpf_live.cc
)

add_executable(reclaimnet_tests ${TEST_SOURCES})

target_link_libraries(reclaimnet_tests
  PRIVATE
    reclaimnet_control
    reclaimnet_agent
    GTest::gtest_main
)

target_compile_features(reclaimnet_tests PRIVATE cxx_std_17)
target_compile_definitions(reclaimnet_tests PRIVATE
  RECLAIMNET_TEST_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures/trace_v3"
  RECLAIMNET_TRACE_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures/trace_v3")
if(RECLAIMNET_WITH_GRPC)
  target_sources(reclaimnet_tests PRIVATE test_grpc.cc test_agent_e2e.cc)
endif()

include(GoogleTest)
gtest_discover_tests(reclaimnet_tests PROPERTIES TIMEOUT 30)
if(Python3_FOUND AND pybind11_FOUND)
  add_test(NAME application_checkpoint_bridge
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_checkpoint_bridge.py)
  set_tests_properties(application_checkpoint_bridge PROPERTIES
    ENVIRONMENT "PYTHONPATH=${PROJECT_SOURCE_DIR}/node_agent/python" TIMEOUT 120)
endif()
