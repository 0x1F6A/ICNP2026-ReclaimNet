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

enable_testing()

set(TEST_SOURCES
  test_p1_allocator.cc
  test_p2_selector.cc
  test_p3_admission.cc
  test_hazard_estimator.cc
)

add_executable(reclaimnet_tests ${TEST_SOURCES})

target_link_libraries(reclaimnet_tests
  PRIVATE
    reclaimnet_control
    GTest::gtest_main
)

target_compile_features(reclaimnet_tests PRIVATE cxx_std_17)

include(GoogleTest)
gtest_discover_tests(reclaimnet_tests)
