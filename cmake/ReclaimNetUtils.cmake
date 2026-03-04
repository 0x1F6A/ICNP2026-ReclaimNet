# ReclaimNetUtils.cmake - Utility macros for ReclaimNet build

macro(reclaimnet_add_test target)
  add_executable(${target} ${ARGN})
  target_link_libraries(${target}
    GTest::gtest_main
    GTest::gmock_main
  )
  add_test(NAME ${target} COMMAND ${target})
  set_tests_properties(${target} PROPERTIES TIMEOUT 60)
endmacro()

macro(reclaimnet_enable_warnings target)
  target_compile_options(${target} PRIVATE
    -Wall -Wextra -Wpedantic
    -Wno-unused-parameter
    -Wshadow
    -Wformat=2
  )
endmacro()

function(reclaimnet_set_cpp17 target)
  set_target_properties(${target} PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
  )
endfunction()
