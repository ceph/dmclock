cmake_minimum_required(VERSION 3.5.1)

project(dmclock CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/modules")

find_package(Threads)

# require c++17
if(CMAKE_VERSION VERSION_LESS "3.8")
  include(CheckCXXCompilerFlag)
  CHECK_CXX_COMPILER_FLAG("-std=c++17" COMPILER_SUPPORTS_CXX17)
  if(NOT COMPILER_SUPPORTS_CXX17)
    message(FATAL_ERROR
      "The compiler ${CMAKE_CXX_COMPILER} has no C++17 support.")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
else()
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

add_subdirectory(src)

# Determine if dmclock is built as a subproject (using add_subdirectory)
# or if it is the master project.
set(MASTER_PROJECT FALSE)
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  set(MASTER_PROJECT TRUE)
endif()

option(dmclock_TEST "Generate test targets" ${MASTER_PROJECT})
if(dmclock_TEST)
  if (NOT(TARGET gtest AND TARGET gtest_main))
    if (NOT GTEST_FOUND)
      find_package(GTest QUIET)
      if (NOT GTEST_FOUND)
        include(BuildGTest)
      endif()
    endif()
  endif()

  enable_testing()
  add_subdirectory(test)
  add_subdirectory(support/test)
  add_subdirectory(sim)
endif()
