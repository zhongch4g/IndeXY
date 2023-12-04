# have to bump up the cmake version to enable clang's lto optimization
cmake_minimum_required(VERSION 3.11)
project(ARTSynchronized_Original)

if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type selected, default to Release")
  set(CMAKE_BUILD_TYPE Release)
else()
  message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -Wextra -march=native")

find_package(Threads)

set(ART_FILES Tree.cpp)
add_library(artree ${ART_FILES})
target_include_directories(artree PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(
  artree
  fmt
  ${tbblib}
  ${jemalloclib}
  dl
  ${CMAKE_THREAD_LIBS_INIT}
  GRPC_PB_redolog
  ${protobuflib})

set(SOURCE_FILES example.cpp)
add_executable(arttest ${SOURCE_FILES})
target_include_directories(arttest PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(arttest artree)

set(target arttest)
add_test(NAME ${target} COMMAND ${target} 20000000 2)
