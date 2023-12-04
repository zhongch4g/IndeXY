add_executable(frontend frontend.cpp)
add_dependencies(frontend leanstore)
target_link_libraries(frontend leanstore)

find_library(JemallocLib jemalloc)
find_library(PthreadLib pthread)
set(JemallocLibs ${JemallocLib} ${PthreadLib})
ADD_DEFINITIONS(-DJEMALLOC_NO_DEMANGLE)

set(CMAKE_CONFIG_VALUE_SIZE 8 CACHE STRING "Value size")
message(STATUS "Current value size is: ${CMAKE_CONFIG_VALUE_SIZE}")

# original leanstore ycsb
add_executable(leanstore_ycsb ycsb/leanstore_ycsb.cpp)
target_link_libraries(leanstore_ycsb leanstore Threads::Threads)
target_include_directories(leanstore_ycsb PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# leanstore ycsb with db_bench (leveldb) benchmark structure
add_executable(leanstore_ycsb_bench ycsb/leanstore_ycsb_bench.cpp)
target_link_libraries(leanstore_ycsb_bench leanstore Threads::Threads)
target_include_directories(leanstore_ycsb_bench PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_compile_definitions(leanstore_ycsb_bench PUBLIC CMAKE_CONFIG_VALUE_SIZE=${CMAKE_CONFIG_VALUE_SIZE})

# leanstore tpcc benchmark
add_executable(leanstore_tpcc tpc-c/leanstore/leanstore_tpcc.cpp)
target_link_libraries(leanstore_tpcc leanstore Threads::Threads)
target_include_directories(leanstore_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# original ART ycsb with db_bench (leveldb) benchmark structure
add_executable(original_artree_ycsb_bench ycsb/original_artree_ycsb_bench.cpp)
target_link_libraries(original_artree_ycsb_bench leanstore ${JemallocLibs} Threads::Threads)
target_include_directories(original_artree_ycsb_bench PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# xy design ycsb benchmark new
add_executable(xystore_ycsb_bench ycsb/xystore_ycsb_bench.cpp)
target_link_libraries(xystore_ycsb_bench leanstore ${JemallocLibs} Threads::Threads speedb)
target_include_directories(xystore_ycsb_bench PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_compile_definitions(xystore_ycsb_bench PUBLIC CMAKE_CONFIG_VALUE_SIZE=${CMAKE_CONFIG_VALUE_SIZE} LEANSTORE=${LEANSTORE} ROCKSDB=${ROCKSDB})

# rocksdb ycsb with db_bench (leveldb) benchmark structure
add_executable(rocksdb_ycsb_bench ycsb/rocksdb_ycsb_bench.cpp)
target_link_libraries(rocksdb_ycsb_bench leanstore ${JemallocLibs} Threads::Threads speedb)
target_include_directories(rocksdb_ycsb_bench PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# xy design tpcc benchmark
add_executable(xystore_tpcc tpc-c/xystore/xystore_tpcc.cpp)
target_link_libraries(xystore_tpcc leanstore Threads::Threads ${JemallocLibs} speedb)
target_include_directories(xystore_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_compile_definitions(xystore_tpcc PUBLIC LEANSTORE=${LEANSTORE} ROCKSDB=${ROCKSDB})

# original artree
add_executable(ori_artree_tpcc tpc-c/ori_artree/ori_artree_tpcc.cpp)
target_link_libraries(ori_artree_tpcc leanstore ${JemallocLibs} Threads::Threads)

# https://source.wiredtiger.com/10.0.0/build-posix.html
# add_executable(wiredtiger_tpcc tpc-c/wiredtiger/wiredtiger_tpcc.cpp)
# target_link_libraries(wiredtiger_tpcc leanstore wiredtiger Threads::Threads dl z libbz2.a lz4 snappy)
# target_include_directories(wiredtiger_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# find_package(GTest 1.11 QUIET MODULE REQUIRED)
# if(DEFINED GTest_VERSION)
#   set(gtestlib GTest::gtest)
# else()
#   find_library(gtestlib gtest)
#   if (${gtestlib} STREQUAL gtestlib-NOTFOUND)
#     message(FATAL_ERROR "Can not find GTest")
#   endif()
# endif()
# message(STATUS "Using GTest library: ${gtestlib}")

# add_executable(arttest tests/art_iterator_test.cc)
# check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
# target_link_libraries(arttest leanstore speedb ${JemallocLibs} ${gtestlib} )
# target_include_directories(arttest PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(xytest tests/xy_test.cc)
# check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
# target_link_libraries(xytest leanstore speedb ${JemallocLibs} ${gtestlib} )
# target_include_directories(xytest PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(simple_example tests/simple_example.cc)
# check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
# target_link_libraries(simple_example leanstore speedb ${JemallocLibs} ${gtestlib} )
# target_include_directories(simple_example PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(rocksdb_simple_example tests/rocksdb_simple_example.cc)
target_link_libraries(rocksdb_simple_example leanstore ${JemallocLibs} ${gtestlib} speedb)
target_include_directories(rocksdb_simple_example PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(create_rocksdb_database tests/create_rocksdb_database.cc)
target_link_libraries(create_rocksdb_database leanstore ${JemallocLibs} ${gtestlib} speedb)
target_include_directories(create_rocksdb_database PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(pro_con_example tests/pro_con_example.cc)
target_link_libraries(pro_con_example leanstore ${gtestlib})
target_include_directories(pro_con_example PRIVATE ${SHARED_INCLUDE_DIRECTORY})
