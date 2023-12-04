# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

# include(ExternalProject)
# find_package(Git REQUIRED)

# # Get rapidjson
# ExternalProject_Add(
#         tbb_src
#         PREFIX "vendor/intel/tbb"
#         GIT_REPOSITORY "https://github.com/wjakob/tbb.git"
#         GIT_TAG b066defc0229a1e92d7a200eb3fe0f7e35945d95
#         TIMEOUT 10
#         BUILD_COMMAND make
#         UPDATE_COMMAND "" # to prevent rebuilding everytime
#         INSTALL_COMMAND ""
#         CMAKE_ARGS
#         -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/tbb_cpp
#         -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
#         -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
#         -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
#         -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
# )

# # Prepare json
# ExternalProject_Get_Property(tbb_src source_dir)
# ExternalProject_Get_Property(tbb_src binary_dir)

# set(TBB_INCLUDE_DIR ${source_dir}/include)
# set(TBB_LIBRARY_PATH ${binary_dir}/libtbb.so)

# file(MAKE_DIRECTORY ${TBB_INCLUDE_DIR})

# add_library(tbb SHARED IMPORTED)
# add_dependencies(tbb tbb_src)

# set_property(TARGET tbb PROPERTY IMPORTED_LOCATION ${TBB_LIBRARY_PATH})
# set_property(TARGET tbb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${TBB_INCLUDE_DIR})


# Assuming you've extracted tbb to libs/tbb
set(TBB_SOURCE_DIR "${CMAKE_SOURCE_DIR}/libs/intel/tbb")
set(TBB_BINARY_DIR "${TBB_SOURCE_DIR}/build")

# Build tbb from the local source
add_subdirectory(${TBB_SOURCE_DIR} ${TBB_BINARY_DIR})

# Prepare tbb for use in the main project
set(TBB_INCLUDE_DIR ${TBB_SOURCE_DIR}/include)
set(TBB_LIBRARY_PATH ${TBB_BINARY_DIR}/libtbb.so)

file(MAKE_DIRECTORY ${TBB_INCLUDE_DIR})

add_library(tbb_imported SHARED IMPORTED)
set_property(TARGET tbb_imported PROPERTY IMPORTED_LOCATION ${TBB_LIBRARY_PATH})
set_property(TARGET tbb_imported APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${TBB_INCLUDE_DIR})
