# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

# Prepare snappy
# ExternalProject_Get_Property(snappy_src source_dir)
set(snappy_SOURCE_DIR "${CMAKE_SOURCE_DIR}/libs/snappy")
set(snappy_INCLUDE_DIR ${snappy_SOURCE_DIR}/build)
set(snappy_LIBRARY_PATH ${snappy_SOURCE_DIR}/build/libsnappy.a)
file(MAKE_DIRECTORY ${snappy_INCLUDE_DIR})
add_library(snappy STATIC IMPORTED)

set_property(TARGET snappy PROPERTY IMPORTED_LOCATION ${snappy_LIBRARY_PATH})
set_property(TARGET snappy APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${snappy_INCLUDE_DIR})
