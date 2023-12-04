# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

# Prepare bz2
# ExternalProject_Get_Property(bz2_src source_dir)
set(bz2_SOURCE_DIR "${CMAKE_SOURCE_DIR}/libs/bzip2")
set(bz2_INCLUDE_DIR ${bz2_SOURCE_DIR}/build)
set(bz2_LIBRARY_PATH ${bz2_SOURCE_DIR}/build/libbz2.so.1)
file(MAKE_DIRECTORY ${bz2_INCLUDE_DIR})
add_library(bz2 STATIC IMPORTED)

set_property(TARGET bz2 PROPERTY IMPORTED_LOCATION ${bz2_LIBRARY_PATH})
set_property(TARGET bz2 APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${bz2_INCLUDE_DIR})
