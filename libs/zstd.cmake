# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

# Prepare zstd
# ExternalProject_Get_Property(zstd_src source_dir)
set(zstd_SOURCE_DIR "${CMAKE_SOURCE_DIR}/libs/zstd")
set(zstd_INCLUDE_DIR ${zstd_SOURCE_DIR}/lib)
set(zstd_LIBRARY_PATH ${zstd_SOURCE_DIR}/lib/libzstd.a)
file(MAKE_DIRECTORY ${zstd_INCLUDE_DIR})
add_library(zstd STATIC IMPORTED)

set_property(TARGET zstd PROPERTY IMPORTED_LOCATION ${zstd_LIBRARY_PATH})
set_property(TARGET zstd APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${zstd_INCLUDE_DIR})
