# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

# Prepare liburing
# ExternalProject_Get_Property(liburing_src source_dir)
set(liburing_SOURCE_DIR "${CMAKE_SOURCE_DIR}/libs/liburing")
set(liburing_INCLUDE_DIR ${liburing_SOURCE_DIR}/src)
set(liburing_LIBRARY_PATH ${liburing_SOURCE_DIR}/src/liburing.a)
file(MAKE_DIRECTORY ${liburing_INCLUDE_DIR})
add_library(liburing STATIC IMPORTED)

set_property(TARGET liburing PROPERTY IMPORTED_LOCATION ${liburing_LIBRARY_PATH})
set_property(TARGET liburing APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${liburing_INCLUDE_DIR})
