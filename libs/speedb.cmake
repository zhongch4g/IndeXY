# ---------------------------------------------------------------------------
# leanstore
# ---------------------------------------------------------------------------

# include(ExternalProject)
# find_package(Git REQUIRED)

# # Get speedb
# ExternalProject_Add(
#         speedb_src
#         PREFIX "vendor/speedb"
#         GIT_REPOSITORY "https://github.com/speedb-io/speedb.git"
#         GIT_TAG bb682ac
#         GIT_SHALLOW TRUE
#         TIMEOUT 10
#         CONFIGURE_COMMAND ""
#         UPDATE_COMMAND ""
#         INSTALL_COMMAND ""
#         BUILD_COMMAND $(MAKE) static_lib USE_RTTI=1
#         BUILD_IN_SOURCE TRUE
# )

# # Prepare speedb
# ExternalProject_Get_Property(speedb_src source_dir)
# set(SPEEDB_INCLUDE_DIR ${source_dir}/include)
# set(SPEEDB_LIBRARY_PATH ${source_dir}/libspeedb.a)
# #set(SPEEDB_LIBRARY_PATH ${source_dir}/libspeedb.so)
# file(MAKE_DIRECTORY ${SPEEDB_INCLUDE_DIR})
# add_library(speedb STATIC IMPORTED)
# set_property(TARGET speedb PROPERTY IMPORTED_LOCATION ${SPEEDB_LIBRARY_PATH})
# set_property(TARGET speedb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${SPEEDB_INCLUDE_DIR})

# # Dependencies
# add_dependencies(speedb speedb_src)

# Build speedb from the local source
# Note: This assumes speedb has a Makefile or similar build system. 
# You might need to adjust this part based on how speedb is meant to be built.
set(SPEEDB_SOURCE_DIR "${CMAKE_SOURCE_DIR}/libs/speedb")

# execute_process(
#     COMMAND $(MAKE) static_lib USE_RTTI=1
#     WORKING_DIRECTORY ${SPEEDB_SOURCE_DIR}
# )

# add_subdirectory(${CMAKE_SOURCE_DIR}/libs/speedb)

# Prepare speedb for use in the main project
set(SPEEDB_INCLUDE_DIR ${SPEEDB_SOURCE_DIR}/include)
set(SPEEDB_LIBRARY_PATH ${SPEEDB_SOURCE_DIR}/libspeedb.a)
# set(SPEEDB_LIBRARY_PATH ${SPEEDB_SOURCE_DIR}/libspeedb.so)
file(MAKE_DIRECTORY ${SPEEDB_INCLUDE_DIR})
add_library(speedb STATIC IMPORTED)
set_property(TARGET speedb PROPERTY IMPORTED_LOCATION ${SPEEDB_LIBRARY_PATH})
set_property(TARGET speedb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${SPEEDB_INCLUDE_DIR})