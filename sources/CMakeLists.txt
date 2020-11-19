file(GLOB_RECURSE hdrs "../headers/*.h")
file(GLOB_RECURSE srcs RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")

add_library(spmd_interface ${hdrs} ${srcs})
