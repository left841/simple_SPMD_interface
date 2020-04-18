file(GLOB hdrs "../headers/*.h")
file(GLOB srcs RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")

add_library(spmd_interface ${hdrs} ${srcs})
