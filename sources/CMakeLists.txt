file(GLOB hdrs "../headers/apl/*.h")
file(GLOB srcs RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "./apl/*.cpp")

file(GLOB pz_hdrs "../headers/apl/parallelizer/*.h")
file(GLOB pz_srcs RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "./apl/parallelizer/*.cpp")

file(GLOB pz2_hdrs "../headers/apl/parallelizer2/*.h")
file(GLOB pz2_srcs RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "./apl/parallelizer2/*.cpp")

file(GLOB pz_shared_hdrs "../headers/apl/parallelizer_shared/*.h")
file(GLOB pz_shared_srcs RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "./apl/parallelizer_shared/*.cpp")


add_library(parallelizer_shared OBJECT ${pz_shared_hdrs} ${pz_shared_srcs})
add_library(parallelizer_lib OBJECT ${pz_hdrs} ${pz_srcs})
add_library(parallelizer2_lib OBJECT ${pz2_hdrs} ${pz2_srcs})

target_link_libraries(parallelizer_lib PUBLIC parallelizer_shared)
target_link_libraries(parallelizer2_lib PUBLIC parallelizer_shared)

add_library(spmd_interface "../headers/parallel.h" ${hdrs} ${srcs})
target_link_libraries(spmd_interface PUBLIC parallelizer_lib parallelizer2_lib parallelizer_shared)
