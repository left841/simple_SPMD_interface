get_filename_component(sample_name  ${CMAKE_CURRENT_SOURCE_DIR} NAME)

project(${sample_name})

file(GLOB sample_h RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.h")
file(GLOB sample_src RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")

add_executable(${sample_name} ${sample_h} ${sample_src})

target_include_directories(${sample_name} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(${sample_name} ${MPI_LIBRARIES} spmd_interface)
