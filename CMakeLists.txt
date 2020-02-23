cmake_minimum_required(VERSION 3.0)

project("auto_parallel")

set(CMAKE_CONFIGURATION_TYPES "Debug;Release" CACHE STRING "Configs" FORCE)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/bin/Debug)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/bin/Release)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/lib/Debug)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/lib/Release)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lgomp -pthread")

find_package(MPI)
if(NOT MPI_FOUND)
	message(STATUS "MPI not found")
	set(MPI_INCLUDE_PATH "")
	set(MPI_LIB_PATH "")
	if((MPI_INCLUDE_PATH STREQUAL "") OR (MPI_LIB_PATH STREQUAL ""))
		message(FATAL_ERROR "You should set MPI_INCLUDE_PATH and MPI_LIB_PATH in CMakeLists.txt")
	endif((MPI_INCLUDE_PATH STREQUAL "") OR (MPI_LIB_PATH STREQUAL ""))
	add_library(mpi_lib STATIC IMPORTED)
	set_property(TARGET mpi_lib PROPERTY IMPORTED_LOCATION ${MPI_LIB_PATH})
	set(MPI_LIBRARIES mpi_lib)
endif(NOT MPI_FOUND)

include_directories(headers ${MPI_INCLUDE_PATH})

add_subdirectory(sources)
add_subdirectory(samples)
