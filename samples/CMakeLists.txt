file(GLOB directory RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*")

foreach(object ${directory})
    if(IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${object})
        add_subdirectory(${object})
    endif(IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${object})
endforeach(object ${directory})
