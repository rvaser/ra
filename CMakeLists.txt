cmake_minimum_required(VERSION 3.2)
project(ra)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)

add_custom_target(minimap2 ALL
    COMMAND make
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/vendor/minimap2)
add_subdirectory(vendor/rala)
add_subdirectory(vendor/racon)

set(minimap_path ${PROJECT_SOURCE_DIR}/vendor/minimap2/minimap2)
set(rala_path ${PROJECT_BINARY_DIR}/vendor/rala/bin/rala)
set(racon_path ${PROJECT_BINARY_DIR}/vendor/racon/bin/racon)

configure_file(${PROJECT_SOURCE_DIR}/scripts/ra.py
    ${PROJECT_BINARY_DIR}/${CMAKE_FILES_DIRECTORY}/ra)
file(COPY ${PROJECT_BINARY_DIR}/${CMAKE_FILES_DIRECTORY}/ra
    DESTINATION ${PROJECT_BINARY_DIR}/bin
    FILE_PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE)
