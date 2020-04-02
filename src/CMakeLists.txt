cmake_minimum_required(VERSION 3.10)

# set the project name and version
project(simplededup VERSION 0.1)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION True)

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} CPPFILES)
add_executable(simplededup ${CPPFILES})

add_compile_options(-Wall)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64")

#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
#set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")

find_package(xxHash 0.7 CONFIG PATHS ../../xxHash/build)
target_link_libraries(simplededup PRIVATE xxHash::xxhash)

configure_file(config.h.in config.h)

target_include_directories(simplededup PUBLIC "${PROJECT_BINARY_DIR}")
