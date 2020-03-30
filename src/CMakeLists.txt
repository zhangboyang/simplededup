cmake_minimum_required(VERSION 3.10)

# set the project name and version
project(simplededup VERSION 0.1)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_executable(simplededup simplededup.cpp)

configure_file(config.h.in config.h)

target_include_directories(simplededup PUBLIC "${PROJECT_BINARY_DIR}")
