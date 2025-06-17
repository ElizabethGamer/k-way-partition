cmake_minimum_required(VERSION 3.10)
project(kway)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)

# Add include directory
include_directories(include)

# Collect all sources
file(GLOB_RECURSE SOURCES "src/*.cpp")

# Create executable
add_executable(benchmark ${SOURCES})

# Include Parlay
find_package(Parlay REQUIRED)