# Set the minimum CMake version
cmake_minimum_required(VERSION 3.16)

# Set the project name and C++ standard
project(CS596-CPU-vs-GPU)
set(CMAKE_CXX_STANDARD 17)

# Enable optimization and debug symbols for Intel VTune analysis
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g -qopenmp")

# Add executable for CPU-only processing
add_executable(CPU_only_processing CPU_only_processing.cpp)

# Add executable for CPU multithreading processing
add_executable(multithreading_in_cpu multithreading_in_cpu.cpp)

# Find Intel threading libraries if applicable
find_package(Threads REQUIRED)

# Link threading libraries to the multithreaded executable
target_link_libraries(multithreading_in_cpu PRIVATE Threads::Threads)

# Include directories for dependencies (adjust path as necessary)
include_directories(${PROJECT_SOURCE_DIR}/include)
