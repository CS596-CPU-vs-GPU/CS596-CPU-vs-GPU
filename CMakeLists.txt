cmake_minimum_required(VERSION 3.10)
project(CS596_CPU_vs_GPU_Project)

# Specify C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Required CUDA Version
find_package(CUDA REQUIRED)
set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -arch=sm_35")

# Include directories for the project
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Libraries
find_package(Threads REQUIRED) # For multithreading support

# Link nlohmann-json library
# Assumes you have nlohmann/json.hpp in your project directory or installed via package manager
find_path(JSON_INCLUDE_DIR "nlohmann/json.hpp")
if (NOT JSON_INCLUDE_DIR)
    message(FATAL_ERROR "nlohmann/json.hpp not found! Install it or place it in the project directory.")
endif()
include_directories(${JSON_INCLUDE_DIR})

# Add executable for CPU-only processing
add_executable(cpu_only_processing
    CPU_only_processing.cpp
)
target_link_libraries(cpu_only_processing Threads::Threads)

# Add executable for GPU aggregation with threads
cuda_add_executable(gpu_aggregation_threads
    GPU_aggregation_threads.cu
)

# Add executable for GPU aggregation without threads
cuda_add_executable(gpu_aggregation_without_threads
    GPU_aggregation_without_threads.cu
)

# Add executable for CPU processing without multithreading
add_executable(cpu_without_multithreading
    cpu_without_multithreading.cpp
)
target_link_libraries(cpu_without_multithreading Threads::Threads rt)

# Add executable for CPU multithreading
add_executable(multithreading_in_cpu
    multithreading_in_cpu.cpp
)
target_link_libraries(multithreading_in_cpu Threads::Threads rt)

# Set output directory for executables
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Print project summary
message(STATUS "Project configured successfully!")
