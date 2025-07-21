cmake_minimum_required(VERSION 3.10.0)
project(mox_db VERSION 0.1.0 LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17 CACHE STRING "C++ standard to conform to")

# Check if platform is x86-64 using CMAKE_SYSTEM_PROCESSOR
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64")
    message(STATUS "Detected x86-64 platform (${CMAKE_SYSTEM_PROCESSOR}), enabling AVX2 for XXHash")
    set(IS_X86_64 TRUE)
else()
    set(IS_X86_64 FALSE)
endif()


# Add XXHash from third_party directory
add_library(xxhash STATIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/xxHash/xxhash.c
)
target_include_directories(xxhash PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/xxHash
)

if(IS_X86_64)
    if(MSVC)
        target_compile_options(xxhash PRIVATE "/arch:AVX2")
    else()
        target_compile_options(xxhash PRIVATE "-mavx2")
    endif()
endif()

add_library(cpp-peglib INTERFACE)
target_include_directories(cpp-peglib INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/cpp-peglib
)

add_subdirectory(src)



