if(POLICY CMP0068)
  cmake_policy(SET CMP0068 NEW)
  set(CMAKE_BUILD_WITH_INSTALL_NAME_DIR ON)
endif()

if(POLICY CMP0075)
  cmake_policy(SET CMP0075 NEW)
endif()

if(POLICY CMP0077)
  cmake_policy(SET CMP0077 NEW)
endif()

set(LLVM_DIR /usr/lib/llvm-20/lib/cmake/llvm/)
find_package(LLVM REQUIRED CONFIG)
set(MLIR_DIR /usr/lib/llvm-20/lib/cmake/mlir/)
find_package(MLIR REQUIRED CONFIG)

message(STATUS "Using MLIRConfig.cmake in: ${MLIR_DIR}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")

set(LLVM_RUNTIME_OUTPUT_INTDIR ${CMAKE_BINARY_DIR}/bin)
set(LLVM_LIBRARY_OUTPUT_INTDIR ${CMAKE_BINARY_DIR}/lib)
set(MLIR_BINARY_DIR ${CMAKE_BINARY_DIR})

list(APPEND CMAKE_MODULE_PATH "${MLIR_CMAKE_DIR}")
list(APPEND CMAKE_MODULE_PATH "${LLVM_CMAKE_DIR}")
include(TableGen)
include(AddLLVM)
include(AddMLIR)
include(HandleLLVMOptions)

include_directories(${LLVM_INCLUDE_DIRS})
include_directories(${MLIR_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_BINARY_DIR}/include)
link_directories(${LLVM_BUILD_LIBRARY_DIR})
add_definitions(${LLVM_DEFINITIONS})

file(GLOB LIB_SOURCES "lib/*.c")
add_executable(column_join columnar_main.c ${LIB_SOURCES})

llvm_map_components_to_libnames(llvm_libs
  Core
  ExecutionEngine
  OrcJIT
  JITLink
  X86CodeGen
  X86AsmParser
)

# Link against libraries defined in the parent CMakeLists.txt
target_link_libraries(column_join PRIVATE xxhash ${llvm_libs})

# Add include directories
target_include_directories(column_join PRIVATE 
    ${CMAKE_SOURCE_DIR}/third_party/xxHash
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
