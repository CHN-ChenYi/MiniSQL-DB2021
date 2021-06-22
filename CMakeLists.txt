cmake_minimum_required(VERSION 3.20)

project(MiniSQL VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -D_DEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")

OPTION(ParallelWrite "Use new threads for writing blocks" ON)
if (ParallelWrite)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DParallelWrite")
    message("ParallelWrite set")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/API)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/BufferManager)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/CatalogManager)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/IndexManager)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/Interpreter)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/RecordManager)

message("-- Compiler flags:")
message("   C++ Compiler ID: ${CMAKE_CXX_COMPILER_ID}")
message("   C++ compilation flags: ${CMAKE_CXX_FLAGS}")

message("-- Build type: ${CMAKE_BUILD_TYPE}")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    message("   C++ debug flags: ${CMAKE_CXX_FLAGS_DEBUG}")
endif()
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
    message("   C++ release flags: ${CMAKE_CXX_FLAGS_RELEASE}")
endif()

add_executable(MiniSQL ${SOURCE_FILES})
