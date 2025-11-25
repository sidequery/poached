cmake_minimum_required(VERSION 3.11...3.29)
option(DUCKDB_WASM_EXTENSION "Whether compiling for Wasm target" OFF)

###
# Configuration
###
if(NOT DEFINED EXTENSION_NAME)
    message(FATAL_ERROR "DuckDB extension name is required")
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MAJOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MAJOR=${TARGET_DUCKDB_VERSION_MAJOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MINOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MINOR=${TARGET_DUCKDB_VERSION_MINOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_PATCH)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_PATCH=${TARGET_DUCKDB_VERSION_PATCH})
endif()

add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})

if (DEFINED DUCKDB_EXTENSION_API_VERSION_UNSTABLE)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=${DUCKDB_EXTENSION_API_VERSION_UNSTABLE})
endif()

###
# Download DuckDB source for C++ headers only (needed for tokenizer)
# Using FetchContent_Populate to avoid building DuckDB
###
include(FetchContent)
set(DUCKDB_VERSION "v1.4.2")
FetchContent_Declare(
    duckdb_src
    URL https://github.com/duckdb/duckdb/archive/refs/tags/${DUCKDB_VERSION}.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_GetProperties(duckdb_src)
if(NOT duckdb_src_POPULATED)
    FetchContent_Populate(duckdb_src)
endif()

###
# Build
###
project(${EXTENSION_NAME} LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Create Extension library
set(EXTENSION_SOURCES
        src/add_numbers.c
        src/parser.c
        src/capi_quack.c
        src/tokenizer.cpp
)

if (DUCKDB_WASM_EXTENSION)
	add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
else()
	add_library(${EXTENSION_NAME} SHARED ${EXTENSION_SOURCES})
endif()

# Hide symbols
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(VISIBILITY_INLINES_HIDDEN ON)
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -s")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -s")

# Include own headers
target_include_directories(${EXTENSION_NAME} PRIVATE src/include)

# Include DuckDB C API headers
target_include_directories(${EXTENSION_NAME} PRIVATE duckdb_capi)

# Include DuckDB source headers (for tokenizer)
target_include_directories(${EXTENSION_NAME} PRIVATE ${duckdb_src_SOURCE_DIR}/src/include)

# Link against libduckdb (for tokenizer implementation)
# Try vcpkg first (for CI), then fall back to system paths (for local dev)
find_package(duckdb CONFIG QUIET)
if(TARGET duckdb::duckdb)
    target_link_libraries(${EXTENSION_NAME} PRIVATE duckdb::duckdb)
else()
    find_library(DUCKDB_LIBRARY duckdb HINTS /opt/homebrew/lib /usr/local/lib)
    if(DUCKDB_LIBRARY)
        target_link_libraries(${EXTENSION_NAME} PRIVATE ${DUCKDB_LIBRARY})
    else()
        message(WARNING "libduckdb not found - tokenize_sql will fail at runtime")
    endif()
endif()
