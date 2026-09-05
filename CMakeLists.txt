cmake_minimum_required(VERSION 3.16)
project(SelfHealingCacheCluster CXX)

# ---- Global compiler settings ----
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -Wpedantic)

# Default to a Debug build if the user didn't specify one.
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# ---- Core library: everything except main() ----
# We build the cache engine, protocol, server, and logger as a static library
# so both the server executable AND the test executable can link against the
# exact same compiled code. This avoids re-compiling/duplicating logic for tests.
add_library(cache_core STATIC
    src/logger.cpp
    src/cache_engine.cpp
    src/protocol.cpp
    src/tcp_server.cpp
)

target_include_directories(cache_core PUBLIC include)

# pthread is required on Linux for std::thread / std::shared_mutex to link correctly.
find_package(Threads REQUIRED)
target_link_libraries(cache_core PUBLIC Threads::Threads)

# ---- Server executable ----
add_executable(cache_server src/main.cpp)
target_link_libraries(cache_server PRIVATE cache_core)

# ---- Client executable ----
add_executable(cache_client client/client_main.cpp)
target_link_libraries(cache_client PRIVATE cache_core)

# ---- Tests ----
enable_testing()
add_subdirectory(tests)
