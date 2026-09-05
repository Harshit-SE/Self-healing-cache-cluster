include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.15.2
)
# Prevent GoogleTest from overriding our own compiler/linker settings.
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(cache_tests
    test_logger.cpp
    test_cache_engine.cpp
    test_protocol.cpp
)

target_link_libraries(cache_tests PRIVATE cache_core GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(cache_tests)
