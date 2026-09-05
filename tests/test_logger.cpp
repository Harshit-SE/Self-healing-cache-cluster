#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "cache/logger.hpp"

using namespace cache;

// We can't easily assert on stdout content without redirecting streams,
// so this test's real purpose is concurrency safety: if the logger's
// internal locking were broken, this test would crash or hang under
// ThreadSanitizer / helgrind even though functionally "it still logs".
TEST(LoggerTest, ConcurrentLoggingDoesNotCrash) {
    constexpr int kThreads = 8;
    constexpr int kMessagesPerThread = 100;

    std::vector<std::thread> threads;
    std::atomic<int> counter{0};

    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&counter]() {
            for (int i = 0; i < kMessagesPerThread; ++i) {
                LOG_INFO("message " << counter.fetch_add(1));
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(counter.load(), kThreads * kMessagesPerThread);
}

TEST(LoggerTest, SingletonReturnsSameInstance) {
    Logger& a = Logger::instance();
    Logger& b = Logger::instance();
    EXPECT_EQ(&a, &b);
}
