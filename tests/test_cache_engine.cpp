#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "cache/cache_engine.hpp"

using namespace cache;
using namespace std::chrono_literals;

// ---- Pre-existing SET/GET/DELETE behavior (unaffected by TTL) ----

TEST(CacheEngineTest, GetOnMissingKeyReturnsNullopt) {
    CacheEngine cache;
    EXPECT_EQ(cache.get("missing"), std::nullopt);
}

TEST(CacheEngineTest, SetThenGetReturnsValue) {
    CacheEngine cache;
    cache.set("name", "harshit");
    auto result = cache.get("name");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "harshit");
}

TEST(CacheEngineTest, SetOverwritesExistingValue) {
    CacheEngine cache;
    cache.set("key", "first");
    cache.set("key", "second");
    EXPECT_EQ(cache.get("key"), "second");
}

TEST(CacheEngineTest, EmptyStringValueIsDistinctFromMissingKey) {
    CacheEngine cache;
    cache.set("key", "");
    auto result = cache.get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "");
    EXPECT_EQ(cache.get("never_set"), std::nullopt);
}

TEST(CacheEngineTest, RemoveExistingKeyReturnsTrue) {
    CacheEngine cache;
    cache.set("key", "value");
    EXPECT_TRUE(cache.remove("key"));
    EXPECT_EQ(cache.get("key"), std::nullopt);
}

TEST(CacheEngineTest, RemoveMissingKeyReturnsFalse) {
    CacheEngine cache;
    EXPECT_FALSE(cache.remove("never_existed"));
}

TEST(CacheEngineTest, SizeReflectsInsertsAndRemoves) {
    CacheEngine cache;
    EXPECT_EQ(cache.size(), 0u);
    cache.set("a", "1");
    cache.set("b", "2");
    EXPECT_EQ(cache.size(), 2u);
    cache.remove("a");
    EXPECT_EQ(cache.size(), 1u);
}

// ---- New: TTL behavior ----

TEST(CacheEngineTest, KeyWithNoTtlNeverExpires) {
    CacheEngine cache;
    cache.set("permanent", "value");  // No TTL argument -> never expires.
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(cache.get("permanent"), "value");
}

TEST(CacheEngineTest, KeyExpiresAfterTtl) {
    CacheEngine cache;
    cache.set("short_lived", "value", 10ms);

    // Immediately after SET, it should still be present.
    EXPECT_EQ(cache.get("short_lived"), "value");

    std::this_thread::sleep_for(20ms);  // Wait past the TTL.
    EXPECT_EQ(cache.get("short_lived"), std::nullopt);
}

TEST(CacheEngineTest, ExpiredKeyIsPhysicallyRemovedOnAccess) {
    CacheEngine cache;
    cache.set("short_lived", "value", 10ms);
    EXPECT_EQ(cache.size(), 1u);

    std::this_thread::sleep_for(20ms);
    cache.get("short_lived");  // Triggers lazy removal as a side effect.

    EXPECT_EQ(cache.size(), 0u);  // Confirms it was actually erased, not
                                  // just hidden from get().
}

TEST(CacheEngineTest, OverwritingExpiredKeyRefreshesTtl) {
    CacheEngine cache;
    cache.set("key", "old", 10ms);
    std::this_thread::sleep_for(20ms);  // Let it expire.

    cache.set("key", "new", 1000ms);    // Overwrite with a fresh, longer TTL.
    EXPECT_EQ(cache.get("key"), "new"); // Should be valid again immediately.
}

// ---- Concurrency (unchanged intent, updated for get() no longer const) ----

TEST(CacheEngineTest, ConcurrentDistinctWritesAllSucceed) {
    CacheEngine cache;
    constexpr int kThreads = 8;
    constexpr int kKeysPerThread = 200;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&cache, t]() {
            for (int i = 0; i < kKeysPerThread; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                cache.set(key, "value");
            }
        });
    }
    for (auto& th : threads) th.join();

    EXPECT_EQ(cache.size(), static_cast<std::size_t>(kThreads * kKeysPerThread));
}

TEST(CacheEngineTest, ConcurrentReadersAndWriterOnSameKey) {
    CacheEngine cache;
    cache.set("shared_key", "initial");

    std::atomic<bool> stop{false};
    std::atomic<int> reads_completed{0};

    std::vector<std::thread> readers;
    for (int r = 0; r < 4; ++r) {
        readers.emplace_back([&]() {
            while (!stop.load()) {
                auto val = cache.get("shared_key");
                EXPECT_TRUE(val.has_value());
                reads_completed.fetch_add(1);
            }
        });
    }

    std::thread writer([&]() {
        for (int i = 0; i < 500; ++i) {
            cache.set("shared_key", "value_" + std::to_string(i));
        }
        stop.store(true);
    });

    writer.join();
    for (auto& r : readers) r.join();

    EXPECT_GT(reads_completed.load(), 0);
}

// Concurrency test targeting the TTL double-checked-locking path
// specifically: many threads racing get() on a key right as it expires.
TEST(CacheEngineTest, ConcurrentAccessDuringExpirationIsRaceFree) {
    CacheEngine cache;
    cache.set("racy_key", "value", 15ms);

    constexpr int kThreads = 8;
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&cache]() {
            // Spin for ~30ms, repeatedly calling get() -- guaranteed to
            // straddle the moment the key transitions to expired.
            auto deadline = std::chrono::steady_clock::now() + 30ms;
            while (std::chrono::steady_clock::now() < deadline) {
                cache.get("racy_key");  // Result ignored; we're checking
                                         // for crashes/races, not value.
            }
        });
    }
    for (auto& th : threads) th.join();

    // After all that, the key must be gone (expired) and the map consistent.
    EXPECT_EQ(cache.get("racy_key"), std::nullopt);
}
