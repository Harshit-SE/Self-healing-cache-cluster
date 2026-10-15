#pragma once

#include <chrono>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <unordered_map>
#include <vector>

namespace cache {

// Thread-safe in-memory key-value store with optional per-key TTL.
//
// Concurrency model:
//   - get()    takes a SHARED lock first (fast path for the common,
//              non-expired case). If the entry turns out to be expired,
//              it upgrades to a UNIQUE lock to remove it (see .cpp for
//              the double-checked locking rationale).
//   - set()/remove() take a UNIQUE lock -> exclusive access while writing.
//
// TTL strategy: LAZY expiration. An expired key is only actually removed
// when it's next accessed via get() (or overwritten via set()). There is
// no background sweep thread. This means a key that is set with a short
// TTL and never read again will remain in memory (using space, but never
// returned as a value) until the process exits or the key is overwritten.
// This is a known, deliberate Phase 1 scope limitation — a production
// system would typically add a periodic active-expiration sweep.
class CacheEngine {
public:
    struct SnapshotEntry {
        std::string key;
        std::string value;
        std::optional<std::chrono::milliseconds> ttl;
    };
    CacheEngine() = default;

    CacheEngine(const CacheEngine&) = delete;
    CacheEngine& operator=(const CacheEngine&) = delete;

    // Inserts or overwrites the value for `key`.
    // If `ttl` is provided, the key expires `ttl` after this call.
    // If `ttl` is std::nullopt (the default), the key never expires.
    void set(const std::string& key,
             const std::string& value,
             std::optional<std::chrono::milliseconds> ttl = std::nullopt);

    // Returns the value for `key`, or std::nullopt if the key doesn't
    // exist OR has expired. An expired key is lazily removed as a side
    // effect of this call.
    std::optional<std::string> get(const std::string& key);

    // Removes `key` if present (regardless of expiry state).
    // Returns true if a key was actually removed.
    bool remove(const std::string& key);

    // Number of entries physically stored right now. NOTE: due to lazy
    // expiration, this MAY include expired-but-not-yet-accessed entries —
    // it is a storage count, not a "logically valid keys" count.
    std::size_t size() const;
    // Approximate payload size: sum of key and value string lengths.
    std::size_t approximateMemoryBytes() const;

    // Copies live entries with their remaining TTL for background migration.
    std::vector<SnapshotEntry> snapshot();

private:
    struct CacheEntry {
        std::string value;
        std::optional<std::chrono::steady_clock::time_point> expires_at;
    };

    static bool isExpired(const CacheEntry& entry,
                           std::chrono::steady_clock::time_point now);

    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, CacheEntry> store_;
};

}  // namespace cache
