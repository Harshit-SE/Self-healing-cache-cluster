#include "cache/cache_engine.hpp"

#include <mutex>

namespace cache {

bool CacheEngine::isExpired(const CacheEntry& entry,
                             std::chrono::steady_clock::time_point now) {
    return entry.expires_at.has_value() && now >= *entry.expires_at;
}

void CacheEngine::set(const std::string& key,
                       const std::string& value,
                       std::optional<std::chrono::milliseconds> ttl) {
    std::optional<std::chrono::steady_clock::time_point> expires_at;
    if (ttl.has_value()) {
        expires_at = std::chrono::steady_clock::now() + *ttl;
    }

    std::unique_lock lock(mutex_);  // Exclusive: writers block everyone.
    store_[key] = CacheEntry{value, expires_at};
}

std::optional<std::string> CacheEngine::get(const std::string& key) {
    const auto now = std::chrono::steady_clock::now();

    // --- Fast path: shared (read) lock ---
    {
        std::shared_lock lock(mutex_);
        auto it = store_.find(key);
        if (it == store_.end()) {
            return std::nullopt;  // Never existed (or already purged).
        }
        if (!isExpired(it->second, now)) {
            return it->second.value;  // Common case: valid, unexpired hit.
        }
        // Falls through to the slow path below once the shared lock
        // releases at the end of this block (RAII scope exit).
    }

    // --- Slow path: entry appeared expired under the shared lock.
    // Upgrade to a unique lock and RE-CHECK before erasing, because
    // another thread could have called set() on this same key in the
    // gap between releasing the shared lock and acquiring this one. ---
    std::unique_lock lock(mutex_);
    auto it = store_.find(key);
    if (it == store_.end()) {
        return std::nullopt;  // Someone else already removed it.
    }
    if (!isExpired(it->second, std::chrono::steady_clock::now())) {
        return it->second.value;  // It was refreshed in the meantime.
    }
    store_.erase(it);  // Genuinely expired: remove it now (lazy expiration).
    return std::nullopt;
}

bool CacheEngine::remove(const std::string& key) {
    std::unique_lock lock(mutex_);
    return store_.erase(key) > 0;
}

std::size_t CacheEngine::size() const {
    std::shared_lock lock(mutex_);
    return store_.size();
}

}  // namespace cache
