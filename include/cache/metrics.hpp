#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace cache {

class CacheMetrics {
public:
    struct HotKey {
        std::string key;
        double access_rate{};
        double read_ratio{};
        double mean_latency_ms{};
    };

    CacheMetrics();
    void observe(const std::string& operation, const std::string& key,
                 std::optional<bool> cache_hit, std::chrono::nanoseconds latency);
    void recordFailureDetectionDuration(std::chrono::nanoseconds duration);
    void recordFailureEvent(std::optional<std::chrono::nanoseconds> detection_time = std::nullopt);
    double nodeRequestRate() const;
    std::string renderPrometheus(const std::string& node_id, std::size_t key_count,
                                 std::size_t payload_bytes, std::size_t process_rss_bytes,
                                 std::uint64_t replication_pending,
                                 std::uint64_t replication_failures, double replication_lag_seconds,
                                 std::uint64_t recovery_pending, std::uint64_t recovery_failures,
                                 const std::vector<std::pair<std::string, std::string>>& states) const;
    std::string renderHotKeys(const std::string& node_id, double node_load) const;

private:
    struct Window {
        std::int64_t epoch{-1};
        std::uint64_t gets{};
        std::uint64_t writes{};
        std::uint64_t requests{};
        std::uint64_t latency_ns{};
    };
    struct KeyWindow { std::array<Window, 6> buckets{}; };
    static std::string escapeLabel(const std::string& value);
    static std::string escapeJson(const std::string& value);

    mutable std::mutex mutex_;
    std::uint64_t gets_{};
    std::uint64_t sets_{};
    std::uint64_t deletes_{};
    std::uint64_t hits_{};
    std::uint64_t misses_{};
    std::uint64_t request_count_{};
    std::uint64_t latency_sum_ns_{};
    std::array<std::uint64_t, 14> latency_buckets_{};
    std::array<Window, 6> node_windows_{};
    std::unordered_map<std::string, KeyWindow> key_windows_;
    std::uint64_t failure_events_{};
    std::uint64_t failure_duration_count_{};
    std::uint64_t failure_duration_sum_ns_{};
    std::array<std::uint64_t, 14> failure_duration_buckets_{};
    std::uint64_t detection_duration_count_{};
    std::uint64_t detection_duration_sum_ns_{};
    std::array<std::uint64_t, 14> detection_duration_buckets_{};
};

}  // namespace cache
