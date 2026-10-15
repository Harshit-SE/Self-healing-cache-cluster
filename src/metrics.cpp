#include "cache/metrics.hpp"

#include <algorithm>
#include <sstream>

namespace cache {
namespace {
constexpr std::array<double, 14> kLatencyBoundsSeconds = {
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0};
constexpr std::int64_t kWindowSeconds = 10;
constexpr std::size_t kMaximumTrackedKeys = 4096;

std::int64_t currentWindow() {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count() / kWindowSeconds;
}

}  // namespace

CacheMetrics::CacheMetrics() = default;

void CacheMetrics::observe(const std::string& operation, const std::string& key,
                           std::optional<bool> cache_hit, std::chrono::nanoseconds latency) {
    const auto latency_ns = static_cast<std::uint64_t>(std::max<std::int64_t>(0, latency.count()));
    const double latency_seconds = static_cast<double>(latency_ns) / 1'000'000'000.0;
    const auto epoch = currentWindow();
    std::lock_guard lock(mutex_);
    ++request_count_;
    latency_sum_ns_ += latency_ns;
    for (std::size_t i = 0; i < kLatencyBoundsSeconds.size(); ++i) {
        if (latency_seconds <= kLatencyBoundsSeconds[i]) { ++latency_buckets_[i]; break; }
    }
    if (operation == "get") {
        ++gets_;
        if (cache_hit.value_or(false)) ++hits_;
        else if (cache_hit.has_value()) ++misses_;
    } else if (operation == "set") ++sets_;
    else if (operation == "delete") ++deletes_;

    auto& node_window = node_windows_[static_cast<std::size_t>(epoch % 6)];
    if (node_window.epoch != epoch) node_window = Window{.epoch = epoch};
    ++node_window.requests;

    if (key.empty()) return;
    auto found = key_windows_.find(key);
    if (found == key_windows_.end()) {
        if (key_windows_.size() >= kMaximumTrackedKeys) {
            const auto stale = std::min_element(key_windows_.begin(), key_windows_.end(),
                [](const auto& left, const auto& right) {
                    const auto left_epoch = std::max_element(left.second.buckets.begin(), left.second.buckets.end(),
                        [](const Window& a, const Window& b) { return a.epoch < b.epoch; })->epoch;
                    const auto right_epoch = std::max_element(right.second.buckets.begin(), right.second.buckets.end(),
                        [](const Window& a, const Window& b) { return a.epoch < b.epoch; })->epoch;
                    return left_epoch < right_epoch;
                });
            if (stale != key_windows_.end()) key_windows_.erase(stale);
        }
        found = key_windows_.try_emplace(key).first;
    }
    auto& window = found->second.buckets[static_cast<std::size_t>(epoch % 6)];
    if (window.epoch != epoch) window = Window{.epoch = epoch};
    ++window.requests;
    window.latency_ns += latency_ns;
    if (operation == "get") ++window.gets;
    else if (operation == "set" || operation == "delete") ++window.writes;
}

void CacheMetrics::recordFailureEvent(std::optional<std::chrono::nanoseconds> detection_time) {
    std::lock_guard lock(mutex_);
    ++failure_events_;
    if (!detection_time) return;
    const auto duration_ns = static_cast<std::uint64_t>(std::max<std::int64_t>(0, detection_time->count()));
    const double seconds = static_cast<double>(duration_ns) / 1'000'000'000.0;
    ++failure_duration_count_;
    failure_duration_sum_ns_ += duration_ns;
    for (std::size_t i = 0; i < kLatencyBoundsSeconds.size(); ++i) {
        if (seconds <= kLatencyBoundsSeconds[i]) { ++failure_duration_buckets_[i]; break; }
    }
}

void CacheMetrics::recordFailureDetectionDuration(std::chrono::nanoseconds duration) {
    const auto duration_ns = static_cast<std::uint64_t>(std::max<std::int64_t>(0, duration.count()));
    const double seconds = static_cast<double>(duration_ns) / 1'000'000'000.0;
    std::lock_guard lock(mutex_);
    ++detection_duration_count_;
    detection_duration_sum_ns_ += duration_ns;
    for (std::size_t i = 0; i < kLatencyBoundsSeconds.size(); ++i) {
        if (seconds <= kLatencyBoundsSeconds[i]) { ++detection_duration_buckets_[i]; break; }
    }
}

std::string CacheMetrics::escapeLabel(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char ch : value) {
        if (ch == '\\' || ch == '"') escaped.push_back('\\');
        if (ch == '\n') escaped += "\\n";
        else escaped.push_back(ch);
    }
    return escaped;
}

std::string CacheMetrics::escapeJson(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    constexpr char hex[] = "0123456789abcdef";
    for (const unsigned char ch : value) {
        switch (ch) {
            case '"': escaped += "\\\""; break;
            case '\\': escaped += "\\\\"; break;
            case '\b': escaped += "\\b"; break;
            case '\f': escaped += "\\f"; break;
            case '\n': escaped += "\\n"; break;
            case '\r': escaped += "\\r"; break;
            case '\t': escaped += "\\t"; break;
            default:
                if (ch < 0x20) { escaped += "\\u00"; escaped.push_back(hex[ch >> 4]); escaped.push_back(hex[ch & 0xf]); }
                else escaped.push_back(static_cast<char>(ch));
        }
    }
    return escaped;
}

std::string CacheMetrics::renderPrometheus(
    const std::string& node_id, std::size_t key_count, std::size_t payload_bytes,
    std::size_t process_rss_bytes,
    std::uint64_t replication_pending, std::uint64_t replication_failures,
    double replication_lag_seconds, std::uint64_t recovery_pending,
    std::uint64_t recovery_failures,
    const std::vector<std::pair<std::string, std::string>>& states) const {
    std::lock_guard lock(mutex_);
    const auto node = escapeLabel(node_id);
    std::ostringstream out;
    out << "# HELP cache_requests_total Client GET, SET, and DELETE requests.\n"
        << "# TYPE cache_requests_total counter\n"
        << "cache_requests_total{node=\"" << node << "\",operation=\"get\"} " << gets_ << '\n'
        << "cache_requests_total{node=\"" << node << "\",operation=\"set\"} " << sets_ << '\n'
        << "cache_requests_total{node=\"" << node << "\",operation=\"delete\"} " << deletes_ << '\n'
        << "# HELP cache_hits_total Successful cache GETs.\n# TYPE cache_hits_total counter\n"
        << "cache_hits_total{node=\"" << node << "\"} " << hits_ << '\n'
        << "# HELP cache_misses_total Missing or expired cache GETs.\n# TYPE cache_misses_total counter\n"
        << "cache_misses_total{node=\"" << node << "\"} " << misses_ << '\n'
        << "# HELP cache_request_duration_seconds Request latency histogram.\n"
        << "# TYPE cache_request_duration_seconds histogram\n";
    std::uint64_t cumulative = 0;
    for (std::size_t i = 0; i < kLatencyBoundsSeconds.size(); ++i) {
        cumulative += latency_buckets_[i];
        out << "cache_request_duration_seconds_bucket{node=\"" << node << "\",le=\""
            << kLatencyBoundsSeconds[i] << "\"} " << cumulative << '\n';
    }
    out << "cache_request_duration_seconds_bucket{node=\"" << node << "\",le=\"+Inf\"} "
        << request_count_ << '\n'
        << "cache_request_duration_seconds_sum{node=\"" << node << "\"} "
        << static_cast<double>(latency_sum_ns_) / 1'000'000'000.0 << '\n'
        << "cache_request_duration_seconds_count{node=\"" << node << "\"} " << request_count_ << '\n'
        << "# HELP cache_keys Current physically stored key count.\n# TYPE cache_keys gauge\n"
        << "cache_keys{node=\"" << node << "\"} " << key_count << '\n'
        << "# HELP cache_memory_bytes Approximate bytes occupied by key and value strings.\n"
        << "# TYPE cache_memory_bytes gauge\ncache_memory_bytes{node=\"" << node << "\"} " << payload_bytes << '\n'
        << "# HELP cache_process_resident_memory_bytes Current process resident set size.\n"
        << "# TYPE cache_process_resident_memory_bytes gauge\ncache_process_resident_memory_bytes{node=\""
        << node << "\"} " << process_rss_bytes << '\n'
        << "# HELP cache_replication_queue_depth Queued and in-flight replica updates.\n"
        << "# TYPE cache_replication_queue_depth gauge\ncache_replication_queue_depth{node=\"" << node
        << "\"} " << replication_pending << '\n'
        << "# HELP cache_replication_failures_total Failed replica deliveries.\n"
        << "# TYPE cache_replication_failures_total counter\ncache_replication_failures_total{node=\"" << node
        << "\"} " << replication_failures << '\n'
        << "# HELP cache_replication_lag_seconds Age of the oldest queued replica update.\n"
        << "# TYPE cache_replication_lag_seconds gauge\ncache_replication_lag_seconds{node=\"" << node
        << "\"} " << replication_lag_seconds << '\n'
        << "# HELP cache_recovery_pending Current pending repair work.\n# TYPE cache_recovery_pending gauge\n"
        << "cache_recovery_pending{node=\"" << node << "\"} " << recovery_pending << '\n'
        << "# HELP cache_recovery_failures_total Failed repair deliveries.\n# TYPE cache_recovery_failures_total counter\n"
        << "cache_recovery_failures_total{node=\"" << node << "\"} " << recovery_failures << '\n'
        << "# HELP cache_failover_events_total Membership transitions to FAILED.\n"
        << "# TYPE cache_failover_events_total counter\ncache_failover_events_total{node=\"" << node << "\"} "
        << failure_events_ << '\n'
        << "# HELP cache_failover_duration_seconds Time from first missed probe until local FAILED state (ownership exclusion starts at SUSPECTED).\n"
        << "# TYPE cache_failover_duration_seconds histogram\n";
    cumulative = 0;
    for (std::size_t i = 0; i < kLatencyBoundsSeconds.size(); ++i) {
        cumulative += failure_duration_buckets_[i];
        out << "cache_failover_duration_seconds_bucket{node=\"" << node << "\",le=\""
            << kLatencyBoundsSeconds[i] << "\"} " << cumulative << '\n';
    }
    out << "cache_failover_duration_seconds_bucket{node=\"" << node << "\",le=\"+Inf\"} "
        << failure_duration_count_ << '\n'
        << "cache_failover_duration_seconds_sum{node=\"" << node << "\"} "
        << static_cast<double>(failure_duration_sum_ns_) / 1'000'000'000.0 << '\n'
        << "cache_failover_duration_seconds_count{node=\"" << node << "\"} "
        << failure_duration_count_ << '\n'
        << "# HELP cache_failure_detection_duration_seconds Time from first missed probe until SUSPECTED.\n"
        << "# TYPE cache_failure_detection_duration_seconds histogram\n";
    cumulative = 0;
    for (std::size_t i = 0; i < kLatencyBoundsSeconds.size(); ++i) {
        cumulative += detection_duration_buckets_[i];
        out << "cache_failure_detection_duration_seconds_bucket{node=\"" << node << "\",le=\""
            << kLatencyBoundsSeconds[i] << "\"} " << cumulative << '\n';
    }
    out << "cache_failure_detection_duration_seconds_bucket{node=\"" << node << "\",le=\"+Inf\"} "
        << detection_duration_count_ << '\n'
        << "cache_failure_detection_duration_seconds_sum{node=\"" << node << "\"} "
        << static_cast<double>(detection_duration_sum_ns_) / 1'000'000'000.0 << '\n'
        << "cache_failure_detection_duration_seconds_count{node=\"" << node << "\"} "
        << detection_duration_count_ << '\n'
        << "# HELP cache_node_state Node membership state (one for active state).\n"
        << "# TYPE cache_node_state gauge\n";
    for (const auto& [member, state] : states) {
        for (const auto* candidate : {"ALIVE", "JOINING", "SUSPECTED", "FAILED"}) {
            out << "cache_node_state{node=\"" << node << "\",member=\"" << escapeLabel(member)
                << "\",state=\"" << candidate << "\"} " << (state == candidate ? 1 : 0) << '\n';
        }
    }
    return out.str();
}

std::string CacheMetrics::renderHotKeys(const std::string& node_id, double node_load) const {
    std::lock_guard lock(mutex_);
    const auto now = currentWindow();
    struct Row { std::string key; std::uint64_t gets; std::uint64_t writes; std::uint64_t requests; std::uint64_t latency_ns; };
    std::vector<Row> rows;
    for (const auto& [key, history] : key_windows_) {
        Row row{key, 0, 0, 0, 0};
        for (const auto& bucket : history.buckets) {
            if (bucket.epoch < now - 5 || bucket.epoch > now) continue;
            row.gets += bucket.gets; row.writes += bucket.writes;
            row.requests += bucket.requests; row.latency_ns += bucket.latency_ns;
        }
        if (row.requests != 0) rows.push_back(std::move(row));
    }
    std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) { return a.requests > b.requests; });
    if (rows.size() > 100) rows.resize(100);
    std::ostringstream out;
    out << "{\"node_id\":\"" << escapeJson(node_id) << "\",\"node_load\":" << node_load << ",\"keys\":[";
    bool first = true;
    for (const auto& row : rows) {
        if (!first) out << ',';
        first = false;
        const double access_rate = static_cast<double>(row.requests) / 60.0;
        const double read_ratio = static_cast<double>(row.gets) / static_cast<double>(row.requests);
        const double mean_latency_ms = static_cast<double>(row.latency_ns) /
            static_cast<double>(row.requests) / 1'000'000.0;
        out << "{\"key\":\"" << escapeJson(row.key) << "\",\"access_rate\":" << access_rate
            << ",\"read_ratio\":" << read_ratio << ",\"mean_latency_ms\":" << mean_latency_ms
            << ",\"accesses_60s\":" << row.requests << "}";
    }
    out << "]}";
    return out.str();
}

double CacheMetrics::nodeRequestRate() const {
    std::lock_guard lock(mutex_);
    const auto now = currentWindow();
    std::uint64_t requests = 0;
    for (const auto& bucket : node_windows_) {
        if (bucket.epoch >= now - 5 && bucket.epoch <= now) requests += bucket.requests;
    }
    return static_cast<double>(requests) / 60.0;
}

}  // namespace cache
