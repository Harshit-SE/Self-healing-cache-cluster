#include <gtest/gtest.h>

#include <chrono>

#include "cache/metrics.hpp"

using namespace cache;

TEST(CacheMetricsTest, ExportsPrometheusCountersHistogramAndHotKeyFeatures) {
    CacheMetrics metrics;
    metrics.observe("set", "hot-key", std::nullopt, std::chrono::milliseconds(3));
    metrics.observe("get", "hot-key", true, std::chrono::milliseconds(2));
    metrics.observe("get", "cold-key", false, std::chrono::milliseconds(4));
    metrics.recordFailureDetectionDuration(std::chrono::milliseconds(60));
    metrics.recordFailureEvent(std::chrono::milliseconds(120));

    const auto exposition = metrics.renderPrometheus("cache-a", 2, 40, 4096, 1, 2, 0.03, 1, 1,
        {{"cache-a", "ALIVE"}, {"cache-b", "FAILED"}});
    EXPECT_NE(exposition.find("cache_requests_total{node=\"cache-a\",operation=\"get\"} 2"), std::string::npos);
    EXPECT_NE(exposition.find("cache_hits_total{node=\"cache-a\"} 1"), std::string::npos);
    EXPECT_NE(exposition.find("cache_misses_total{node=\"cache-a\"} 1"), std::string::npos);
    EXPECT_NE(exposition.find("cache_keys{node=\"cache-a\"} 2"), std::string::npos);
    EXPECT_NE(exposition.find("cache_process_resident_memory_bytes{node=\"cache-a\"} 4096"), std::string::npos);
    EXPECT_NE(exposition.find("cache_failover_events_total{node=\"cache-a\"} 1"), std::string::npos);
    EXPECT_NE(exposition.find("cache_failure_detection_duration_seconds_count{node=\"cache-a\"} 1"), std::string::npos);
    EXPECT_NE(exposition.find("cache_failover_duration_seconds_count{node=\"cache-a\"} 1"), std::string::npos);
    EXPECT_NE(exposition.find("cache_node_state{node=\"cache-a\",member=\"cache-b\",state=\"FAILED\"} 1"), std::string::npos);

    const auto hotkeys = metrics.renderHotKeys("cache-a", 0.05);
    EXPECT_NE(hotkeys.find("\"node_id\":\"cache-a\""), std::string::npos);
    EXPECT_NE(hotkeys.find("\"key\":\"hot-key\""), std::string::npos);
    EXPECT_NE(hotkeys.find("\"read_ratio\":0.5"), std::string::npos);
}

TEST(CacheMetricsTest, EscapesArbitraryKeysInHotKeyJson) {
    CacheMetrics metrics;
    metrics.observe("get", "quoted\"key", true, std::chrono::microseconds(50));
    const auto hotkeys = metrics.renderHotKeys("cache-a", 0.01);
    EXPECT_NE(hotkeys.find("quoted\\\"key"), std::string::npos);
}
