#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "cache/cache_engine.hpp"
#include "cache/cluster_config.hpp"
#include "cache/consistent_hash_ring.hpp"
#include "cache/protocol.hpp"
#include "cache/metrics.hpp"

namespace cache {

class TcpServer {
public:
    explicit TcpServer(std::uint16_t port, std::size_t max_line_bytes = 64 * 1024,
                       std::optional<std::uint16_t> metrics_port = std::nullopt);
    TcpServer(std::uint16_t port, std::string node_id, std::size_t cluster_size,
              std::size_t max_line_bytes = 64 * 1024,
              std::optional<std::uint16_t> metrics_port = std::nullopt);
    TcpServer(const ClusterConfig& config, const std::string& node_id,
              std::size_t max_line_bytes = 64 * 1024,
              std::optional<std::uint16_t> metrics_port = std::nullopt);
    ~TcpServer();
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Blocks until stop() is called or the listener fails. Port 0 asks the OS
    // for an ephemeral port, available via boundPort() after start().
    bool start();
    void stop();
    std::uint16_t boundPort() const noexcept { return bound_port_.load(); }
    std::uint16_t boundMetricsPort() const noexcept { return bound_metrics_port_.load(); }

private:
    void serveClient(int fd);
    void heartbeatLoop();
    void replicationLoop();
    void recoveryLoop();
    void metricsLoop();
    void applyMembershipState(const std::string& node_id, const std::string& state,
                              bool propagate);
    bool applyNodeJoin(const CacheNode& node, bool propagate);
    bool applyNodeRemove(const std::string& node_id, bool propagate);
    void maybeCompleteJoin();
    std::string membershipStatus() const;
    std::vector<CacheNode> activeOwners(const std::string& key) const;
    std::vector<CacheNode> replicationOwners(const std::string& key) const;
    std::vector<CacheNode> configuredNodes() const;
    ClusterConfig clusterConfigSnapshot() const;
    bool isClustered() const;
    void enqueueReplicas(const Request& request);

    struct ReplicationTask {
        CacheNode destination;
        Request::Operation operation;
        std::string key;
        std::string value;
        std::optional<std::chrono::milliseconds> ttl;
        std::chrono::steady_clock::time_point queued_at;
        bool recovery{false};
    };

    std::uint16_t requested_port_;
    std::optional<std::uint16_t> requested_metrics_port_;
    std::string node_id_;
    std::size_t cluster_size_;
    std::optional<ClusterConfig> cluster_config_;
    std::optional<ConsistentHashRing> ring_;
    mutable std::mutex topology_mutex_;
    std::atomic<std::uint16_t> bound_port_{0};
    std::atomic<std::uint16_t> bound_metrics_port_{0};
    std::size_t max_line_bytes_;
    std::atomic<bool> stopping_{false};
    std::atomic<int> listen_fd_{-1};
    std::mutex clients_mutex_;
    std::vector<int> clients_;
    std::vector<std::thread> workers_;
    CacheEngine cache_;
    CacheMetrics metrics_;
    // Keeps primary mutations and their queue order aligned without holding a
    // cache lock during replication network I/O.
    std::mutex mutation_mutex_;
    std::thread heartbeat_thread_;
    std::thread replication_thread_;
    std::thread recovery_thread_;
    std::thread metrics_thread_;
    mutable std::mutex membership_mutex_;
    std::map<std::string, std::string> member_states_;
    std::map<std::string, std::size_t> missed_heartbeats_;
    std::map<std::string, std::chrono::steady_clock::time_point> failure_start_times_;
    std::map<std::string, bool> join_announced_;
    std::set<std::string> sync_completed_;
    mutable std::mutex heartbeat_wait_mutex_;
    std::condition_variable heartbeat_wait_cv_;
    std::mutex replication_mutex_;
    std::condition_variable replication_cv_;
    std::deque<ReplicationTask> replication_queue_;
    bool replication_stopping_{false};
    std::atomic<std::uint64_t> replication_failures_{0};
    std::atomic<std::uint64_t> recovery_generation_{1};
    std::atomic<std::uint64_t> recovery_completed_{0};
    std::atomic<std::uint64_t> recovery_tasks_pending_{0};
    std::atomic<std::uint64_t> recovery_failures_{0};
    std::atomic<std::uint64_t> replication_tasks_pending_{0};
    std::atomic<bool> recovery_retry_needed_{false};
};

}  // namespace cache
