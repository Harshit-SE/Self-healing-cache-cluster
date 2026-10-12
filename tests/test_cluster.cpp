#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "cache/cluster_config.hpp"
#include "cache/consistent_hash_ring.hpp"
#include "cache/tcp_server.hpp"

using namespace cache;

namespace {
CacheNode node(std::string id, std::uint16_t port) {
    return CacheNode{std::move(id), "127.0.0.1", port};
}
std::string ownerId(const ConsistentHashRing& ring, const std::string& key) {
    return ring.owner(key).id;
}

std::uint16_t unusedLoopbackPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        ::close(fd);
        return 0;
    }
    socklen_t length = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) < 0) {
        ::close(fd);
        return 0;
    }
    const auto port = ntohs(address.sin_port);
    ::close(fd);
    return port;
}

std::string requestNode(const CacheNode& node, const std::string& line) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return "<socket-error>";
    timeval timeout{1, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(node.port);
    ::inet_pton(AF_INET, node.host.c_str(), &address.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        ::close(fd);
        return "<connect-error>";
    }
    const auto wire = line + "\n";
    std::size_t sent = 0;
    while (sent < wire.size()) {
        const auto count = ::send(fd, wire.data() + sent, wire.size() - sent, MSG_NOSIGNAL);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) { ::close(fd); return "<send-error>"; }
        sent += static_cast<std::size_t>(count);
    }
    std::string response;
    char buffer[1024];
    while (response.find('\n') == std::string::npos) {
        const auto count = ::recv(fd, buffer, sizeof(buffer), 0);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) { ::close(fd); return "<receive-error>"; }
        response.append(buffer, static_cast<std::size_t>(count));
    }
    ::close(fd);
    response.resize(response.find('\n'));
    return response;
}
}

TEST(ConsistentHashRingTest, StableHashAndRoutingAreDeterministic) {
    const ClusterConfig first{96, 2, 500, 2, 4,
        {node("cache-a", 7401), node("cache-b", 7402), node("cache-c", 7403)}};
    const ClusterConfig different_order{96, 2, 500, 2, 4,
        {node("cache-c", 7403), node("cache-a", 7401), node("cache-b", 7402)}};
    const ConsistentHashRing ring_a(first);
    const ConsistentHashRing ring_b(different_order);
    EXPECT_EQ(ConsistentHashRing::stableHash("hello"), 0xa430d84680aabd0bULL);
    for (int i = 0; i < 1000; ++i) {
        const auto key = "key-" + std::to_string(i);
        EXPECT_EQ(ownerId(ring_a, key), ownerId(ring_b, key));
    }
}

TEST(ConsistentHashRingTest, AddingNodeMovesOnlyItsNewOwnershipIntervals) {
    ConsistentHashRing before(128);
    before.addNode(node("cache-a", 7401));
    before.addNode(node("cache-b", 7402));
    before.addNode(node("cache-c", 7403));
    auto after = before;
    after.addNode(node("cache-d", 7404));

    constexpr int key_count = 10000;
    int unchanged = 0;
    int moved_to_new_node = 0;
    for (int i = 0; i < key_count; ++i) {
        const auto key = "user-record-" + std::to_string(i);
        const auto original_owner = ownerId(before, key);
        const auto updated_owner = ownerId(after, key);
        if (original_owner == updated_owner) ++unchanged;
        if (updated_owner == "cache-d") ++moved_to_new_node;
    }
    EXPECT_GT(unchanged, key_count * 0.65);
    EXPECT_LT(unchanged, key_count * 0.95);
    EXPECT_GT(moved_to_new_node, key_count * 0.12);
    EXPECT_LT(moved_to_new_node, key_count * 0.38);
}

TEST(ConsistentHashRingTest, RemovingNodeIsDeterministicAndRejectsUnknownIds) {
    ConsistentHashRing ring(64);
    ring.addNode(node("cache-a", 7401));
    ring.addNode(node("cache-b", 7402));
    ring.addNode(node("cache-c", 7403));
    EXPECT_TRUE(ring.removeNode("cache-b"));
    EXPECT_FALSE(ring.removeNode("missing"));
    EXPECT_EQ(ring.nodeCount(), 2u);
    for (int i = 0; i < 1000; ++i) {
        const auto id = ownerId(ring, "k" + std::to_string(i));
        EXPECT_TRUE(id == "cache-a" || id == "cache-c");
    }
}

TEST(ConsistentHashRingTest, ReplicaOwnersAreDistinctAndPrimaryComesFirst) {
    ConsistentHashRing ring(32);
    ring.addNode(node("cache-a", 7401));
    ring.addNode(node("cache-b", 7402));
    ring.addNode(node("cache-c", 7403));
    const auto selected = ring.owners("replica-key", 3);
    ASSERT_EQ(selected.size(), 3u);
    EXPECT_EQ(selected.front().id, ring.owner("replica-key").id);
    EXPECT_NE(selected[0].id, selected[1].id);
    EXPECT_NE(selected[0].id, selected[2].id);
    EXPECT_NE(selected[1].id, selected[2].id);
    EXPECT_EQ(ring.owners("replica-key", 10).size(), 3u);
}

TEST(ClusterConfigTest, LoadsVirtualNodesAndNodeMembership) {
    const auto path = std::filesystem::temp_directory_path() /
        ("self-healing-cache-cluster-test-" + std::to_string(::getpid()) + ".conf");
    {
        std::ofstream output(path);
        ASSERT_TRUE(output);
        output << "# static membership\n"
                  "vnodes 24\n"
                  "replicas 3\n"
                  "heartbeat_ms 250\n"
                  "suspect_after 2\n"
                  "fail_after 5\n"
                  "node cache-a 127.0.0.1 7401\n"
                  "node cache-b 127.0.0.1 7402\n";
    }
    const auto config = ClusterConfig::load(path.string());
    EXPECT_EQ(config.virtual_nodes, 24u);
    EXPECT_EQ(config.replication_factor, 3u);
    EXPECT_EQ(config.heartbeat_interval_ms, 250u);
    EXPECT_EQ(config.suspect_after_missed_heartbeats, 2u);
    EXPECT_EQ(config.fail_after_missed_heartbeats, 5u);
    EXPECT_EQ(config.nodes.size(), 2u);
    EXPECT_EQ(config.nodeById("cache-b").port, 7402);
    std::filesystem::remove(path);
}

TEST(ClusterConfigTest, RejectsFailureThresholdNotGreaterThanSuspectThreshold) {
    const auto path = std::filesystem::temp_directory_path() /
        ("self-healing-cache-cluster-threshold-test-" + std::to_string(::getpid()) + ".conf");
    {
        std::ofstream output(path);
        ASSERT_TRUE(output);
        output << "suspect_after 4\nfail_after 4\nnode cache-a 127.0.0.1 7401\n";
    }
    EXPECT_THROW(ClusterConfig::load(path.string()), std::runtime_error);
    std::filesystem::remove(path);
}

TEST(ClusterConfigTest, RejectsDuplicateNodeIds) {
    const auto path = std::filesystem::temp_directory_path() /
        ("self-healing-cache-cluster-duplicate-test-" + std::to_string(::getpid()) + ".conf");
    {
        std::ofstream output(path);
        ASSERT_TRUE(output);
        output << "node cache-a 127.0.0.1 7401\nnode cache-a 127.0.0.1 7402\n";
    }
    EXPECT_THROW(ClusterConfig::load(path.string()), std::runtime_error);
    std::filesystem::remove(path);
}

TEST(ClusterRuntimeTest, AsyncReplicaSurvivesPrimaryShutdownAndFailureIsDetected) {
    const auto first_port = unusedLoopbackPort();
    const auto second_port = unusedLoopbackPort();
    if (first_port == 0 || second_port == 0 || first_port == second_port) {
        FAIL() << "could not allocate two temporary test ports";
        return;
    }
    ClusterConfig config;
    config.virtual_nodes = 48;
    config.replication_factor = 2;
    config.heartbeat_interval_ms = 50;
    config.suspect_after_missed_heartbeats = 2;
    config.fail_after_missed_heartbeats = 4;
    config.nodes = {node("runtime-a", first_port), node("runtime-b", second_port)};

    TcpServer first(config, "runtime-a");
    TcpServer second(config, "runtime-b");
    std::atomic<bool> first_returned{false}, second_returned{false};
    bool first_ok = false, second_ok = false;
    std::thread first_thread([&] { first_ok = first.start(); first_returned.store(true); });
    std::thread second_thread([&] { second_ok = second.start(); second_returned.store(true); });
    const auto start_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while ((first.boundPort() == 0 || second.boundPort() == 0) &&
           !first_returned.load() && !second_returned.load() &&
           std::chrono::steady_clock::now() < start_deadline) std::this_thread::yield();
    if (first.boundPort() == 0 || second.boundPort() == 0) {
        first.stop(); second.stop();
        if (first_thread.joinable()) first_thread.join();
        if (second_thread.joinable()) second_thread.join();
        FAIL() << "test cache nodes did not bind";
        return;
    }

    const ConsistentHashRing ring(config);
    const auto copies = ring.owners("failover-check", 2);
    const CacheNode primary = copies.front();
    const CacheNode replica = copies.back();
    // Multi-node servers stay JOINING until the peer has completed its initial
    // recovery pass. Wait for both nodes to become writable before seeding data.
    const auto ready_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    std::string primary_status;
    std::string replica_status;
    do {
        primary_status = requestNode(primary, "v1 STATUS");
        replica_status = requestNode(replica, "v1 STATUS");
        if (primary_status.find(primary.id + "=ALIVE") != std::string::npos &&
            replica_status.find(replica.id + "=ALIVE") != std::string::npos) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < ready_deadline);
    const std::string write_response = requestNode(primary, "v1 SET failover-check - replica-payload");
    std::string replica_read;
    const auto replicate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        replica_read = requestNode(replica, "v1 GET failover-check");
        if (replica_read == "OK VALUE replica-payload") break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < replicate_deadline);

    const std::string delete_response = requestNode(primary, "v1 DELETE failover-check");
    std::string replica_after_delete;
    const auto delete_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        replica_after_delete = requestNode(replica, "v1 GET failover-check");
        if (replica_after_delete == "OK NOT_FOUND") break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < delete_deadline);
    const std::string rewrite_response = requestNode(primary, "v1 SET failover-check - replica-payload");
    replica_read.clear();
    const auto rewrite_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        replica_read = requestNode(replica, "v1 GET failover-check");
        if (replica_read == "OK VALUE replica-payload") break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < rewrite_deadline);

    TcpServer& primary_server = primary.id == "runtime-a" ? first : second;
    TcpServer& replica_server = replica.id == "runtime-a" ? first : second;
    std::thread& primary_thread = primary.id == "runtime-a" ? first_thread : second_thread;
    std::thread& replica_thread = replica.id == "runtime-a" ? first_thread : second_thread;
    primary_server.stop();
    if (primary_thread.joinable()) primary_thread.join();

    std::string status;
    const auto failure_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        status = requestNode(replica, "v1 STATUS");
        if (status.find(primary.id + "=FAILED") != std::string::npos) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < failure_deadline);
    const auto after_failure_read = requestNode(replica, "v1 GET failover-check");
    const auto failure_status = requestNode(replica, "v1 STATUS");
    const auto promoted_write = requestNode(replica, "v1 SET failover-check - written-during-failover");
    const auto promoted_read = requestNode(replica, "v1 GET failover-check");

    // Restart the failed process. Its in-memory state may be stale; the live
    // replica must synchronize the write accepted during promotion.
    std::thread restarted_primary([&] { primary_server.start(); });
    std::string recovered_status;
    const auto recovery_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    do {
        recovered_status = requestNode(replica, "v1 STATUS");
        if (recovered_status.find(primary.id + "=ALIVE") != std::string::npos) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < recovery_deadline);
    std::string recovered_value;
    const auto sync_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    do {
        recovered_value = requestNode(primary, "v1 GET failover-check");
        if (recovered_value == "OK VALUE written-during-failover") break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < sync_deadline);
    primary_server.stop();
    replica_server.stop();
    if (restarted_primary.joinable()) restarted_primary.join();
    if (replica_thread.joinable()) replica_thread.join();

    EXPECT_TRUE(first_ok);
    EXPECT_TRUE(second_ok);
    EXPECT_EQ(write_response, "OK STORED");
    EXPECT_EQ(delete_response, "OK DELETED");
    EXPECT_EQ(replica_after_delete, "OK NOT_FOUND");
    EXPECT_EQ(rewrite_response, "OK STORED");
    EXPECT_EQ(replica_read, "OK VALUE replica-payload");
    EXPECT_NE(status.find(primary.id + "=FAILED"), std::string::npos);
    EXPECT_EQ(after_failure_read, "OK VALUE replica-payload");
    EXPECT_EQ(promoted_write, "OK STORED") << "membership=" << failure_status;
    EXPECT_EQ(promoted_read, "OK VALUE written-during-failover");
    EXPECT_NE(recovered_status.find(primary.id + "=ALIVE"), std::string::npos)
        << "recovered membership=" << recovered_status;
    EXPECT_EQ(recovered_value, "OK VALUE written-during-failover")
        << "recovered membership=" << recovered_status;
}

TEST(ClusterRuntimeTest, OnlineJoinAndRemovalRebalanceExistingData) {
    const auto first_port = unusedLoopbackPort();
    const auto second_port = unusedLoopbackPort();
    const auto third_port = unusedLoopbackPort();
    if (first_port == 0 || second_port == 0 || third_port == 0 ||
        first_port == second_port || first_port == third_port || second_port == third_port) {
        FAIL() << "could not allocate three temporary test ports";
        return;
    }
    ClusterConfig original;
    original.virtual_nodes = 64;
    original.replication_factor = 2;
    original.heartbeat_interval_ms = 50;
    original.suspect_after_missed_heartbeats = 2;
    original.fail_after_missed_heartbeats = 4;
    original.nodes = {node("join-a", first_port), node("join-b", second_port)};
    ClusterConfig expanded = original;
    expanded.nodes.push_back(node("join-c", third_port));

    TcpServer first(original, "join-a");
    TcpServer second(original, "join-b");
    TcpServer joining(expanded, "join-c");
    std::thread first_thread([&] { first.start(); });
    std::thread second_thread([&] { second.start(); });
    const auto startup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while ((first.boundPort() == 0 || second.boundPort() == 0) &&
           std::chrono::steady_clock::now() < startup_deadline) std::this_thread::yield();
    if (first.boundPort() == 0 || second.boundPort() == 0) {
        first.stop(); second.stop();
        if (first_thread.joinable()) first_thread.join();
        if (second_thread.joinable()) second_thread.join();
        FAIL() << "initial cluster nodes did not bind";
        return;
    }

    const ConsistentHashRing old_ring(original);
    const ConsistentHashRing new_ring(expanded);
    std::string key;
    for (int i = 0; i < 10000; ++i) {
        const auto candidate = "rebalance-key-" + std::to_string(i);
        if (new_ring.owner(candidate).id == "join-c") { key = candidate; break; }
    }
    ASSERT_FALSE(key.empty());
    const auto& former_primary = old_ring.owner(key);
    const std::string initial_write = requestNode(former_primary, "v1 SET " + key + " - migrated-value");
    const auto& former_replica = former_primary.id == "join-a" ? original.nodes[1] : original.nodes[0];
    std::string initial_replica;
    const auto initial_replica_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        initial_replica = requestNode(former_replica, "v1 GET " + key);
        if (initial_replica == "OK VALUE migrated-value") break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < initial_replica_deadline);
    std::string ttl_key;
    for (int i = 0; i < 10000; ++i) {
        const auto candidate = key + "-ttl-" + std::to_string(i);
        if (new_ring.owner(candidate).id == "join-c") { ttl_key = candidate; break; }
    }
    ASSERT_FALSE(ttl_key.empty());
    const auto ttl_write_time = std::chrono::steady_clock::now();
    const std::string ttl_write = requestNode(former_primary, "v1 SET " + ttl_key + " 2200 short-lived");

    std::thread joining_thread([&] { joining.start(); });
    std::string joined_status;
    const auto join_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
    do {
        joined_status = requestNode(original.nodes[0], "v1 STATUS");
        if (joined_status.find("join-c=ALIVE") != std::string::npos) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < join_deadline);
    std::string migrated_value;
    std::string migrated_ttl_value;
    const auto migrate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
    do {
        migrated_value = requestNode(expanded.nodes[2], "v1 GET " + key);
        migrated_ttl_value = requestNode(expanded.nodes[2], "v1 GET " + ttl_key);
        if (migrated_value == "OK VALUE migrated-value" && migrated_ttl_value == "OK VALUE short-lived") break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < migrate_deadline);

    const std::string remove_response = requestNode(expanded.nodes[2], "v1 REMOVE join-c");
    std::string removed_status;
    const auto remove_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    do {
        removed_status = requestNode(original.nodes[0], "v1 STATUS");
        if (removed_status.find("join-c=") == std::string::npos) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < remove_deadline);
    std::vector<std::string> recovery_statuses;
    const auto repair_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    do {
        recovery_statuses = {requestNode(original.nodes[0], "v1 STATUS"),
                             requestNode(original.nodes[1], "v1 STATUS"),
                             requestNode(expanded.nodes[2], "v1 STATUS")};
        const bool complete = std::all_of(recovery_statuses.begin(), recovery_statuses.end(), [](const std::string& status) {
            return status.find("RECOVERY_PENDING=0") != std::string::npos &&
                   status.find("RECOVERY_FAILURES=0") != std::string::npos;
        });
        if (complete) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < repair_deadline);
    const std::string after_removal = requestNode(former_primary, "v1 GET " + key);
    std::this_thread::sleep_until(ttl_write_time + std::chrono::milliseconds(2300));
    const std::string expired_after_migration = requestNode(expanded.nodes[2], "v1 GET " + ttl_key);

    first.stop(); second.stop(); joining.stop();
    if (first_thread.joinable()) first_thread.join();
    if (second_thread.joinable()) second_thread.join();
    if (joining_thread.joinable()) joining_thread.join();

    EXPECT_EQ(initial_write, "OK STORED");
    EXPECT_EQ(initial_replica, "OK VALUE migrated-value");
    EXPECT_EQ(ttl_write, "OK STORED");
    EXPECT_NE(joined_status.find("join-c=ALIVE"), std::string::npos);
    EXPECT_EQ(migrated_value, "OK VALUE migrated-value");
    EXPECT_EQ(migrated_ttl_value, "OK VALUE short-lived");
    EXPECT_EQ(expired_after_migration, "OK NOT_FOUND");
    EXPECT_EQ(remove_response, "OK MEMBER REMOVED");
    EXPECT_EQ(removed_status.find("join-c="), std::string::npos);
    ASSERT_EQ(recovery_statuses.size(), 3u);
    for (const auto& status : recovery_statuses) {
        EXPECT_NE(status.find("RECOVERY_PENDING=0"), std::string::npos);
        EXPECT_NE(status.find("RECOVERY_FAILURES=0"), std::string::npos);
    }
    EXPECT_EQ(after_removal, "OK VALUE migrated-value");
}
