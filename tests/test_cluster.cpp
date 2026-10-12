#include <gtest/gtest.h>

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
    replica_server.stop();
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
}
