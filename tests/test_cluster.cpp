#include <gtest/gtest.h>

#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <utility>
#include <unistd.h>
#include <vector>

#include "cache/cluster_config.hpp"
#include "cache/consistent_hash_ring.hpp"

using namespace cache;

namespace {
CacheNode node(std::string id, std::uint16_t port) {
    return CacheNode{std::move(id), "127.0.0.1", port};
}
std::string ownerId(const ConsistentHashRing& ring, const std::string& key) {
    return ring.owner(key).id;
}
}

TEST(ConsistentHashRingTest, StableHashAndRoutingAreDeterministic) {
    const ClusterConfig first{96, {node("cache-a", 7401), node("cache-b", 7402), node("cache-c", 7403)}};
    const ClusterConfig different_order{96, {node("cache-c", 7403), node("cache-a", 7401), node("cache-b", 7402)}};
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

TEST(ClusterConfigTest, LoadsVirtualNodesAndNodeMembership) {
    const auto path = std::filesystem::temp_directory_path() /
        ("self-healing-cache-cluster-test-" + std::to_string(::getpid()) + ".conf");
    {
        std::ofstream output(path);
        ASSERT_TRUE(output);
        output << "# static membership\n"
                  "vnodes 24\n"
                  "node cache-a 127.0.0.1 7401\n"
                  "node cache-b 127.0.0.1 7402\n";
    }
    const auto config = ClusterConfig::load(path.string());
    EXPECT_EQ(config.virtual_nodes, 24u);
    EXPECT_EQ(config.nodes.size(), 2u);
    EXPECT_EQ(config.nodeById("cache-b").port, 7402);
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
