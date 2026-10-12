#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "cache/cluster_config.hpp"

namespace cache {

// Deterministic FNV-1a based consistent-hashing ring. The hash is deliberately
// specified here instead of using std::hash, whose result is not portable
// between implementations or process runs.
class ConsistentHashRing {
public:
    explicit ConsistentHashRing(std::size_t virtual_nodes = 128);
    explicit ConsistentHashRing(const ClusterConfig& config);

    void addNode(const CacheNode& node);
    bool removeNode(const std::string& id);
    const CacheNode& owner(const std::string& key) const;
    // Returns distinct nodes in clockwise ring order, beginning with the primary.
    std::vector<CacheNode> owners(const std::string& key, std::size_t copies) const;
    std::size_t nodeCount() const noexcept { return nodes_.size(); }
    std::size_t virtualNodeCount() const noexcept { return virtual_nodes_; }
    std::vector<std::string> nodeIds() const;

    static std::uint64_t stableHash(const std::string& value) noexcept;

private:
    std::size_t virtual_nodes_;
    std::map<std::string, CacheNode> nodes_;
    // The point name provides deterministic ordering if two virtual points
    // happen to have the same 64-bit hash.
    std::map<std::pair<std::uint64_t, std::string>, std::string> points_;
};

}  // namespace cache
