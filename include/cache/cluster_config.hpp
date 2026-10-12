#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace cache {

struct CacheNode {
    std::string id;
    std::string host;
    std::uint16_t port{};
};

struct ClusterConfig {
    std::size_t virtual_nodes{128};
    std::vector<CacheNode> nodes;

    static ClusterConfig load(const std::string& path);
    const CacheNode& nodeById(const std::string& id) const;
};

}  // namespace cache
