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
    std::size_t replication_factor{2};
    std::size_t heartbeat_interval_ms{500};
    std::size_t suspect_after_missed_heartbeats{2};
    std::size_t fail_after_missed_heartbeats{4};
    std::vector<CacheNode> nodes;

    static ClusterConfig load(const std::string& path);
    const CacheNode& nodeById(const std::string& id) const;
};

}  // namespace cache
