#include "cache/consistent_hash_ring.hpp"

#include <algorithm>
#include <stdexcept>

namespace cache {

ConsistentHashRing::ConsistentHashRing(std::size_t virtual_nodes) : virtual_nodes_(virtual_nodes) {
    if (virtual_nodes_ == 0) throw std::invalid_argument("virtual node count must be greater than zero");
}

ConsistentHashRing::ConsistentHashRing(const ClusterConfig& config) : ConsistentHashRing(config.virtual_nodes) {
    for (const auto& node : config.nodes) addNode(node);
}

std::uint64_t ConsistentHashRing::stableHash(const std::string& value) noexcept {
    std::uint64_t hash = 14695981039346656037ULL;
    for (const unsigned char byte : value) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

void ConsistentHashRing::addNode(const CacheNode& node) {
    if (node.id.empty() || node.host.empty() || node.port == 0)
        throw std::invalid_argument("node id, host, and non-zero port are required");
    if (nodes_.contains(node.id)) throw std::invalid_argument("duplicate node id: " + node.id);
    nodes_.emplace(node.id, node);
    for (std::size_t vnode = 0; vnode < virtual_nodes_; ++vnode) {
        auto point_id = node.id;
        point_id.push_back('\0');
        point_id += std::to_string(vnode);
        points_.emplace(std::make_pair(stableHash(point_id), point_id), node.id);
    }
}

bool ConsistentHashRing::removeNode(const std::string& id) {
    if (nodes_.erase(id) == 0) return false;
    for (auto point = points_.begin(); point != points_.end();) {
        if (point->second == id) point = points_.erase(point);
        else ++point;
    }
    return true;
}

const CacheNode& ConsistentHashRing::owner(const std::string& key) const {
    if (nodes_.empty()) throw std::logic_error("cannot route with an empty consistent-hash ring");
    const auto key_hash = stableHash(key);
    auto point = points_.lower_bound({key_hash, std::string{}});
    if (point == points_.end()) point = points_.begin();
    return nodes_.at(point->second);
}

std::vector<CacheNode> ConsistentHashRing::owners(const std::string& key, std::size_t copies) const {
    std::vector<CacheNode> result;
    if (nodes_.empty() || copies == 0) return result;
    const auto desired = std::min(copies, nodes_.size());
    auto point = points_.lower_bound({stableHash(key), std::string{}});
    if (point == points_.end()) point = points_.begin();
    const auto start = point;
    do {
        const auto& id = point->second;
        if (result.empty() || result.back().id != id) {
            // A node may have multiple consecutive virtual points, and the
            // same owner can also recur after another node, so check all seen IDs.
            const bool already_added = std::any_of(result.begin(), result.end(),
                [&](const CacheNode& node) { return node.id == id; });
            if (!already_added) result.push_back(nodes_.at(id));
        }
        if (result.size() == desired) break;
        ++point;
        if (point == points_.end()) point = points_.begin();
    } while (point != start);
    return result;
}

std::vector<std::string> ConsistentHashRing::nodeIds() const {
    std::vector<std::string> ids;
    ids.reserve(nodes_.size());
    for (const auto& [id, node] : nodes_) { (void)node; ids.push_back(id); }
    return ids;
}

}  // namespace cache
