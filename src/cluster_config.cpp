#include "cache/cluster_config.hpp"

#include <charconv>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

namespace cache {

ClusterConfig ClusterConfig::load(const std::string& path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("cannot open cluster config: " + path);

    ClusterConfig config;
    bool saw_vnodes = false;
    std::unordered_set<std::string> ids;
    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        if (const auto comment = line.find('#'); comment != std::string::npos) line.erase(comment);
        std::istringstream row(line);
        std::string kind;
        if (!(row >> kind)) continue;
        if (kind == "vnodes") {
            std::string value, extra;
            if (saw_vnodes || !(row >> value) || (row >> extra))
                throw std::runtime_error("invalid vnodes setting on line " + std::to_string(line_number));
            std::size_t parsed = 0;
            const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
            if (error != std::errc{} || end != value.data() + value.size() || parsed == 0 || parsed > 100000)
                throw std::runtime_error("vnodes must be an integer from 1 to 100000 on line " + std::to_string(line_number));
            config.virtual_nodes = parsed;
            saw_vnodes = true;
        } else if (kind == "node") {
            CacheNode node;
            unsigned int port = 0;
            std::string port_text, extra;
            if (!(row >> node.id >> node.host >> port_text) || (row >> extra))
                throw std::runtime_error("expected: node <id> <host> <port> on line " + std::to_string(line_number));
            const auto [end, error] = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
            if (error != std::errc{} || end != port_text.data() + port_text.size() || port == 0 || port > 65535)
                throw std::runtime_error("node port must be an integer from 1 to 65535 on line " + std::to_string(line_number));
            if (node.id.empty() || node.host.empty() || !ids.insert(node.id).second)
                throw std::runtime_error("node id and host must be non-empty and node ids unique on line " + std::to_string(line_number));
            node.port = static_cast<std::uint16_t>(port);
            config.nodes.push_back(std::move(node));
        } else {
            throw std::runtime_error("unknown config directive on line " + std::to_string(line_number) + ": " + kind);
        }
    }
    if (config.nodes.empty()) throw std::runtime_error("cluster config must contain at least one node");
    return config;
}

const CacheNode& ClusterConfig::nodeById(const std::string& id) const {
    for (const auto& node : nodes) if (node.id == id) return node;
    throw std::runtime_error("node id is not present in cluster config: " + id);
}

}  // namespace cache
