#include "cache/cluster_config.hpp"

#include <charconv>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <set>
#include <unordered_set>

namespace cache {

ClusterConfig ClusterConfig::load(const std::string& path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("cannot open cluster config: " + path);

    ClusterConfig config;
    std::set<std::string> seen_settings;
    std::unordered_set<std::string> ids;
    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        if (const auto comment = line.find('#'); comment != std::string::npos) line.erase(comment);
        std::istringstream row(line);
        std::string kind;
        if (!(row >> kind)) continue;
        if (kind == "vnodes" || kind == "replicas" || kind == "heartbeat_ms" ||
            kind == "suspect_after" || kind == "fail_after") {
            std::string value, extra;
            if (!seen_settings.insert(kind).second || !(row >> value) || (row >> extra))
                throw std::runtime_error("invalid or duplicate " + kind + " setting on line " + std::to_string(line_number));
            std::size_t parsed = 0;
            const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
            std::size_t minimum = 1, maximum = 100000;
            if (kind == "replicas") maximum = 1000;
            else if (kind == "heartbeat_ms") { minimum = 50; maximum = 60000; }
            else if (kind == "suspect_after") maximum = 100;
            else if (kind == "fail_after") { minimum = 2; maximum = 100; }
            if (error != std::errc{} || end != value.data() + value.size() || parsed < minimum || parsed > maximum)
                throw std::runtime_error(kind + " must be an integer from " + std::to_string(minimum) +
                                         " to " + std::to_string(maximum) + " on line " + std::to_string(line_number));
            if (kind == "vnodes") config.virtual_nodes = parsed;
            else if (kind == "replicas") config.replication_factor = parsed;
            else if (kind == "heartbeat_ms") config.heartbeat_interval_ms = parsed;
            else if (kind == "suspect_after") config.suspect_after_missed_heartbeats = parsed;
            else config.fail_after_missed_heartbeats = parsed;
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
    if (config.fail_after_missed_heartbeats <= config.suspect_after_missed_heartbeats)
        throw std::runtime_error("fail_after must be greater than suspect_after");
    return config;
}

const CacheNode& ClusterConfig::nodeById(const std::string& id) const {
    for (const auto& node : nodes) if (node.id == id) return node;
    throw std::runtime_error("node id is not present in cluster config: " + id);
}

}  // namespace cache
