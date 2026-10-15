#include "cache/logger.hpp"
#include "cache/tcp_server.hpp"
#include "cache/cluster_config.hpp"

#include <charconv>
#include <cstdlib>
#include <exception>
#include <optional>
#include <string>

namespace {
bool parsePort(const char* text, std::uint16_t& port) {
    unsigned int parsed = 0;
    const std::string value(text);
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size() || parsed > 65535) return false;
    port = static_cast<std::uint16_t>(parsed);
    return true;
}
}

int main(int argc, char** argv) {
    if (argc == 2) {
        std::uint16_t port = 0;
        if (!parsePort(argv[1], port)) { LOG_ERROR("port must be an integer from 0 to 65535"); return 2; }
        cache::TcpServer server(port);
        return server.start() ? 0 : 1;
    }
    if (argc == 4 && std::string(argv[2]) == "--metrics-port") {
        std::uint16_t port = 0, metrics_port = 0;
        if (!parsePort(argv[1], port) || !parsePort(argv[3], metrics_port)) {
            LOG_ERROR("cache and metrics ports must be integers from 0 to 65535"); return 2;
        }
        cache::TcpServer server(port, 64 * 1024, metrics_port);
        return server.start() ? 0 : 1;
    }
    if ((argc == 5 || argc == 7) && std::string(argv[1]) == "--config" &&
        std::string(argv[3]) == "--id" && (argc == 5 || std::string(argv[5]) == "--metrics-port")) {
        try {
            const auto config = cache::ClusterConfig::load(argv[2]);
            (void)config.nodeById(argv[4]);
            std::optional<std::uint16_t> metrics_port;
            if (argc == 7) {
                std::uint16_t parsed = 0;
                if (!parsePort(argv[6], parsed)) { LOG_ERROR("metrics port must be an integer from 0 to 65535"); return 2; }
                metrics_port = parsed;
            }
            cache::TcpServer server(config, argv[4], 64 * 1024, metrics_port);
            return server.start() ? 0 : 1;
        } catch (const std::exception& error) {
            LOG_ERROR(error.what());
            return 2;
        }
    }
    LOG_ERROR("usage: cache_server <port> | cache_server --config <file> --id <node-id> [--metrics-port <port>]");
    return 2;
}
