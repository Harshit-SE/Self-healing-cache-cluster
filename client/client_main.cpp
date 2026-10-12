#include "cache/cluster_config.hpp"
#include "cache/consistent_hash_ring.hpp"
#include "cache/protocol.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <exception>
#include <iostream>
#include <sstream>
#include <string>

namespace {
bool connectWithTimeout(int fd, const sockaddr* address, socklen_t length, int timeout_ms) {
    const int old_flags = ::fcntl(fd, F_GETFL, 0);
    if (old_flags < 0 || ::fcntl(fd, F_SETFL, old_flags | O_NONBLOCK) < 0) return false;
    int result = ::connect(fd, address, length);
    if (result < 0 && errno == EINPROGRESS) {
        pollfd ready{fd, POLLOUT, 0};
        result = ::poll(&ready, 1, timeout_ms);
        if (result > 0) {
            int socket_error = 0;
            socklen_t error_length = sizeof(socket_error);
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_length) < 0 || socket_error != 0) {
                errno = socket_error == 0 ? errno : socket_error;
                result = -1;
            } else result = 0;
        } else if (result == 0) { errno = ETIMEDOUT; result = -1; }
    }
    ::fcntl(fd, F_SETFL, old_flags);
    return result == 0;
}

int sendRequest(const std::string& host, const std::string& port, const std::string& request,
                std::string* captured_response = nullptr) {
    if (captured_response) captured_response->clear();
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* results = nullptr;
    const int lookup = ::getaddrinfo(host.c_str(), port.c_str(), &hints, &results);
    if (lookup != 0) { std::cerr << "address lookup failed: " << gai_strerror(lookup) << '\n'; return 1; }
    int fd = -1;
    for (auto* item = results; item; item = item->ai_next) {
        fd = ::socket(item->ai_family, item->ai_socktype, item->ai_protocol);
        if (fd >= 0 && connectWithTimeout(fd, item->ai_addr, item->ai_addrlen, 500)) break;
        if (fd >= 0) ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(results);
    if (fd < 0) { std::cerr << "could not connect to " << host << ':' << port << '\n'; return 1; }
    timeval timeout{1, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    std::size_t sent = 0;
    while (sent < request.size()) {
        const auto n = ::send(fd, request.data() + sent, request.size() - sent, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { std::cerr << "send failed: " << std::strerror(errno) << '\n'; ::close(fd); return 1; }
        sent += static_cast<std::size_t>(n);
    }
    std::string response;
    char buffer[4096];
    while (response.find('\n') == std::string::npos) {
        const auto n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { std::cerr << "server closed connection before replying\n"; ::close(fd); return 1; }
        response.append(buffer, static_cast<std::size_t>(n));
        if (response.size() > 64 * 1024) { std::cerr << "response too large\n"; ::close(fd); return 1; }
    }
    ::close(fd);
    response.resize(response.find('\n'));
    if (captured_response) *captured_response = response;
    else std::cout << response << '\n';
    return response.rfind("ERR ", 0) == 0 ? 1 : 0;
}

std::string joinRequest(int argc, char** argv, int first) {
    std::string request;
    for (int i = first; i < argc; ++i) {
        if (i != first) request += ' ';
        request += argv[i];
    }
    request += '\n';
    return request;
}

bool reportsAlive(const std::string& status, const std::string& node_id) {
    std::istringstream fields(status);
    std::string field;
    while (fields >> field) if (field == node_id + "=ALIVE") return true;
    return false;
}
}

int main(int argc, char** argv) {
    if (argc >= 4 && std::string(argv[1]) == "--config") {
        try {
            const auto config = cache::ClusterConfig::load(argv[2]);
            const auto request_line = joinRequest(argc, argv, 3);
            std::string error;
            const auto request = cache::parseRequest(request_line.substr(0, request_line.size() - 1), error);
            if (!request) { std::cerr << "invalid request: " << error << '\n'; return 2; }
            const cache::ConsistentHashRing ring(config);
            const auto candidates = ring.owners(request->key, config.nodes.size());
            std::string last_response = "ERR no reachable owner";
            // Bound failover to one retry so request latency stays predictable.
            for (std::size_t i = 0; i < candidates.size() && i < 2; ++i) {
                const auto& node = candidates[i];
                std::string health;
                if (sendRequest(node.host, std::to_string(node.port), "v1 STATUS\n", &health) != 0) continue;
                if (!reportsAlive(health, node.id)) continue;
                std::cerr << "routed key to node " << node.id << " (" << node.host << ':' << node.port << ")\n";
                const auto result = sendRequest(node.host, std::to_string(node.port), request_line, &last_response);
                if (result != 0 && last_response.empty()) continue;
                if ((last_response.rfind("ERR NOT_PRIMARY ", 0) == 0 ||
                     last_response == "ERR NO_ACTIVE_OWNER") &&
                    i + 1 < candidates.size() && i + 1 < 2) continue;
                std::cout << last_response << '\n';
                return result;
            }
            std::cerr << last_response << '\n';
            return 1;
        } catch (const std::exception& error) {
            std::cerr << error.what() << '\n';
            return 2;
        }
    }
    if (argc < 4) {
        std::cerr << "usage: cache_client <host> <port> <v1 request...>\n"
                     "       cache_client --config <cluster-file> <v1 request...>\n"
                     "example: cache_client --config config/cluster.conf 'v1 SET greeting - hello'\n";
        return 2;
    }
    return sendRequest(argv[1], argv[2], joinRequest(argc, argv, 3));
}
