#include "cache/cluster_config.hpp"
#include "cache/consistent_hash_ring.hpp"
#include "cache/protocol.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>

namespace {
int sendRequest(const std::string& host, const std::string& port, const std::string& request) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* results = nullptr;
    const int lookup = ::getaddrinfo(host.c_str(), port.c_str(), &hints, &results);
    if (lookup != 0) { std::cerr << "address lookup failed: " << gai_strerror(lookup) << '\n'; return 1; }
    int fd = -1;
    for (auto* item = results; item; item = item->ai_next) {
        fd = ::socket(item->ai_family, item->ai_socktype, item->ai_protocol);
        if (fd >= 0 && ::connect(fd, item->ai_addr, item->ai_addrlen) == 0) break;
        if (fd >= 0) ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(results);
    if (fd < 0) { std::cerr << "could not connect to " << host << ':' << port << '\n'; return 1; }
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
    std::cout << response.substr(0, response.find('\n')) << '\n';
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
            const auto& node = ring.owner(request->key);
            std::cerr << "routed key to node " << node.id << " (" << node.host << ':' << node.port << ")\n";
            return sendRequest(node.host, std::to_string(node.port), request_line);
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
