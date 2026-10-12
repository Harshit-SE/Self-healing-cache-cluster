#include "cache/tcp_server.hpp"

#include "cache/logger.hpp"
#include "cache/protocol.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <algorithm>
#include <limits>
#include <string>

namespace cache {
namespace {
bool sendAll(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const auto n = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        sent += static_cast<std::size_t>(n);
    }
    return true;
}
}

TcpServer::TcpServer(std::uint16_t port, std::size_t max_line_bytes)
    : requested_port_(port), max_line_bytes_(max_line_bytes) {}

TcpServer::~TcpServer() { stop(); }

bool TcpServer::start() {
    if (listen_fd_.load() >= 0) return false;
    stopping_.store(false);
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { LOG_ERROR("socket failed: " << std::strerror(errno)); return false; }
    int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(requested_port_);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 || ::listen(fd, 128) < 0) {
        LOG_ERROR("bind/listen failed: " << std::strerror(errno)); ::close(fd); return false;
    }
    socklen_t length = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) == 0) bound_port_ = ntohs(address.sin_port);
    listen_fd_.store(fd);
    LOG_INFO("cache server listening on port " << bound_port_);
    while (!stopping_.load()) {
        pollfd ready{fd, POLLIN, 0};
        const int polled = ::poll(&ready, 1, 200);
        if (polled < 0 && errno == EINTR) continue;
        if (polled == 0) continue;
        if (polled < 0 || (ready.revents & (POLLERR | POLLHUP | POLLNVAL))) {
            if (!stopping_.load()) LOG_ERROR("listener poll failed: " << std::strerror(errno));
            break;
        }
        sockaddr_in peer{};
        socklen_t peer_length = sizeof(peer);
        const int client = ::accept(fd, reinterpret_cast<sockaddr*>(&peer), &peer_length);
        if (client < 0) {
            if (errno == EINTR) continue;
            if (!stopping_.load()) LOG_ERROR("accept failed: " << std::strerror(errno));
            break;
        }
        { std::lock_guard lock(clients_mutex_); clients_.push_back(client); }
        workers_.emplace_back(&TcpServer::serveClient, this, client);
    }
    listen_fd_.store(-1);
    ::close(fd);
    for (auto& worker : workers_) if (worker.joinable()) worker.join();
    workers_.clear();
    return true;
}

void TcpServer::stop() {
    stopping_.store(true);
    const int fd = listen_fd_.load();
    (void)fd;  // start() observes stopping_ through a bounded listener poll.
    { std::lock_guard lock(clients_mutex_); for (int client : clients_) ::shutdown(client, SHUT_RDWR); }
}

void TcpServer::serveClient(int fd) {
    timeval timeout{5, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    std::string pending;
    char buffer[4096];
    bool connected = true;
    while (connected && !stopping_.load()) {
        const auto n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        pending.append(buffer, static_cast<std::size_t>(n));
        if (pending.size() > max_line_bytes_ && pending.find('\n') == std::string::npos) {
            sendAll(fd, "ERR request too large\n"); break;
        }
        std::size_t newline;
        while ((newline = pending.find('\n')) != std::string::npos) {
            if (newline > max_line_bytes_) { sendAll(fd, "ERR request too large\n"); connected = false; break; }
            std::string error;
            auto request = parseRequest(std::string_view(pending).substr(0, newline), error);
            pending.erase(0, newline + 1);
            std::string response;
            if (!request) response = "ERR " + error + "\n";
            else if (request->operation == Request::Operation::Get) {
                auto value = cache_.get(request->key);
                response = value ? "OK VALUE " + *value + "\n" : "OK NOT_FOUND\n";
            } else if (request->operation == Request::Operation::Set) {
                std::optional<std::chrono::milliseconds> ttl;
                if (request->ttl_ms) {
                    if (*request->ttl_ms > std::chrono::milliseconds::max().count()) response = "ERR TTL out of range\n";
                    else ttl = std::chrono::milliseconds(*request->ttl_ms);
                }
                if (response.empty()) { cache_.set(request->key, request->value, ttl); response = "OK STORED\n"; }
            } else response = cache_.remove(request->key) ? "OK DELETED\n" : "OK MISSING\n";
            if (!sendAll(fd, response)) { connected = false; break; }
        }
        if (pending.size() > max_line_bytes_) { sendAll(fd, "ERR request too large\n"); break; }
    }
    { std::lock_guard lock(clients_mutex_);
      clients_.erase(std::remove(clients_.begin(), clients_.end(), fd), clients_.end()); }
    ::close(fd);
}
}  // namespace cache
