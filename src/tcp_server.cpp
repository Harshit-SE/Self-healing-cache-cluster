#include "cache/tcp_server.hpp"

#include "cache/logger.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <string>
#include <utility>

namespace cache {
namespace {
constexpr std::size_t kMaximumReplicationQueue = 10000;

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

bool rpc(const std::string& host, std::uint16_t port, const std::string& command,
         std::string& response) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* results = nullptr;
    const auto service = std::to_string(port);
    if (::getaddrinfo(host.c_str(), service.c_str(), &hints, &results) != 0) return false;
    int fd = -1;
    for (auto* candidate = results; candidate; candidate = candidate->ai_next) {
        fd = ::socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
        if (fd < 0) continue;
        if (connectWithTimeout(fd, candidate->ai_addr, candidate->ai_addrlen, 250)) break;
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(results);
    if (fd < 0) return false;
    timeval timeout{0, 300000};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    if (!sendAll(fd, command + "\n")) { ::close(fd); return false; }
    char buffer[4096];
    while (response.find('\n') == std::string::npos && response.size() <= 4096) {
        const auto n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { ::close(fd); return false; }
        response.append(buffer, static_cast<std::size_t>(n));
    }
    ::close(fd);
    if (const auto newline = response.find('\n'); newline != std::string::npos) response.resize(newline);
    return response.rfind("OK ", 0) == 0;
}

}  // namespace

TcpServer::TcpServer(std::uint16_t port, std::size_t max_line_bytes)
    : TcpServer(port, "standalone", 1, max_line_bytes) {}

TcpServer::TcpServer(std::uint16_t port, std::string node_id, std::size_t cluster_size,
                     std::size_t max_line_bytes)
    : requested_port_(port), node_id_(std::move(node_id)), cluster_size_(cluster_size),
      max_line_bytes_(max_line_bytes) {}

TcpServer::TcpServer(const ClusterConfig& config, const std::string& node_id,
                     std::size_t max_line_bytes)
    : TcpServer(config.nodeById(node_id).port, node_id, config.nodes.size(), max_line_bytes) {
    cluster_config_ = config;
    ring_.emplace(config);
    for (const auto& node : config.nodes) {
        member_states_[node.id] = "ALIVE";
        missed_heartbeats_[node.id] = 0;
    }
}

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
    LOG_INFO("cache node " << node_id_ << " (membership size " << cluster_size_
             << ") listening on port " << bound_port_);
    if (cluster_config_) {
        { std::lock_guard lock(replication_mutex_); replication_stopping_ = false; }
        replication_thread_ = std::thread(&TcpServer::replicationLoop, this);
        heartbeat_thread_ = std::thread(&TcpServer::heartbeatLoop, this);
    }
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
    stopping_.store(true);
    heartbeat_wait_cv_.notify_all();
    listen_fd_.store(-1);
    ::close(fd);
    for (auto& worker : workers_) if (worker.joinable()) worker.join();
    workers_.clear();
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
    if (replication_thread_.joinable()) {
        std::size_t discarded = 0;
        {
            std::lock_guard lock(replication_mutex_);
            replication_stopping_ = true;
            discarded = replication_queue_.size();
            replication_queue_.clear();
        }
        if (discarded != 0) {
            replication_failures_ += discarded;
            LOG_WARN("shutdown discarded " << discarded << " queued replica updates");
        }
        replication_cv_.notify_all();
        replication_thread_.join();
    }
    return true;
}

void TcpServer::stop() {
    stopping_.store(true);
    heartbeat_wait_cv_.notify_all();
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
            else if (request->operation == Request::Operation::Ping) {
                response = "OK PONG " + node_id_ + "\n";
            } else if (request->operation == Request::Operation::Status) {
                response = membershipStatus() + "\n";
            } else if (request->operation == Request::Operation::MemberUpdate) {
                const bool known = !cluster_config_ || std::any_of(cluster_config_->nodes.begin(), cluster_config_->nodes.end(),
                    [&](const CacheNode& node) { return node.id == request->key; });
                if (!known) response = "ERR unknown cluster member\n";
                else { applyMembershipState(request->key, request->value, false); response = "OK MEMBER UPDATED\n"; }
            } else if (request->operation == Request::Operation::ReplicaSet) {
                std::optional<std::chrono::milliseconds> ttl;
                if (request->ttl_ms) ttl = std::chrono::milliseconds(*request->ttl_ms);
                cache_.set(request->key, request->value, ttl);
                response = "OK REPLICA STORED\n";
            } else if (request->operation == Request::Operation::ReplicaDelete) {
                cache_.remove(request->key);
                response = "OK REPLICA DELETED\n";
            } else if (request->operation == Request::Operation::Get) {
                auto value = cache_.get(request->key);
                response = value ? "OK VALUE " + *value + "\n" : "OK NOT_FOUND\n";
            } else if (request->operation == Request::Operation::Set || request->operation == Request::Operation::Delete) {
                const bool is_primary = !ring_ || ring_->owner(request->key).id == node_id_;
                if (!is_primary) {
                    response = "ERR NOT_PRIMARY " + ring_->owner(request->key).id + "\n";
                } else if (request->operation == Request::Operation::Set) {
                    std::optional<std::chrono::milliseconds> ttl;
                    if (request->ttl_ms) {
                        if (*request->ttl_ms > std::chrono::milliseconds::max().count()) response = "ERR TTL out of range\n";
                        else ttl = std::chrono::milliseconds(*request->ttl_ms);
                    }
                    if (response.empty()) {
                        std::lock_guard mutation_lock(mutation_mutex_);
                        cache_.set(request->key, request->value, ttl);
                        enqueueReplicas(*request);
                        response = "OK STORED\n";
                    }
                } else {
                    std::lock_guard mutation_lock(mutation_mutex_);
                    const bool removed = cache_.remove(request->key);
                    enqueueReplicas(*request);
                    response = removed ? "OK DELETED\n" : "OK MISSING\n";
                }
            }
            if (!sendAll(fd, response)) { connected = false; break; }
        }
        if (pending.size() > max_line_bytes_) { sendAll(fd, "ERR request too large\n"); break; }
    }
    { std::lock_guard lock(clients_mutex_);
      clients_.erase(std::remove(clients_.begin(), clients_.end(), fd), clients_.end()); }
    ::close(fd);
}

void TcpServer::enqueueReplicas(const Request& request) {
    if (!cluster_config_ || !ring_) return;
    const auto copies = ring_->owners(request.key, cluster_config_->replication_factor);
    const auto now = std::chrono::steady_clock::now();
    for (const auto& destination : copies) {
        if (destination.id == node_id_) continue;
        ReplicationTask task{destination,
            request.operation == Request::Operation::Set ? Request::Operation::ReplicaSet : Request::Operation::ReplicaDelete,
            request.key, request.value, request.ttl_ms ? std::optional(std::chrono::milliseconds(*request.ttl_ms)) : std::nullopt,
            now};
        bool queued = false;
        {
            std::lock_guard lock(replication_mutex_);
            if (replication_queue_.size() < kMaximumReplicationQueue) {
                replication_queue_.push_back(std::move(task));
                queued = true;
            }
        }
        if (queued) replication_cv_.notify_one();
        else {
            ++replication_failures_;
            LOG_ERROR("replication queue full; replica update was dropped");
        }
    }
}

void TcpServer::replicationLoop() {
    for (;;) {
        ReplicationTask task;
        {
            std::unique_lock lock(replication_mutex_);
            replication_cv_.wait(lock, [&] { return replication_stopping_ || !replication_queue_.empty(); });
            if (replication_queue_.empty() && replication_stopping_) return;
            task = std::move(replication_queue_.front());
            replication_queue_.pop_front();
        }
        std::string command;
        if (task.operation == Request::Operation::ReplicaDelete) {
            command = "v1 REPLICA_DELETE " + task.key;
        } else {
            auto ttl = task.ttl;
            if (ttl) {
                const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - task.queued_at);
                *ttl -= elapsed;
                if (*ttl <= std::chrono::milliseconds::zero()) {
                    command = "v1 REPLICA_DELETE " + task.key;
                }
            }
            if (command.empty()) {
                command = "v1 REPLICA_SET " + task.key + " " +
                    (ttl ? std::to_string(ttl->count()) : "-") + " " + task.value;
            }
        }
        bool success = false;
        std::string response;
        for (int attempt = 0; attempt < 2 && !success; ++attempt) {
            response.clear();
            success = rpc(task.destination.host, task.destination.port, command, response);
            if (!success && attempt == 0) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!success) {
            ++replication_failures_;
            LOG_ERROR("replication to node " << task.destination.id << " failed after retry: " << response);
        }
    }
}

void TcpServer::heartbeatLoop() {
    if (!cluster_config_) return;
    const auto interval = std::chrono::milliseconds(cluster_config_->heartbeat_interval_ms);
    while (!stopping_.load()) {
        for (const auto& peer : cluster_config_->nodes) {
            if (peer.id == node_id_ || stopping_.load()) continue;
            std::string response;
            const bool healthy = rpc(peer.host, peer.port, "v1 PING", response) &&
                                 response == "OK PONG " + peer.id;
            std::string updated_state;
            {
                std::lock_guard lock(membership_mutex_);
                auto& misses = missed_heartbeats_[peer.id];
                if (healthy) {
                    misses = 0;
                    updated_state = "ALIVE";
                } else {
                    ++misses;
                    if (misses >= cluster_config_->fail_after_missed_heartbeats) updated_state = "FAILED";
                    else if (misses >= cluster_config_->suspect_after_missed_heartbeats) updated_state = "SUSPECTED";
                }
            }
            if (!updated_state.empty()) applyMembershipState(peer.id, updated_state, true);
        }
        std::unique_lock wait_lock(heartbeat_wait_mutex_);
        heartbeat_wait_cv_.wait_for(wait_lock, interval, [&] { return stopping_.load(); });
    }
}

void TcpServer::applyMembershipState(const std::string& node_id, const std::string& state,
                                     bool propagate) {
    bool changed = false;
    {
        std::lock_guard lock(membership_mutex_);
        const auto found = member_states_.find(node_id);
        if (found == member_states_.end()) return;
        if (found->second != state) { found->second = state; changed = true; }
    }
    if (!changed) return;
    if (state == "ALIVE") LOG_INFO("membership: node " << node_id << " is ALIVE");
    else LOG_WARN("membership: node " << node_id << " is " << state);
    if (!propagate || !cluster_config_) return;
    for (const auto& peer : cluster_config_->nodes) {
        if (peer.id == node_id || peer.id == node_id_) continue;
        std::string response;
        if (!rpc(peer.host, peer.port, "v1 MEMBER " + node_id + " " + state, response))
            LOG_WARN("could not propagate membership state for " << node_id << " to " << peer.id);
    }
}

std::string TcpServer::membershipStatus() const {
    std::lock_guard lock(membership_mutex_);
    std::string response = "OK MEMBERS";
    for (const auto& [id, state] : member_states_) response += " " + id + "=" + state;
    response += " REPLICATION_FAILURES=" + std::to_string(replication_failures_.load());
    return response;
}

}  // namespace cache
