#include "cache/tcp_server.hpp"

#include "cache/logger.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fstream>
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

TcpServer::TcpServer(std::uint16_t port, std::size_t max_line_bytes,
                     std::optional<std::uint16_t> metrics_port)
    : TcpServer(port, "standalone", 1, max_line_bytes, metrics_port) {}

TcpServer::TcpServer(std::uint16_t port, std::string node_id, std::size_t cluster_size,
                     std::size_t max_line_bytes, std::optional<std::uint16_t> metrics_port)
    : requested_port_(port), requested_metrics_port_(metrics_port), node_id_(std::move(node_id)), cluster_size_(cluster_size),
      max_line_bytes_(max_line_bytes) {}

TcpServer::TcpServer(const ClusterConfig& config, const std::string& node_id,
                     std::size_t max_line_bytes, std::optional<std::uint16_t> metrics_port)
    : TcpServer(config.nodeById(node_id).port, node_id, config.nodes.size(), max_line_bytes, metrics_port) {
    cluster_config_ = config;
    ring_.emplace(config);
    for (const auto& node : config.nodes) {
        member_states_[node.id] = node.id == node_id_
            ? (config.nodes.size() == 1 ? "ALIVE" : "JOINING") : "SUSPECTED";
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
    if (requested_metrics_port_) metrics_thread_ = std::thread(&TcpServer::metricsLoop, this);
    if (cluster_config_) {
        if (cluster_config_->nodes.size() > 1) {
            std::lock_guard lock(membership_mutex_);
            // A stopped server may retain an ALIVE state and stale cache data
            // in this process. Every clustered restart must rejoin through the
            // peer recovery barrier before it can own requests or replicate.
            member_states_[node_id_] = "JOINING";
            sync_completed_.clear();
        }
        { std::lock_guard lock(replication_mutex_); replication_stopping_ = false; }
        replication_thread_ = std::thread(&TcpServer::replicationLoop, this);
        heartbeat_thread_ = std::thread(&TcpServer::heartbeatLoop, this);
        recovery_thread_ = std::thread(&TcpServer::recoveryLoop, this);
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
    if (metrics_thread_.joinable()) metrics_thread_.join();
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
    if (recovery_thread_.joinable()) recovery_thread_.join();
    if (replication_thread_.joinable()) {
        std::size_t discarded = 0;
        std::size_t discarded_recovery = 0;
        {
            std::lock_guard lock(replication_mutex_);
            replication_stopping_ = true;
            discarded = replication_queue_.size();
            discarded_recovery = static_cast<std::size_t>(std::count_if(
                replication_queue_.begin(), replication_queue_.end(),
                [](const ReplicationTask& task) { return task.recovery; }));
            replication_queue_.clear();
        }
        if (discarded_recovery != 0) recovery_tasks_pending_ -= discarded_recovery;
        if (discarded != 0) replication_tasks_pending_ -= discarded;
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

void TcpServer::metricsLoop() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { LOG_WARN("metrics endpoint socket failed: " << std::strerror(errno)); return; }
    int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(*requested_metrics_port_);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 || ::listen(fd, 32) < 0) {
        LOG_WARN("metrics endpoint could not bind port " << *requested_metrics_port_ << ": " << std::strerror(errno));
        ::close(fd);
        return;
    }
    socklen_t address_length = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_length) == 0)
        bound_metrics_port_.store(ntohs(address.sin_port));
    LOG_INFO("metrics endpoint for " << node_id_ << " listening on port " << bound_metrics_port_);
    while (!stopping_.load()) {
        pollfd ready{fd, POLLIN, 0};
        const int polled = ::poll(&ready, 1, 200);
        if (polled < 0) { if (errno == EINTR) continue; break; }
        if (polled == 0) continue;
        const int client = ::accept(fd, nullptr, nullptr);
        if (client < 0) { if (errno == EINTR) continue; break; }
        timeval timeout{1, 0};
        ::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        ::setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        std::string request;
        char buffer[1024];
        while (request.size() < 4096 && request.find("\r\n\r\n") == std::string::npos) {
            const auto n = ::recv(client, buffer, sizeof(buffer), 0);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) break;
            request.append(buffer, static_cast<std::size_t>(n));
        }
        const auto line_end = request.find("\r\n");
        const auto request_line = request.substr(0, line_end);
        std::string body;
        std::string content_type;
        int status_code = 200;
        if (request_line.rfind("GET /metrics ", 0) == 0) {
            const auto live_bytes = cache_.approximateMemoryBytes();
            std::size_t process_rss_bytes = 0;
            std::ifstream memory_stats("/proc/self/statm");
            std::size_t virtual_pages = 0, resident_pages = 0;
            if (memory_stats >> virtual_pages >> resident_pages) {
                const long page_size = ::sysconf(_SC_PAGESIZE);
                if (page_size > 0) process_rss_bytes = resident_pages * static_cast<std::size_t>(page_size);
            }
            std::uint64_t pending = 0;
            double lag_seconds = 0.0;
            {
                std::lock_guard lock(replication_mutex_);
                pending = replication_tasks_pending_.load();
                if (!replication_queue_.empty()) {
                    lag_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() -
                        replication_queue_.front().queued_at).count();
                }
            }
            std::vector<std::pair<std::string, std::string>> states;
            {
                std::lock_guard lock(membership_mutex_);
                states.assign(member_states_.begin(), member_states_.end());
            }
            body = metrics_.renderPrometheus(node_id_, cache_.size(), live_bytes, process_rss_bytes, pending,
                replication_failures_.load(), lag_seconds,
                recovery_tasks_pending_.load() + (recovery_generation_.load() > recovery_completed_.load()
                    ? recovery_generation_.load() - recovery_completed_.load() : 0),
                recovery_failures_.load(), states);
            content_type = "text/plain; version=0.0.4; charset=utf-8";
        } else if (request_line.rfind("GET /hotkeys ", 0) == 0) {
            body = metrics_.renderHotKeys(node_id_, metrics_.nodeRequestRate());
            content_type = "application/json";
        } else {
            status_code = 404;
            body = "not found\n";
            content_type = "text/plain; charset=utf-8";
        }
        const std::string reason = status_code == 200 ? "OK" : "Not Found";
        const auto response = "HTTP/1.1 " + std::to_string(status_code) + " " + reason + "\r\n" +
            "Content-Type: " + content_type + "\r\nContent-Length: " + std::to_string(body.size()) +
            "\r\nConnection: close\r\n\r\n" + body;
        sendAll(client, response);
        ::close(client);
    }
    ::close(fd);
    bound_metrics_port_.store(0);
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
            std::optional<bool> cache_hit;
            const bool measure = request && (request->operation == Request::Operation::Get ||
                request->operation == Request::Operation::Set || request->operation == Request::Operation::Delete);
            const auto request_started = std::chrono::steady_clock::now();
            if (!request) response = "ERR " + error + "\n";
            else if (request->operation == Request::Operation::Ping) {
                response = "OK PONG " + node_id_ + "\n";
            } else if (request->operation == Request::Operation::Status) {
                response = membershipStatus() + "\n";
            } else if (request->operation == Request::Operation::NodeJoin) {
                if (!isClustered() || !request->ttl_ms) {
                    response = "ERR JOIN requires cluster membership\n";
                } else {
                    CacheNode joining{request->key, request->value,
                        static_cast<std::uint16_t>(*request->ttl_ms)};
                    response = applyNodeJoin(joining, !request->internal_control) ?
                        "OK MEMBER JOINED\n" : "ERR invalid or conflicting member\n";
                }
            } else if (request->operation == Request::Operation::NodeRemove) {
                response = applyNodeRemove(request->key, !request->internal_control) ?
                    "OK MEMBER REMOVED\n" : "ERR unknown cluster member\n";
            } else if (request->operation == Request::Operation::SyncDone) {
                const auto nodes = configuredNodes();
                const bool known = std::any_of(nodes.begin(), nodes.end(),
                    [&](const CacheNode& node) { return node.id == request->key; });
                if (!known || request->key == node_id_) response = "ERR unknown sync source\n";
                else {
                    { std::lock_guard lock(membership_mutex_); sync_completed_.insert(request->key); }
                    response = "OK SYNC RECORDED\n";
                    maybeCompleteJoin();
                }
            } else if (request->operation == Request::Operation::MemberUpdate) {
                const auto nodes = configuredNodes();
                const bool known = !isClustered() || std::any_of(nodes.begin(), nodes.end(),
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
                cache_hit = value.has_value();
                response = value ? "OK VALUE " + *value + "\n" : "OK NOT_FOUND\n";
            } else if (request->operation == Request::Operation::Set || request->operation == Request::Operation::Delete) {
                const auto copies = activeOwners(request->key);
                const bool is_primary = !isClustered() || (!copies.empty() && copies.front().id == node_id_);
                if (!is_primary) {
                    response = copies.empty() ? "ERR NO_ACTIVE_OWNER\n" : "ERR NOT_PRIMARY " + copies.front().id + "\n";
                } else if (request->operation == Request::Operation::Set) {
                    std::optional<std::chrono::milliseconds> ttl;
                    if (request->ttl_ms) {
                        if (*request->ttl_ms > std::chrono::milliseconds::max().count()) response = "ERR TTL out of range\n";
                        else ttl = std::chrono::milliseconds(*request->ttl_ms);
                    }
                    if (response.empty()) {
                        std::lock_guard mutation_lock(mutation_mutex_);
                        const auto current_owners = activeOwners(request->key);
                        if (isClustered() && (current_owners.empty() || current_owners.front().id != node_id_)) {
                            response = current_owners.empty() ? "ERR NO_ACTIVE_OWNER\n" :
                                "ERR NOT_PRIMARY " + current_owners.front().id + "\n";
                        } else {
                            cache_.set(request->key, request->value, ttl);
                            enqueueReplicas(*request);
                            response = "OK STORED\n";
                        }
                    }
                } else {
                    std::lock_guard mutation_lock(mutation_mutex_);
                    const auto current_owners = activeOwners(request->key);
                    if (isClustered() && (current_owners.empty() || current_owners.front().id != node_id_)) {
                        response = current_owners.empty() ? "ERR NO_ACTIVE_OWNER\n" :
                            "ERR NOT_PRIMARY " + current_owners.front().id + "\n";
                    } else {
                        const bool removed = cache_.remove(request->key);
                        enqueueReplicas(*request);
                        response = removed ? "OK DELETED\n" : "OK MISSING\n";
                    }
                }
            }
            if (measure) {
                const char* operation = request->operation == Request::Operation::Get ? "get" :
                    (request->operation == Request::Operation::Set ? "set" : "delete");
                metrics_.observe(operation, request->key, cache_hit,
                    std::chrono::steady_clock::now() - request_started);
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
    const auto copies = replicationOwners(request.key);
    const auto now = std::chrono::steady_clock::now();
    for (const auto& destination : copies) {
        if (destination.id == node_id_) continue;
        bool joining = false;
        {
            std::lock_guard lock(membership_mutex_);
            const auto found = member_states_.find(destination.id);
            joining = found != member_states_.end() && found->second == "JOINING";
        }
        ReplicationTask task{destination,
            request.operation == Request::Operation::Set ? Request::Operation::ReplicaSet : Request::Operation::ReplicaDelete,
            request.key, request.value, request.ttl_ms ? std::optional(std::chrono::milliseconds(*request.ttl_ms)) : std::nullopt,
            now, joining};
        bool queued = false;
        {
            std::lock_guard lock(replication_mutex_);
            if (replication_queue_.size() < kMaximumReplicationQueue) {
                replication_queue_.push_back(std::move(task));
                ++replication_tasks_pending_;
                if (joining) ++recovery_tasks_pending_;
                queued = true;
            }
        }
        if (queued) replication_cv_.notify_one();
        else {
            ++replication_failures_;
            if (joining) ++recovery_failures_;
            LOG_ERROR("replication queue full; replica update was dropped");
        }
    }
}

std::vector<CacheNode> TcpServer::activeOwners(const std::string& key) const {
    std::vector<CacheNode> configured;
    std::size_t desired_copies = 0;
    {
        std::lock_guard topology_lock(topology_mutex_);
        if (!ring_ || !cluster_config_) return {};
        configured = ring_->owners(key, cluster_config_->nodes.size());
        desired_copies = cluster_config_->replication_factor;
    }
    std::vector<CacheNode> active;
    std::lock_guard lock(membership_mutex_);
    for (const auto& candidate : configured) {
        const auto state = member_states_.find(candidate.id);
        if (state != member_states_.end() && state->second == "ALIVE") active.push_back(candidate);
        if (active.size() >= desired_copies) break;
    }
    return active;
}

std::vector<CacheNode> TcpServer::replicationOwners(const std::string& key) const {
    std::vector<CacheNode> configured;
    std::size_t desired_copies = 0;
    {
        std::lock_guard topology_lock(topology_mutex_);
        if (!ring_ || !cluster_config_) return {};
        configured = ring_->owners(key, cluster_config_->nodes.size());
        desired_copies = cluster_config_->replication_factor;
    }
    std::vector<CacheNode> selected;
    std::lock_guard lock(membership_mutex_);
    for (const auto& candidate : configured) {
        const auto state = member_states_.find(candidate.id);
        if (state != member_states_.end() &&
            (state->second == "ALIVE" || state->second == "JOINING")) selected.push_back(candidate);
        if (selected.size() >= desired_copies) break;
    }
    return selected;
}

std::vector<CacheNode> TcpServer::configuredNodes() const {
    std::lock_guard topology_lock(topology_mutex_);
    return cluster_config_ ? cluster_config_->nodes : std::vector<CacheNode>{};
}

bool TcpServer::isClustered() const {
    std::lock_guard topology_lock(topology_mutex_);
    return cluster_config_.has_value();
}

ClusterConfig TcpServer::clusterConfigSnapshot() const {
    std::lock_guard topology_lock(topology_mutex_);
    return cluster_config_.value_or(ClusterConfig{});
}

bool TcpServer::applyNodeJoin(const CacheNode& node, bool propagate) {
    std::vector<CacheNode> peers;
    bool added = false;
    {
        std::lock_guard topology_lock(topology_mutex_);
        if (!cluster_config_ || node.id.empty() || node.host.empty() || node.port == 0) return false;
        const auto existing = std::find_if(cluster_config_->nodes.begin(), cluster_config_->nodes.end(),
            [&](const CacheNode& item) { return item.id == node.id; });
        if (existing != cluster_config_->nodes.end()) {
            if (existing->host != node.host || existing->port != node.port) return false;
            peers = cluster_config_->nodes;
        } else {
            peers = cluster_config_->nodes;
            cluster_config_->nodes.push_back(node);
            ring_.emplace(*cluster_config_);
            cluster_size_ = cluster_config_->nodes.size();
            added = true;
        }
    }
    bool state_changed = false;
    {
        std::lock_guard lock(membership_mutex_);
        auto& state = member_states_[node.id];
        state_changed = state != "JOINING" &&
            (propagate || state.empty() || state == "FAILED" || state == "SUSPECTED");
        if (state_changed) state = "JOINING";
        missed_heartbeats_[node.id] = 0;
        join_announced_[node.id] = true;
        if (state_changed) sync_completed_.erase(node.id);
    }
    if (added || state_changed) {
        ++recovery_generation_;
        heartbeat_wait_cv_.notify_all();
        LOG_INFO("membership: node " << node.id << " is joining at " << node.host << ':' << node.port);
    }
    if (propagate && added) {
        const auto command = "v1 JOIN_PEER " + node.id + " " + node.host + " " + std::to_string(node.port);
        for (const auto& peer : peers) {
            if (peer.id == node.id || peer.id == node_id_) continue;
            std::string response;
            if (!rpc(peer.host, peer.port, command, response))
                LOG_WARN("could not propagate join for " << node.id << " to " << peer.id);
        }
    } else if (propagate && state_changed) {
        for (const auto& peer : peers) {
            if (peer.id == node.id || peer.id == node_id_) continue;
            std::string response;
            if (!rpc(peer.host, peer.port, "v1 MEMBER " + node.id + " JOINING", response))
                LOG_WARN("could not propagate joining state for " << node.id << " to " << peer.id);
        }
    }
    return true;
}

bool TcpServer::applyNodeRemove(const std::string& node_id, bool propagate) {
    std::vector<CacheNode> peers;
    {
        std::lock_guard topology_lock(topology_mutex_);
        if (!cluster_config_ || cluster_config_->nodes.size() <= 1) return false;
        const auto existing = std::find_if(cluster_config_->nodes.begin(), cluster_config_->nodes.end(),
            [&](const CacheNode& item) { return item.id == node_id; });
        if (existing == cluster_config_->nodes.end()) return false;
        peers = cluster_config_->nodes;
        cluster_config_->nodes.erase(existing);
        ring_.emplace(*cluster_config_);
        cluster_size_ = cluster_config_->nodes.size();
    }
    {
        std::lock_guard lock(membership_mutex_);
        member_states_.erase(node_id);
        missed_heartbeats_.erase(node_id);
        join_announced_.erase(node_id);
        sync_completed_.erase(node_id);
    }
    ++recovery_generation_;
    heartbeat_wait_cv_.notify_all();
    LOG_INFO("membership: node " << node_id << " removed; ownership repair scheduled");
    if (propagate) {
        const auto command = "v1 REMOVE_PEER " + node_id;
        for (const auto& peer : peers) {
            if (peer.id == node_id_) continue;
            std::string response;
            if (!rpc(peer.host, peer.port, command, response))
                LOG_WARN("could not propagate removal for " << node_id << " to " << peer.id);
        }
    }
    maybeCompleteJoin();
    return true;
}

void TcpServer::maybeCompleteJoin() {
    const auto nodes = configuredNodes();
    bool ready = true;
    {
        std::lock_guard lock(membership_mutex_);
        const auto self = member_states_.find(node_id_);
        if (self == member_states_.end() || self->second != "JOINING") return;
        for (const auto& node : nodes) {
            if (node.id == node_id_) continue;
            const auto state = member_states_.find(node.id);
            if (state == member_states_.end() || state->second == "FAILED") continue;
            if (!sync_completed_.contains(node.id)) { ready = false; break; }
        }
    }
    if (ready) applyMembershipState(node_id_, "ALIVE", true);
}

void TcpServer::recoveryLoop() {
    std::size_t heartbeat_ms = 500;
    {
        std::lock_guard topology_lock(topology_mutex_);
        if (!cluster_config_) return;
        heartbeat_ms = cluster_config_->heartbeat_interval_ms;
    }
    const auto interval = std::chrono::milliseconds(std::max<std::size_t>(heartbeat_ms * 2, 100));
    std::uint64_t processed_generation = 0;
    while (!stopping_.load()) {
        {
            std::unique_lock wait_lock(heartbeat_wait_mutex_);
            heartbeat_wait_cv_.wait_for(wait_lock, interval, [&] {
                return stopping_.load() || recovery_generation_.load() != processed_generation ||
                       recovery_retry_needed_.load();
            });
        }
        if (stopping_.load()) break;
        const auto current_generation = recovery_generation_.load();
        const bool retrying = recovery_retry_needed_.exchange(false);
        if (current_generation == processed_generation && !retrying) continue;
        processed_generation = current_generation;
        const auto failures_before_pass = recovery_failures_.load();
        bool self_is_joining = false;
        {
            std::lock_guard lock(membership_mutex_);
            const auto self = member_states_.find(node_id_);
            self_is_joining = self != member_states_.end() && self->second == "JOINING";
        }
        {
            // Serialize the snapshot and queue order against foreground writes.
            std::lock_guard mutation_lock(mutation_mutex_);
            // Every live copy may repair a missing owner. TTL is measured from
            // the snapshot and reduced again by the ordinary replication queue.
            const auto entries = self_is_joining ? decltype(cache_.snapshot()){} : cache_.snapshot();
            for (auto& entry : entries) {
                if (stopping_.load()) break;
                const auto owners = replicationOwners(entry.key);
                for (const auto& destination : owners) {
                    if (destination.id == node_id_) continue;
                    // Send directly to each current owner so repair does not
                    // depend on the node that was primary before a failure.
                    ReplicationTask task{destination, Request::Operation::ReplicaSet,
                        entry.key, entry.value, entry.ttl, std::chrono::steady_clock::now(), true};
                    bool queued = false;
                    {
                        std::lock_guard lock(replication_mutex_);
                        if (replication_queue_.size() < kMaximumReplicationQueue) {
                            replication_queue_.push_back(std::move(task));
                            ++replication_tasks_pending_;
                            ++recovery_tasks_pending_;
                            queued = true;
                        }
                    }
                    if (queued) replication_cv_.notify_one();
                    else { ++recovery_failures_; recovery_retry_needed_.store(true); }
                }
            }
        }
        recovery_completed_.store(processed_generation);
        {
            std::unique_lock wait_lock(heartbeat_wait_mutex_);
            heartbeat_wait_cv_.wait(wait_lock, [&] {
                return stopping_.load() || replication_tasks_pending_.load() == 0;
            });
        }
        if (stopping_.load() || recovery_generation_.load() != processed_generation) continue;
        if (recovery_failures_.load() != failures_before_pass) {
            recovery_retry_needed_.store(true);
            continue;
        }
        for (const auto& peer : configuredNodes()) {
            bool joining = false;
            {
                std::lock_guard lock(membership_mutex_);
                const auto state = member_states_.find(peer.id);
                joining = state != member_states_.end() && state->second == "JOINING";
            }
            if (!joining || peer.id == node_id_) continue;
            std::string response;
            if (!rpc(peer.host, peer.port, "v1 SYNC_DONE " + node_id_, response))
                LOG_WARN("could not report recovery completion to joining node " << peer.id);
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
        if (task.recovery) {
            if (!success) { ++recovery_failures_; recovery_retry_needed_.store(true); }
            --recovery_tasks_pending_;
        }
        --replication_tasks_pending_;
        heartbeat_wait_cv_.notify_all();
    }
}

void TcpServer::heartbeatLoop() {
    if (!isClustered()) return;
    const auto interval = std::chrono::milliseconds(clusterConfigSnapshot().heartbeat_interval_ms);
    while (!stopping_.load()) {
        const auto peers = configuredNodes();
        const auto local_node = std::find_if(peers.begin(), peers.end(),
            [&](const CacheNode& peer) { return peer.id == node_id_; });
        for (const auto& peer : peers) {
            if (peer.id == node_id_ || stopping_.load()) continue;
            bool announce_join = false;
            if (local_node != peers.end()) {
                std::lock_guard lock(membership_mutex_);
                announce_join = !join_announced_[peer.id];
            }
            if (announce_join && local_node != peers.end()) {
                std::string joined;
                if (rpc(peer.host, peer.port, "v1 JOIN " + local_node->id + " " + local_node->host + " " +
                        std::to_string(local_node->port), joined) && joined.rfind("OK ", 0) == 0) {
                    std::lock_guard lock(membership_mutex_);
                    join_announced_[peer.id] = true;
                }
            }
            std::string response;
            const bool healthy = rpc(peer.host, peer.port, "v1 PING", response) &&
                                 response == "OK PONG " + peer.id;
            std::string updated_state;
            const auto config = clusterConfigSnapshot();
            {
                std::lock_guard lock(membership_mutex_);
                auto& misses = missed_heartbeats_[peer.id];
                if (healthy) {
                    misses = 0;
                    failure_start_times_.erase(peer.id);
                    const auto state = member_states_.find(peer.id);
                    if (state != member_states_.end() && state->second != "JOINING") {
                        // A peer that was unavailable must rejoin through a
                        // recovery pass before it can own client writes. The
                        // JOINING state makes healthy owners stream their live
                        // data back to this peer before SYNC_DONE promotes it.
                        updated_state = state->second == "FAILED" ? "JOINING" : "ALIVE";
                    }
                } else {
                    if (misses == 0) failure_start_times_[peer.id] = std::chrono::steady_clock::now();
                    ++misses;
                    if (misses >= config.fail_after_missed_heartbeats) updated_state = "FAILED";
                    else if (misses >= config.suspect_after_missed_heartbeats) updated_state = "SUSPECTED";
                }
            }
            if (!updated_state.empty()) {
                applyMembershipState(peer.id, updated_state, true);
                if (updated_state == "JOINING") {
                    std::string response;
                    if (!rpc(peer.host, peer.port, "v1 MEMBER " + peer.id + " JOINING", response))
                        LOG_WARN("could not start recovery on rejoining node " << peer.id);
                }
            }
        }
        std::unique_lock wait_lock(heartbeat_wait_mutex_);
        heartbeat_wait_cv_.wait_for(wait_lock, interval, [&] { return stopping_.load(); });
    }
}

void TcpServer::applyMembershipState(const std::string& node_id, const std::string& state,
                                     bool propagate) {
    std::unique_lock<std::mutex> promotion_lock(mutation_mutex_, std::defer_lock);
    if (state == "ALIVE") {
        bool joining = false;
        {
            std::lock_guard lock(membership_mutex_);
            const auto found = member_states_.find(node_id);
            joining = found != member_states_.end() && found->second == "JOINING";
        }
        if (joining) {
            promotion_lock.lock();
            std::unique_lock wait_lock(heartbeat_wait_mutex_);
            heartbeat_wait_cv_.wait(wait_lock, [&] {
                return stopping_.load() || replication_tasks_pending_.load() == 0;
            });
        }
    }
    bool changed = false;
    std::optional<std::chrono::nanoseconds> failure_duration;
    std::optional<std::chrono::nanoseconds> detection_duration;
    {
        std::lock_guard lock(membership_mutex_);
        const auto found = member_states_.find(node_id);
        if (found == member_states_.end()) return;
        if (found->second != state) {
            found->second = state;
            changed = true;
            const auto now = std::chrono::steady_clock::now();
            if (state == "SUSPECTED") {
                auto [started, inserted] = failure_start_times_.try_emplace(node_id, now);
                (void)inserted;
                detection_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now - started->second);
            } else if (state == "FAILED") {
                const auto started = failure_start_times_.find(node_id);
                if (started != failure_start_times_.end()) {
                    failure_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now - started->second);
                    failure_start_times_.erase(started);
                }
            } else if (state == "ALIVE" || state == "JOINING") {
                failure_start_times_.erase(node_id);
            }
        }
    }
    if (promotion_lock.owns_lock()) promotion_lock.unlock();
    if (!changed) return;
    if (detection_duration) metrics_.recordFailureDetectionDuration(*detection_duration);
    if (state == "FAILED") metrics_.recordFailureEvent(failure_duration);
    ++recovery_generation_;
    heartbeat_wait_cv_.notify_all();
    if (state == "ALIVE") LOG_INFO("membership: node " << node_id << " is ALIVE");
    else LOG_WARN("membership: node " << node_id << " is " << state);
    if (propagate && isClustered()) {
        for (const auto& peer : configuredNodes()) {
            if (peer.id == node_id || peer.id == node_id_) continue;
            std::string response;
            if (!rpc(peer.host, peer.port, "v1 MEMBER " + node_id + " " + state, response))
                LOG_WARN("could not propagate membership state for " << node_id << " to " << peer.id);
        }
    }
    maybeCompleteJoin();
}

std::string TcpServer::membershipStatus() const {
    std::lock_guard lock(membership_mutex_);
    std::string response = "OK MEMBERS";
    for (const auto& [id, state] : member_states_) response += " " + id + "=" + state;
    response += " REPLICATION_FAILURES=" + std::to_string(replication_failures_.load());
    const auto generation = recovery_generation_.load();
    const auto completed = recovery_completed_.load();
    const auto waiting_for_pass = generation > completed ? generation - completed : 0;
    response += " RECOVERY_PENDING=" + std::to_string(waiting_for_pass + recovery_tasks_pending_.load());
    response += " RECOVERY_FAILURES=" + std::to_string(recovery_failures_.load());
    return response;
}

}  // namespace cache
