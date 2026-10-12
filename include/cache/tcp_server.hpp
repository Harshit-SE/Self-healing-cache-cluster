#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cache/cache_engine.hpp"

namespace cache {

class TcpServer {
public:
    explicit TcpServer(std::uint16_t port, std::size_t max_line_bytes = 64 * 1024);
    TcpServer(std::uint16_t port, std::string node_id, std::size_t cluster_size,
              std::size_t max_line_bytes = 64 * 1024);
    ~TcpServer();
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Blocks until stop() is called or the listener fails. Port 0 asks the OS
    // for an ephemeral port, available via boundPort() after start().
    bool start();
    void stop();
    std::uint16_t boundPort() const noexcept { return bound_port_.load(); }

private:
    void serveClient(int fd);
    std::uint16_t requested_port_;
    std::string node_id_;
    std::size_t cluster_size_;
    std::atomic<std::uint16_t> bound_port_{0};
    std::size_t max_line_bytes_;
    std::atomic<bool> stopping_{false};
    std::atomic<int> listen_fd_{-1};
    std::mutex clients_mutex_;
    std::vector<int> clients_;
    std::vector<std::thread> workers_;
    CacheEngine cache_;
};

}  // namespace cache
