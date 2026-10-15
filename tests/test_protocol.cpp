#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "cache/protocol.hpp"
#include "cache/tcp_server.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace cache;

TEST(ProtocolTest, ParsesGetAndDelete) {
    std::string error;
    auto get = parseRequest("v1 GET a", error);
    ASSERT_TRUE(get); EXPECT_EQ(get->operation, Request::Operation::Get); EXPECT_EQ(get->key, "a");
    auto del = parseRequest("v1 DELETE a", error);
    ASSERT_TRUE(del); EXPECT_EQ(del->operation, Request::Operation::Delete);
}
TEST(ProtocolTest, ParsesSetWithSpacesAndTtl) {
    std::string error;
    auto request = parseRequest("v1 SET greeting 150 hello world", error);
    ASSERT_TRUE(request) << error;
    EXPECT_EQ(request->key, "greeting"); EXPECT_EQ(request->value, "hello world"); EXPECT_EQ(request->ttl_ms, 150);
}
TEST(ProtocolTest, AllowsEmptySetValue) {
    std::string error;
    auto request = parseRequest("v1 SET empty - ", error);
    ASSERT_TRUE(request) << error;
    EXPECT_TRUE(request->value.empty());
}
TEST(ProtocolTest, RejectsMalformedOrUnsupportedRequests) {
    std::string error;
    EXPECT_FALSE(parseRequest("v2 GET k", error));
    EXPECT_FALSE(parseRequest("v1 SET k -", error));
    EXPECT_FALSE(parseRequest("v1 SET k - value\nother", error));
    EXPECT_FALSE(parseRequest("v1 SET k -1 value", error));
    EXPECT_FALSE(parseRequest("v1 GET", error));
}
TEST(ProtocolTest, ParsesHeartbeatMembershipAndReplicaMessages) {
    std::string error;
    auto ping = parseRequest("v1 PING", error);
    ASSERT_TRUE(ping); EXPECT_EQ(ping->operation, Request::Operation::Ping);
    auto status = parseRequest("v1 STATUS", error);
    ASSERT_TRUE(status); EXPECT_EQ(status->operation, Request::Operation::Status);
    auto replica = parseRequest("v1 REPLICA_SET key 100 value with spaces", error);
    ASSERT_TRUE(replica) << error;
    EXPECT_EQ(replica->operation, Request::Operation::ReplicaSet);
    EXPECT_EQ(replica->ttl_ms, 100);
    EXPECT_EQ(replica->value, "value with spaces");
    auto update = parseRequest("v1 MEMBER cache-b FAILED", error);
    ASSERT_TRUE(update) << error;
    EXPECT_EQ(update->operation, Request::Operation::MemberUpdate);
    EXPECT_EQ(update->key, "cache-b");
    EXPECT_EQ(update->value, "FAILED");
    auto joining = parseRequest("v1 MEMBER cache-c JOINING", error);
    ASSERT_TRUE(joining) << error;
    EXPECT_EQ(joining->value, "JOINING");
    EXPECT_FALSE(parseRequest("v1 MEMBER cache-b UNKNOWN", error));
}

TEST(ProtocolTest, ParsesOnlineMembershipChanges) {
    std::string error;
    const auto join = parseRequest("v1 JOIN cache-d 127.0.0.1 7404", error);
    ASSERT_TRUE(join) << error;
    EXPECT_EQ(join->operation, Request::Operation::NodeJoin);
    EXPECT_EQ(join->key, "cache-d");
    EXPECT_EQ(join->value, "127.0.0.1");
    EXPECT_EQ(join->ttl_ms, 7404);
    const auto gossip_join = parseRequest("v1 JOIN_PEER cache-d 127.0.0.1 7404", error);
    ASSERT_TRUE(gossip_join) << error;
    EXPECT_TRUE(gossip_join->internal_control);

    const auto remove = parseRequest("v1 REMOVE cache-d", error);
    ASSERT_TRUE(remove) << error;
    EXPECT_EQ(remove->operation, Request::Operation::NodeRemove);
    EXPECT_EQ(remove->key, "cache-d");
    EXPECT_FALSE(parseRequest("v1 JOIN cache-d 127.0.0.1 0", error));
    EXPECT_FALSE(parseRequest("v1 REMOVE", error));
}
TEST(ProtocolTest, ServerHandlesRealTcpRoundTrip) {
    TcpServer server(0);
    std::thread server_thread([&] { EXPECT_TRUE(server.start()); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (server.boundPort() == 0 && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
    if (server.boundPort() == 0) { server.stop(); server_thread.join(); FAIL() << "server did not bind"; }
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(fd, 0);
    sockaddr_in address{}; address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(server.boundPort());
    ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
    const std::string commands = "v1 SET k - hello world\nv1 GET k\nv1 DELETE k\nv1 GET k\nv1 SET empty - \nv1 GET empty\nv1 SET x -\n";
    ASSERT_EQ(::send(fd, commands.data(), commands.size(), 0), static_cast<ssize_t>(commands.size()));
    std::string response; char buffer[256];
    while (response.find("ERR expected key") == std::string::npos) {
        const auto n = ::recv(fd, buffer, sizeof(buffer), 0);
        ASSERT_GT(n, 0); response.append(buffer, static_cast<std::size_t>(n));
        if (response.size() > 1024) break;
    }
    EXPECT_EQ(response, "OK STORED\nOK VALUE hello world\nOK DELETED\nOK NOT_FOUND\nOK STORED\nOK VALUE \nERR expected key, ttl-ms (or -), and value\n");
    ::close(fd); server.stop(); server_thread.join();
}

TEST(ProtocolTest, MetricsHttpEndpointExportsMetricsAndHotKeyFeed) {
    TcpServer server(0, 64 * 1024, static_cast<std::uint16_t>(0));
    std::thread server_thread([&] { server.start(); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while ((server.boundPort() == 0 || server.boundMetricsPort() == 0) &&
           std::chrono::steady_clock::now() < deadline) std::this_thread::yield();

    auto exchange = [](std::uint16_t port, const std::string& payload, bool read_to_close) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return std::string{};
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            ::close(fd); return std::string{};
        }
        if (::send(fd, payload.data(), payload.size(), MSG_NOSIGNAL) < 0) { ::close(fd); return std::string{}; }
        std::string response;
        char buffer[4096];
        while (response.size() < 256 * 1024) {
            const auto n = ::recv(fd, buffer, sizeof(buffer), 0);
            if (n <= 0) break;
            response.append(buffer, static_cast<std::size_t>(n));
            if (!read_to_close && response.find('\n') != std::string::npos) break;
        }
        ::close(fd);
        return response;
    };

    const auto write = exchange(server.boundPort(), "v1 SET metrics-hot-key - value\n", false);
    const auto hit = exchange(server.boundPort(), "v1 GET metrics-hot-key\n", false);
    const auto miss = exchange(server.boundPort(), "v1 GET absent-key\n", false);
    const auto metrics = exchange(server.boundMetricsPort(), "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n", true);
    const auto hotkeys = exchange(server.boundMetricsPort(), "GET /hotkeys HTTP/1.1\r\nHost: localhost\r\n\r\n", true);
    server.stop();
    server_thread.join();

    EXPECT_NE(write.find("OK STORED"), std::string::npos);
    EXPECT_NE(hit.find("OK VALUE value"), std::string::npos);
    EXPECT_NE(miss.find("OK NOT_FOUND"), std::string::npos);
    EXPECT_NE(metrics.find("HTTP/1.1 200 OK"), std::string::npos);
    EXPECT_NE(metrics.find("cache_hits_total"), std::string::npos);
    EXPECT_NE(metrics.find("cache_request_duration_seconds_bucket"), std::string::npos);
    EXPECT_NE(metrics.find("cache_keys"), std::string::npos);
    EXPECT_NE(hotkeys.find("HTTP/1.1 200 OK"), std::string::npos);
    EXPECT_NE(hotkeys.find("metrics-hot-key"), std::string::npos);
}
