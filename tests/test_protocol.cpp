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
    EXPECT_FALSE(parseRequest("v1 MEMBER cache-b UNKNOWN", error));
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
