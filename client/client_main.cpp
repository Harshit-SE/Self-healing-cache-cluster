#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "usage: cache_client <host> <port> <v1 request...>\n"
                     "example: cache_client 127.0.0.1 7379 v1 SET greeting - hello\n";
        return 2;
    }
    const std::string host = argv[1];
    const std::string port = argv[2];
    std::string request;
    for (int i = 3; i < argc; ++i) { if (!request.empty()) request += ' '; request += argv[i]; }
    request += '\n';
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
        const auto n = ::send(fd, request.data() + sent, request.size() - sent, 0);
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
