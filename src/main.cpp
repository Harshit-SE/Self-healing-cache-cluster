#include "cache/logger.hpp"
#include "cache/tcp_server.hpp"

#include <cstdlib>

int main(int argc, char** argv) {
    if (argc != 2) { LOG_ERROR("usage: cache_server <port>"); return 2; }
    char* end = nullptr;
    const auto port = std::strtoul(argv[1], &end, 10);
    if (!end || *end != '\0' || port > 65535) { LOG_ERROR("port must be an integer from 0 to 65535"); return 2; }
    cache::TcpServer tcp_server(static_cast<std::uint16_t>(port));
    const bool ok = tcp_server.start();
    return ok ? 0 : 1;
}
