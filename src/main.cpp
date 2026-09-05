#include "cache/logger.hpp"

int main() {
    LOG_INFO("cache_server starting up");
    LOG_DEBUG("This is a debug line (port binding logic added in Step 8)");
    LOG_WARN("This is a warning line (example only)");
    LOG_ERROR("This is an error line (example only)");
    LOG_INFO("cache_server shutting down");
    return 0;
}
