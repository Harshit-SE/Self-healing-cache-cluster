#pragma once

#include <sstream>
#include <string>

namespace cache {

enum class LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR
};

// Thread-safe singleton logger.
//
// Usage:
//     LOG_INFO("Server started on port " << port);
//
// Design notes:
// - Meyer's Singleton (static local in instance()) is thread-safe
//   initialization since C++11 — no manual locking needed for construction.
// - A single mutex guards the actual write to stdout, since concurrent
//   threads (Step 8 onward) will all log through this same object.
class Logger {
public:
    static Logger& instance();

    // Not copyable or movable — there is exactly one logger.
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    void log(LogLevel level, const std::string& message);

private:
    Logger() = default;

    static const char* levelToString(LogLevel level);
};

}  // namespace cache

// Convenience macros so call sites can use stream syntax (operator<<)
// instead of manually building a std::string every time.
// Wrapping in a do/while(0) makes the macro behave like a single statement
// even when used inside an if-without-braces.
#define LOG_MESSAGE(level, msg)                                 \
    do {                                                        \
        std::ostringstream oss_;                                \
        oss_ << msg;                                             \
        cache::Logger::instance().log(level, oss_.str());       \
    } while (0)

#define LOG_DEBUG(msg) LOG_MESSAGE(cache::LogLevel::DEBUG, msg)
#define LOG_INFO(msg)  LOG_MESSAGE(cache::LogLevel::INFO, msg)
#define LOG_WARN(msg)  LOG_MESSAGE(cache::LogLevel::WARN, msg)
#define LOG_ERROR(msg) LOG_MESSAGE(cache::LogLevel::ERROR, msg)
