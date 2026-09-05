#include "cache/logger.hpp"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>

namespace cache {

namespace {
std::mutex g_write_mutex;  // Guards the actual write to stdout.
}  // namespace

Logger& Logger::instance() {
    static Logger logger;  // Constructed once, thread-safe since C++11.
    return logger;
}

const char* Logger::levelToString(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO ";
        case LogLevel::WARN:  return "WARN ";
        case LogLevel::ERROR: return "ERROR";
    }
    return "?????";  // Unreachable for valid enum values, but keeps the
                      // compiler from warning about a missing return.
}

void Logger::log(LogLevel level, const std::string& message) {
    // Build the timestamp using steady, portable C++ facilities.
    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf{};
    localtime_r(&now_c, &tm_buf);  // POSIX thread-safe variant of localtime.

    // Lock only around the actual write — formatting above needs no lock
    // since it only touches local variables.
    std::lock_guard<std::mutex> lock(g_write_mutex);

    std::cout << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S")
              << " [" << levelToString(level) << "] "
              << "[thread " << std::this_thread::get_id() << "] "
              << message << '\n';
}

}  // namespace cache
