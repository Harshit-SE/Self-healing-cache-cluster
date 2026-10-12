#include "cache/protocol.hpp"

#include <charconv>
#include <limits>

namespace cache {
namespace {
std::string_view takeToken(std::string_view& input) {
    const auto end = input.find(' ');
    auto token = input.substr(0, end);
    input = end == std::string_view::npos ? std::string_view{} : input.substr(end + 1);
    return token;
}
bool validField(std::string_view text) {
    return text.find('\0') == std::string_view::npos && text.find('\n') == std::string_view::npos &&
           text.find('\r') == std::string_view::npos;
}
}  // namespace

std::optional<Request> parseRequest(std::string_view line, std::string& error) {
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    if (!validField(line)) { error = "invalid control character"; return std::nullopt; }
    const auto version = takeToken(line);
    const auto operation = takeToken(line);
    if (version != "v1") { error = "unsupported protocol version"; return std::nullopt; }
    if (operation == "GET" || operation == "DELETE") {
        if (line.empty() || line.find(' ') != std::string_view::npos) {
            error = "expected exactly one key"; return std::nullopt;
        }
        if (operation == "GET") return Request{Request::Operation::Get, std::string(line), {}, std::nullopt};
        return Request{Request::Operation::Delete, std::string(line), {}, std::nullopt};
    }
    if (operation == "SET") {
        const auto key = takeToken(line);
        const auto separator = line.find(' ');
        if (key.empty() || separator == std::string_view::npos) {
            error = "expected key, ttl-ms (or -), and value"; return std::nullopt;
        }
        const auto ttl = line.substr(0, separator);
        const auto value = line.substr(separator + 1);
        if (ttl.empty()) { error = "expected key, ttl-ms (or -), and value"; return std::nullopt; }
        std::optional<long long> ttl_ms;
        if (ttl != "-") {
            long long parsed = 0;
            const auto [ptr, ec] = std::from_chars(ttl.data(), ttl.data() + ttl.size(), parsed);
            if (ec != std::errc{} || ptr != ttl.data() + ttl.size() || parsed < 0) {
                error = "TTL must be a non-negative integer"; return std::nullopt;
            }
            ttl_ms = parsed;
        }
        return Request{Request::Operation::Set, std::string(key), std::string(value), ttl_ms};
    }
    error = "unknown operation";
    return std::nullopt;
}

}  // namespace cache
