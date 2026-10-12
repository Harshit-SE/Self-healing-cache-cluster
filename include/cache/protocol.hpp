#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace cache {

struct Request {
    enum class Operation { Get, Set, Delete, Ping, Status, ReplicaSet, ReplicaDelete,
                           MemberUpdate, NodeJoin, NodeRemove, SyncDone } operation;
    std::string key;
    std::string value;
    std::optional<long long> ttl_ms;
    bool internal_control{false};
};

// Version 1 is one UTF-8/ASCII line per message, terminated by LF. Fields
// are separated by single spaces; SET's value is the remaining line and may
// contain spaces. Keys and values cannot contain LF or NUL. TTL is decimal
// milliseconds; '-' means no expiry. Responses are OK VALUE <value>, OK
// STORED, OK DELETED|MISSING, or ERR <message>. Empty values are supported.
// Control-plane records include PING, STATUS, MEMBER, JOIN, REMOVE,
// REPLICA_SET, and REPLICA_DELETE; all share the client listener and are not
// authenticated.
std::optional<Request> parseRequest(std::string_view line, std::string& error);

}  // namespace cache
