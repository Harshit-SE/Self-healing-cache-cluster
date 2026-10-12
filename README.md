# Self-Healing Cache Cluster

An educational C++20 cache project. The current milestone is **Phase 1: a
thread-safe single-node cache over TCP**. It is not a Redis replacement and
does not yet implement clustering, replication, or failover.

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

CMake fetches GoogleTest if it is not already available in the build cache.

## Run

Start the server (port `0` asks Linux to select an available port):

```sh
./build/cache_server 7379
```

In another terminal, send one request per client invocation:

```sh
./build/cache_client 127.0.0.1 7379 'v1 SET greeting - hello world'
./build/cache_client 127.0.0.1 7379 'v1 GET greeting'
./build/cache_client 127.0.0.1 7379 'v1 DELETE greeting'
```

## Wire protocol v1

TCP carries newline-terminated UTF-8/ASCII records. One connection may send
multiple requests. Fields are separated by spaces; SET's value is the
remaining text, so it may contain spaces. Keys and values cannot contain
newlines or NUL bytes. Requests and responses are limited to 64 KiB by default.

| Request | Response |
|---|---|
| `v1 GET <key>` | `OK VALUE <value>` or `OK NOT_FOUND` |
| `v1 SET <key> <ttl-ms-or-> <value>` | `OK STORED` |
| `v1 DELETE <key>` | `OK DELETED` or `OK MISSING` |
| malformed request | `ERR <reason>` |

`-` means no expiry; TTL is a non-negative integer in milliseconds. Expiration
is lazy: an expired entry is removed when accessed, not by a background sweep.
The server handles clients concurrently with one worker thread per connection,
and each connection has five-second socket I/O timeouts. This is intentionally
simple for Phase 1; a bounded worker pool is a future scalability improvement.

## Current guarantees and limits

Operations on the in-process map are protected by a shared mutex. Cache data is
volatile and is lost on process exit. The server binds all interfaces, so use a
trusted network while the protocol has no authentication or encryption. Later
phases will add cluster membership, consistent hashing, replicas, and
self-healing behavior; none of those features should be inferred from this
single-node milestone.
