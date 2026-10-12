# Self-Healing Cache Cluster

An educational C++20 cache project. **Phase 2** adds independent cache-node
processes and client-side routing with a consistent-hashing ring. This project
is not a Redis replacement.

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

CMake fetches GoogleTest if it is not already available in the build cache.

## Run a cluster

The sample [cluster config](config/cluster.conf) defines three nodes and 128
virtual nodes per cache node. Start each node in a separate terminal:

```sh
./build/cache_server --config config/cluster.conf --id cache-a
./build/cache_server --config config/cluster.conf --id cache-b
./build/cache_server --config config/cluster.conf --id cache-c
```

The client reads the same membership and routes each operation directly to the
node that owns its key:

```sh
./build/cache_client --config config/cluster.conf 'v1 SET greeting - hello world'
./build/cache_client --config config/cluster.conf 'v1 GET greeting'
./build/cache_client --config config/cluster.conf 'v1 DELETE greeting'
```

Each process owns a separate in-memory cache. The client makes the same key
route to the same node when membership and virtual-node count are unchanged.
To add or remove a node, update the config and restart node processes with that
membership. The hash ring then gives predictable new ownership. Phase 2 does
not migrate existing data: a key whose owner changes may be missing until later
replication and rebalancing phases are implemented.

For a single-node Phase 1 run, the original commands remain available:

```sh
./build/cache_server 7379
./build/cache_client 127.0.0.1 7379 'v1 SET greeting - hello'
```

## Membership format

The file is whitespace-delimited. Blank lines and `#` comments are ignored.

```text
vnodes 128
node <node-id> <host> <port>
```

`vnodes` is optional and defaults to 128; valid values are 1 through 100000.
Node IDs must be unique and ports must be from 1 through 65535. Every cluster
server and routing client should use the same file. Membership is static for
this phase; there is no gossip or automatic health detection.

## Wire protocol v1

TCP carries newline-terminated UTF-8/ASCII records. A connection may send
multiple requests. Fields are separated by spaces; SET's value is the
remaining text, so it may contain spaces or be empty. Keys and values cannot
contain newlines or NUL bytes. Requests are limited to 64 KiB by default.

| Request | Response |
|---|---|
| `v1 GET <key>` | `OK VALUE <value>` or `OK NOT_FOUND` |
| `v1 SET <key> <ttl-ms-or-> <value>` | `OK STORED` |
| `v1 DELETE <key>` | `OK DELETED` or `OK MISSING` |
| malformed request | `ERR <reason>` |

`-` means no expiry; TTL is a non-negative integer in milliseconds. Expiration
is lazy: an expired entry is removed when accessed, not by a background sweep.
The server handles clients concurrently with one worker thread per connection,
and each connection has five-second socket I/O timeouts.

## Current guarantees and limits

The ring uses a specified 64-bit FNV-1a hash rather than `std::hash`, so node
ownership is stable across process runs and standard-library implementations.
Virtual nodes spread keys across the ring. Nodes serve only their locally owned
requests; Phase 2 has no replication, failover, or cross-node migration. Cache
data is volatile and is lost on process exit. Servers bind all interfaces, so
use a trusted network while the protocol has no authentication or encryption.
