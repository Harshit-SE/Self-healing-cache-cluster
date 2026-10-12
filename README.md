# Self-Healing Cache Cluster

An educational C++20 cache project. It has independent cache-node processes,
client-side consistent-hash routing, asynchronous replication, and heartbeat
failure detection. This project is not a Redis replacement.

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

CMake fetches GoogleTest if it is not already available in the build cache.

## Run a cluster

The sample [cluster config](config/cluster.conf) defines three nodes, 128
virtual nodes per cache node, a two-copy replication factor, and heartbeat
thresholds. Start each node in a separate terminal:

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
membership. The hash ring gives predictable new ownership. Data is not migrated
when ownership changes; rebalancing and recovery are later-phase work.

## Replication and failure detection

`replicas` is the total number of copies, including the primary, with a default
of two. A primary acknowledges a write after updating its local memory and
queuing updates to the next distinct ring owners. A single background worker
applies replica writes and deletes in order, retrying a failed delivery once.
TTL is reduced by queue delay before a replica applies the value. Queue overflow
and exhausted retries are logged and counted in the node's `STATUS` response;
updates still queued at graceful shutdown are dropped and counted as failures.

Replication is asynchronous: `OK STORED` confirms the primary's in-memory write,
not replica acknowledgement. A primary failure before a queued update reaches
a replica can lose that write. Reads through the normal cluster client continue
to target the primary; a direct host/port client can inspect a replica:

```sh
./build/cache_client 127.0.0.1 7402 'v1 GET greeting'
```

Each node probes its configured peers over TCP. `heartbeat_ms` sets the probe
interval; after `suspect_after` missed probes a peer becomes `SUSPECTED`, and
after `fail_after` it becomes `FAILED`. State changes are logged and shared
with other reachable members. A successful probe marks a returning peer
`ALIVE`. Use `v1 STATUS` against a node to inspect its current view:

```sh
./build/cache_client 127.0.0.1 7401 'v1 STATUS'
```

Phase 3 detects and reports failures but does not promote replicas or reroute
cluster-client requests after primary failure; those are Phase 4.

For a single-node Phase 1 run, the original commands remain available:

```sh
./build/cache_server 7379
./build/cache_client 127.0.0.1 7379 'v1 SET greeting - hello'
```

## Membership format

The file is whitespace-delimited. Blank lines and `#` comments are ignored.

```text
vnodes 128
replicas 2
heartbeat_ms 500
suspect_after 2
fail_after 4
node <node-id> <host> <port>
```

`vnodes` is optional and defaults to 128; valid values are 1 through 100000.
`replicas` defaults to 2; valid values are 1 through 1000. Heartbeat defaults
are 500 ms, 2 missed probes to suspect, and 4 to fail. Node IDs must be unique
and ports must be from 1 through 65535. Every cluster server and routing client
should use the same file. Membership is static; health states are runtime views
and do not change configured ring membership.

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
Virtual nodes spread keys across the ring. Data is volatile and is lost when
all copies are lost. Membership and replica-control messages share the client
TCP listener and have no authentication or encryption. Servers bind all
interfaces, so use a trusted network. Failure detection can produce false
positives during delays or network partitions; it is advisory and does not
change routing or ownership in this phase.
