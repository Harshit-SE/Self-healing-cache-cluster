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

The client reads the membership config, probes the ring owners in order, and
routes each operation to the first live owner available:

```sh
./build/cache_client --config config/cluster.conf 'v1 SET greeting - hello world'
./build/cache_client --config config/cluster.conf 'v1 GET greeting'
./build/cache_client --config config/cluster.conf 'v1 DELETE greeting'
```

Each process owns a separate in-memory cache. The client makes the same key
route to the same node when membership and virtual-node count are unchanged.
Servers can update membership online. To add a node, create an expanded config
for the joining process and clients, then start that process; it announces its
identity to existing members, which update their rings and repair data to the
new owners. Existing cache processes do not restart. The joining node remains
in `JOINING` and is excluded from client routing until every reachable member
has completed a handoff pass. Old owners continue serving and send concurrent
replica updates to the joining node during that interval.

To remove a node, send `v1 REMOVE <node-id>` to any live member. The removal is
propagated, ownership is recalculated, and live nodes queue a repair pass. Keep
the removed process running until all remaining members report
`RECOVERY_PENDING=0` and `RECOVERY_FAILURES=0` in `v1 STATUS`; this gives the
handoff time to finish before its only copy is stopped. The client uses the
updated config for requests after a node is removed.

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
a replica can lose that write. A direct host/port client can inspect a replica:

```sh
./build/cache_client 127.0.0.1 7402 'v1 GET greeting'
```

Each node probes its configured peers over TCP. `heartbeat_ms` sets the probe
interval; after `suspect_after` missed probes a peer becomes `SUSPECTED`, and
after `fail_after` it becomes `FAILED`. A joining or returning peer remains
`JOINING` until synchronization completes; other successful peers become
`ALIVE`. State changes are logged and shared with other reachable members. Use
`v1 STATUS` against a node to inspect its current view:

```sh
./build/cache_client 127.0.0.1 7401 'v1 STATUS'
```

The cluster client probes ring owners in order and routes to the first reachable
owner. If a primary is down, it can reach a surviving replica. A replica accepts
writes only after its local membership view excludes the failed/suspected
primary. The failure detector excludes SUSPECTED peers from new ownership, so
temporary delays can cause early promotion and split-brain writes.

After a health-state transition, live nodes run a background repair pass: each
stored key is sent to its currently selected owners, with its remaining TTL.
This repairs an available replica after failover and synchronizes a restarted
node. This is best-effort anti-entropy, not conflict resolution: the protocol
has no per-key versions, write quorum, or delete tombstones. Concurrent writes
during a partition can overwrite newer data, and a missed delete can resurrect
a stale value. Do not treat failover as zero-data-loss or strongly consistent.
`v1 STATUS` reports `RECOVERY_PENDING` and `RECOVERY_FAILURES` so operators can
observe queued/in-flight repairs and exhausted repair deliveries.
New nodes join online and trigger ring recalculation and best-effort data repair.
Removal also recalculates ownership and repairs the surviving owners. Clients
must use the current membership config to calculate candidate owners. Repair
copies data asynchronously and retains old extra copies; it does not reclaim
stale entries from former owners.

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
and ports must be from 1 through 65535. Existing server membership can change
online through JOIN announcements and REMOVE messages. A joining node and
clients need a config containing the expanded member set; servers learn the
new node through the joining node's bootstrap announcement. Health states are
runtime views and do not change membership.

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
| `v1 JOIN <node-id> <host> <port>` | `OK MEMBER JOINED` |
| `v1 REMOVE <node-id>` | `OK MEMBER REMOVED` |
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
positives during delays or network partitions. Durable storage, authentication,
and versioned conflict resolution are not implemented.
