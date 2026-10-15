# Self-Healing Distributed Cache Cluster

An educational distributed-systems project that demonstrates how several
independent in-memory cache servers can share key ownership, replicate data,
detect unavailable peers, and repair copies after membership changes. A small
machine-learning service analyzes recent hot-key activity and offers
replication suggestions. The project is intended for learning and demonstration;
it is not a production cache or a Redis replacement.

## Project report

### What problem does this project address?

Applications often request the same data repeatedly from a database, file
store, or remote service. Repeating those reads adds latency and consumes
backend capacity. A cache keeps frequently used values closer to the
application so later reads can be served quickly. One cache process, however,
can become a capacity bottleneck or a single point of failure. A distributed
cache spreads keys over multiple nodes and keeps copies on other nodes so that
the system can continue serving data when a node becomes unavailable.

Building that behavior requires more than putting a hash table behind a TCP
socket. The client and servers need a shared view of key ownership; writes
need to reach replicas; failures need to be detected; and data ownership needs
repair after nodes join or fail. Operators also need visibility into traffic,
latency, memory use, replication backlog, and hot keys. This project provides
a compact, inspectable implementation of those ideas for a final-year college
project and live demonstration.

### Project objectives

- Store string keys and values in independent in-memory cache processes.
- Route requests consistently using a deterministic consistent-hash ring.
- Replicate writes asynchronously to a configurable number of owners.
- Track peer health with TCP heartbeats and expose membership state.
- Attempt best-effort repair after failover, node restart, join, or removal.
- Export operational metrics and a bounded recent hot-key feed.
- Evaluate an optional hot-key advisor and return human-reviewed scaling
  recommendations.
- Demonstrate behavior with automated tests and a repeatable load generator.

### Scope and design decisions

This implementation favors clarity and testability over production guarantees.
It uses volatile memory, simple text protocols, asynchronous replication, and
best-effort anti-entropy. The AI advisor is an optional sidecar: the cache
continues serving requests if the advisor is stopped, and its recommendations
are never applied automatically.

## Architecture

```text
Application / demo terminal
          |
          v
  cache_client --config
  consistent-hash routing
          |
    +-----+------+----------------+
    |            |                |
    v            v                v
 cache-a       cache-b          cache-c
 memory +      memory +         memory +
 replicas      replicas         replicas
 heartbeats    heartbeats       heartbeats
 metrics       metrics          metrics
    |             |               |
    +-------------+---------------+----> Prometheus --> Grafana dashboard
    |             |
    +---- /hotkeys feeds ----> collector --> AI advisor API
                                      (advisory output only)
```

### Main components

| Component | Responsibility |
|---|---|
| `cache_server` | Serves the line-based TCP cache protocol, stores values in memory, replicates writes, probes peers, and runs repair work. |
| `cache_client` | Sends requests directly to a host/port or routes a key to candidate owners from the cluster configuration. |
| Consistent-hash ring | Uses a stable 64-bit FNV-1a hash and virtual nodes to assign keys to owners. |
| Membership and recovery | Tracks `JOINING`, `ALIVE`, `SUSPECTED`, and `FAILED` states; schedules best-effort repair after changes. |
| Metrics endpoint | Optionally exposes Prometheus text at `/metrics` and recent key activity at `/hotkeys` on a separate HTTP port. |
| AI advisor | A FastAPI service with `/health`, `/evaluation`, `/predict`, and `/predict/batch` endpoints. |
| Collector | Reads node hot-key feeds, submits features to the advisor, and prints recommendations. |
| Prometheus/Grafana files | Provide scrape configuration, datasource provisioning, and a cache-cluster dashboard definition. |
| Benchmark | Generates concurrent persistent-connection traffic and reports throughput and latency percentiles. |

### Request and recovery flow

1. The client hashes a key and tries its candidate owners in ring order.
2. A live primary updates its local in-memory value and queues replica updates.
3. Background heartbeats update each node's membership view. After configured
   missed probes, peers move through `SUSPECTED` and `FAILED` states.
4. The client can try a surviving owner if the first candidate is unavailable.
   Nodes also run best-effort repair to copy live entries to current owners.
5. Metrics describe request counts, latency, stored entries, memory, queue and
   repair state, failures, and membership. The advisor uses hot-key activity to
   produce a suggestion such as considering one additional replica.

## Repository layout

```text
client/                 Cluster-aware and direct TCP client
config/                 Sample three-node cluster configuration
include/cache/          Public C++ interfaces
src/                    Cache server, protocol, cluster, and metrics code
tests/                  C++ unit and integration tests
ai_service/             FastAPI advisor, collector, evaluation, and tests
monitoring/              Prometheus and Grafana configuration/dashboard
tools/                   Load benchmark
```

## Requirements

- Linux (the sample commands and process RSS metric target Linux).
- C++20 compiler, CMake 3.16 or newer, and pthread support.
- Python 3.12 recommended for the pinned advisor dependencies.
- Internet access the first time CMake fetches GoogleTest or pip installs the
  advisor dependencies, unless those packages are already cached.
- Optional: Prometheus and Grafana to display the included monitoring setup.

On Ubuntu 24.04, install build tools and Python virtual-environment support:

```sh
sudo apt update
sudo apt install build-essential cmake git python3 python3.12-venv
```

If your `python3` is a different minor version, install its matching `venv`
package (for example, `python3.11-venv`).

## Build and run automated tests

From the repository root:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

CMake downloads GoogleTest if it is not already present in the build cache.
The verified C++ test run passed **37 of 37 tests**. The tests cover:

- Logger singleton behavior and concurrent logging.
- Cache GET/SET/DELETE behavior, TTL expiration, snapshots, memory estimates,
  and concurrent access.
- Protocol parsing, malformed requests, membership operations, and real TCP
  round trips.
- Stable ring ownership, virtual-node movement, and distinct replica owners.
- Cluster configuration validation, runtime failure recovery, online join,
  node removal, and data repair.
- Prometheus metrics, hot-key reporting, JSON escaping, and the HTTP metrics
  endpoint.

Run the AI advisor tests after installing its dependencies as described below:

```sh
ai_service/.venv/bin/python -m unittest discover \
  -s ai_service -p 'test_*.py' -v
```

The verified Python suite passed **4 of 4 tests**: health/evaluation reporting,
controlled replica recommendations, invalid feature rejection, and ranking of
batch predictions.

## Run the three-node cluster

For the fastest local demonstration, run the launcher from the repository root:

```sh
./demo.sh
```

It starts any missing cache nodes and advisor, starts or reuses the named
Prometheus and Grafana containers, starts the advisor collector, and generates
light sample traffic so the dashboard has live data. On first use it may build the C++ binaries, download
the monitoring container images, and ask for your sudo password for Docker.
Open the printed Grafana URL to present the dashboard. Use `./demo.sh status`
to check service endpoints and `./demo.sh stop` to stop the processes and
containers that the launcher started. Existing services detected before the
launcher runs are reused and left running by `stop`.

The launcher requires Linux, Docker, curl, a built C++ project (it builds it if
the binaries are missing), and the advisor virtual environment described in
[Run the AI advisor](#run-the-ai-advisor). It uses host networking for this
single-machine demo and binds Grafana and Prometheus to localhost.

The sample [cluster configuration](config/cluster.conf) defines three nodes,
128 virtual nodes per node, two total copies per key, and heartbeat thresholds.
Open three terminals at the repository root and start one server in each:

```sh
./build/cache_server --config config/cluster.conf --id cache-a --metrics-port 17401
```

```sh
./build/cache_server --config config/cluster.conf --id cache-b --metrics-port 17402
```

```sh
./build/cache_server --config config/cluster.conf --id cache-c --metrics-port 17403
```

Allow the nodes to exchange membership and finish initial recovery before the
demo. Check status from another terminal:

```sh
./build/cache_client --config config/cluster.conf 'v1 STATUS'
```

For a steady-state demo, wait until every configured node is reported as
`ALIVE`. `JOINING` means that a node is not yet ready for normal routing; if it
persists, inspect the server logs and recovery counters.

### Cache operations

```sh
./build/cache_client --config config/cluster.conf 'v1 SET greeting - hello professor'
./build/cache_client --config config/cluster.conf 'v1 GET greeting'
./build/cache_client --config config/cluster.conf 'v1 DELETE greeting'
```

The cluster client prints which node it selected. To inspect a node directly or
check whether a replica has received a value, use host and port form:

```sh
./build/cache_client 127.0.0.1 7402 'v1 GET greeting'
```

### Demonstrate failure detection and failover

With a value stored, stop one server using Ctrl+C. Wait for heartbeat state
changes in the remaining server logs, then check membership and read through
the cluster client again:

```sh
./build/cache_client --config config/cluster.conf 'v1 STATUS'
./build/cache_client --config config/cluster.conf 'v1 GET greeting'
```

The sample replication factor is two, so a second copy is expected. This is a
best-effort demonstration, not a zero-data-loss guarantee. Restart the stopped
server and allow it to rejoin and synchronize before ending the demo.

## Run the AI advisor

Create the virtual environment once and install the pinned Python dependencies:

```sh
python3 -m venv ai_service/.venv
ai_service/.venv/bin/pip install -r ai_service/requirements.txt
```

On Ubuntu, if virtual-environment creation reports that `ensurepip` is missing,
install the matching OS package (for Python 3.12, `sudo apt install
python3.12-venv`) and then recreate it with:

```sh
python3 -m venv --clear ai_service/.venv
```

Generate the evaluation report and model artifact:

```sh
ai_service/.venv/bin/python ai_service/evaluate.py
```

Start the service in its own terminal and leave it running:

```sh
ai_service/.venv/bin/uvicorn main:app --app-dir ai_service \
  --host 127.0.0.1 --port 8000
```

The `main:app` module target is intentional: `--app-dir ai_service` makes
`ai_service/` the import directory. Check service health and evaluation from
another terminal:

```sh
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/evaluation
```

### Generate hot-key observations and collect recommendations

First create traffic while the cache nodes are running. For example, after
storing `greeting`, issue repeated reads directly to one node:

```sh
for i in {1..50}; do
  ./build/cache_client 127.0.0.1 7402 'v1 GET greeting' >/dev/null
done
```

Then, from another terminal, read the three node feeds and submit them to the
advisor:

```sh
ai_service/.venv/bin/python ai_service/collector.py \
  --node http://127.0.0.1:17401 --node http://127.0.0.1:17402 \
  --node http://127.0.0.1:17403 --advisor http://127.0.0.1:8000 --once
```

The service can also be tried interactively at `http://127.0.0.1:8000/docs`.
The advisor's output is informational. It does not alter replication factor or
cluster membership.

## Monitoring and performance demonstration

Each node's metrics listener is enabled only when `--metrics-port` is set. The
sample ports are 17401, 17402, and 17403:

```sh
curl http://127.0.0.1:17401/metrics
curl http://127.0.0.1:17402/hotkeys
```

Metrics are per-node; requests routed to `cache-b` increase `cache-b` counters,
not `cache-a` counters. `/hotkeys` reports recent key activity in a bounded
window. These HTTP endpoints have no authentication and should only be
accessible on a trusted network.

Prometheus scrape settings are in [monitoring/prometheus.yml](monitoring/prometheus.yml).
Grafana datasource provisioning and the dashboard definition are under
`monitoring/grafana/`. Import the dashboard JSON or use the provisioning files
with a local Grafana installation. There is no custom web GUI in this project.
Prometheus/Grafana were not runtime-verified in the development environment
because Docker access was unavailable; the configuration files were validated.

Run the benchmark against a running node or cluster client endpoint:

```sh
python3 tools/benchmark.py --host 127.0.0.1 --port 7401 --requests 10000 \
  --workers 8 --keys 1000 --hot-keys 20 --read-ratio 0.9 --hot-share 0.8
```

### Recorded results

#### Synthetic hot-key evaluation

The advisor trains on the first 80% of ordered synthetic time windows and
reports scores on the final 20%. The run contains 11,960 rows: 9,568 training
rows and 2,392 held-out rows. The selected probability threshold is 0.42.

| Method | Accuracy | Precision | Recall | F1 |
|---|---:|---:|---:|---:|
| Logistic regression advisor | 0.9532 | 0.9534 | 0.9549 | 0.9541 |
| Fixed rate/read-ratio rule baseline | 0.9218 | 0.9616 | 0.8820 | 0.9201 |

These numbers describe the seeded synthetic workload only. They do **not**
estimate accuracy on real user traffic. The saved report is
[`ai_service/artifacts/evaluation.json`](ai_service/artifacts/evaluation.json).
Recreate it with `ai_service/.venv/bin/python ai_service/evaluate.py`.

#### Single-node loopback benchmark

An initial run on an AMD Ryzen 5 5600H system (12 logical CPUs, Linux
7.0.0-28) used 10,000 requests, 8 workers, 90% reads, 1,000 keys, and 80% of
accesses directed at 20 hot keys. With metrics enabled, it measured:

| Measurement | Result |
|---|---:|
| Throughput | 5,618.32 requests/second |
| Mean latency | 1.4151 ms |
| p50 latency | 1.4475 ms |
| p95 latency | 2.0545 ms |
| p99 latency | 2.3324 ms |
| Failed requests | 0 |

This is one local loopback measurement, not a multi-node capacity test or a
production performance claim. Results vary with hardware, build settings,
workload, and system load.

## Wire protocol and configuration reference

The TCP protocol uses newline-terminated records. Requests may be sent
sequentially over a connection. Values may contain spaces; keys and values
cannot contain newlines or NUL bytes. Requests are limited to 64 KiB by
default.

| Request | Response |
|---|---|
| `v1 GET <key>` | `OK VALUE <value>` or `OK NOT_FOUND` |
| `v1 SET <key> <ttl-ms-or-> <value>` | `OK STORED` |
| `v1 DELETE <key>` | `OK DELETED` or `OK MISSING` |
| `v1 STATUS` | Membership and replication/recovery counters |
| `v1 JOIN <node-id> <host> <port>` | `OK MEMBER JOINED` |
| `v1 REMOVE <node-id>` | `OK MEMBER REMOVED` |

The `-` TTL means no expiry; numeric TTL values are in milliseconds. Expiration
is lazy: expired entries are removed when accessed. The sample membership file
uses this format:

```text
vnodes 128
replicas 2
heartbeat_ms 500
suspect_after 2
fail_after 4
node cache-a 127.0.0.1 7401
node cache-b 127.0.0.1 7402
node cache-c 127.0.0.1 7403
```

`replicas` is the total number of copies including the primary. Each server
uses one background replication worker; replica deliveries are queued and
retried once. `heartbeat_ms` sets the probe interval. `suspect_after` and
`fail_after` count missed probes before the corresponding state transitions.

## Limitations and risks

- **Volatile storage:** all cache data is in memory. A value is lost if every
  copy is lost or the process stops before the replica update is delivered.
- **Asynchronous replication:** `OK STORED` confirms the primary's local write,
  not acknowledgement from a replica. There is no write quorum.
- **Best-effort recovery:** repair is not a consensus protocol. It has no
  per-key versions, conflict resolution, or delete tombstones. Concurrent
  partitioned writes may overwrite newer values; missed deletes can allow stale
  values to reappear. Former owners can retain extra stale entries.
- **Failure-detector uncertainty:** network delays can be mistaken for failure.
  A suspected node is excluded from new ownership, which can promote replicas
  early and permit split-brain writes.
- **Security:** the cache protocol and metrics endpoints have no authentication
  or encryption. Cache servers bind to all interfaces; run only on a trusted,
  isolated network. Do not expose these ports publicly.
- **Advisor scope:** the classifier was trained and evaluated on generated
  synthetic data. Real, representative, labeled traffic is required before
  assessing real-world accuracy. Suggestions are not automatically applied.
- **Monitoring and memory measures:** metric support and dashboard definitions
  are included, but Grafana/Prometheus runtime deployment depends on the local
  environment. Key/value string bytes are approximate and differ from actual
  process RSS.
- **Benchmark scope:** the recorded performance result is for one node over
  loopback on one machine. It does not represent multi-node throughput,
  durability, or production capacity.
- **Not a production cache:** persistence, TLS, authentication, access control,
  robust consensus, conflict resolution, and production-grade operational
  controls are outside this college-project scope.

## Further reading in this repository

- [Sample cluster configuration](config/cluster.conf)
- [Advisor requirements](ai_service/requirements.txt)
- [Saved synthetic evaluation](ai_service/artifacts/evaluation.json)
- [Prometheus configuration](monitoring/prometheus.yml)
- [Grafana dashboard](monitoring/grafana/dashboards/cache-cluster.json)
- [Benchmark tool](tools/benchmark.py)
