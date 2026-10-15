#!/usr/bin/env python3
"""Concurrent persistent-connection benchmark for cache client requests."""

from __future__ import annotations

import argparse
import json
import random
import socket
import statistics
import threading
import time


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--hot-keys", type=int, default=20)
    parser.add_argument("--read-ratio", type=float, default=0.9)
    parser.add_argument("--hot-share", type=float, default=0.8)
    args = parser.parse_args()
    if args.requests < 1 or args.workers < 1 or args.keys < 1 or not 0 <= args.read_ratio <= 1:
        parser.error("requests, workers, and keys must be positive; read-ratio must be in [0,1]")
    if not 1 <= args.hot_keys <= args.keys or not 0 <= args.hot_share <= 1:
        parser.error("hot-keys must be in [1, keys] and hot-share must be in [0,1]")

    per_worker = [args.requests // args.workers] * args.workers
    for i in range(args.requests % args.workers):
        per_worker[i] += 1
    latencies: list[float] = []
    failures: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(args.workers + 1)

    def run_worker(worker_id: int, count: int) -> None:
        rng = random.Random(9001 + worker_id)
        local_latencies: list[float] = []
        local_failures: list[str] = []
        try:
            connection = socket.create_connection((args.host, args.port), timeout=3)
            connection.settimeout(5)
            stream = connection.makefile("rwb", buffering=0)
        except OSError as error:
            with lock:
                failures.append(str(error))
            barrier.wait()
            return
        barrier.wait()
        for sequence in range(count):
            if rng.random() < args.hot_share:
                key = f"bench-hot-{rng.randrange(args.hot_keys)}"
            else:
                key = f"bench-cold-{rng.randrange(args.keys)}"
            if rng.random() < args.read_ratio:
                command = f"v1 GET {key}\n".encode()
            else:
                command = f"v1 SET {key} - value-{worker_id}-{sequence}\n".encode()
            started = time.perf_counter_ns()
            try:
                stream.write(command)
                response = stream.readline()
                elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
                if not response.startswith(b"OK "):
                    local_failures.append(response.decode(errors="replace").strip() or "empty response")
                local_latencies.append(elapsed_ms)
            except (OSError, TimeoutError) as error:
                local_failures.append(str(error))
                break
        connection.close()
        with lock:
            latencies.extend(local_latencies)
            failures.extend(local_failures)

    threads = [threading.Thread(target=run_worker, args=(worker, count))
               for worker, count in enumerate(per_worker)]
    for thread in threads:
        thread.start()
    barrier.wait()
    started = time.perf_counter()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started
    result = {
        "host": args.host,
        "port": args.port,
        "requested": args.requests,
        "completed": len(latencies),
        "workers": args.workers,
        "elapsed_seconds": round(elapsed, 4),
        "throughput_requests_per_second": round(len(latencies) / elapsed if elapsed else 0.0, 2),
        "latency_ms": {
            "mean": round(statistics.fmean(latencies), 4) if latencies else 0,
            "p50": round(percentile(latencies, 0.50), 4),
            "p95": round(percentile(latencies, 0.95), 4),
            "p99": round(percentile(latencies, 0.99), 4),
        },
        "failed_requests": len(failures),
        "failure_examples": failures[:5],
        "workload": {"read_ratio": args.read_ratio, "hot_key_share": args.hot_share,
                     "hot_keys": args.hot_keys, "cold_key_space": args.keys},
    }
    print(json.dumps(result, indent=2))
    return 0 if not failures and len(latencies) == args.requests else 1


if __name__ == "__main__":
    raise SystemExit(main())
