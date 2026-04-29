"""Measure end-to-end query RTT against the sketch server for the
locally-supported aggregations (percentiles + sum).

Mirrors the benchmark setup: ingest 1M rows distributed across 30 nodes
for epoch=1, then issue queries against /cluster-metrics/_search (per-key,
the post-DSL-integration shape) and /cluster-metrics/_batch (current
production shape, used by run_rtt_sweep_epoch_full_ortools.py).
"""
from __future__ import annotations

import json
import random
import statistics
import sys
import time

import requests

SERVER = "http://localhost:10101"
SEARCH = f"{SERVER}/cluster-metrics/_search"
BATCH = f"{SERVER}/cluster-metrics/_batch"
INGEST = f"{SERVER}/cluster-metrics"

NODES = [f"N{i:03d}" for i in range(1, 31)]
ROWS = 1_000_000
INGEST_BATCH = 1000

WARMUP = 20
ITERS = 200


def ingest_payload(epoch: int, rng: random.Random) -> dict:
    cluster = []
    cpu, mem, net = [], [], []
    for i in range(INGEST_BATCH):
        cluster.append(NODES[i % len(NODES)])
        cpu.append(rng.uniform(0.1, 64.0))
        mem.append(rng.uniform(0.1, 256.0))
        net.append(rng.uniform(0.1, 10000.0))
    return {
        "epoch": epoch,
        "cluster": cluster,
        "cpu_cores": cpu,
        "memory_gb": mem,
        "network_mbps": net,
    }


def seed(epoch: int = 1):
    rng = random.Random(42)
    sess = requests.Session()
    total_batches = ROWS // INGEST_BATCH
    print(f"seeding {ROWS} rows for epoch={epoch} ...")
    t0 = time.perf_counter()
    for b in range(total_batches):
        body = ingest_payload(epoch, rng)
        r = sess.post(INGEST, json=body, timeout=30)
        r.raise_for_status()
        if (b + 1) % (total_batches // 10) == 0:
            print(f"  {b+1}/{total_batches}")
    dt = time.perf_counter() - t0
    print(f"seeded in {dt:.1f}s")


def search_query(node: str) -> dict:
    """_search with the supported subset (term filter + percentiles + sum)."""
    return {
        "size": 0,
        "query": {"bool": {"filter": [
            {"term": {"cluster": node}},
            {"term": {"epoch": 1}},
        ]}},
        "aggs": {
            "cpu_pct": {"percentiles": {"field": "cpu_cores",     "percents": [0, 50, 90, 100]}},
            "mem_pct": {"percentiles": {"field": "memory_gb",     "percents": [0, 50, 90, 100]}},
            "net_pct": {"percentiles": {"field": "network_mbps",  "percents": [0, 50, 90, 100]}},
            "cpu_sum": {"sum":  {"field": "cpu_cores"}},
            "mem_sum": {"sum":  {"field": "memory_gb"}},
            "net_sum": {"sum":  {"field": "network_mbps"}},
        },
    }


def batch_query() -> dict:
    return {
        "keys": NODES,
        "fields": ["cpu_cores", "memory_gb", "network_mbps"],
        "aggs": ["percentiles", "sum"],
        "percents": [0, 50, 90, 100],
    }


def bench(label: str, url: str, body_factory, validate):
    sess = requests.Session()
    for _ in range(WARMUP):
        body = body_factory()
        sess.post(url, json=body, timeout=10)

    samples_ms = []
    last_status = None
    last_body = None
    for _ in range(ITERS):
        body = body_factory()
        t0 = time.perf_counter()
        r = sess.post(url, json=body, timeout=10)
        samples_ms.append((time.perf_counter() - t0) * 1000.0)
        last_status = r.status_code
        last_body = r.text

    samples_ms.sort()
    p50 = samples_ms[len(samples_ms) // 2]
    p95 = samples_ms[int(len(samples_ms) * 0.95)]
    avg = statistics.mean(samples_ms)
    print(
        f"{label:<55} status={last_status} "
        f"avg={avg:6.3f}ms  p50={p50:6.3f}ms  p95={p95:6.3f}ms  (n={ITERS})"
    )
    j = json.loads(last_body)
    forwarded = validate(j)
    if forwarded:
        print(f"  ⚠ unexpected forward/upstream signals: {forwarded}")
    else:
        print(f"  ✓ all-local response (no forward markers)")


def validate_search(j: dict) -> str | None:
    """Return reason string if response indicates upstream forwarding."""
    if "unsupported_features" in j:
        return f"unsupported_features={j['unsupported_features']}"
    aggs = j.get("aggregations", {})
    expected = {"cpu_pct", "mem_pct", "net_pct", "cpu_sum", "mem_sum", "net_sum"}
    missing = expected - set(aggs)
    if missing:
        return f"missing aggs locally: {missing}"
    return None


def validate_batch(j: dict) -> str | None:
    if "results" not in j:
        return f"unexpected response: {list(j)[:5]}"
    if len(j["results"]) != len(NODES):
        return f"results count {len(j['results'])} != {len(NODES)}"
    return None


def main():
    try:
        requests.get(SERVER, timeout=2)
    except Exception as e:
        print(f"server not reachable at {SERVER}: {e}", file=sys.stderr)
        sys.exit(1)

    seed(epoch=1)

    print()
    rng = random.Random(0)
    bench(
        "_search 1 key  + percentiles+sum (×3 fields)",
        SEARCH,
        lambda: search_query(rng.choice(NODES)),
        validate_search,
    )
    bench(
        "_batch  30 keys + percentiles+sum (×3 fields)",
        BATCH,
        batch_query,
        validate_batch,
    )


if __name__ == "__main__":
    main()
