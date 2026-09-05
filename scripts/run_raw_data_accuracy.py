#!/usr/bin/env python3
"""Quantile accuracy vs ground truth: sketch server vs Elasticsearch (paper Fig. 6).

Three estimators answer the same p50/p90 question over the same ingested rows,
and each is scored against the exact quantile of those rows:

    approximate (sketch)          -- whatever backend the server is built with
                                     (KLL on feat/raw-data-experiments,
                                      DDSketch alpha=1e-3 on feat/ddsketch-variant)
    Elasticsearch, default        -- percentiles agg, t-digest default compression
    Elasticsearch, compression N  -- percentiles agg, --es-compression (default 1000)

The two Elasticsearch arms are the baseline the paper measures the approximate
layer against; the compression parameter is a *query-time* t-digest setting, so
both arms read the same index.

Setup mirrors the paper's Fig. 6: 30 keys (`N001..N030`, the `range_key_catalog`
in `server-config.yaml`), three metrics, `--epochs` epochs per run, `--runs`
runs. Reported error is the per-run mean over keys, then mean +- sd across runs.

CPU and memory samples are drawn from the real `raw_data/` trace
(`synthetic_cpu_var.csv`: `cpu_usage_millicores` and `memory_available`), one
trace node feeding one catalog key, so the distributions are the measured ones.
`raw_data/` has no per-node network metric, so the network panel is synthetic
(lognormal) -- it exists for parity with the paper's 3x2 layout and is labelled
as synthetic. `--no-network` drops it and plots 2x2 instead.

Ground truth uses numpy's default (linear-interpolation) quantile over the rows
as sent, after the same 6-decimal rounding the ingest path applies.

    # ES on :9200; this script starts and stops its own sketch server on 10101
    cd solver_experimental
    uv run python ../scripts/run_raw_data_accuracy.py --runs 10 --epochs 10
    uv run python ../scripts/plot_raw_data_accuracy.py
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Dict, List

import numpy as np
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rtt_sweep_common import (  # noqa: E402
    DEFAULT_ES_API_KEY,
    es_headers,
    resolve_repo_path,
    start_server,
    stop_server,
    wait_for_server,
)
from run_raw_data_assignment import _default_raw_dir, _pick  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVER_CONFIG = REPO_ROOT / "single_node_server/network-control-server/server-config.yaml"

# The metrics the 3-metric server config carries, with the ES field name used
# for each and where its samples come from.
METRICS = [
    ("cpu_cores", "cpu_cores", "trace_cpu"),
    ("memory_gb", "memory_gb", "trace_mem"),
    ("network_mbps", "network_mbps", "synthetic"),
]
PERCENTS = [50.0, 90.0]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--runs", type=int, default=10)
    p.add_argument("--epochs", type=int, default=10)
    p.add_argument("--keys", type=int, default=30,
                   help="Number of N### keys; must not exceed the server config's catalog.")
    p.add_argument("--rows-per-key", type=int, default=30000,
                   help=("Samples per key per epoch (default 30000 -> 900k rows/epoch, "
                         "matching the ~1M rows/epoch the other experiments ingest). "
                         "Below ~30k samples/key a t-digest at compression 1000 keeps "
                         "every centroid and answers *exactly*, which collapses the "
                         "high-accuracy ES arm to a flat 0% and makes the figure "
                         "meaningless."))
    p.add_argument("--no-network", action="store_true",
                   help="Drop the synthetic network metric and report CPU + memory only.")
    p.add_argument("--server-url", default="http://127.0.0.1:10101")
    p.add_argument("--server-config", type=Path, default=SERVER_CONFIG)
    p.add_argument("--server-log", type=str, default="logs/server_raw_data_accuracy.log")
    p.add_argument("--es-url", default="http://localhost:9200")
    p.add_argument("--es-index", default="accuracy-metrics")
    p.add_argument("--es-api-key", default=DEFAULT_ES_API_KEY)
    p.add_argument("--es-compression", type=float, default=1000.0,
                   help="Compression for the high-accuracy ES arm (paper uses 1000).")
    p.add_argument("--batch-size", type=int, default=20000)
    p.add_argument("--raw-dir", type=Path, default=None)
    p.add_argument("--connect-timeout", type=float, default=5.0)
    p.add_argument("--ingest-timeout", type=float, default=300.0)
    p.add_argument("--query-timeout", type=float, default=300.0)
    p.add_argument("--server-start-timeout", type=float, default=180.0)
    p.add_argument("--seed", type=int, default=20260904)
    p.add_argument("--out-csv", type=str, default="data/raw_data_accuracy.csv")
    args = p.parse_args()
    if args.raw_dir is None:
        args.raw_dir = _default_raw_dir()
    return args


# ---------------------------------------------------------------------------
# Sample pools
# ---------------------------------------------------------------------------

def load_trace_pools(raw_dir: Path, n_keys: int) -> Dict[str, List[np.ndarray]]:
    """Per-trace-node CPU (cores) and memory (GB) sample pools.

    One trace node feeds one catalog key. Nodes are taken in descending sample
    count so every key has a deep pool to draw from.
    """
    path = raw_dir / "synthetic_cpu_var.csv"
    if not path.exists():
        raise SystemExit(f"{path} not found; set --raw-dir or $RAW_DATA_DIR")
    cpu: Dict[str, List[float]] = {}
    mem: Dict[str, List[float]] = {}
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            node = row["scenario_node"]
            try:
                cpu.setdefault(node, []).append(float(row["cpu_usage_millicores"]) / 1000.0)
                mem.setdefault(node, []).append(float(row["memory_available"]) / 1e9)
            except (TypeError, ValueError):
                continue
    order = sorted(cpu, key=lambda n: len(cpu[n]), reverse=True)[:n_keys]
    if len(order) < n_keys:
        raise SystemExit(f"trace has {len(order)} usable nodes, need {n_keys}")
    return {
        "trace_cpu": [np.asarray(cpu[n], dtype=float) for n in order],
        "trace_mem": [np.asarray(mem[n], dtype=float) for n in order],
    }


def draw(pools, source: str, key_idx: int, n: int, rng: np.random.Generator) -> np.ndarray:
    if source == "synthetic":
        # Stand-in for a per-node network metric, which raw_data does not have.
        return rng.lognormal(mean=np.log(120.0 + 20.0 * (key_idx % 5)), sigma=0.55, size=n)
    pool = pools[source][key_idx]
    return pool[rng.integers(0, len(pool), size=n)]


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

def reset_es(args: argparse.Namespace) -> None:
    headers = es_headers(args.es_api_key)
    to = (args.connect_timeout, args.ingest_timeout)
    requests.delete(f"{args.es_url}/{args.es_index}", headers=headers, timeout=to)
    props = {"epoch": {"type": "integer"}, "cluster": {"type": "keyword"}}
    for name, es_field, _ in METRICS:
        props[es_field] = {"type": "double"}
    r = requests.put(f"{args.es_url}/{args.es_index}", headers=headers,
                     json={"settings": {"number_of_shards": 1, "number_of_replicas": 0},
                           "mappings": {"properties": props}}, timeout=to)
    r.raise_for_status()


def ingest_sketch(args, epoch: int, keys: List[str], cols: Dict[str, np.ndarray],
                  metrics: List[tuple]) -> None:
    """One POST per batch; every row of a batch carries the same key list."""
    url = f"{args.server_url}/cluster-metrics"
    to = (args.connect_timeout, args.ingest_timeout)
    n = len(cols[metrics[0][0]])
    for start in range(0, n, args.batch_size):
        end = min(n, start + args.batch_size)
        payload = {"epoch": epoch, "cluster": keys[start:end]}
        for name, _es_field, _src in metrics:
            payload[name] = [round(float(v), 6) for v in cols[name][start:end]]
        r = requests.post(url, json=payload, timeout=to)
        r.raise_for_status()
        inserted = r.json().get("inserted")
        if inserted != end - start:
            raise RuntimeError(f"sketch ingest mismatch: {inserted} != {end - start}")


def ingest_es(args, epoch: int, keys: List[str], cols: Dict[str, np.ndarray],
              metrics: List[tuple]) -> None:
    headers = es_headers(args.es_api_key)
    to = (args.connect_timeout, args.ingest_timeout)
    url = f"{args.es_url}/{args.es_index}/_bulk"
    action = json.dumps({"index": {}})
    n = len(keys)
    for start in range(0, n, args.batch_size):
        end = min(n, start + args.batch_size)
        lines = []
        for i in range(start, end):
            doc = {"epoch": epoch, "cluster": keys[i]}
            for name, es_field, _src in metrics:
                doc[es_field] = round(float(cols[name][i]), 6)
            lines.append(action)
            lines.append(json.dumps(doc))
        params = {"refresh": "wait_for"} if end >= n else None
        r = requests.post(url, headers=headers, data="\n".join(lines) + "\n",
                          params=params, timeout=to)
        r.raise_for_status()
        if r.json().get("errors"):
            raise RuntimeError("ES bulk errors")


def query_sketch(args, keys: List[str], metrics: List[tuple]):
    payload = {"keys": keys, "fields": [m[0] for m in metrics],
               "aggs": ["percentiles"], "percents": PERCENTS}
    r = requests.post(f"{args.server_url}/cluster-metrics/_batch", json=payload,
                      timeout=(args.connect_timeout, args.query_timeout))
    r.raise_for_status()
    out: Dict[str, Dict[tuple, float]] = {}
    for item in r.json().get("results", []):
        pct = item.get("percentiles") or {}
        rec = {}
        for name, _es_field, _src in metrics:
            vals = pct.get(name, {})
            for q in PERCENTS:
                rec[(name, q)] = _pick(vals, q)
        out[item["key"]] = rec
    return out


def query_es(args, epoch: int, metrics: List[tuple], compression: float | None):
    """One terms agg over the keys, percentile sub-aggs per metric."""
    aggs = {}
    for name, es_field, _src in metrics:
        pct: Dict[str, object] = {"field": es_field, "percents": PERCENTS}
        if compression is not None:
            pct["tdigest"] = {"compression": compression}
        aggs[f"{name}_pct"] = {"percentiles": pct}
    body = {
        "size": 0,
        "query": {"bool": {"filter": [{"term": {"epoch": epoch}}]}},
        "aggs": {"by_key": {"terms": {"field": "cluster", "size": args.keys},
                            "aggs": aggs}},
    }
    r = requests.post(f"{args.es_url}/{args.es_index}/_search",
                      headers=es_headers(args.es_api_key), json=body,
                      params={"request_cache": "false"},
                      timeout=(args.connect_timeout, args.query_timeout))
    r.raise_for_status()
    out: Dict[str, Dict[tuple, float]] = {}
    for bucket in r.json()["aggregations"]["by_key"]["buckets"]:
        rec = {}
        for name, _es_field, _src in metrics:
            vals = bucket[f"{name}_pct"]["values"]
            for q in PERCENTS:
                rec[(name, q)] = _pick(vals, q)
        out[bucket["key"]] = rec
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    metrics = [m for m in METRICS if not (args.no_network and m[0] == "network_mbps")]
    keys = [f"N{i:03d}" for i in range(1, args.keys + 1)]
    pools = load_trace_pools(args.raw_dir, args.keys)
    print(f"{args.keys} keys, {args.rows_per_key} rows/key/epoch "
          f"({args.keys * args.rows_per_key} rows/epoch), "
          f"metrics: {', '.join(m[0] for m in metrics)}")
    print(f"arms: sketch, ES default compression, ES compression {args.es_compression:g}")

    rows: List[dict] = []
    for run in range(args.runs):
        rng = np.random.default_rng([args.seed, run])
        os.environ["NCS_CONFIG_PATH"] = str(args.server_config)
        server = start_server(resolve_repo_path(args.server_log), truncate_log=(run == 0))
        try:
            wait_for_server(f"{args.server_url}/healthz", args.server_start_timeout,
                            args.connect_timeout, 5.0)
            reset_es(args)
            for epoch in range(1, args.epochs + 1):
                # Draw once; the exact quantile, the sketch and both ES arms all
                # score against these same values.
                per_key: Dict[str, Dict[str, np.ndarray]] = {}
                flat_keys: List[str] = []
                cols: Dict[str, List[float]] = {m[0]: [] for m in metrics}
                for idx, key in enumerate(keys):
                    per_key[key] = {}
                    for name, _es_field, src in metrics:
                        vals = np.round(draw(pools, src, idx, args.rows_per_key, rng), 6)
                        per_key[key][name] = vals
                        cols[name].extend(vals.tolist())
                    flat_keys.extend([key] * args.rows_per_key)
                col_arrays = {k: np.asarray(v, dtype=float) for k, v in cols.items()}

                ingest_sketch(args, epoch, flat_keys, col_arrays, metrics)
                ingest_es(args, epoch, flat_keys, col_arrays, metrics)

                got = {
                    "sketch": query_sketch(args, keys, metrics),
                    "es_default": query_es(args, epoch, metrics, None),
                    "es_compression": query_es(args, epoch, metrics, args.es_compression),
                }
                for backend, table in got.items():
                    for key in keys:
                        for name, _es_field, _src in metrics:
                            for q in PERCENTS:
                                exact = float(np.quantile(per_key[key][name], q / 100.0))
                                est = table[key][(name, q)]
                                rows.append({
                                    "run": run, "epoch": epoch, "key": key,
                                    "metric": name, "quantile": q, "backend": backend,
                                    "estimate": est, "exact": exact,
                                    "rel_err_pct": abs(est - exact) / exact * 100.0
                                    if exact else float("nan"),
                                })
                per_backend = {
                    b: statistics.fmean(r["rel_err_pct"] for r in rows
                                        if r["run"] == run and r["epoch"] == epoch
                                        and r["backend"] == b)
                    for b in got
                }
                print(f"run {run} epoch {epoch}: mean rel err  "
                      + "  ".join(f"{b}={v:.4f}%" for b, v in per_backend.items()))
        finally:
            stop_server(server)
            time.sleep(0.5)

    out_csv = resolve_repo_path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"\nwrote {out_csv}  ({len(rows)} rows)")

    print(f"\n{'backend':<16}{'metric':<14}{'q':>5}{'mean err':>11}{'sd across runs':>17}")
    for backend in ("sketch", "es_default", "es_compression"):
        for name, _es_field, _src in metrics:
            for q in PERCENTS:
                per_run = []
                for run in range(args.runs):
                    vals = [r["rel_err_pct"] for r in rows
                            if r["backend"] == backend and r["metric"] == name
                            and r["quantile"] == q and r["run"] == run]
                    if vals:
                        per_run.append(statistics.fmean(vals))
                sd = statistics.stdev(per_run) if len(per_run) > 1 else 0.0
                print(f"{backend:<16}{name:<14}{q:>5.0f}"
                      f"{statistics.fmean(per_run):>10.4f}%{sd:>16.4f}%")


if __name__ == "__main__":
    main()
