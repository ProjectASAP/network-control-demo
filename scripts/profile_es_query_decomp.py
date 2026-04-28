#!/usr/bin/env python3
"""Decompose ES query latency by variant (which aggs) x transport (serial vs msearch).

Assumes data is already ingested in the cluster-metrics index for epochs 1..N
(e.g. left over from a previous profile_es_query.py run). Does NOT reset.

Variants:
  A     - filter only (no aggs)
  B     - filter + 3 sum aggs
  C_td  - filter + 3 percentiles (t-digest, default algorithm)
  C_hdr - filter + 3 percentiles (HDR histogram)
  D_td  - filter + sum + percentiles t-digest  (= production query)
  D_hdr - filter + sum + percentiles HDR

Transports:
  serial  - 30 individual _search HTTP calls
  msearch - 1 _msearch HTTP call bundling 30 sub-searches

Cache control: every query goes with ?request_cache=false. Run = 1 warmup pass +
N measurement passes; tasks are shuffled within each pass to interleave variants.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rtt_sweep_common import (  # noqa: E402
    DEFAULT_ES_API_KEY,
    DEFAULT_ES_INDEX,
    DEFAULT_ES_URL,
    REPO_ROOT,
    es_headers,
    parse_nodes_config,
)

VARIANTS = ["A", "B", "C_td", "C_hdr", "D_td", "D_hdr"]
TRANSPORTS = ["serial", "msearch"]


def build_payload(variant: str, node: str, epoch: int) -> dict:
    body = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"node": node}},
                    {"term": {"epoch": epoch}},
                ]
            }
        },
    }
    aggs: Dict[str, dict] = {}
    if variant in ("B", "D_td", "D_hdr"):
        aggs["cpu_sum"] = {"sum": {"field": "cpu"}}
        aggs["mem_sum"] = {"sum": {"field": "mem"}}
        aggs["net_sum"] = {"sum": {"field": "net"}}
    if variant in ("C_td", "D_td"):
        for f in ("cpu", "mem", "net"):
            aggs[f"{f}_pct"] = {
                "percentiles": {"field": f, "percents": [0, 50, 90, 100]}
            }
    if variant in ("C_hdr", "D_hdr"):
        for f in ("cpu", "mem", "net"):
            aggs[f"{f}_pct"] = {
                "percentiles": {
                    "field": f,
                    "percents": [0, 50, 90, 100],
                    "hdr": {"number_of_significant_value_digits": 3},
                }
            }
    if aggs:
        body["aggs"] = aggs
    return body


def run_serial(es_url: str, idx: str, headers: dict,
               variant: str, nodes: List[str], epoch: int
               ) -> List[Tuple[str, float, int]]:
    """Returns list of (node, wall_ms, took_ms) — one entry per node call."""
    url = f"{es_url}/{idx}/_search?request_cache=false"
    rows = []
    for node in nodes:
        payload = build_payload(variant, node, epoch)
        t0 = time.perf_counter()
        r = requests.post(url, headers=headers, json=payload, timeout=(5, 60))
        wall = (time.perf_counter() - t0) * 1000.0
        r.raise_for_status()
        took = int(r.json().get("took", 0))
        rows.append((node, wall, took))
    return rows


def run_msearch(es_url: str, idx: str, headers: dict,
                variant: str, nodes: List[str], epoch: int
                ) -> Tuple[float, int, int]:
    """Returns (wall_ms, took_sum_ms, n_subs)."""
    url = f"{es_url}/{idx}/_msearch"
    lines = []
    for node in nodes:
        # _msearch sets request_cache via the per-sub-search header, not URL.
        lines.append(json.dumps({"request_cache": False}))
        lines.append(json.dumps(build_payload(variant, node, epoch)))
    body = "\n".join(lines) + "\n"
    msearch_headers = dict(headers)
    msearch_headers["Content-Type"] = "application/x-ndjson"
    t0 = time.perf_counter()
    r = requests.post(url, headers=msearch_headers, data=body, timeout=(5, 120))
    wall = (time.perf_counter() - t0) * 1000.0
    r.raise_for_status()
    js = r.json()
    responses = js.get("responses", []) or []
    took_sum = sum(int(resp.get("took", 0)) for resp in responses)
    return wall, took_sum, len(responses)


def percentile(sorted_values: List[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = max(0, min(len(sorted_values) - 1, int(round(q * (len(sorted_values) - 1)))))
    return sorted_values[idx]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--es-url", default=DEFAULT_ES_URL)
    ap.add_argument("--es-index", default=DEFAULT_ES_INDEX)
    ap.add_argument("--es-api-key", default=DEFAULT_ES_API_KEY)
    ap.add_argument("--epochs", type=int, default=5, help="Use epochs 1..N")
    ap.add_argument(
        "--nodes-config",
        default="single_node_server/network-control-server/server-config.yaml",
    )
    ap.add_argument("--passes", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out-csv", default="data/es_profile_decomp.csv")
    args = ap.parse_args()

    headers = es_headers(args.es_api_key)
    nodes = parse_nodes_config(args.nodes_config)
    epochs = list(range(1, args.epochs + 1))

    # Sanity check: index has expected data
    r = requests.get(
        f"{args.es_url}/{args.es_index}/_count", headers=headers, timeout=(5, 10)
    )
    r.raise_for_status()
    total = int(r.json().get("count", 0))
    expected_min = args.epochs * 100_000
    if total < expected_min:
        print(
            f"WARN: index has only {total} docs (expected >= {expected_min}). "
            f"Run scripts/profile_es_query.py first to ingest data."
        )
    print(
        f"[setup] index docs={total}  epochs=1..{args.epochs}  "
        f"nodes={len(nodes)}  passes={args.passes}",
        flush=True,
    )

    # Build task list: each task = one (variant, transport, epoch).
    tasks: List[Tuple[str, str, int]] = [
        (v, t, e) for v in VARIANTS for t in TRANSPORTS for e in epochs
    ]
    rng = random.Random(args.seed)
    csv_rows: List[dict] = []

    def execute(task: Tuple[str, str, int], pass_idx: int, record: bool) -> None:
        v, t, e = task
        if t == "serial":
            results = run_serial(args.es_url, args.es_index, headers, v, nodes, e)
            if record:
                for node, wall, took in results:
                    csv_rows.append(
                        {
                            "variant": v,
                            "transport": "serial",
                            "epoch": e,
                            "pass": pass_idx,
                            "scope": node,
                            "n_subs": 1,
                            "wall_ms": f"{wall:.3f}",
                            "took_ms": took,
                        }
                    )
        else:
            wall, took_sum, n = run_msearch(
                args.es_url, args.es_index, headers, v, nodes, e
            )
            if record:
                csv_rows.append(
                    {
                        "variant": v,
                        "transport": "msearch",
                        "epoch": e,
                        "pass": pass_idx,
                        "scope": "BATCH",
                        "n_subs": n,
                        "wall_ms": f"{wall:.3f}",
                        "took_ms": took_sum,
                    }
                )

    # Warmup
    print(f"[warmup] {len(tasks)} tasks (not recorded)", flush=True)
    t0 = time.perf_counter()
    for task in tasks:
        execute(task, pass_idx=0, record=False)
    print(f"[warmup] done in {(time.perf_counter() - t0):.1f}s", flush=True)

    # Measurement
    for p in range(1, args.passes + 1):
        shuffled = list(tasks)
        rng.shuffle(shuffled)
        t0 = time.perf_counter()
        for task in shuffled:
            execute(task, pass_idx=p, record=True)
        print(
            f"[pass {p}/{args.passes}] {len(shuffled)} tasks done in "
            f"{(time.perf_counter() - t0):.1f}s",
            flush=True,
        )

    # Write CSV
    out_path = REPO_ROOT / args.out_csv
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "variant", "transport", "epoch", "pass",
        "scope", "n_subs", "wall_ms", "took_ms",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(csv_rows)
    print(f"\nCSV written: {out_path}\n")

    # Aggregate to "30-node coverage" units (per task = one epoch's 30 nodes).
    # serial: sum the 30 row entries; msearch: 1 row already covers 30.
    by_task: Dict[Tuple[str, str, int, int], Dict[str, float]] = {}
    for row in csv_rows:
        key = (row["variant"], row["transport"], row["epoch"], row["pass"])
        slot = by_task.setdefault(key, {"wall": 0.0, "took": 0.0, "n": 0})
        slot["wall"] += float(row["wall_ms"])
        slot["took"] += float(row["took_ms"])
        slot["n"] += 1

    groups: Dict[Tuple[str, str], List[Tuple[float, float]]] = {}
    for (v, t, e, p), s in by_task.items():
        groups.setdefault((v, t), []).append((s["wall"], s["took"]))

    print("=" * 90)
    print("PER 30-NODE COVERAGE  (one record = one epoch's 30 nodes; values in ms)")
    print("=" * 90)
    print(
        f"{'variant':<8} {'transport':<9} {'n':>3}  "
        f"{'wall_avg':>9} {'wall_p50':>9} {'wall_p90':>9}  "
        f"{'took_avg':>9} {'took_p50':>9} {'took_p90':>9}"
    )
    for v in VARIANTS:
        for t in TRANSPORTS:
            samples = groups.get((v, t), [])
            if not samples:
                continue
            walls = sorted(s[0] for s in samples)
            tooks = sorted(s[1] for s in samples)
            print(
                f"{v:<8} {t:<9} {len(samples):>3}  "
                f"{sum(walls) / len(walls):>9.2f} "
                f"{percentile(walls, 0.5):>9.2f} {percentile(walls, 0.9):>9.2f}  "
                f"{sum(tooks) / len(tooks):>9.2f} "
                f"{percentile(tooks, 0.5):>9.2f} {percentile(tooks, 0.9):>9.2f}"
            )

    def avg_took(v: str, t: str) -> float:
        s = groups.get((v, t), [])
        return sum(x[1] for x in s) / len(s) if s else 0.0

    a = avg_took("A", "serial")
    b = avg_took("B", "serial")
    c_td = avg_took("C_td", "serial")
    c_hdr = avg_took("C_hdr", "serial")
    d_td = avg_took("D_td", "serial")
    d_hdr = avg_took("D_hdr", "serial")

    print()
    print("=" * 90)
    print("COST DECOMPOSITION  (serial transport, took_avg per 30-node coverage, ms)")
    print("=" * 90)
    print(f"  A     (filter only)              : {a:>9.2f}")
    print(f"  B - A (sum aggs cost)            : {b - a:>9.2f}")
    print(f"  C_td  - A (t-digest cost)        : {c_td - a:>9.2f}")
    print(f"  C_hdr - A (HDR cost)             : {c_hdr - a:>9.2f}")
    print(f"  D_td  (current production)       : {d_td:>9.2f}")
    print(f"  D_hdr (HDR replacing t-digest)   : {d_hdr:>9.2f}")
    if d_td > 0:
        save = d_td - d_hdr
        print(f"  Switching D_td -> D_hdr saves    : "
              f"{save:>9.2f}  ({save / d_td * 100:.1f}%)")

    print()
    print("=" * 90)
    print("TRANSPORT EFFECT  (msearch vs serial; wall_ms per 30-node coverage)")
    print("=" * 90)
    for v in VARIANTS:
        s_walls = [x[0] for x in groups.get((v, "serial"), [])]
        m_walls = [x[0] for x in groups.get((v, "msearch"), [])]
        if not s_walls or not m_walls:
            continue
        s_avg = sum(s_walls) / len(s_walls)
        m_avg = sum(m_walls) / len(m_walls)
        savings = s_avg - m_avg
        pct_save = savings / s_avg * 100 if s_avg else 0.0
        print(
            f"  {v:<8} serial={s_avg:>8.2f}  msearch={m_avg:>8.2f}  "
            f"saved={savings:>8.2f}  ({pct_save:>5.1f}%)"
        )

    # Sanity check vs prior full run (took_avg=40.83 ms/call * 30 = 1224.9 ms/coverage)
    prior_d_per_30 = 40.83 * 30
    print()
    print("=" * 90)
    print("SANITY CHECK  (D_td serial vs prior profile_es_query.py run)")
    print("=" * 90)
    print(f"  Prior run all-30 took (profile=OFF): {prior_d_per_30:>9.2f} ms/coverage")
    print(f"  This run D_td serial took         : {d_td:>9.2f} ms/coverage")
    if prior_d_per_30 > 0:
        drift_pct = abs(d_td - prior_d_per_30) / prior_d_per_30 * 100
        print(f"  Drift                              : {drift_pct:>8.1f}%")


if __name__ == "__main__":
    main()
