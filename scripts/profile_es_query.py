#!/usr/bin/env python3
"""Profile Elasticsearch ingest and query time for the rtt_sweep workload.

Each run:
  1. Resets the cluster-metrics index.
  2. Ingests epochs 1..N (default 5) x rows_per_epoch (default 1M) via _bulk,
     capturing per-batch wall-clock + ES `took`, and per-epoch
     /_stats deltas (indexing / refresh / merges / flush).
  3. For each epoch, runs two query groups:
        - "all":    all 30 nodes, sequential _search calls
        - "single": only N001
     each with profile=true (instrumented) and once without (control),
     to measure profile overhead.
  4. Writes 4 CSVs and prints a text summary.
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
    iter_batches,
    parse_nodes_config,
    reset_es_index,
)


# ---------------------------------------------------------------------------
# ES helpers
# ---------------------------------------------------------------------------

def fetch_index_stats(es_url: str, es_index: str, headers: dict) -> dict:
    r = requests.get(f"{es_url}/{es_index}/_stats", headers=headers, timeout=(5, 30))
    r.raise_for_status()
    return r.json()["_all"]["total"]


def stat_delta(before: dict, after: dict, section: str, key: str) -> int:
    return after.get(section, {}).get(key, 0) - before.get(section, {}).get(key, 0)


def bulk_once(es_url: str, es_index: str, headers: dict, batch: List[dict]) -> Tuple[float, int]:
    lines = []
    for row in batch:
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(row))
    payload = "\n".join(lines) + "\n"
    t0 = time.perf_counter()
    r = requests.post(
        f"{es_url}/{es_index}/_bulk",
        headers=headers,
        data=payload,
        timeout=(5, 120),
    )
    wall_ms = (time.perf_counter() - t0) * 1000.0
    r.raise_for_status()
    body = r.json()
    if body.get("errors"):
        raise RuntimeError("bulk reported errors")
    return wall_ms, int(body.get("took", 0))


def build_search_payload(node: str, epoch: int) -> dict:
    """Identical to query_es_nodes() in rtt_sweep_common.py (epoch variant)."""
    return {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"node": node}},
                    {"term": {"epoch": epoch}},
                ]
            }
        },
        "aggs": {
            "cpu_pct": {"percentiles": {"field": "cpu", "percents": [0, 50, 90, 100]}},
            "mem_pct": {"percentiles": {"field": "mem", "percents": [0, 50, 90, 100]}},
            "net_pct": {"percentiles": {"field": "net", "percents": [0, 50, 90, 100]}},
            "cpu_sum": {"sum": {"field": "cpu"}},
            "mem_sum": {"sum": {"field": "mem"}},
            "net_sum": {"sum": {"field": "net"}},
        },
    }


def search_once(
    es_url: str, es_index: str, headers: dict, payload: dict, profile: bool
) -> Tuple[float, dict]:
    body = dict(payload)
    if profile:
        body["profile"] = True
    t0 = time.perf_counter()
    r = requests.post(
        f"{es_url}/{es_index}/_search", headers=headers, json=body, timeout=(5, 60)
    )
    wall_ms = (time.perf_counter() - t0) * 1000.0
    r.raise_for_status()
    return wall_ms, r.json()


# ---------------------------------------------------------------------------
# Profile JSON parsing
# ---------------------------------------------------------------------------

def sum_query_ns(node: dict) -> int:
    total = int(node.get("time_in_nanos", 0))
    for child in node.get("children") or []:
        total += sum_query_ns(child)
    return total


def parse_profile(profile_json: dict) -> Tuple[int, int, int, List[dict]]:
    """Aggregate profile across all shards.

    Returns (query_ns, collector_ns, aggs_total_ns, agg_rows) where each
    agg_row is {name, time_ns, build_ns, collect_ns, post_ns, reduce_ns}.
    """
    query_ns = 0
    collector_ns = 0
    aggs_total = 0
    agg_acc: Dict[str, Dict[str, int]] = {}

    for shard in profile_json.get("shards", []) or []:
        for search in shard.get("searches", []) or []:
            for q in search.get("query", []) or []:
                query_ns += sum_query_ns(q)
            for c in search.get("collector", []) or []:
                collector_ns += int(c.get("time_in_nanos", 0))
                for cc in c.get("children") or []:
                    collector_ns += int(cc.get("time_in_nanos", 0))
        for agg in shard.get("aggregations", []) or []:
            name = agg.get("description") or agg.get("type") or "unknown"
            t = int(agg.get("time_in_nanos", 0))
            br = agg.get("breakdown") or {}
            slot = agg_acc.setdefault(
                name,
                {"time_ns": 0, "build_ns": 0, "collect_ns": 0, "post_ns": 0, "reduce_ns": 0},
            )
            slot["time_ns"] += t
            slot["build_ns"] += int(br.get("build_aggregation", 0))
            slot["collect_ns"] += int(br.get("collect", 0))
            slot["post_ns"] += int(br.get("post_collection", 0))
            slot["reduce_ns"] += int(br.get("reduce", 0))
            aggs_total += t

    agg_rows = [{"name": n, **vals} for n, vals in agg_acc.items()]
    return query_ns, collector_ns, aggs_total, agg_rows


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def percentile(sorted_values: List[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = max(0, min(len(sorted_values) - 1, int(round(q * (len(sorted_values) - 1)))))
    return sorted_values[idx]


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--es-url", default=DEFAULT_ES_URL)
    ap.add_argument("--es-index", default=DEFAULT_ES_INDEX)
    ap.add_argument("--es-api-key", default=DEFAULT_ES_API_KEY)
    ap.add_argument("--epochs", type=int, default=5, help="Profile epochs 1..N")
    ap.add_argument("--rows-per-epoch", type=int, default=1_000_000)
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--nodes-config",
        default="single_node_server/network-control-server/server-config.yaml",
    )
    ap.add_argument("--single-node", default="N001")
    ap.add_argument("--out-dir", default="data")
    args = ap.parse_args()

    headers = es_headers(args.es_api_key)
    nodes = parse_nodes_config(args.nodes_config)
    out_dir = REPO_ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[setup] es={args.es_url} index={args.es_index} nodes={len(nodes)}", flush=True)
    print(f"[setup] resetting index", flush=True)
    reset_es_index(args.es_url, args.es_index, args.es_api_key, 5.0, 30.0)

    rng = random.Random(args.seed)

    # ----------------- INGEST -----------------
    ingest_batch_rows: List[dict] = []
    ingest_epoch_rows: List[dict] = []

    for epoch in range(1, args.epochs + 1):
        before = fetch_index_stats(args.es_url, args.es_index, headers)
        epoch_t0 = time.perf_counter()
        epoch_took = 0
        bidx = 0
        for batch in iter_batches(
            args.rows_per_epoch, nodes, rng, args.batch_size, epoch=epoch
        ):
            wall_ms, took_ms = bulk_once(args.es_url, args.es_index, headers, batch)
            epoch_took += took_ms
            ingest_batch_rows.append(
                {
                    "epoch": epoch,
                    "batch_idx": bidx,
                    "rows": len(batch),
                    "wall_ms": f"{wall_ms:.3f}",
                    "took_ms": took_ms,
                }
            )
            bidx += 1
        # refresh so newly-indexed docs are visible to queries
        requests.post(
            f"{args.es_url}/{args.es_index}/_refresh",
            headers=headers,
            timeout=(5, 60),
        ).raise_for_status()
        epoch_wall = (time.perf_counter() - epoch_t0) * 1000.0
        after = fetch_index_stats(args.es_url, args.es_index, headers)

        row = {
            "epoch": epoch,
            "rows": args.rows_per_epoch,
            "wall_total_ms": f"{epoch_wall:.3f}",
            "took_sum_ms": epoch_took,
            "d_index_total": stat_delta(before, after, "indexing", "index_total"),
            "d_index_time_ms": stat_delta(before, after, "indexing", "index_time_in_millis"),
            "d_refresh_total": stat_delta(before, after, "refresh", "total"),
            "d_refresh_time_ms": stat_delta(before, after, "refresh", "total_time_in_millis"),
            "d_merge_total": stat_delta(before, after, "merges", "total"),
            "d_merge_time_ms": stat_delta(before, after, "merges", "total_time_in_millis"),
            "d_merge_bytes": stat_delta(before, after, "merges", "total_size_in_bytes"),
            "d_flush_total": stat_delta(before, after, "flush", "total"),
            "d_flush_time_ms": stat_delta(before, after, "flush", "total_time_in_millis"),
        }
        ingest_epoch_rows.append(row)
        print(
            f"[ingest] epoch {epoch}: wall={epoch_wall:.0f}ms  took_sum={epoch_took}ms  "
            f"index={row['d_index_time_ms']}ms  refresh={row['d_refresh_time_ms']}ms  "
            f"merge={row['d_merge_time_ms']}ms  flush={row['d_flush_time_ms']}ms",
            flush=True,
        )

    # ----------------- QUERY -----------------
    query_call_rows: List[dict] = []
    query_agg_rows: List[dict] = []

    def run_group(group: str, target_nodes: List[str], epoch: int, profile_on: bool) -> float:
        t0 = time.perf_counter()
        for node in target_nodes:
            payload = build_search_payload(node, epoch)
            wall_ms, js = search_once(args.es_url, args.es_index, headers, payload, profile_on)
            took_ms = int(js.get("took", 0))
            if profile_on:
                q_ns, c_ns, a_ns, agg_rows = parse_profile(js.get("profile", {}) or {})
            else:
                q_ns = c_ns = a_ns = 0
                agg_rows = []
            query_call_rows.append(
                {
                    "group": group,
                    "epoch": epoch,
                    "node": node,
                    "profile": int(profile_on),
                    "wall_ms": f"{wall_ms:.3f}",
                    "took_ms": took_ms,
                    "query_ns": q_ns,
                    "collector_ns": c_ns,
                    "aggs_ns": a_ns,
                }
            )
            for ar in agg_rows:
                query_agg_rows.append(
                    {
                        "group": group,
                        "epoch": epoch,
                        "node": node,
                        "agg_name": ar["name"],
                        "time_ns": ar["time_ns"],
                        "build_ns": ar["build_ns"],
                        "collect_ns": ar["collect_ns"],
                        "post_ns": ar["post_ns"],
                        "reduce_ns": ar["reduce_ns"],
                    }
                )
        return (time.perf_counter() - t0) * 1000.0

    for epoch in range(1, args.epochs + 1):
        for profile_on in (True, False):
            tag = "prof" if profile_on else "ctrl"
            all_ms = run_group("all", nodes, epoch, profile_on)
            single_ms = run_group("single", [args.single_node], epoch, profile_on)
            print(
                f"[query] epoch {epoch} {tag}: all{len(nodes)}={all_ms:.0f}ms  "
                f"single1={single_ms:.0f}ms",
                flush=True,
            )

    # ----------------- WRITE CSVs -----------------
    write_csv(
        out_dir / "es_profile_ingest_batches.csv",
        ingest_batch_rows,
        ["epoch", "batch_idx", "rows", "wall_ms", "took_ms"],
    )
    write_csv(
        out_dir / "es_profile_ingest_epoch.csv",
        ingest_epoch_rows,
        list(ingest_epoch_rows[0].keys()),
    )
    write_csv(
        out_dir / "es_profile_query_calls.csv",
        query_call_rows,
        ["group", "epoch", "node", "profile", "wall_ms", "took_ms",
         "query_ns", "collector_ns", "aggs_ns"],
    )
    write_csv(
        out_dir / "es_profile_query_aggs.csv",
        query_agg_rows,
        ["group", "epoch", "node", "agg_name", "time_ns",
         "build_ns", "collect_ns", "post_ns", "reduce_ns"],
    )

    # ----------------- TEXT SUMMARY -----------------
    print()
    print("=" * 78)
    print("INGEST SUMMARY  (per epoch, all values in ms unless noted)")
    print("=" * 78)
    print(f"{'epoch':>5} {'rows':>9} {'wall':>8} {'took_sum':>9} "
          f"{'index':>8} {'refresh':>8} {'merge':>8} {'flush':>8} {'merge_MB':>9}")
    for r in ingest_epoch_rows:
        print(
            f"{r['epoch']:>5} {r['rows']:>9} "
            f"{float(r['wall_total_ms']):>8.0f} {r['took_sum_ms']:>9} "
            f"{r['d_index_time_ms']:>8} {r['d_refresh_time_ms']:>8} "
            f"{r['d_merge_time_ms']:>8} {r['d_flush_time_ms']:>8} "
            f"{r['d_merge_bytes']/1e6:>9.1f}"
        )
    print("  Notes: 'wall' = client wall-clock for ingest+refresh of the epoch;")
    print("         'took_sum' = sum of _bulk response 'took' values;")
    print("         index/refresh/merge/flush = /_stats deltas (server-side ms).")

    print()
    print("=" * 78)
    print("QUERY SUMMARY  (one row per HTTP call; profile=ON instrumented, OFF=control)")
    print("=" * 78)

    def summarize(rows: List[dict]) -> dict:
        wall = sorted(float(r["wall_ms"]) for r in rows)
        took = sorted(int(r["took_ms"]) for r in rows)
        q_ns = sorted(int(r["query_ns"]) for r in rows)
        c_ns = sorted(int(r["collector_ns"]) for r in rows)
        a_ns = sorted(int(r["aggs_ns"]) for r in rows)
        return {
            "n": len(rows),
            "wall_avg": sum(wall) / len(wall),
            "wall_p50": percentile(wall, 0.5),
            "wall_p90": percentile(wall, 0.9),
            "took_avg": sum(took) / len(took),
            "q_ms_avg": sum(q_ns) / len(q_ns) / 1e6,
            "c_ms_avg": sum(c_ns) / len(c_ns) / 1e6,
            "a_ms_avg": sum(a_ns) / len(a_ns) / 1e6,
        }

    for group in ("all", "single"):
        for prof in (1, 0):
            sub = [r for r in query_call_rows
                   if r["group"] == group and r["profile"] == prof]
            if not sub:
                continue
            s = summarize(sub)
            tag = "profile=ON " if prof else "profile=OFF"
            print(
                f"  {group:<6} {tag} n={s['n']:>3}  "
                f"wall avg={s['wall_avg']:.2f}ms p50={s['wall_p50']:.2f} p90={s['wall_p90']:.2f}  "
                f"took avg={s['took_avg']:.2f}ms"
            )
            if prof:
                print(
                    f"          server-side avg:  query={s['q_ms_avg']:.3f}ms  "
                    f"collector={s['c_ms_avg']:.3f}ms  aggs={s['a_ms_avg']:.3f}ms"
                )

    print()
    print("AGG BREAKDOWN  (avg ms per call, profile=ON only)")
    print(f"  {'group':<7} {'agg':<8} {'n':>4} {'time':>8} {'build':>8} "
          f"{'collect':>8} {'post':>8} {'reduce':>8}")
    by_agg: Dict[Tuple[str, str], List[dict]] = {}
    for r in query_agg_rows:
        by_agg.setdefault((r["group"], r["agg_name"]), []).append(r)
    for (group, name), rows in sorted(by_agg.items()):
        n = len(rows)
        def avg(k: str) -> float:
            return sum(int(r[k]) for r in rows) / n / 1e6
        print(
            f"  {group:<7} {name:<8} {n:>4} "
            f"{avg('time_ns'):>8.3f} {avg('build_ns'):>8.3f} "
            f"{avg('collect_ns'):>8.3f} {avg('post_ns'):>8.3f} {avg('reduce_ns'):>8.3f}"
        )

    print()
    print(f"CSVs written to {out_dir}/")
    print("  es_profile_ingest_batches.csv  es_profile_ingest_epoch.csv")
    print("  es_profile_query_calls.csv     es_profile_query_aggs.csv")


if __name__ == "__main__":
    main()
