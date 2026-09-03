#!/usr/bin/env python3
"""Build solver topology + task workload from the raw_data telemetry dump.

Reads ~/Downloads/raw_data/{cpu_alloc,pod_reqs,cpu_var,bw}.csv and emits the
nodes/edges/tasks JSONL trio that scheduler.load_info expects.

Mapping (per raw_data/README.md):
    cpu_available[n] = allocatable_millicpu_cores[n]
                     - resource_requests_millicpu_units[n]
                     - cpu_usage_millicores[n]

So the *static* part becomes the solver node capacity and the *telemetry* part
(cpu_usage_millicores) becomes `used_cpu`, refreshed each epoch from the
sketch/ES quantile query:

    cpu_capacity[n]    = (allocatable[n] - pod_reqs[n]) / 1000        cores
    used_cpu[n]        = quantile(cpu_usage_millicores) / 1000        cores
    memory_capacity[n] = max(memory_available[n]) / 1e9               GB
    used_memory[n]     = memory_capacity[n] - quantile(mem_available) GB

Topology comes from bw.csv. The MILP's path constraint assumes a *unique* path
between any node pair, which only holds on a tree, so we take the maximum
spanning tree of the observed graph weighted by median link bandwidth.
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import math
import random
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_DIR = Path.home() / "Downloads" / "raw_data"
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "raw_topology"

# Nodes present in cpu_alloc.csv but with no telemetry in cpu_var.csv are
# dropped: the solver would have no way to refresh their usage.
BITS_PER_MBIT = 1e6
MILLI = 1000.0
BYTES_PER_GB = 1e9


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--seed", type=int, default=20260903)
    p.add_argument(
        "--num-tasks", type=int, default=300,
        help="Number of tasks to generate.",
    )
    p.add_argument(
        "--oversubscribe", type=float, default=1.5,
        help=(
            "Total task CPU demand as a multiple of cluster free CPU at the p50 "
            "baseline. >1 keeps the solver at the feasibility boundary, which is "
            "where the telemetry source can actually change the outcome."
        ),
    )
    p.add_argument(
        "--comm-fraction", type=float, default=0.25,
        help="Fraction of tasks that get a communication peer.",
    )
    p.add_argument("--duration-min-s", type=float, default=180.0)
    p.add_argument("--duration-max-s", type=float, default=900.0)
    return p.parse_args()


def read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, max(0, int(q * (len(sorted_vals) - 1))))]


def build_nodes(raw_dir: Path) -> tuple[dict[str, dict], dict[str, float]]:
    """Return (nodes, baseline_p50_cpu_cores_used)."""
    alloc = {r["scenario_node"]: int(r["allocatable_millicpu_cores"]) for r in read_csv(raw_dir / "cpu_alloc.csv")}
    reqs = {r["scenario_node"]: int(r["resource_requests_millicpu_units"]) for r in read_csv(raw_dir / "pod_reqs.csv")}

    cpu_by_node: dict[str, list[float]] = collections.defaultdict(list)
    mem_by_node: dict[str, list[int]] = collections.defaultdict(list)
    for r in read_csv(raw_dir / "cpu_var.csv"):
        cpu_by_node[r["scenario_node"]].append(float(r["cpu_usage_millicores"]))
        mem_by_node[r["scenario_node"]].append(int(r["memory_available"]))

    nodes: dict[str, dict] = {}
    baseline_used: dict[str, float] = {}
    for node_id in sorted(cpu_by_node, key=lambda n: (n.split("-")[0], int(n.split("-")[1]))):
        if node_id not in alloc:
            continue
        cpu_capacity = (alloc[node_id] - reqs.get(node_id, 0)) / MILLI
        memory_capacity = max(mem_by_node[node_id]) / BYTES_PER_GB
        nodes[node_id] = {
            "node_id": node_id,
            "cpu_capacity": round(cpu_capacity, 4),
            "memory_capacity": round(memory_capacity, 4),
            "network_capacity": None,
            "used_cpu": 0.0,
            "used_memory": 0.0,
            "used_network": 0.0,
        }
        baseline_used[node_id] = quantile(sorted(cpu_by_node[node_id]), 0.5) / MILLI
    return nodes, baseline_used


def build_spanning_tree(raw_dir: Path, node_ids: set[str]) -> list[dict]:
    """Maximum spanning tree of bw.csv restricted to `node_ids`, capacity in Mbps."""
    samples: dict[tuple[str, str], list[float]] = collections.defaultdict(list)
    for r in read_csv(raw_dir / "bw.csv"):
        a, b = r["edge"].split(",")
        if a not in node_ids or b not in node_ids or a == b:
            continue
        samples[tuple(sorted((a, b)))].append(float(r["bandwidth"]))

    # Median bandwidth per link, in Mbps.
    weighted = sorted(
        ((quantile(sorted(v), 0.5) / BITS_PER_MBIT, k) for k, v in samples.items()),
        key=lambda t: -t[0],
    )

    parent = {n: n for n in node_ids}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    edges: list[dict] = []
    for capacity, (a, b) in weighted:
        ra, rb = find(a), find(b)
        if ra == rb:
            continue
        parent[ra] = rb
        edges.append({"edge_id": [a, b], "capacity": round(capacity, 3), "used_bandwidth": 0.0})

    components = len({find(n) for n in node_ids})
    if components != 1:
        raise RuntimeError(
            f"bw.csv does not connect all {len(node_ids)} telemetry nodes "
            f"({components} components). Cannot build a spanning tree."
        )
    return edges


def rayleigh(rng: random.Random, mode: float) -> float:
    """Rayleigh sample with the given mode (sigma)."""
    return mode * math.sqrt(-2.0 * math.log(1.0 - rng.random()))


def zipf_bandwidth(rng: random.Random, centre_mbps: float, alpha: float = 1.6) -> float:
    """Zipf-ish bandwidth draw centred near `centre_mbps`."""
    k = 1
    while k < 64 and rng.random() < 1.0 / (k + 1) ** (1.0 / alpha):
        k += 1
    return centre_mbps * k


def build_tasks(
    args: argparse.Namespace,
    nodes: dict[str, dict],
    baseline_used: dict[str, float],
    edges: list[dict],
) -> tuple[list[dict], dict[str, float]]:
    rng = random.Random(args.seed)

    free_cpu = sum(max(n["cpu_capacity"] - baseline_used[nid], 0.0) for nid, n in nodes.items())
    free_mem = sum(n["memory_capacity"] for n in nodes.values())

    # Rayleigh mean is sigma*sqrt(pi/2); pick sigma so the expected total demand
    # hits the requested oversubscription of free capacity.
    target_cpu_mean = free_cpu * args.oversubscribe / args.num_tasks
    cpu_mode = target_cpu_mean / math.sqrt(math.pi / 2.0)
    target_mem_mean = free_mem * args.oversubscribe / args.num_tasks
    mem_mode = target_mem_mean / math.sqrt(math.pi / 2.0)

    # Never ask for more than the largest node can offer, or the task is
    # unassignable for reasons that have nothing to do with telemetry.
    max_cpu = max(n["cpu_capacity"] for n in nodes.values())
    max_mem = max(n["memory_capacity"] for n in nodes.values())

    median_link = sorted(e["capacity"] for e in edges)[len(edges) // 2]
    comm_centre = median_link * 0.02  # ~2% of a typical link per talking pair

    tasks: list[dict] = []
    for i in range(args.num_tasks):
        tasks.append(
            {
                "task_id": f"T{i:04d}",
                "arrival_offset_s": 0,
                "duration_s": round(rng.uniform(args.duration_min_s, args.duration_max_s), 3),
                "initial_cpu": round(min(rayleigh(rng, cpu_mode), max_cpu), 6),
                "initial_memory": round(min(rayleigh(rng, mem_mode), max_mem), 6),
                "peer_bandwidths": {},
            }
        )

    n_comm = int(args.comm_fraction * len(tasks)) // 2 * 2
    pool = rng.sample(range(len(tasks)), n_comm)
    for a, b in zip(pool[0::2], pool[1::2]):
        bw = round(zipf_bandwidth(rng, comm_centre), 4)
        tasks[a]["peer_bandwidths"][tasks[b]["task_id"]] = bw

    stats = {
        "free_cpu_cores_p50": free_cpu,
        "total_mem_gb": free_mem,
        "demand_cpu_cores": sum(t["initial_cpu"] for t in tasks),
        "demand_mem_gb": sum(t["initial_memory"] for t in tasks),
        "cpu_mode": cpu_mode,
        "mem_mode": mem_mode,
        "comm_pairs": n_comm // 2,
        "median_link_mbps": median_link,
    }
    return tasks, stats


def write_jsonl(path: Path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def main() -> None:
    args = parse_args()

    nodes, baseline_used = build_nodes(args.raw_dir)
    edges = build_spanning_tree(args.raw_dir, set(nodes))
    tasks, stats = build_tasks(args, nodes, baseline_used, edges)

    write_jsonl(args.out_dir / "nodes.jsonl", nodes.values())
    write_jsonl(args.out_dir / "edges.jsonl", edges)
    write_jsonl(args.out_dir / "tasks.jsonl", tasks)
    (args.out_dir / "node_ids.json").write_text(json.dumps(sorted(nodes), indent=2))

    by_prefix = collections.Counter(n.split("-")[0] for n in nodes)
    print(f"nodes: {len(nodes)}  {dict(by_prefix)}")
    caps = collections.Counter(n["cpu_capacity"] for n in nodes.values())
    print(f"  cpu_capacity (cores):    {dict(sorted(caps.items()))}")
    print(f"  memory_capacity (GB):    {min(n['memory_capacity'] for n in nodes.values()):.2f}"
          f" .. {max(n['memory_capacity'] for n in nodes.values()):.2f}")
    print(f"edges: {len(edges)} (spanning tree)  capacity Mbps "
          f"{min(e['capacity'] for e in edges):.1f} .. {max(e['capacity'] for e in edges):.1f}")
    print(f"tasks: {len(tasks)}  comm pairs={stats['comm_pairs']}")
    print(f"  cluster free CPU @p50 baseline: {stats['free_cpu_cores_p50']:.1f} cores")
    print(f"  total task CPU demand:          {stats['demand_cpu_cores']:.1f} cores "
          f"({stats['demand_cpu_cores'] / stats['free_cpu_cores_p50']:.2f}x)")
    print(f"  cluster memory:                 {stats['total_mem_gb']:.1f} GB")
    print(f"  total task memory demand:       {stats['demand_mem_gb']:.1f} GB "
          f"({stats['demand_mem_gb'] / stats['total_mem_gb']:.2f}x)")
    print(f"wrote {args.out_dir}/{{nodes,edges,tasks}}.jsonl")


if __name__ == "__main__":
    main()
