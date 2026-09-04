#!/usr/bin/env python3
"""Build solver topology + task workload from the raw_data telemetry dump.

Reads $RAW_DATA_DIR (default ~/raw_data, else ~/Downloads/raw_data)
{cpu_alloc,pod_reqs,cpu_var,bw}.csv and emits the
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

Workload (synthetic -- raw_data/ has no task-level data)
-------------------------------------------------------
Tasks arrive on a rolling schedule instead of all landing at epoch 0, so the
scheduler faces a persistent backlog rather than draining in a few epochs:

    arrivals per epoch  = --tasks-per-epoch, spread uniformly over
                          --arrival-epochs * --epoch-length-s seconds
    offered load        = --load-factor, as a fraction of cluster free CPU:
                          mean_task_cpu * tasks_per_epoch * mean_lifetime_epochs
                                                    = load_factor * free_cpu

`load_factor` slightly below 1 is the interesting regime: the cluster runs near
saturation, so a wrong per-node usage estimate flips marginal placements, but
the pending queue stays small enough for the MILP to prove optimality inside
its time limit (a queue of ~300 does not -- see `--solver-time-limit-s`).

CPU is deliberately the binding resource. In this trace the background load
leaves ~90% of CPU and ~99.9% of memory free, and the sketch/ES estimates
diverge by ~40% on CPU versus ~0.1% on memory, so memory demand is sized by a
separate, slacker `--memory-load-factor`.
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import math
import os
import random
import statistics
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "raw_topology"

# Nodes present in cpu_alloc.csv but with no telemetry in cpu_var.csv are
# dropped: the solver would have no way to refresh their usage.
BITS_PER_MBIT = 1e6
MILLI = 1000.0
BYTES_PER_GB = 1e9


def _default_raw_dir() -> Path:
    """First existing of $RAW_DATA_DIR, ~/raw_data, ~/Downloads/raw_data."""
    candidates = []
    env = os.environ.get("RAW_DATA_DIR")
    if env:
        candidates.append(Path(env))
    candidates += [Path.home() / "raw_data", Path.home() / "Downloads" / "raw_data"]
    for c in candidates:
        if c.is_dir():
            return c
    return candidates[-1]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-dir", type=Path, default=_default_raw_dir())
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--seed", type=int, default=20260903)
    p.add_argument("--epoch-length-s", type=float, default=300.0,
                   help="Must match run_raw_data_assignment.py --epoch-length-s.")
    p.add_argument(
        "--arrival-epochs", type=int, default=40,
        help="Arrivals are spread uniformly over this many epochs.",
    )
    p.add_argument(
        "--tasks-per-epoch", type=float, default=60.0,
        help="Mean arrivals per epoch. Total tasks = this * --arrival-epochs.",
    )
    p.add_argument(
        "--load-factor", type=float, default=0.95,
        help=(
            "Offered CPU load as a fraction of cluster free CPU at the p50 "
            "baseline, accounting for task lifetime. Just under 1 keeps the "
            "cluster near saturation with a small pending queue."
        ),
    )
    p.add_argument(
        "--memory-load-factor", type=float, default=0.6,
        help="Same, for memory. Slacker than CPU so CPU stays the binding resource.",
    )
    p.add_argument(
        "--comm-fraction", type=float, default=0.25,
        help="Fraction of tasks that get a communication peer.",
    )
    p.add_argument("--duration-min-s", type=float, default=180.0)
    p.add_argument("--duration-max-s", type=float, default=900.0)
    p.add_argument(
        "--max-task-cpu-frac", type=float, default=1.0,
        help=(
            "Clip task CPU at this fraction of the largest node's capacity. "
            "Tasks above it would be unassignable for reasons unrelated to telemetry."
        ),
    )
    return p.parse_args()


def read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, max(0, int(q * (len(sorted_vals) - 1))))]


def build_nodes(raw_dir: Path) -> tuple[dict[str, dict], dict[str, float], dict[str, float]]:
    """Return (nodes, baseline_p50_cpu_cores_used, baseline_p50_mem_gb_free)."""
    alloc = {r["scenario_node"]: int(r["allocatable_millicpu_cores"]) for r in read_csv(raw_dir / "cpu_alloc.csv")}
    reqs = {r["scenario_node"]: int(r["resource_requests_millicpu_units"]) for r in read_csv(raw_dir / "pod_reqs.csv")}

    cpu_by_node: dict[str, list[float]] = collections.defaultdict(list)
    mem_by_node: dict[str, list[int]] = collections.defaultdict(list)
    for r in read_csv(raw_dir / "cpu_var.csv"):
        cpu_by_node[r["scenario_node"]].append(float(r["cpu_usage_millicores"]))
        mem_by_node[r["scenario_node"]].append(int(r["memory_available"]))

    nodes: dict[str, dict] = {}
    baseline_used: dict[str, float] = {}
    baseline_free_mem: dict[str, float] = {}
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
        baseline_free_mem[node_id] = quantile(sorted(mem_by_node[node_id]), 0.5) / BYTES_PER_GB
    return nodes, baseline_used, baseline_free_mem


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
    baseline_free_mem: dict[str, float],
    edges: list[dict],
) -> tuple[list[dict], dict[str, float]]:
    rng = random.Random(args.seed)

    free_cpu = sum(max(n["cpu_capacity"] - baseline_used[nid], 0.0) for nid, n in nodes.items())
    free_mem = sum(baseline_free_mem[nid] for nid in nodes)

    num_tasks = max(1, int(round(args.tasks_per_epoch * args.arrival_epochs)))
    arrival_span_s = args.arrival_epochs * args.epoch_length_s

    # A task occupies its node for ceil(duration / epoch_length) epochs, which is
    # what the simulation charges it, so the lifetime that consumes capacity is
    # the *rounded-up* one -- size demand against that or the cluster ends up
    # over-committed relative to the requested load factor.
    span = args.duration_max_s - args.duration_min_s
    grid = [args.duration_min_s + span * i / 999.0 for i in range(1000)]
    lifetime_epochs = statistics.fmean(math.ceil(d / args.epoch_length_s) for d in grid)

    # Offered load = mean_demand * arrivals_per_epoch * lifetime_epochs.
    target_cpu_mean = args.load_factor * free_cpu / (args.tasks_per_epoch * lifetime_epochs)
    target_mem_mean = args.memory_load_factor * free_mem / (args.tasks_per_epoch * lifetime_epochs)

    # Rayleigh mean is sigma*sqrt(pi/2); solve for the sigma that hits the target.
    cpu_mode = target_cpu_mean / math.sqrt(math.pi / 2.0)
    mem_mode = target_mem_mean / math.sqrt(math.pi / 2.0)

    # Never ask for more than the largest node can offer, or the task is
    # unassignable for reasons that have nothing to do with telemetry.
    max_cpu = max(n["cpu_capacity"] for n in nodes.values()) * args.max_task_cpu_frac
    max_mem = max(n["memory_capacity"] for n in nodes.values())
    median_node_cpu = statistics.median(n["cpu_capacity"] for n in nodes.values())

    median_link = sorted(e["capacity"] for e in edges)[len(edges) // 2]
    comm_centre = median_link * 0.02  # ~2% of a typical link per talking pair

    tasks: list[dict] = []
    for i in range(num_tasks):
        tasks.append(
            {
                "task_id": f"T{i:05d}",
                "arrival_offset_s": round(rng.uniform(0.0, arrival_span_s), 3),
                "duration_s": round(rng.uniform(args.duration_min_s, args.duration_max_s), 3),
                "initial_cpu": round(min(rayleigh(rng, cpu_mode), max_cpu), 6),
                "initial_memory": round(min(rayleigh(rng, mem_mode), max_mem), 6),
                "peer_bandwidths": {},
            }
        )
    tasks.sort(key=lambda t: t["arrival_offset_s"])
    for i, t in enumerate(tasks):
        t["task_id"] = f"T{i:05d}"

    n_comm = int(args.comm_fraction * len(tasks)) // 2 * 2
    pool = rng.sample(range(len(tasks)), n_comm)
    for a, b in zip(pool[0::2], pool[1::2]):
        bw = round(zipf_bandwidth(rng, comm_centre), 4)
        tasks[a]["peer_bandwidths"][tasks[b]["task_id"]] = bw

    cpus = sorted(t["initial_cpu"] for t in tasks)
    stats = {
        "free_cpu_cores_p50": free_cpu,
        "free_mem_gb_p50": free_mem,
        "num_tasks": len(tasks),
        "arrival_span_s": arrival_span_s,
        "lifetime_epochs": lifetime_epochs,
        "offered_cpu_per_epoch": sum(t["initial_cpu"] for t in tasks) / args.arrival_epochs
        * lifetime_epochs,
        "offered_mem_per_epoch": sum(t["initial_memory"] for t in tasks) / args.arrival_epochs
        * lifetime_epochs,
        "demand_cpu_cores": sum(t["initial_cpu"] for t in tasks),
        "demand_mem_gb": sum(t["initial_memory"] for t in tasks),
        "cpu_mean": statistics.fmean(cpus),
        "cpu_p50": quantile(cpus, 0.5),
        "cpu_p95": quantile(cpus, 0.95),
        "cpu_max": cpus[-1],
        "frac_needing_large_node": sum(c > median_node_cpu for c in cpus) / len(cpus),
        "median_node_cpu": median_node_cpu,
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

    nodes, baseline_used, baseline_free_mem = build_nodes(args.raw_dir)
    edges = build_spanning_tree(args.raw_dir, set(nodes))
    tasks, stats = build_tasks(args, nodes, baseline_used, baseline_free_mem, edges)

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

    print(f"cluster free @p50 baseline: {stats['free_cpu_cores_p50']:.1f} CPU cores, "
          f"{stats['free_mem_gb_p50']:.1f} GB memory")
    print(f"tasks: {stats['num_tasks']}  ({args.tasks_per_epoch:g}/epoch over "
          f"{args.arrival_epochs} epochs = {stats['arrival_span_s']:.0f}s of arrivals)")
    print(f"  mean lifetime:      {stats['lifetime_epochs']:.2f} epochs "
          f"(duration {args.duration_min_s:.0f}..{args.duration_max_s:.0f}s, "
          f"charged as whole epochs)")
    print(f"  task CPU (cores):   mean={stats['cpu_mean']:.3f} p50={stats['cpu_p50']:.3f} "
          f"p95={stats['cpu_p95']:.3f} max={stats['cpu_max']:.3f}")
    print(f"  need a > median ({stats['median_node_cpu']:.2f}-core) node: "
          f"{stats['frac_needing_large_node'] * 100:.1f}% of tasks")
    print(f"  offered CPU load:   {stats['offered_cpu_per_epoch']:.1f} core-epochs/epoch "
          f"= {stats['offered_cpu_per_epoch'] / stats['free_cpu_cores_p50']:.2f}x free CPU")
    print(f"  offered mem load:   {stats['offered_mem_per_epoch']:.1f} GB-epochs/epoch "
          f"= {stats['offered_mem_per_epoch'] / stats['free_mem_gb_p50']:.2f}x free memory")
    print(f"  comm pairs:         {stats['comm_pairs']}")
    print(f"wrote {args.out_dir}/{{nodes,edges,tasks}}.jsonl")


if __name__ == "__main__":
    main()
