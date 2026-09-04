#!/usr/bin/env python3
"""Task completion under approximate telemetry -- the paper's Fig. 8/9/10, on the raw_data cluster.

This is the experiment `run_raw_data_assignment.py` cannot do. That script asks
"does the telemetry source change the assignment?" and the answer is no -- the
paper says so itself (Fig. 9: Elasticsearch +14.3% vs the sketch layer +15.1%
over a static baseline, a <1% difference). The claim being tested here is the
other one: *dynamic* estimates beat *static* ones, and approximate dynamic
estimates capture that gain as well as exact ones do.

Three things make that measurable, all of which the assignment script lacks:

1. **A static baseline.** A controller whose resource estimates never refresh:
   node background load frozen at its epoch-0 reading, and every running task
   charged at its original *request* rather than its measured usage.

2. **Per-task telemetry.** The paper's update rule refreshes *running tasks*
   ("a running task's CPU/memory estimate is set to the p50 quantile, unless it
   approaches the current allocation, which triggers a 20% allocation
   increase"), keyed by task id. raw_data has no task-level data, so each
   running task emits a synthetic usage stream that drifts away from its
   request -- typically consuming less, occasionally bursting above it. A
   static controller cannot see either.

3. **Contention.** Placements have consequences: "constrained tasks experience
   a performance penalty, increasing their execution duration beyond their
   default duration proportionally to their excess resource demand." Each epoch
   a node's *true* load (exact background quantile + true task usage) is
   compared against capacity; tasks on an over-committed node make partial
   progress. Without this, an estimate can be arbitrarily wrong at no cost and
   the only thing left to measure is MILP tie-breaking noise.

Reassignments use the paper's parameters: gamma (`--gamma`, max reassignments,
paper 10) and lambda (`--lam`, penalty per reassignment, paper 1). A running
task offered for reassignment carries a dominant priority, so the solver keeps
it placed and only ever moves it; the `evicted` column should stay at zero and
is worth checking if a scenario looks anomalous.

Scenarios are independent trajectories over the same epochs and the same
telemetry, so differences between them are attributable to the estimator and
the update rule. Spec: `name:estimator:rule:gamma:lam`, e.g.
`dyn:sketch:p50:10:1`. Presets: `--figure 8|9|10|all`.

    estimator  static | sketch | es
    rule       request | p50 | p90 | avg | p50bump | window

Note that `avg` is an **oracle**, not a competitor: work delivered over an epoch
is usage x epoch length, so the contention model charges each task its *mean*
usage -- which is exactly what `avg` (sum/samples) reports. Its estimate error
is 0 by construction, and it bounds how much of any scenario gap is estimator
error at all. `window` averages the same statistic over the last
`--window-epochs` epochs, so it is stale rather than wrong, which is the paper's
"recent window averaging" baseline.

The MILP is bounded by a scheduling window (`--max-candidates`, the oldest N
pending tasks) and a reassignment-candidate cap (`--max-reassign-candidates`,
the running tasks on the most over-committed nodes). With an over-subscribed
workload the pending queue grows without limit, and an unbounded model stops
being solvable to proven optimality -- at which point scenario gaps become
solver search artefacts. Both caps apply identically to every scenario.

Task communication is omitted: tasks are handed to the MILP with zero
bandwidth demand, so the link-capacity constraint is inactive and placements are
decided purely on CPU/memory -- which is what the estimators estimate. It also
keeps the MILP small enough to solve to proven optimality, so a scenario gap is
not solver search noise.

Per the paper, control-loop overhead is deliberately excluded here: this
measures assignment quality, not latency (that is Fig. 4/7, i.e.
`run_raw_data_assignment.py`). Background telemetry is therefore subsampled by
default -- accuracy is what matters, not ingest cost.
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import math
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Sequence

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
from run_raw_data_assignment import (  # noqa: E402
    Telemetry,
    _default_raw_dir,
    _pick,
    epoch_values,
    load_telemetry,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SOLVER_ROOT = REPO_ROOT / "solver_experimental"
# This experiment needs a heavier workload than the latency one: see
# `warn_if_uncontended`. `raw_data_prep.py --load-factor 1.4` writes it.
_COMPLETION_TOPOLOGY = REPO_ROOT / "data" / "raw_topology_completion"
DEFAULT_TOPOLOGY_DIR = (_COMPLETION_TOPOLOGY if _COMPLETION_TOPOLOGY.is_dir()
                        else REPO_ROOT / "data" / "raw_topology")
SERVER_CONFIG = REPO_ROOT / "single_node_server/network-control-server/raw-data-full-config.yaml"

MILLI = 1000.0
BYTES_PER_GB = 1e9
PERCENTS = [50.0, 90.0]
# A running task offered for reassignment is worth far more than a new
# placement, so the solver keeps it unless that is impossible. `must_assign`
# would say this exactly, but a node whose *estimated* load already exceeds
# capacity leaves nowhere to put its tasks, making the model INFEASIBLE and
# forcing a relaxed retry that evicts them wholesale. A dominant priority gets
# the same behaviour without ever being infeasible.
#
# This does not distort lambda: the migration penalty is weighed against the
# priority of the *pending* tasks a move would let in (1.0 each), so lambda=1
# still means "move only if it admits more than one extra task", as in the paper.
RUNNING_PRIORITY = 100.0

ESTIMATORS = ("static", "sketch", "es")
RULES = ("request", "p50", "p90", "avg", "p50bump", "window")


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Scenario:
    name: str
    estimator: str
    rule: str
    gamma: int
    lam: float

    @property
    def dynamic(self) -> bool:
        return self.estimator != "static"

    @property
    def reassigns(self) -> bool:
        return self.gamma > 0


def parse_scenario(spec: str) -> Scenario:
    parts = spec.split(":")
    if len(parts) != 5:
        raise argparse.ArgumentTypeError(
            f"scenario '{spec}' must be name:estimator:rule:gamma:lam")
    name, estimator, rule, gamma, lam = parts
    if estimator not in ESTIMATORS:
        raise argparse.ArgumentTypeError(f"estimator must be one of {ESTIMATORS}")
    if rule not in RULES:
        raise argparse.ArgumentTypeError(f"rule must be one of {RULES}")
    if estimator == "static" and rule != "request":
        raise argparse.ArgumentTypeError("the static estimator only supports rule 'request'")
    if estimator != "static" and rule == "request":
        raise argparse.ArgumentTypeError("rule 'request' is the static estimator's rule")
    return Scenario(name, estimator, rule, int(gamma), float(lam))


def preset(figure: str, gamma: int, lam: float) -> List[Scenario]:
    """The scenario sets behind the paper's figures."""
    g, l = gamma, lam
    if figure == "8":
        # Static baseline vs dynamic vs reassignments, isolated and combined.
        return [
            Scenario("static", "static", "request", 0, 0.0),
            Scenario("dynamic", "sketch", "p50", 0, 0.0),
            Scenario("reassign", "static", "request", g, l),
            Scenario("dynamic+reassign", "sketch", "p50", g, l),
        ]
    if figure == "9":
        # Approximate vs exact percentiles, both against the static baseline.
        return [
            Scenario("static", "static", "request", g, l),
            Scenario("sketch", "sketch", "p50", g, l),
            Scenario("es", "es", "p50", g, l),
        ]
    if figure == "10":
        # Does the gain depend on a carefully chosen update rule?
        return [
            Scenario("static", "static", "request", g, l),
            Scenario("p50", "sketch", "p50", g, l),
            Scenario("p90", "sketch", "p90", g, l),
            Scenario("avg", "sketch", "avg", g, l),
            Scenario("p50-1.2xalloc", "sketch", "p50bump", g, l),
            Scenario("window-avg", "sketch", "window", g, l),
        ]
    raise ValueError(figure)


def all_presets(gamma: int, lam: float) -> List[Scenario]:
    seen: Dict[str, Scenario] = {}
    for fig in ("8", "9", "10"):
        for sc in preset(fig, gamma, lam):
            # Fig. 8's bare "static" (no reassignments) and Fig. 9/10's static
            # baseline (with them) are different scenarios; keep both.
            key = f"{sc.estimator}:{sc.rule}:{sc.gamma}:{sc.lam}"
            seen.setdefault(key, sc)
    return list(seen.values())


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--telemetry-csv", type=Path,
                   default=_default_raw_dir() / "synthetic_cpu_var.csv")
    p.add_argument("--topology-dir", type=Path, default=DEFAULT_TOPOLOGY_DIR)
    p.add_argument("--out-csv", type=str, default="data/raw_data_completion.csv")

    p.add_argument("--figure", type=str, default="all", choices=["8", "9", "10", "all"],
                   help="Scenario preset. Ignored if --scenario is given.")
    p.add_argument("--scenario", type=parse_scenario, action="append", default=None,
                   help="Explicit scenario, repeatable: name:estimator:rule:gamma:lam")
    p.add_argument("--gamma", type=int, default=10, help="Max reassignments (paper: 10).")
    p.add_argument("--lam", type=float, default=1.0, help="Reassignment penalty (paper: 1).")

    p.add_argument("--runs", type=int, default=1)
    p.add_argument("--epochs", type=int, default=30)
    p.add_argument("--epoch-length-s", type=float, default=300.0)
    p.add_argument("--seed", type=int, default=20260903)

    p.add_argument("--rows-per-epoch", type=int, default=100_000,
                   help="Background telemetry rows per epoch (subsampled; overhead is excluded here).")
    p.add_argument("--task-samples", type=int, default=200,
                   help="Usage samples emitted per running task per epoch.")
    p.add_argument("--epoch-jitter", type=float, default=0.02)

    # Per-task true-usage process: usage = request * base * drift * burst.
    p.add_argument("--usage-base-lo", type=float, default=0.50,
                   help="Per-task mean usage as a fraction of its request (low end).")
    p.add_argument("--usage-base-hi", type=float, default=0.95)
    p.add_argument("--usage-drift-sigma", type=float, default=0.15,
                   help="Lognormal sigma of a task's per-epoch usage drift.")
    p.add_argument("--usage-within-sigma", type=float, default=0.10,
                   help="Lognormal sigma of samples within one epoch.")
    p.add_argument("--burst-prob", type=float, default=0.10)
    p.add_argument("--burst-factor", type=float, default=1.6)
    p.add_argument("--window-epochs", type=int, default=3,
                   help="Window length for the 'window' update rule.")
    p.add_argument("--bump-threshold", type=float, default=0.90,
                   help="'p50bump': raise the allocation when p50 reaches this fraction of it.")
    p.add_argument("--bump-factor", type=float, default=1.20)

    p.add_argument(
        "--max-candidates", type=int, default=80,
        help=(
            "Scheduling window: only the oldest N pending tasks are candidates each "
            "epoch. Bounds the MILP -- with an over-subscribed workload the queue grows "
            "without limit, and an unbounded model stops being solvable to optimality."
        ),
    )
    p.add_argument(
        "--max-reassign-candidates", type=int, default=40,
        help=(
            "Cap on running tasks offered for reassignment, taken from the most "
            "over-committed nodes first -- where moving one actually helps. The rest "
            "stay pinned and are charged as node load."
        ),
    )
    p.add_argument("--solver-backend", type=str, choices=["CBC", "SCIP", "GLPK"], default="SCIP")
    p.add_argument("--solver-time-limit-s", type=float, default=60.0)
    p.add_argument("--mip-gap", type=float, default=0.0,
                   help="Relative MIP gap to stop at (0 = prove optimality).")

    p.add_argument("--server-url", type=str, default="http://localhost:10101")
    p.add_argument("--server-config", type=Path, default=SERVER_CONFIG,
                   help="Sketch server config (its `server.port` must match --server-url).")
    p.add_argument("--server-log", type=str, default="logs/server_raw_data_completion.log")
    p.add_argument("--server-ready-timeout", type=float, default=60.0)
    p.add_argument("--skip-server-start", action="store_true", default=False)
    p.add_argument("--es-url", type=str, default="http://localhost:9200")
    p.add_argument("--es-node-index", type=str, default="raw-cluster-metrics")
    p.add_argument("--es-task-index", type=str, default="raw-task-metrics")
    p.add_argument("--es-api-key", type=str, default=DEFAULT_ES_API_KEY)

    p.add_argument("--batch-size", type=int, default=10_000)
    p.add_argument("--connect-timeout", type=float, default=5.0)
    p.add_argument("--ingest-timeout", type=float, default=300.0)
    p.add_argument("--query-timeout", type=float, default=300.0)
    return p.parse_args()


def warn_if_uncontended(args: argparse.Namespace, assets: dict) -> None:
    """Refuse to silently run a workload where no estimator can matter.

    A task is *charged* its request when the controller has no telemetry, but
    actually *uses* roughly `usage_base` x request. If requests alone fit inside
    the cluster, the static controller places everything too and the estimator
    is irrelevant -- the experiment returns a null for a reason that has nothing
    to do with telemetry. The regime that matters is: requests over-subscribe
    the cluster, true usage roughly fills it.
    """
    free_cpu = sum(cap for cap, _ in assets["capacity"].values())
    tasks = assets["request"]
    arrivals = assets["arrivals"]
    span_epochs = max(1.0, max(a for a, _ in arrivals) / args.epoch_length_s)
    lifetime = statistics.fmean(
        max(1.0, math.ceil(d / args.epoch_length_s)) for d in assets["duration"].values())
    offered = sum(c for c, _ in tasks.values()) / span_epochs * lifetime
    base = (args.usage_base_lo + args.usage_base_hi) / 2 * (
        1 + args.burst_prob * (args.burst_factor - 1))
    print(f"workload: requests {offered:.0f} core-epochs/epoch = "
          f"{offered / free_cpu:.0%} of cluster CPU; true usage ~{base:.2f}x that = "
          f"{offered * base / free_cpu:.0%}")
    if offered / free_cpu < 1.05:
        print("  !! requests already fit -- a static controller places everything too, so\n"
              "     no estimator can change the outcome. Regenerate the workload with\n"
              "     `raw_data_prep.py --load-factor 1.4 --out-dir data/raw_topology_completion`.")
    elif offered * base / free_cpu < 0.85:
        print("  !! true usage leaves the cluster loose; contention will be rare.")


# ---------------------------------------------------------------------------
# True per-task usage
# ---------------------------------------------------------------------------

class TaskUsage:
    """Ground-truth usage stream for every task, shared by all scenarios.

    Keyed by (task, epoch) rather than by simulation step, so two scenarios that
    happen to run the same task in the same epoch see identical usage. Without
    that, scenarios would differ by their random draws instead of by their
    estimator.
    """

    def __init__(self, args: argparse.Namespace, task_ids: Sequence[str]):
        import numpy as np

        self._np = np
        self.args = args
        self.index = {tid: i for i, tid in enumerate(task_ids)}
        rng = np.random.default_rng(args.seed)
        self.base = rng.uniform(args.usage_base_lo, args.usage_base_hi, len(task_ids))

    def samples(self, task_id: str, epoch: int, run: int, request_cpu: float,
                request_mem: float):
        """K usage samples for one task in one epoch, as (cpu[], mem[])."""
        np = self._np
        a = self.args
        i = self.index[task_id]
        rng = np.random.default_rng([a.seed, run, i, epoch])
        mult = self.base[i] * math.exp(rng.normal(0.0, a.usage_drift_sigma))
        if rng.random() < a.burst_prob:
            mult *= a.burst_factor
        k = a.task_samples
        cpu = request_cpu * mult * np.exp(rng.normal(0.0, a.usage_within_sigma, k))
        mem = request_mem * mult * np.exp(rng.normal(0.0, a.usage_within_sigma, k))
        return cpu.astype("float32"), mem.astype("float32")


# ---------------------------------------------------------------------------
# Simulation state
# ---------------------------------------------------------------------------

@dataclass
class RunTask:
    node_id: str
    remaining_s: float
    alloc_cpu: float
    alloc_mem: float
    est_cpu: float
    est_mem: float


@dataclass
class Sim:
    """One scenario's independent trajectory."""
    scenario: Scenario
    unreleased: List[tuple]
    pending: List[str] = field(default_factory=list)
    running: Dict[str, RunTask] = field(default_factory=dict)
    completed: List[str] = field(default_factory=list)
    evicted: int = 0
    moves: int = 0
    constrained_task_epochs: int = 0
    overcommitted_node_epochs: int = 0
    window: Dict[str, collections.deque] = field(default_factory=dict)
    frozen_background: Dict[str, Dict[str, float]] | None = None
    remaining: Dict[str, float] = field(default_factory=dict)
    """Work left per task, kept outside `running` so a task that gets evicted
    and re-placed resumes instead of restarting."""

    def release(self, epoch: int, epoch_length_s: float) -> int:
        cutoff = (epoch + 1) * epoch_length_s
        n = 0
        while self.unreleased and self.unreleased[0][0] < cutoff:
            self.pending.append(self.unreleased.pop(0)[1])
            n += 1
        return n

    def drained(self) -> bool:
        return not self.unreleased and not self.pending and not self.running


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

def reset_es(args: argparse.Namespace) -> None:
    headers = es_headers(args.es_api_key)
    to = (args.connect_timeout, args.ingest_timeout)
    for index, key_field in ((args.es_node_index, "node"), (args.es_task_index, "task_id")):
        requests.delete(f"{args.es_url}/{index}", headers=headers, timeout=to)
        mapping = {
            "mappings": {
                "properties": {
                    "epoch": {"type": "long"},
                    key_field: {"type": "keyword"},
                    "cpu": {"type": "float"},
                    "mem": {"type": "float"},
                }
            }
        }
        r = requests.put(f"{args.es_url}/{index}", headers=headers, json=mapping, timeout=to)
        r.raise_for_status()


def ingest_sketch(args: argparse.Namespace, index: str, key_field: str, epoch: int,
                  keys: List[str], cpu, mem) -> None:
    url = f"{args.server_url}/{index}"
    to = (args.connect_timeout, args.ingest_timeout)
    for start in range(0, len(keys), args.batch_size):
        end = min(len(keys), start + args.batch_size)
        payload = {
            "epoch": epoch,
            key_field: keys[start:end],
            "cpu_cores": [round(float(v), 6) for v in cpu[start:end]],
            "memory_gb": [round(float(v), 6) for v in mem[start:end]],
        }
        r = requests.post(url, json=payload, timeout=to)
        r.raise_for_status()
        inserted = r.json().get("inserted")
        if inserted != end - start:
            raise RuntimeError(f"sketch ingest mismatch on {index}: {inserted} != {end - start}")


def ingest_es(args: argparse.Namespace, index: str, key_field: str, epoch: int,
              keys: List[str], cpu, mem, refresh: bool) -> None:
    headers = es_headers(args.es_api_key)
    to = (args.connect_timeout, args.ingest_timeout)
    url = f"{args.es_url}/{index}/_bulk"
    action = json.dumps({"index": {}})
    for start in range(0, len(keys), args.batch_size):
        end = min(len(keys), start + args.batch_size)
        lines = []
        for k, c, m in zip(keys[start:end], cpu[start:end], mem[start:end]):
            lines.append(action)
            lines.append(json.dumps({"epoch": epoch, key_field: k,
                                     "cpu": round(float(c), 6), "mem": round(float(m), 6)}))
        params = {"refresh": "wait_for"} if (refresh and end >= len(keys)) else None
        r = requests.post(url, headers=headers, data="\n".join(lines) + "\n",
                          params=params, timeout=to)
        r.raise_for_status()
        if r.json().get("errors"):
            raise RuntimeError(f"ES bulk errors on {index}")


def query_sketch(args: argparse.Namespace, index: str, keys: List[str]) -> Dict[str, Dict[str, float]]:
    """p50/p90/sum per key, per metric."""
    if not keys:
        return {}
    payload = {"keys": keys, "fields": ["cpu_cores", "memory_gb"],
               "aggs": ["percentiles", "sum"], "percents": PERCENTS}
    r = requests.post(f"{args.server_url}/{index}/_batch", json=payload,
                      timeout=(args.connect_timeout, args.query_timeout))
    r.raise_for_status()
    out: Dict[str, Dict[str, float]] = {}
    for item in r.json().get("results", []):
        pct = item.get("percentiles") or {}
        tot = item.get("sum") or {}
        rec: Dict[str, float] = {}
        for metric, short in (("cpu_cores", "cpu"), ("memory_gb", "mem")):
            vals = pct.get(metric, {})
            rec[f"{short}_p50"] = _pick(vals, 50.0)
            rec[f"{short}_p90"] = _pick(vals, 90.0)
            rec[f"{short}_sum"] = float(tot.get(metric, 0.0))
        out[item["key"]] = rec
    return out


def query_es_grouped(args: argparse.Namespace, index: str, key_field: str,
                     epoch: int) -> Dict[str, Dict[str, float]]:
    """One terms aggregation for every key in the epoch.

    Cheaper than the paper's per-task queries, which is fine: this experiment
    excludes control-loop overhead by design. Latency is measured in
    run_raw_data_assignment.py.
    """
    body = {
        "size": 0,
        "query": {"bool": {"filter": [{"term": {"epoch": epoch}}]}},
        "aggs": {
            "by_key": {
                "terms": {"field": key_field, "size": 20000},
                "aggs": {
                    "cpu_pct": {"percentiles": {"field": "cpu", "percents": PERCENTS}},
                    "mem_pct": {"percentiles": {"field": "mem", "percents": PERCENTS}},
                    "cpu_sum": {"sum": {"field": "cpu"}},
                    "mem_sum": {"sum": {"field": "mem"}},
                },
            }
        },
    }
    r = requests.post(f"{args.es_url}/{index}/_search", headers=es_headers(args.es_api_key),
                      json=body, params={"request_cache": "false"},
                      timeout=(args.connect_timeout, args.query_timeout))
    r.raise_for_status()
    out: Dict[str, Dict[str, float]] = {}
    for b in r.json()["aggregations"]["by_key"]["buckets"]:
        out[b["key"]] = {
            "cpu_p50": _pick(b["cpu_pct"]["values"], 50.0),
            "cpu_p90": _pick(b["cpu_pct"]["values"], 90.0),
            "cpu_sum": float(b["cpu_sum"]["value"] or 0.0),
            "mem_p50": _pick(b["mem_pct"]["values"], 50.0),
            "mem_p90": _pick(b["mem_pct"]["values"], 90.0),
            "mem_sum": float(b["mem_sum"]["value"] or 0.0),
        }
    return out


# ---------------------------------------------------------------------------
# Solver assets
# ---------------------------------------------------------------------------

def load_assets(topology_dir: Path) -> dict:
    if str(SOLVER_ROOT) not in sys.path:
        sys.path.insert(0, str(SOLVER_ROOT))
    from scheduler.load_info import load_edges, load_nodes, load_tasks  # type: ignore
    from python_solver.src.network_controller.solver import (  # type: ignore
        Edge as OrtEdge,
        NetworkControllerSolver,
        Node as OrtNode,
        Task as OrtTask,
    )

    raw_nodes = load_nodes(topology_dir / "nodes.jsonl")
    raw_edges = load_edges(topology_dir / "edges.jsonl")
    raw_tasks = load_tasks(topology_dir / "tasks.jsonl")

    return {
        "capacity": {nid: (n.cpu_capacity, n.memory_capacity) for nid, n in raw_nodes.items()},
        "edges": {eid: OrtEdge(edge_id=eid, capacity=e.capacity, used_bandwidth=0.0)
                  for eid, e in raw_edges.items()},
        "request": {tid: (t.initial_cpu, t.initial_memory) for tid, t in raw_tasks.items()},
        "duration": {tid: t.duration_s for tid, t in raw_tasks.items()},
        "arrivals": sorted((t.arrival_offset_s, tid) for tid, t in raw_tasks.items()),
        "OrtNode": OrtNode,
        "OrtTask": OrtTask,
        "Solver": NetworkControllerSolver,
    }


# ---------------------------------------------------------------------------
# One epoch of one scenario
# ---------------------------------------------------------------------------

def true_node_load(sim: Sim, assets: dict, usage: TaskUsage, epoch: int, run: int,
                   bg_true: Dict[str, tuple]) -> tuple[Dict[str, tuple], Dict[str, tuple]]:
    """Actual CPU/memory demand per node this epoch, and per-task true usage."""
    load: Dict[str, list] = {nid: [bg[0], bg[1]] for nid, bg in bg_true.items()}
    per_task: Dict[str, tuple] = {}
    for tid, rt in sim.running.items():
        req_cpu, req_mem = assets["request"][tid]
        cpu, mem = usage.samples(tid, epoch, run, req_cpu, req_mem)
        t_cpu, t_mem = float(cpu.mean()), float(mem.mean())
        per_task[tid] = (t_cpu, t_mem)
        acc = load.setdefault(rt.node_id, [0.0, 0.0])
        acc[0] += t_cpu
        acc[1] += t_mem
    return {nid: (v[0], v[1]) for nid, v in load.items()}, per_task


def advance(sim: Sim, assets: dict, node_load: Dict[str, tuple],
            bg_true: Dict[str, tuple], epoch_length_s: float) -> tuple[int, int, int]:
    """Charge one epoch of work, slowing tasks on over-committed nodes.

    A node's background load is not schedulable, so the tasks on it share
    whatever capacity is left; if they collectively want more than that, each
    makes progress in proportion to the shortfall. This is the paper's
    "performance penalty ... proportionally to their excess resource demand".
    """
    served: Dict[str, float] = {}
    overcommitted = 0
    for nid, (cpu_cap, mem_cap) in assets["capacity"].items():
        bg_cpu, bg_mem = bg_true.get(nid, (0.0, 0.0))
        tot_cpu, tot_mem = node_load.get(nid, (bg_cpu, bg_mem))
        task_cpu = max(tot_cpu - bg_cpu, 0.0)
        task_mem = max(tot_mem - bg_mem, 0.0)
        free_cpu = max(cpu_cap - bg_cpu, 0.0)
        free_mem = max(mem_cap - bg_mem, 0.0)
        s_cpu = 1.0 if task_cpu <= free_cpu else (free_cpu / task_cpu if task_cpu > 0 else 1.0)
        s_mem = 1.0 if task_mem <= free_mem else (free_mem / task_mem if task_mem > 0 else 1.0)
        s = min(s_cpu, s_mem)
        served[nid] = s
        if s < 1.0:
            overcommitted += 1

    constrained = 0
    done: List[str] = []
    for tid, rt in sim.running.items():
        s = served.get(rt.node_id, 1.0)
        if s < 1.0:
            constrained += 1
        rt.remaining_s -= epoch_length_s * s
        sim.remaining[tid] = rt.remaining_s
        if rt.remaining_s <= 0.0:
            done.append(tid)
    for tid in done:
        sim.running.pop(tid)
        sim.remaining.pop(tid, None)
        sim.completed.append(tid)

    sim.constrained_task_epochs += constrained
    sim.overcommitted_node_epochs += overcommitted
    return len(done), constrained, overcommitted


def task_estimate(sim: Sim, args: argparse.Namespace, tid: str, rt: RunTask,
                  reading: Dict[str, float] | None) -> tuple[float, float]:
    """Turn one task's telemetry summary into the value handed to the MILP."""
    rule = sim.scenario.rule
    if reading is None:
        return rt.alloc_cpu, rt.alloc_mem
    if rule == "p50":
        return reading["cpu_p50"], reading["mem_p50"]
    if rule == "p90":
        return reading["cpu_p90"], reading["mem_p90"]
    if rule == "avg":
        n = max(args.task_samples, 1)
        return reading["cpu_sum"] / n, reading["mem_sum"] / n
    if rule == "window":
        w = sim.window.setdefault(tid, collections.deque(maxlen=args.window_epochs))
        n = max(args.task_samples, 1)
        w.append((reading["cpu_sum"] / n, reading["mem_sum"] / n))
        return (statistics.fmean(c for c, _ in w), statistics.fmean(m for _, m in w))
    if rule == "p50bump":
        cpu, mem = reading["cpu_p50"], reading["mem_p50"]
        if cpu >= args.bump_threshold * rt.alloc_cpu:
            rt.alloc_cpu *= args.bump_factor
            cpu = rt.alloc_cpu
        if mem >= args.bump_threshold * rt.alloc_mem:
            rt.alloc_mem *= args.bump_factor
            mem = rt.alloc_mem
        return cpu, mem
    raise ValueError(rule)


def solve_epoch(sim: Sim, args: argparse.Namespace, assets: dict,
                background: Dict[str, Dict[str, float]]) -> tuple[object, float, set]:
    """Build and solve this scenario's MILP for the epoch."""
    OrtNode, OrtTask = assets["OrtNode"], assets["OrtTask"]
    sc = sim.scenario

    # Node state = background estimate + the estimated cost of what already runs
    # there. With reassignments the running tasks are decision variables, so
    # their load must NOT also be baked into used_cpu.
    node_used: Dict[str, list] = {nid: [0.0, 0.0] for nid in assets["capacity"]}
    for nid in assets["capacity"]:
        bg = background.get(nid)
        if bg is not None:
            node_used[nid][0] += max(bg["cpu_p50"], 0.0)
            cap_mem = assets["capacity"][nid][1]
            node_used[nid][1] += max(cap_mem - bg["mem_p50"], 0.0)

    # Full estimated state, including everything already running.
    for rt in sim.running.values():
        acc = node_used.get(rt.node_id)
        if acc is not None:
            acc[0] += rt.est_cpu
            acc[1] += rt.est_mem

    # Which running tasks are offered for reassignment. Two limits, both real:
    #
    #  * A move only helps on a node the controller believes is over-committed,
    #    and offering every running task makes the model grow with the running
    #    set for no benefit -- so candidates come from the most pressured nodes.
    #  * Offering a task removes its load from its node, and the model must then
    #    find it a legal slot. On an over-subscribed cluster there may be none --
    #    not even its own node, which the estimate already fills -- and the
    #    solver's only option is to drop it. Physically it is still running, so
    #    that is not a decision the controller may make. Candidates are therefore
    #    limited to what the cluster's spare capacity can actually absorb.
    movable: set[str] = set()
    if sc.reassigns and sim.running:
        spare_cpu = sum(max(cap - node_used[nid][0], 0.0)
                        for nid, (cap, _) in assets["capacity"].items())
        spare_mem = sum(max(cap - node_used[nid][1], 0.0)
                        for nid, (_, cap) in assets["capacity"].items())
        pressure = {
            nid: node_used[nid][0] / max(cap, 1e-9)
            for nid, (cap, _) in assets["capacity"].items()
        }
        ranked = sorted(sim.running,
                        key=lambda t: (-pressure.get(sim.running[t].node_id, 0.0),
                                       sim.running[t].est_cpu))
        used_cpu = used_mem = 0.0
        for tid in ranked:
            if len(movable) >= args.max_reassign_candidates:
                break
            rt = sim.running[tid]
            if used_cpu + rt.est_cpu > spare_cpu or used_mem + rt.est_mem > spare_mem:
                continue
            movable.add(tid)
            used_cpu += rt.est_cpu
            used_mem += rt.est_mem

    # Offered tasks become decision variables, so their load must not also be
    # baked into the node they currently sit on.
    for tid in movable:
        rt = sim.running[tid]
        acc = node_used.get(rt.node_id)
        if acc is not None:
            acc[0] = max(acc[0] - rt.est_cpu, 0.0)
            acc[1] = max(acc[1] - rt.est_mem, 0.0)

    nodes = {}
    for nid, (cpu_cap, mem_cap) in assets["capacity"].items():
        used_cpu, used_mem = node_used[nid]
        nodes[nid] = OrtNode(node_id=nid, cpu_capacity=cpu_cap, memory_capacity=mem_cap,
                             used_cpu=min(used_cpu, cpu_cap), used_memory=min(used_mem, mem_cap))

    task_list = []
    for tid in sim.pending[:args.max_candidates]:
        cpu, mem = assets["request"][tid]
        task_list.append(OrtTask(task_id=tid, cpu=cpu, memory=mem, bandwidth=0.0, priority=1.0))
    previous: Dict[str, str] = {}
    for tid in sorted(movable):
        rt = sim.running[tid]
        task_list.append(OrtTask(task_id=tid, cpu=rt.est_cpu, memory=rt.est_mem,
                                 bandwidth=0.0, priority=RUNNING_PRIORITY))
        previous[tid] = rt.node_id

    solver = assets["Solver"](nodes, assets["edges"], solver_backend=args.solver_backend)
    t0 = time.perf_counter()
    kwargs = dict(time_limit_s=args.solver_time_limit_s, raise_on_no_solution=False,
                  mip_gap=args.mip_gap or None)
    try:
        result = solver.solve(task_list, previous_assignments=previous or None,
                              max_task_movements=sc.gamma or None,
                              migration_penalty=sc.lam, **kwargs)
    except RuntimeError:
        # Nothing in this model should be infeasible now that running tasks are
        # skippable, but a pre-optimisation capacity check can still trip on a
        # node whose estimate exceeds capacity. Drop the migration terms and
        # retry so the epoch is recorded rather than lost.
        result = solver.solve(task_list, time_limit_s=args.solver_time_limit_s,
                              raise_on_no_solution=False, mip_gap=args.mip_gap or None)
    return result, (time.perf_counter() - t0) * 1000.0, movable


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

CSV_HEADER = [
    "timestamp_utc", "run", "epoch", "scenario", "estimator", "rule", "gamma", "lam",
    "arrivals", "pending_before", "assigned", "running", "completed_this_epoch",
    "completed_total", "evicted_total", "moves_this_epoch",
    "constrained_tasks", "overcommitted_nodes",
    "solver_ms", "solver_status", "objective",
    "est_cpu_err_mean", "est_cpu_err_p90",
]


def run_one(args: argparse.Namespace, run: int, tel: Telemetry, assets: dict,
            scenarios: List[Scenario], usage: TaskUsage, writer, fh) -> None:
    import numpy as np

    node_ids = sorted(assets["capacity"])
    need_sketch = any(s.estimator == "sketch" for s in scenarios)
    need_es = any(s.estimator == "es" for s in scenarios)

    reset_es(args)
    server_proc = None
    if not args.skip_server_start:
        os.environ["NCS_CONFIG_PATH"] = str(args.server_config)
        server_proc = start_server(resolve_repo_path(args.server_log), truncate_log=(run == 0))

    sims = {s.name: Sim(scenario=s, unreleased=list(assets["arrivals"])) for s in scenarios}

    try:
        if server_proc is not None:
            wait_for_server(args.server_url, args.server_ready_timeout,
                            args.connect_timeout, args.query_timeout)

        for epoch in range(args.epochs):
            cpu, mem = epoch_values(tel, epoch, args.epoch_jitter, args.seed, run)

            # --- background telemetry: one ingest, shared by every scenario ---
            keys = [tel.node_ids[int(i)] for i in tel.node_idx]
            if need_sketch:
                ingest_sketch(args, "cluster-metrics", "cluster", epoch, keys, cpu, mem)
            if need_es:
                ingest_es(args, args.es_node_index, "node", epoch, keys, cpu, mem, refresh=True)

            # Ground truth straight from the arrays -- no estimator involved.
            bg_true: Dict[str, tuple] = {}
            for i, nid in enumerate(tel.node_ids):
                sel = tel.node_idx == i
                if not sel.any():
                    continue
                cap_mem = assets["capacity"][nid][1]
                bg_true[nid] = (float(np.median(cpu[sel])),
                                max(cap_mem - float(np.median(mem[sel])), 0.0))

            bg_read = {
                "sketch": query_sketch(args, "cluster-metrics", node_ids) if need_sketch else {},
                "es": query_es_grouped(args, args.es_node_index, "node", epoch) if need_es else {},
            }

            for sc in scenarios:
                sim = sims[sc.name]

                # 1. charge work with true usage, retire finished tasks
                node_load, per_task = true_node_load(sim, assets, usage, epoch, run, bg_true)
                done, constrained, overcommitted = advance(
                    sim, assets, node_load, bg_true, args.epoch_length_s)

                # 2. new arrivals
                arrivals = sim.release(epoch, args.epoch_length_s)

                # 3. refresh running-task estimates from this scenario's backend
                errs: List[float] = []
                if sim.scenario.dynamic and sim.running:
                    prefix = f"{sc.name}|"
                    tids = sorted(sim.running)
                    t_keys, t_cpu, t_mem = [], [], []
                    for tid in tids:
                        req_cpu, req_mem = assets["request"][tid]
                        c, m = usage.samples(tid, epoch, run, req_cpu, req_mem)
                        t_keys.extend([prefix + tid] * len(c))
                        t_cpu.extend(c.tolist())
                        t_mem.extend(m.tolist())
                    if sim.scenario.estimator == "sketch":
                        ingest_sketch(args, "task-metrics", "task_id", epoch, t_keys, t_cpu, t_mem)
                        reads = query_sketch(args, "task-metrics", [prefix + t for t in tids])
                    else:
                        ingest_es(args, args.es_task_index, "task_id", epoch,
                                  t_keys, t_cpu, t_mem, refresh=True)
                        reads = query_es_grouped(args, args.es_task_index, "task_id", epoch)
                    for tid in tids:
                        rt = sim.running[tid]
                        rec = reads.get(prefix + tid)
                        est_cpu, est_mem = task_estimate(sim, args, tid, rt, rec)
                        rt.est_cpu, rt.est_mem = est_cpu, est_mem
                        true_cpu = per_task.get(tid, (est_cpu, 0.0))[0]
                        if true_cpu > 1e-9:
                            errs.append(abs(est_cpu - true_cpu) / true_cpu)
                elif sim.running:
                    # Static: the estimate is, and stays, the original request.
                    for tid, rt in sim.running.items():
                        true_cpu = per_task.get(tid, (rt.est_cpu, 0.0))[0]
                        if true_cpu > 1e-9:
                            errs.append(abs(rt.est_cpu - true_cpu) / true_cpu)

                # 4. background estimate for this scenario (frozen if static)
                if sim.scenario.dynamic:
                    background = bg_read[sim.scenario.estimator]
                else:
                    if sim.frozen_background is None:
                        sim.frozen_background = {
                            nid: {"cpu_p50": bg_true[nid][0],
                                  "mem_p50": assets["capacity"][nid][1] - bg_true[nid][1]}
                            for nid in bg_true
                        }
                    background = sim.frozen_background

                # 5. solve and apply
                pending_before = len(sim.pending)
                result, ms, movable = solve_epoch(sim, args, assets, background)

                moved = 0
                still_running = set()
                for tid, decision in result.decisions.items():
                    if tid in sim.running:
                        rt = sim.running[tid]
                        if decision.node_id != rt.node_id:
                            rt.node_id = decision.node_id
                            moved += 1
                        still_running.add(tid)
                    else:
                        req_cpu, req_mem = assets["request"][tid]
                        sim.running[tid] = RunTask(
                            node_id=decision.node_id,
                            remaining_s=sim.remaining.get(tid, assets["duration"][tid]),
                            alloc_cpu=req_cpu, alloc_mem=req_mem,
                            est_cpu=req_cpu, est_mem=req_mem,
                        )
                        still_running.add(tid)
                # Only tasks actually offered for reassignment can be dropped.
                # Everything else was never in the model and keeps running
                # untouched -- counting those as evictions silently destroys the
                # running set every epoch.
                evicted = [t for t in movable if t not in still_running]
                for tid in evicted:
                    sim.running.pop(tid, None)
                sim.evicted += len(evicted)
                sim.moves += moved
                offered = set(sim.pending[:args.max_candidates])
                sim.pending = sorted(
                    [t for t in sim.pending if t not in offered]
                    + [t for t in result.unassigned_tasks if t not in sim.running]
                )

                writer.writerow([
                    datetime.now(timezone.utc).isoformat(), run, epoch, sc.name,
                    sc.estimator, sc.rule, sc.gamma, sc.lam,
                    arrivals, pending_before, len(result.decisions), len(sim.running),
                    done, len(sim.completed), sim.evicted, moved,
                    constrained, overcommitted,
                    f"{ms:.3f}", result.status, f"{result.objective_value:.6f}",
                    f"{statistics.fmean(errs) * 100:.4f}" if errs else "",
                    f"{sorted(errs)[int(0.9 * (len(errs) - 1))] * 100:.4f}" if errs else "",
                ])
            fh.flush()

            line = "  ".join(
                f"{s.name}={len(sims[s.name].completed)}"
                f"(r{len(sims[s.name].running)},p{len(sims[s.name].pending)})"
                for s in scenarios
            )
            print(f"run {run} epoch {epoch}: completed {line}", flush=True)

            if all(sims[s.name].drained() for s in scenarios):
                print("all scenarios drained; ending this run.")
                break
    finally:
        if server_proc is not None:
            stop_server(server_proc)


def main() -> None:
    args = parse_args()
    scenarios = args.scenario or (
        all_presets(args.gamma, args.lam) if args.figure == "all"
        else preset(args.figure, args.gamma, args.lam)
    )
    out_csv = resolve_repo_path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    assets = load_assets(args.topology_dir)
    node_ids = sorted(assets["capacity"])
    print(f"topology: {len(node_ids)} nodes, {len(assets['edges'])} edges, "
          f"{len(assets['request'])} tasks")
    print(f"scenarios ({len(scenarios)}):")
    for s in scenarios:
        print(f"  {s.name:<18} estimator={s.estimator:<7} rule={s.rule:<8} "
              f"gamma={s.gamma:<3} lambda={s.lam}")

    warn_if_uncontended(args, assets)

    print(f"loading telemetry from {args.telemetry_csv} ...", flush=True)
    tel = load_telemetry(args.telemetry_csv, node_ids, args.rows_per_epoch, args.seed)
    print(f"  {len(tel.node_idx):,} background rows/epoch over {len(tel.node_ids)} nodes")

    usage = TaskUsage(args, sorted(assets["request"]))

    with open(out_csv, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_HEADER)
        for run in range(args.runs):
            run_one(args, run, tel, assets, scenarios, usage, writer, fh)

    print(f"\nwrote {out_csv}")


if __name__ == "__main__":
    main()
