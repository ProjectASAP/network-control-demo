#!/usr/bin/env python3
"""Live CLI dashboard for the network-control demo.

Drives the same ingest -> query -> solve loop that
scripts/run_rtt_sweep_epoch_full_ortools.py runs, against the Sketch
server, Elasticsearch, or both, and renders progress in real time with rich.

The Sketch server must already be running on --server-url (default
http://localhost:10101). Start it however you normally would, e.g.:

    cd single_node_server/network-control-server && ./docker-build.sh -t network-control-server:latest
    docker run --rm -p 10101:10101 network-control-server:latest

For ES, pass --es-url / --es-api-key (or set ES_API_KEY env / .es_api_key file).

Examples:

    python visualization/demo.py --epochs 5 --rows-per-epoch 200000 --backend SCIP
    python visualization/demo.py --backends es --es-url http://localhost:9200
    python visualization/demo.py --backends both
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
VENDORED = Path(__file__).resolve().parent / "_vendored"
sys.path.insert(0, str(VENDORED))

# Vendored modules (see visualization/_vendored/).
import rtt_sweep_common as rsc  # noqa: E402
import ort_solver as ort  # noqa: E402
import load_info  # noqa: E402

from rich.console import Console, Group  # noqa: E402
from rich.live import Live  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.progress import BarColumn, Progress, TextColumn  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.text import Text  # noqa: E402


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

@dataclass
class BackendEpoch:
    ingest_ms: float = 0.0
    query_ms: float = 0.0


@dataclass
class EpochRecord:
    epoch: int
    sketch: BackendEpoch = field(default_factory=BackendEpoch)
    es: BackendEpoch = field(default_factory=BackendEpoch)
    solve_ms: float = 0.0
    rows: int = 0
    assignments: int = 0
    unassigned: int = 0
    objective: float = 0.0

    @property
    def total_ms(self) -> float:
        return (
            self.sketch.ingest_ms + self.sketch.query_ms
            + self.es.ingest_ms + self.es.query_ms
            + self.solve_ms
        )


@dataclass
class DemoState:
    total_epochs: int
    backend: str
    backends: List[str]  # subset of ["sketch", "es"]
    server_url: str
    es_url: str
    node_count: int
    task_count: int
    rows_per_epoch: int
    batch_size: int

    current_epoch: int = 0
    phase: str = "starting"
    phase_detail: str = ""

    batches_done: int = 0
    batches_total: int = 0
    running_sketch_ingest_ms: float = 0.0
    running_sketch_query_ms: float = 0.0
    running_es_ingest_ms: float = 0.0
    running_es_query_ms: float = 0.0
    running_solve_ms: float = 0.0

    history: List[EpochRecord] = field(default_factory=list)
    log: Deque[str] = field(default_factory=lambda: deque(maxlen=12))
    top_nodes: List[tuple] = field(default_factory=list)

    start_wall: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

PHASE_COLORS = {
    "starting": "grey70",
    "sketch-ingest": "cyan",
    "sketch-query": "magenta",
    "es-ingest": "blue",
    "es-query": "bright_magenta",
    "solve": "green",
    "idle": "yellow",
    "done": "bold green",
    "error": "bold red",
}


def _sparkline(values: List[float]) -> str:
    if not values:
        return ""
    bars = "▁▂▃▄▅▆▇█"
    lo, hi = min(values), max(values)
    if hi - lo < 1e-9:
        return bars[0] * len(values)
    return "".join(bars[min(7, int((v - lo) / (hi - lo) * 7))] for v in values)


def _fmt_ms(x: float) -> str:
    if x <= 0:
        return "     —"
    if x >= 1000:
        return f"{x / 1000:7.2f} s"
    return f"{x:7.1f} ms"


def render(state: DemoState) -> Panel:
    phase_color = PHASE_COLORS.get(state.phase, "white")
    elapsed = int(time.time() - state.start_wall)
    mm, ss = divmod(elapsed, 60)

    header = Table.grid(padding=(0, 2), expand=True)
    header.add_column(justify="left")
    header.add_column(justify="right")
    header.add_row(
        Text.from_markup(
            f"[bold]Epoch[/bold] {state.current_epoch}/{state.total_epochs}   "
            f"[bold]Phase[/bold] [{phase_color}]{state.phase.upper()}[/{phase_color}]   "
            f"[dim]{state.phase_detail}[/dim]"
        ),
        Text.from_markup(
            f"[bold]Backend[/bold] {state.backend}   "
            f"[bold]Targets[/bold] {'+'.join(state.backends)}   "
            f"[bold]⏱[/bold] {mm:02d}:{ss:02d}"
        ),
    )

    # Current-epoch phase bars.
    phase_tbl = Table.grid(padding=(0, 1), expand=True)
    phase_tbl.add_column(width=14)
    phase_tbl.add_column(ratio=1)
    phase_tbl.add_column(justify="right", width=14)
    phase_tbl.add_column(justify="right", width=22)

    batch_frac = (
        state.batches_done / state.batches_total if state.batches_total else 0.0
    )

    def _phase_bar(name: str, active_phase: str, running_ms: float, color: str, detail: str):
        if state.phase == active_phase:
            frac = batch_frac if "ingest" in active_phase else 0.5
        else:
            frac = 1.0 if running_ms else 0.0
        phase_tbl.add_row(name, _bar(frac, color), _fmt_ms(running_ms), detail)

    if "sketch" in state.backends:
        _phase_bar("sketch-ing", "sketch-ingest", state.running_sketch_ingest_ms, "cyan",
                   f"{state.batches_done}/{state.batches_total} batches")
        _phase_bar("sketch-qry", "sketch-query", state.running_sketch_query_ms, "magenta",
                   f"{state.node_count} nodes")
    if "es" in state.backends:
        _phase_bar("es-ingest", "es-ingest", state.running_es_ingest_ms, "blue",
                   f"{state.batches_done}/{state.batches_total} batches")
        _phase_bar("es-query", "es-query", state.running_es_query_ms, "bright_magenta",
                   f"{state.node_count} nodes")

    solve_detail = (
        f"assigned {state.history[-1].assignments}/{state.task_count}"
        if state.history and state.phase in ("solve", "idle", "done")
        and state.current_epoch == state.history[-1].epoch
        else f"tasks={state.task_count}"
    )
    _phase_bar("solve", "solve", state.running_solve_ms, "green", solve_detail)

    # History.
    hist = state.history
    totals = [h.total_ms for h in hist]
    sk_ingests = [h.sketch.ingest_ms for h in hist]
    sk_queries = [h.sketch.query_ms for h in hist]
    es_ingests = [h.es.ingest_ms for h in hist]
    es_queries = [h.es.query_ms for h in hist]
    solves = [h.solve_ms for h in hist]

    hist_tbl = Table.grid(padding=(0, 2), expand=True)
    hist_tbl.add_column(width=12)
    hist_tbl.add_column(ratio=1)
    hist_tbl.add_column(justify="right", width=28)
    hist_tbl.add_row("total", Text(_sparkline(totals), style="bold white"), _stats_line(totals))
    if "sketch" in state.backends:
        hist_tbl.add_row("sketch-ing", Text(_sparkline(sk_ingests), style="cyan"), _stats_line(sk_ingests))
        hist_tbl.add_row("sketch-qry", Text(_sparkline(sk_queries), style="magenta"), _stats_line(sk_queries))
    if "es" in state.backends:
        hist_tbl.add_row("es-ingest", Text(_sparkline(es_ingests), style="blue"), _stats_line(es_ingests))
        hist_tbl.add_row("es-query", Text(_sparkline(es_queries), style="bright_magenta"), _stats_line(es_queries))
    hist_tbl.add_row("solve", Text(_sparkline(solves), style="green"), _stats_line(solves))

    # Epoch results table (last 8).
    res_tbl = Table(show_header=True, header_style="bold", expand=True, padding=(0, 1))
    res_tbl.add_column("ep", justify="right")
    res_tbl.add_column("rows", justify="right")
    if "sketch" in state.backends:
        res_tbl.add_column("sk-ing", justify="right")
        res_tbl.add_column("sk-qry", justify="right")
    if "es" in state.backends:
        res_tbl.add_column("es-ing", justify="right")
        res_tbl.add_column("es-qry", justify="right")
    res_tbl.add_column("solve", justify="right")
    res_tbl.add_column("total", justify="right", style="bold")
    res_tbl.add_column("assigned", justify="right")
    res_tbl.add_column("obj", justify="right")
    for h in hist[-8:]:
        row = [str(h.epoch), f"{h.rows:,}"]
        if "sketch" in state.backends:
            row += [_fmt_ms(h.sketch.ingest_ms), _fmt_ms(h.sketch.query_ms)]
        if "es" in state.backends:
            row += [_fmt_ms(h.es.ingest_ms), _fmt_ms(h.es.query_ms)]
        row += [
            _fmt_ms(h.solve_ms),
            _fmt_ms(h.total_ms),
            f"{h.assignments}/{h.assignments + h.unassigned}",
            f"{h.objective:.1f}",
        ]
        res_tbl.add_row(*row)

    log_text = Text("\n".join(state.log) or "(waiting...)", style="dim")

    body = Group(
        Panel(header, border_style=phase_color, title="network-control demo"),
        Panel(phase_tbl, title="this epoch", border_style="blue"),
        Panel(hist_tbl, title="history", border_style="blue"),
        Panel(res_tbl, title="per-epoch results", border_style="blue"),
        Panel(log_text, title="log", border_style="grey50"),
    )
    return Panel(body, border_style="grey50")


def _bar(frac: float, color: str, width: int = 32) -> Text:
    frac = max(0.0, min(1.0, frac))
    filled = int(frac * width)
    return Text.from_markup(f"[{color}]{'█' * filled}[/{color}][grey30]{'░' * (width - filled)}[/grey30]")


def _stats_line(vs: List[float]) -> str:
    if not vs:
        return ""
    last = vs[-1]
    mean = sum(vs) / len(vs)
    return f"last={last:7.0f}ms mean={mean:7.0f}ms"


# ---------------------------------------------------------------------------
# Solver helpers
# ---------------------------------------------------------------------------

def load_solver_assets(data_dir: Path):
    raw_nodes = load_info.load_nodes(data_dir / "nodes.jsonl")
    raw_edges = load_info.load_edges(data_dir / "edges.jsonl")
    raw_tasks = load_info.load_tasks(data_dir / "tasks.jsonl")

    ort_nodes: Dict[str, ort.Node] = {}
    for nid, n in raw_nodes.items():
        ort_nodes[nid] = ort.Node(
            node_id=n.node_id,
            cpu_capacity=n.cpu_capacity,
            memory_capacity=n.memory_capacity,
            used_cpu=n.used_cpu,
            used_memory=n.used_memory,
        )
    ort_edges: Dict[tuple, ort.Edge] = {}
    for k, e in raw_edges.items():
        ort_edges[k] = ort.Edge(edge_id=k, capacity=e.capacity, used_bandwidth=e.used_bandwidth)

    ort_tasks: Dict[str, ort.Task] = {}
    for tid, t in raw_tasks.items():
        comms = tuple(
            ort.TaskCommunication(target_task_id=peer, bandwidth=bw)
            for peer, bw in t.peer_bandwidths.items()
        )
        ort_tasks[tid] = ort.Task(
            task_id=t.task_id,
            cpu=t.initial_cpu,
            memory=t.initial_memory,
            bandwidth=sum(t.peer_bandwidths.values()),
            priority=1.0,
            communications=comms,
        )
    return ort_nodes, ort_edges, ort_tasks


def select_first(items, n):
    s = sorted(items)
    return s if n <= 0 or n >= len(s) else s[:n]


def build_context(all_nodes, all_edges, all_tasks, node_count, task_count):
    node_ids = select_first(list(all_nodes.keys()), node_count)
    task_ids = select_first(list(all_tasks.keys()), task_count)
    nodes = {n: all_nodes[n] for n in node_ids}
    tasks = {t: all_tasks[t] for t in task_ids}
    node_set = set(nodes)
    edges = {k: v for k, v in all_edges.items() if k[0] in node_set and k[1] in node_set}
    return nodes, edges, tasks


def extract_sketch_usage(server_json) -> Dict[str, Dict[str, float]]:
    usage = {}
    for item in server_json.get("results", []):
        nid = item.get("key")
        if not nid:
            continue
        c = item.get("cumulative") or {}
        usage[str(nid)] = {
            "cpu": float(c.get("cpu_cores", 0.0) or 0.0),
            "memory": float(c.get("memory_gb", 0.0) or 0.0),
        }
    return usage


def extract_es_usage(es_json) -> Dict[str, Dict[str, float]]:
    usage = {}
    for node_id, payload in es_json.items():
        aggs = payload.get("aggregations", {})
        usage[str(node_id)] = {
            "cpu": float(aggs.get("cpu_sum", {}).get("value", 0.0) or 0.0),
            "memory": float(aggs.get("mem_sum", {}).get("value", 0.0) or 0.0),
        }
    return usage


def apply_usage(base_nodes, usage):
    out = {}
    for nid, n in base_nodes.items():
        u = usage.get(nid, {})
        out[nid] = ort.Node(
            node_id=n.node_id,
            cpu_capacity=n.cpu_capacity,
            memory_capacity=n.memory_capacity,
            used_cpu=min(u.get("cpu", n.used_cpu), n.cpu_capacity),
            used_memory=min(u.get("memory", n.used_memory), n.memory_capacity),
        )
    return out


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--server-url", default="http://localhost:10101")
    p.add_argument("--es-url", default=rsc.DEFAULT_ES_URL)
    p.add_argument("--es-index", default=rsc.DEFAULT_ES_INDEX)
    # Resolve ES API key: env var, ES_API_KEY_FILE, or .es_api_key at repo root.
    import os as _os
    _default_es_key = rsc.DEFAULT_ES_API_KEY or _os.getenv("ES_API_KEY")
    if not _default_es_key:
        _kf = Path(_os.getenv("ES_API_KEY_FILE", REPO_ROOT / ".es_api_key"))
        if _kf.exists():
            _default_es_key = _kf.read_text().strip()
    p.add_argument("--es-api-key", default=_default_es_key)
    p.add_argument("--es-timeout", type=float, default=rsc.DEFAULT_ES_TIMEOUT)
    p.add_argument("--es-refresh", default=None,
                   help="ES bulk refresh param (e.g. 'wait_for'); default: none")
    p.add_argument("--es-reset-index", action="store_true",
                   help="Delete + recreate ES index before the run")
    p.add_argument(
        "--backends",
        choices=["sketch", "es", "both"],
        default="sketch",
        help="Which backend(s) to ingest/query: sketch, es, or both",
    )
    p.add_argument("--epochs", type=int, default=5)
    p.add_argument("--rows-per-epoch", type=int, default=200_000)
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--backend", choices=["CBC", "SCIP", "GLPK"], default="CBC",
                   help="OR-Tools solver backend")
    p.add_argument("--solver-node-count", type=int, default=30)
    p.add_argument("--solver-task-count", type=int, default=0, help="0 = all")
    p.add_argument("--query-node-count", type=int, default=30)
    p.add_argument(
        "--nodes-config",
        default=str(REPO_ROOT / "single_node_server/network-control-server/server-config.yaml"),
    )
    p.add_argument("--solver-data-dir", default=str(REPO_ROOT / "solver_experimental/dummy_data"))
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--connect-timeout", type=float, default=5.0)
    p.add_argument("--ingest-timeout", type=float, default=60.0)
    p.add_argument("--query-timeout", type=float, default=60.0)
    p.add_argument("--ingest-retries", type=int, default=2)
    p.add_argument("--ingest-retry-backoff", type=float, default=2.0)
    p.add_argument("--refresh-hz", type=float, default=10.0)
    p.add_argument("--no-dashboard", action="store_true", help="plain logs instead of rich dashboard")
    return p.parse_args()


def _backends_list(flag: str) -> List[str]:
    if flag == "both":
        return ["sketch", "es"]
    return [flag]


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)

    backends = _backends_list(args.backends)

    all_nodes_ids = rsc.parse_nodes_config(args.nodes_config)
    query_nodes = select_first(all_nodes_ids, args.query_node_count)

    console = Console()

    if "sketch" in backends:
        console.print(f"[dim]checking sketch server at {args.server_url} ...[/dim]")
        try:
            rsc.wait_for_server(args.server_url, 5.0, args.connect_timeout, args.query_timeout)
        except Exception as exc:
            console.print(f"[bold red]Sketch server not reachable at {args.server_url}:[/bold red] {exc}")
            sys.exit(1)

    if "es" in backends:
        console.print(f"[dim]checking ES at {args.es_url} ...[/dim]")
        try:
            import requests
            resp = requests.get(
                args.es_url,
                headers=rsc.es_headers(args.es_api_key),
                timeout=(args.connect_timeout, args.es_timeout),
            )
            resp.raise_for_status()
        except Exception as exc:
            console.print(f"[bold red]ES not reachable at {args.es_url}:[/bold red] {exc}")
            sys.exit(1)
        if args.es_reset_index:
            console.print(f"[dim]resetting ES index {args.es_index} ...[/dim]")
            rsc.reset_es_index(
                args.es_url, args.es_index, args.es_api_key,
                args.connect_timeout, args.es_timeout,
            )

    console.print(f"[dim]loading solver assets from {args.solver_data_dir} ...[/dim]")
    all_n, all_e, all_t = load_solver_assets(Path(args.solver_data_dir))
    nodes, edges, tasks = build_context(
        all_n, all_e, all_t, args.solver_node_count, args.solver_task_count
    )

    state = DemoState(
        total_epochs=args.epochs,
        backend=args.backend,
        backends=backends,
        server_url=args.server_url,
        es_url=args.es_url,
        node_count=len(query_nodes),
        task_count=len(tasks),
        rows_per_epoch=args.rows_per_epoch,
        batch_size=args.batch_size,
    )
    state.log.append(f"loaded {len(nodes)} solver nodes, {len(tasks)} tasks, {len(edges)} edges")
    state.log.append(f"querying {len(query_nodes)} nodes per epoch against {'+'.join(backends)}")

    total_batches = (args.rows_per_epoch + args.batch_size - 1) // args.batch_size

    def run_loop(live: Optional[Live]) -> None:
        def refresh():
            if live is not None:
                live.update(render(state))

        for epoch in range(1, args.epochs + 1):
            state.current_epoch = epoch
            state.batches_done = 0
            state.batches_total = total_batches
            state.running_sketch_ingest_ms = 0.0
            state.running_sketch_query_ms = 0.0
            state.running_es_ingest_ms = 0.0
            state.running_es_query_ms = 0.0
            state.running_solve_ms = 0.0

            # Generate batches once per epoch and reuse across backends to keep
            # data identical.
            batches = list(
                rsc.iter_batches(args.rows_per_epoch, all_nodes_ids, rng, args.batch_size, epoch)
            )

            sketch_ingest_ms = 0.0
            es_ingest_ms = 0.0
            sketch_query_ms = 0.0
            es_query_ms = 0.0
            sketch_json = None
            es_json = None

            # --- SKETCH INGEST ---
            if "sketch" in backends:
                state.phase = "sketch-ingest"
                state.phase_detail = f"pushing {args.rows_per_epoch:,} rows in {total_batches} batches"
                state.batches_done = 0
                refresh()
                for batch_idx, batch in enumerate(batches, start=1):
                    t0 = time.perf_counter()
                    rsc.ingest_server(
                        args.server_url, batch, epoch,
                        args.connect_timeout, args.ingest_timeout,
                        args.ingest_retries, args.ingest_retry_backoff,
                    )
                    sketch_ingest_ms += (time.perf_counter() - t0) * 1000.0
                    state.batches_done = batch_idx
                    state.running_sketch_ingest_ms = sketch_ingest_ms
                    if batch_idx % max(1, total_batches // 40) == 0 or batch_idx == total_batches:
                        refresh()
                state.log.append(f"epoch {epoch}: sketch-ingest {sketch_ingest_ms:.0f} ms")

            # --- ES INGEST ---
            if "es" in backends:
                state.phase = "es-ingest"
                state.phase_detail = f"bulk-indexing {args.rows_per_epoch:,} rows"
                state.batches_done = 0
                refresh()
                for batch_idx, batch in enumerate(batches, start=1):
                    t0 = time.perf_counter()
                    rsc.bulk_ingest_es(
                        args.es_url, args.es_index, args.es_api_key, batch,
                        args.connect_timeout, args.es_timeout, args.es_refresh,
                    )
                    es_ingest_ms += (time.perf_counter() - t0) * 1000.0
                    state.batches_done = batch_idx
                    state.running_es_ingest_ms = es_ingest_ms
                    if batch_idx % max(1, total_batches // 40) == 0 or batch_idx == total_batches:
                        refresh()
                state.log.append(f"epoch {epoch}: es-ingest {es_ingest_ms:.0f} ms")

            # --- SKETCH QUERY ---
            if "sketch" in backends:
                state.phase = "sketch-query"
                state.phase_detail = f"aggregating {len(query_nodes)} nodes (sketch)"
                refresh()
                sketch_json, sketch_query_ms = rsc.query_server_batch(
                    args.server_url, query_nodes, args.connect_timeout, args.query_timeout
                )
                state.running_sketch_query_ms = sketch_query_ms
                state.log.append(f"epoch {epoch}: sketch-query {sketch_query_ms:.1f} ms")
                refresh()

            # --- ES QUERY ---
            if "es" in backends:
                state.phase = "es-query"
                state.phase_detail = f"aggregating {len(query_nodes)} nodes (ES)"
                refresh()
                es_json, es_query_ms = rsc.query_es_nodes(
                    args.es_url, args.es_index, args.es_api_key, query_nodes,
                    args.connect_timeout, args.es_timeout, epoch=epoch,
                )
                state.running_es_query_ms = es_query_ms
                state.log.append(f"epoch {epoch}: es-query {es_query_ms:.1f} ms")
                refresh()

            # --- SOLVE ---
            # Prefer sketch usage when available; fall back to ES.
            if sketch_json is not None:
                usage = extract_sketch_usage(sketch_json)
                usage_src = "sketch"
            elif es_json is not None:
                usage = extract_es_usage(es_json)
                usage_src = "es"
            else:
                usage = {}
                usage_src = "none"

            state.phase = "solve"
            state.phase_detail = f"OR-Tools / {args.backend} (usage={usage_src})"
            refresh()
            nodes_with_usage = apply_usage(nodes, usage)
            solver = ort.NetworkControllerSolver(nodes_with_usage, edges, solver_backend=args.backend)
            t0 = time.perf_counter()
            result = solver.solve(list(tasks.values()))
            solve_ms = (time.perf_counter() - t0) * 1000.0
            state.running_solve_ms = solve_ms

            rec = EpochRecord(
                epoch=epoch,
                sketch=BackendEpoch(ingest_ms=sketch_ingest_ms, query_ms=sketch_query_ms),
                es=BackendEpoch(ingest_ms=es_ingest_ms, query_ms=es_query_ms),
                solve_ms=solve_ms,
                rows=args.rows_per_epoch,
                assignments=len(result.decisions),
                unassigned=len(result.unassigned_tasks),
                objective=float(result.objective_value),
            )
            state.history.append(rec)
            state.log.append(
                f"epoch {epoch}: solve {solve_ms:.1f} ms  "
                f"assigned={rec.assignments}/{rec.assignments + rec.unassigned}  obj={rec.objective:.1f}"
            )

            state.phase = "idle" if epoch < args.epochs else "done"
            state.phase_detail = ""
            refresh()

    if args.no_dashboard:
        run_loop(None)
        for h in state.history:
            parts = [f"epoch {h.epoch}:"]
            if "sketch" in backends:
                parts.append(f"sk-ing={h.sketch.ingest_ms:.1f}ms sk-qry={h.sketch.query_ms:.1f}ms")
            if "es" in backends:
                parts.append(f"es-ing={h.es.ingest_ms:.1f}ms es-qry={h.es.query_ms:.1f}ms")
            parts.append(f"solve={h.solve_ms:.1f}ms total={h.total_ms:.1f}ms "
                         f"assigned={h.assignments}/{h.assignments + h.unassigned}")
            print(" ".join(parts))
    else:
        with Live(render(state), console=console, refresh_per_second=args.refresh_hz, screen=False) as live:
            try:
                run_loop(live)
            except KeyboardInterrupt:
                state.phase = "error"
                state.phase_detail = "interrupted"
                live.update(render(state))
            except Exception as exc:
                state.phase = "error"
                state.phase_detail = str(exc)[:120]
                state.log.append(f"ERROR: {exc!r}")
                live.update(render(state))
                raise


if __name__ == "__main__":
    main()
