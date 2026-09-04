#!/usr/bin/env python3
"""Per-epoch background telemetry from *different* slices of the real trace.

`raw_data/synthetic_cpu_var.csv` (built by the user's `gen_synth.py`) compresses
the whole ~21-minute observed span into a **single** 300 s window at ~430x the
real ingest rate. Replaying that one window every epoch means every epoch sees
the same grand aggregate, so a controller that froze its estimate at epoch 0 is
never wrong -- the real trace becomes decorative.

This module keeps gen_synth's construction but cuts the source the other way:
the observed span is divided into consecutive `epoch_length_s` slices, and each
slice is expanded on its own to ~1M rows. Epoch k replays slice k (cycling when
the run is longer than the trace), so the per-node background actually moves
between epochs the way it really did.

Construction, following gen_synth.py:
  * the first `--cut-s` seconds are dropped -- the collector had 8 of 44 nodes
    for 30 s and then recorded nothing for 88 s, and holding values across that
    hole manufactures a flat plateau at the head of every trace;
  * values are held piecewise-constant between source samples, so a sample is
    replicated in proportion to how long it stood (gen_synth reaches the same
    distribution by bootstrapping each node's own inter-arrival gaps and
    sampling the step function at ~13 ms; weighting by the observed hold is the
    same weighting without the resampling detour);
  * holds outside [0.5 s, 30 s] are dropped, the same window gen_synth uses to
    exclude degenerate sub-second gaps and the collection outage;
  * every emitted row gets multiplicative lognormal jitter, so no two rows are
    byte-identical and the ingest path sees ~1M distinct values.

What this does NOT fix: gen_synth's declared assumption that usage is flat
between samples is inherited, and it remains an ingest-stress fixture rather
than a physics claim. Nothing in `cpu_var.csv` constrains what happens inside a
5-second window.
"""

from __future__ import annotations

import argparse
import collections
import csv
import datetime as _dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

MILLI = 1000.0
BYTES_PER_GB = 1e9
GAP_LO = 0.5
GAP_HI = 30.0
CUT_S = 120.0
JITTER = 0.02


@dataclass
class EpochSlice:
    """One epoch's worth of background telemetry, expanded to `rows_per_epoch`."""
    index: int
    start_s: float
    end_s: float
    node_idx: "object"    # int16 array, indexes into `node_ids`
    cpu_cores: "object"   # float32
    mem_gb: "object"      # float32
    source_rows: int


def _read_trace(path: Path, keep: set[str]):
    rows = []
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            n = r["scenario_node"]
            if n not in keep:
                continue
            t = _dt.datetime.fromisoformat(r["_ingest_timestamp"].replace("Z", "+00:00"))
            rows.append((t, n, float(r["cpu_usage_millicores"]), float(r["memory_available"])))
    rows.sort(key=lambda x: x[0])
    if not rows:
        raise RuntimeError(f"no rows for the requested nodes in {path}")
    t0 = rows[0][0]
    return [((t - t0).total_seconds(), n, c, m) for t, n, c, m in rows]


def build_epoch_slices(
    raw_dir: Path,
    keep_nodes: Sequence[str],
    epoch_length_s: float,
    rows_per_epoch: int,
    seed: int,
    cut_s: float = CUT_S,
    jitter: float = JITTER,
) -> tuple[List[str], List[EpochSlice]]:
    """Expand each consecutive slice of the real trace to `rows_per_epoch` rows."""
    import numpy as np

    node_ids = sorted(set(keep_nodes))
    order = {n: i for i, n in enumerate(node_ids)}
    trace = _read_trace(raw_dir / "cpu_var.csv", set(node_ids))
    trace = [row for row in trace if row[0] >= cut_s]
    if not trace:
        raise RuntimeError(f"nothing left after the {cut_s:g}s cut")

    base = trace[0][0]
    span = trace[-1][0] - base
    n_slices = max(1, int(span // epoch_length_s))

    by_slice: Dict[int, Dict[str, list]] = collections.defaultdict(
        lambda: collections.defaultdict(list))
    for t, n, c, m in trace:
        k = int((t - base) // epoch_length_s)
        if k < n_slices:
            by_slice[k][n].append((t, c, m))

    rng = np.random.default_rng(seed)
    out: List[EpochSlice] = []
    for k in range(n_slices):
        per_node = by_slice[k]
        total_src = sum(len(v) for v in per_node.values())
        if total_src == 0:
            continue
        idx_parts, cpu_parts, mem_parts = [], [], []
        for node, samples in per_node.items():
            samples.sort()
            ts = np.asarray([s[0] for s in samples])
            cv = np.asarray([s[1] for s in samples])
            mv = np.asarray([s[2] for s in samples])

            # How long each sample stood. The last one is charged the node's
            # median hold; holds outside the gen_synth window are dropped.
            hold = np.diff(ts, append=ts[-1] + (np.median(np.diff(ts)) if len(ts) > 1 else 1.0))
            ok = (hold >= GAP_LO) & (hold <= GAP_HI)
            if not ok.any():
                ok = np.ones(len(hold), dtype=bool)
            ts, cv, mv, hold = ts[ok], cv[ok], mv[ok], hold[ok]

            share = len(samples) / total_src
            want = max(1, int(round(rows_per_epoch * share)))
            reps = np.maximum(1, np.round(hold / hold.sum() * want).astype(int))

            cpu = np.repeat(cv, reps)
            mem = np.repeat(mv, reps)
            idx_parts.append(np.full(len(cpu), order[node], dtype=np.int16))
            cpu_parts.append(cpu)
            mem_parts.append(mem)

        node_idx = np.concatenate(idx_parts)
        cpu = np.concatenate(cpu_parts).astype("float64")
        mem = np.concatenate(mem_parts).astype("float64")
        if jitter > 0:
            cpu = cpu * np.exp(rng.normal(0.0, jitter, len(cpu)))
            msd = max(float(np.std(mem)), 1.0)
            mem = mem + rng.normal(0.0, msd * 0.01, len(mem))

        out.append(EpochSlice(
            index=k,
            start_s=base + k * epoch_length_s,
            end_s=base + (k + 1) * epoch_length_s,
            node_idx=node_idx,
            cpu_cores=(cpu / MILLI).astype("float32"),
            mem_gb=(mem / BYTES_PER_GB).astype("float32"),
            source_rows=total_src,
        ))
    if not out:
        raise RuntimeError("no usable slices")
    return node_ids, out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-dir", type=Path, default=Path.home() / "raw_data")
    p.add_argument("--topology-dir", type=Path,
                   default=Path(__file__).resolve().parents[1] / "data" / "raw_topology_completion")
    p.add_argument("--epoch-length-s", type=float, default=150.0)
    p.add_argument("--rows-per-epoch", type=int, default=1_000_000)
    p.add_argument("--seed", type=int, default=20260903)
    args = p.parse_args()

    import json
    import numpy as np

    nodes = [json.loads(l)["node_id"] for l in open(args.topology_dir / "nodes.jsonl")]
    node_ids, slices = build_epoch_slices(
        args.raw_dir, nodes, args.epoch_length_s, args.rows_per_epoch, args.seed)

    print(f"{len(slices)} slices of {args.epoch_length_s:g}s over {len(node_ids)} nodes")
    print(f"{'slice':>6}{'src rows':>10}{'emitted':>10}{'cluster p50 CPU':>18}")
    tot = []
    for s in slices:
        p50 = sum(float(np.median(s.cpu_cores[s.node_idx == i]))
                  for i in range(len(node_ids)) if (s.node_idx == i).any())
        tot.append(p50)
        print(f"{s.index:>6}{s.source_rows:>10}{len(s.cpu_cores):>10,}{p50:>18.2f}")
    print(f"\ncluster background p50 across slices: {min(tot):.1f}..{max(tot):.1f} cores "
          f"(swing {(max(tot) - min(tot)) / (sum(tot) / len(tot)) * 100:.0f}% of its own mean)")
    print("compare: replaying one gen_synth window gives the same value every epoch")


if __name__ == "__main__":
    main()
