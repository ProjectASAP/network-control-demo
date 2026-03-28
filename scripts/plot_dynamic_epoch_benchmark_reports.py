#!/usr/bin/env python3
"""Generate dynamic epoch benchmark report plots from fixed CSV inputs."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
PLOTS_DIR = REPO_ROOT / "plots"

INPUT_CSVS = [
    REPO_ROOT / "data/dynamic_epoch_benchmark.csv",
    REPO_ROOT / "data/dynamic_epoch_benchmark_army.csv",
    REPO_ROOT / "data/dynamic_epoch_benchmark_nopad.csv",
    REPO_ROOT / "data/dynamic_epoch_benchmark_nopad_army.csv",
]

REQUIRED_COLUMNS = {
    "epoch",
    "rows_ingested",
    "server_query_ms",
    "server_solver_ms",
    "es_query_ms",
    "es_solver_ms",
    "server_assigned_count",
    "es_assigned_count",
}


@dataclass(frozen=True)
class BenchmarkRow:
    epoch: int
    rows_ingested: int
    server_query_ms: float
    server_solver_ms: float
    es_query_ms: float
    es_solver_ms: float
    server_assigned_count: int
    es_assigned_count: int


def _parse_row(raw: dict[str, str]) -> BenchmarkRow:
    return BenchmarkRow(
        epoch=int(raw["epoch"]),
        rows_ingested=int(float(raw["rows_ingested"])),
        server_query_ms=float(raw["server_query_ms"]),
        server_solver_ms=float(raw["server_solver_ms"]),
        es_query_ms=float(raw["es_query_ms"]),
        es_solver_ms=float(raw["es_solver_ms"]),
        server_assigned_count=int(float(raw["server_assigned_count"])),
        es_assigned_count=int(float(raw["es_assigned_count"])),
    )


def load_rows(csv_path: Path) -> list[BenchmarkRow]:
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("CSV has no header.")
        missing = sorted(REQUIRED_COLUMNS - set(reader.fieldnames))
        if missing:
            raise ValueError(f"CSV missing required columns: {', '.join(missing)}")

        rows: list[BenchmarkRow] = []
        for idx, raw in enumerate(reader, start=2):
            if not raw:
                continue
            try:
                parsed = _parse_row(raw)
            except Exception as exc:
                raise ValueError(f"Invalid row at line {idx}: {exc}") from exc
            if parsed.rows_ingested == 0 and parsed.epoch != 0:
                continue
            rows.append(parsed)

    rows.sort(key=lambda r: r.epoch)
    return rows


def _plot_query_solver_breakdown(rows: list[BenchmarkRow], out_path: Path, title_tag: str) -> None:
    import matplotlib.pyplot as plt
    import numpy as np

    epochs = [r.epoch for r in rows]
    x = np.arange(len(epochs))
    width = 0.36

    server_query = [r.server_query_ms for r in rows]
    server_solver = [r.server_solver_ms for r in rows]
    es_query = [r.es_query_ms for r in rows]
    es_solver = [r.es_solver_ms for r in rows]
    rows_ingested = [r.rows_ingested for r in rows]

    fig, ax = plt.subplots(figsize=(max(10, len(epochs) * 1.2), 6))
    ax.bar(x - width / 2, server_query, width, label="Sketch query (ms)", color="#4e79a7")
    ax.bar(
        x - width / 2,
        server_solver,
        width,
        bottom=server_query,
        label="Sketch solver (ms)",
        color="#59a14f",
    )
    ax.bar(x + width / 2, es_query, width, label="ES query (ms)", color="#e15759")
    ax.bar(
        x + width / 2,
        es_solver,
        width,
        bottom=es_query,
        label="ES solver (ms)",
        color="#f28e2b",
    )
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Time (ms)")
    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs])
    ax.grid(axis="y", alpha=0.3)

    ax2 = ax.twinx()
    line = ax2.plot(
        x,
        rows_ingested,
        color="#222222",
        marker="o",
        linewidth=1.8,
        label="Rows ingested",
    )[0]
    ax2.set_ylabel("Rows Ingested")

    handles1, labels1 = ax.get_legend_handles_labels()
    handles2, labels2 = [line], ["Rows ingested"]
    ax.legend(handles1 + handles2, labels1 + labels2, loc="upper left", fontsize=9)
    ax.set_title(f"{title_tag}: Sketch vs ES Query+Solver Breakdown + Rows")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_tasks_assigned(rows: list[BenchmarkRow], out_path: Path, title_tag: str) -> None:
    import matplotlib.pyplot as plt
    import numpy as np

    epochs = [r.epoch for r in rows]
    x = np.arange(len(epochs))
    width = 0.38

    server_assigned = [r.server_assigned_count for r in rows]
    es_assigned = [r.es_assigned_count for r in rows]

    fig, ax = plt.subplots(figsize=(max(10, len(epochs) * 1.2), 5))
    ax.bar(x - width / 2, server_assigned, width, label="Sketch assigned", color="#4e79a7")
    ax.bar(x + width / 2, es_assigned, width, label="ES assigned", color="#e15759")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Tasks Assigned")
    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs])
    ax.set_title(f"{title_tag}: Sketch vs ES Tasks Assigned per Epoch")
    ax.grid(axis="y", alpha=0.3)
    ax.legend()

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _title_tag(stem: str) -> str:
    return stem.replace("_", " ")


def _short_output_stem(input_stem: str) -> str:
    if input_stem == "dynamic_epoch_benchmark":
        return "dynamic_epoch"
    if input_stem == "dynamic_epoch_benchmark_army":
        return "dynamic_epoch_army"
    if input_stem == "dynamic_epoch_benchmark_nopad":
        return "dynamic_epoch_nopad"
    if input_stem == "dynamic_epoch_benchmark_nopad_army":
        return "dynamic_epoch_nopad_army"
    return input_stem.replace("_benchmark", "")


def _iter_outputs(stem: str) -> Iterable[Path]:
    yield PLOTS_DIR / f"{stem}_query_solver_breakdown_rows.png"
    yield PLOTS_DIR / f"{stem}_query_solver_breakdown_rows_no_epoch0.png"
    yield PLOTS_DIR / f"{stem}_tasks_assigned.png"


def main() -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    generated = 0
    skipped_missing = 0
    skipped_empty = 0
    skipped_invalid = 0

    for csv_path in INPUT_CSVS:
        if not csv_path.exists():
            print(f"[WARN] Missing CSV, skipped: {csv_path}")
            skipped_missing += 1
            continue

        try:
            rows = load_rows(csv_path)
        except ValueError as exc:
            print(f"[WARN] Invalid CSV, skipped: {csv_path} ({exc})")
            skipped_invalid += 1
            continue

        if not rows:
            print(f"[WARN] No rows left after rows_ingested>0 filter, skipped: {csv_path}")
            skipped_empty += 1
            continue

        input_stem = csv_path.stem
        out_stem = _short_output_stem(input_stem)
        title_tag = _title_tag(out_stem)
        out_breakdown, out_breakdown_no_epoch0, out_tasks = _iter_outputs(out_stem)

        _plot_query_solver_breakdown(rows, out_breakdown, title_tag)
        print(f"Wrote {out_breakdown}")
        generated += 1

        rows_no_epoch0 = [r for r in rows if r.epoch != 0]
        if rows_no_epoch0:
            _plot_query_solver_breakdown(
                rows_no_epoch0,
                out_breakdown_no_epoch0,
                f"{title_tag} (epoch>0)",
            )
            print(f"Wrote {out_breakdown_no_epoch0}")
            generated += 1
        else:
            print(
                "[WARN] No rows left for no-epoch0 breakdown plot, skipped: "
                f"{out_breakdown_no_epoch0}"
            )

        _plot_tasks_assigned(rows, out_tasks, title_tag)
        print(f"Wrote {out_tasks}")
        generated += 1

    print(
        "Summary: "
        f"generated={generated}, "
        f"skipped_missing={skipped_missing}, "
        f"skipped_empty_after_filter={skipped_empty}, "
        f"skipped_invalid={skipped_invalid}"
    )


if __name__ == "__main__":
    main()
