import argparse
import csv
import json
import time
import uuid
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import requests

from config import ES_API_KEY, ES_INDEX_NAME, ES_URL, SKETCH_API_KEY, SKETCH_URL
from es_query import (
    CumulativeResult,
    NodeMetricsSnapshot,
    build_es_node_metrics_payload,
    build_sketch_node_metrics_payload,
    compare_node_metrics,
)
from scheduler.load_info import load_nodes

# To run (writes manifest, clears RTT + compare CSVs, reruns benchmarks, rewrites plot):
# python bench_queries.py --backend both --runs 5 --reset --compare-values

# Resolve node IDs from explicit list or by loading the nodes file.
def _resolve_node_ids(node_ids_arg: str | None, node_path: str) -> list[str]:
    if node_ids_arg:
        return [node_id.strip() for node_id in node_ids_arg.split(",") if node_id.strip()]
    nodes = load_nodes(node_path)
    return list(nodes.keys())


# Build synthetic node IDs like N001..N030 for workload sizing.
def _build_synthetic_node_ids(count: int) -> list[str]:
    return [f"N{idx:03d}" for idx in range(1, count + 1)]

# Sort node IDs like N001, N002 by numeric suffix when available.
def _sort_node_ids(node_ids: list[str]) -> list[str]:
    def _key(node_id: str) -> tuple[int, str]:
        digits = "".join(ch for ch in node_id if ch.isdigit())
        return (int(digits) if digits else 0, node_id)

    return sorted(node_ids, key=_key)


# Issue a search request and return success + status + response payload.
def _post_payload(
    session: requests.Session,
    url: str,
    index_name: str,
    api_key: str,
    payload: dict,
    request_type: str,
) -> tuple[bool, int | None, dict | None]:
    request_id = uuid.uuid4().hex[:8]
    endpoint = f"{url}/{index_name}/_search"
    headers = {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json",
        "X-Request-Id": request_id,
        "X-Request-Type": request_type,
    }
    response = session.post(endpoint, json=payload, headers=headers)
    status = response.status_code if response is not None else None
    ok = bool(response.ok)
    data = response.json() if ok else None
    return ok, status, data


# Append a single benchmark row to the output CSV (create header if needed).
def _write_row(path: Path, row: dict) -> None:
    needs_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        if needs_header:
            writer.writeheader()
        writer.writerow(row)


# Append comparison discrepancies to the output CSV.
def _write_compare_rows(
    path: Path,
    variant: str,
    run: int,
    workload: str,
    node_count: int,
    discrepancies: list[str],
) -> None:
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
    if not discrepancies:
        _write_row(
            path,
            {
                "timestamp": timestamp,
                "variant": variant,
                "run": run,
                "workload": workload,
                "node_count": node_count,
                "message": "ok",
            },
        )
        return
    for message in discrepancies:
        _write_row(
            path,
            {
                "timestamp": timestamp,
                "variant": variant,
                "run": run,
                "workload": workload,
                "node_count": node_count,
                "message": message,
            },
        )


# Plot mean and stddev RTT by variant/backend from the output CSV.
# Plot per-variant RTT traces with workload lines.
def _plot_results(csv_path: Path, out_dir: Path) -> None:
    df = pd.read_csv(csv_path)
    if df.empty:
        print(f"No data to plot in {csv_path}")
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    df = df.dropna(subset=["variant", "workload", "run"])
    def _plot_subset(
        subset: pd.DataFrame,
        title: str,
        filename: str,
        include_variant_label: bool,
        figsize: tuple[float, float],
    ) -> None:
        values = sorted(subset["duration_ms"].dropna().tolist())
        max_val = values[-1] if values else 0.0
        use_broken_axis = False
        lower_max = 0.0
        upper_min = 0.0
        if len(values) >= 2:
            gaps = []
            for idx in range(1, len(values)):
                prev = values[idx - 1]
                curr = values[idx]
                gaps.append((curr - prev, idx, prev, curr))
            max_gap, idx, prev, curr = max(gaps, key=lambda x: x[0])
            if max_gap > max(5.0, prev * 0.5):
                use_broken_axis = True
                lower_max = prev * 1.05
                upper_min = curr * 0.95
        if use_broken_axis:
            fig, (ax_top, ax_bottom) = plt.subplots(
                2,
                1,
                sharex=True,
                figsize=figsize,
                gridspec_kw={"height_ratios": [1, 2]},
            )
            for (variant_name, backend, workload), group_df in subset.groupby(
                ["variant", "backend", "workload"]
            ):
                group_df = group_df.sort_values("run")
                label = (
                    f"{variant_name}|{backend}:{workload}"
                    if include_variant_label
                    else f"{backend}:{workload}"
                )
                ax_top.plot(
                    group_df["run"],
                    group_df["duration_ms"],
                    marker="o",
                    label=label,
                )
                ax_bottom.plot(
                    group_df["run"],
                    group_df["duration_ms"],
                    marker="o",
                    label=label,
                )
            ax_bottom.set_ylim(0, lower_max)
            top_end = max(upper_min * 1.05, max_val * 1.02)
            ax_top.set_ylim(upper_min, top_end)
            ax_top.spines["bottom"].set_visible(False)
            ax_bottom.spines["top"].set_visible(False)
            ax_top.tick_params(labeltop=False)
            ax_bottom.set_xticks(sorted(subset["run"].unique()))
            ax_bottom.xaxis.tick_bottom()
            d = 0.012
            kwargs = dict(transform=ax_top.transAxes, color="k", clip_on=False)
            ax_top.plot((-d, +d), (-d, +d), **kwargs)
            ax_top.plot((1 - d, 1 + d), (-d, +d), **kwargs)
            kwargs.update(transform=ax_bottom.transAxes)
            ax_bottom.plot((-d, +d), (1 - d, 1 + d), **kwargs)
            ax_bottom.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)
            ax_top.set_title(title)
            ax_bottom.set_xlabel("run")
            ax_bottom.set_ylabel("duration_ms")
            ax_bottom.legend(
                loc="upper left",
                bbox_to_anchor=(1.02, 1.0),
                borderaxespad=0.0,
                frameon=False,
            )
            plt.tight_layout(rect=(0, 0, 0.82, 1))
        else:
            plt.figure(figsize=figsize)
            for (variant_name, backend, workload), group_df in subset.groupby(
                ["variant", "backend", "workload"]
            ):
                group_df = group_df.sort_values("run")
                label = (
                    f"{variant_name}|{backend}:{workload}"
                    if include_variant_label
                    else f"{backend}:{workload}"
                )
                plt.plot(
                    group_df["run"],
                    group_df["duration_ms"],
                    marker="o",
                    label=label,
                )
            plt.title(title)
            plt.xlabel("run")
            plt.ylabel("duration_ms")
            plt.xticks(sorted(subset["run"].unique()))
            plt.legend(
                loc="upper left",
                bbox_to_anchor=(1.02, 1.0),
                borderaxespad=0.0,
                frameon=False,
            )
            plt.tight_layout(rect=(0, 0, 0.82, 1))
        out_path = out_dir / filename
        plt.savefig(out_path)
        plt.close()

    def _plot_with_optional_warmup(
        subset: pd.DataFrame,
        title: str,
        filename: str,
        include_variant_label: bool,
        figsize: tuple[float, float],
    ) -> None:
        if subset.empty:
            return
        _plot_subset(subset, title, filename, include_variant_label, figsize)
        no_first = subset[subset["run"] != 1]
        if not no_first.empty:
            stem = Path(filename).stem
            suffix = Path(filename).suffix
            no_first_name = f"{stem}_no_first_run{suffix}"
            _plot_subset(
                no_first,
                f"{title} (no first run)",
                no_first_name,
                include_variant_label,
                figsize,
            )

    for variant in sorted(df["variant"].unique()):
        subset = df[df["variant"] == variant]
        safe_variant = "".join(
            ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in variant
        )
        _plot_with_optional_warmup(
            subset,
            f"{variant} (sketch vs es)",
            f"{safe_variant}.png",
            include_variant_label=False,
            figsize=(10, 6),
        )

    quantile_variants = {
        "p50_cpu",
        "p25_p50_p75_p90_cpu",
        "p50_cpu_mem_net",
        "p25_p50_p75_p90_cpu_mem_net",
    }
    quantile_workloads = {"N001", "N001-N030"}
    quantile_subset = df[
        df["variant"].isin(quantile_variants) & df["workload"].isin(quantile_workloads)
    ]
    if not quantile_subset.empty:
        for backend in sorted(quantile_subset["backend"].unique()):
            backend_subset = quantile_subset[quantile_subset["backend"] == backend]
            _plot_with_optional_warmup(
                backend_subset,
                f"Mixed workload quantiles ({backend})",
                f"mixed_workload_quantiles_{backend}.png",
                include_variant_label=True,
                figsize=(16, 9),
            )
    cumulative_variants = {"cumulative_cpu", "cumulative_cpu_mem_net"}
    cumulative_workloads = {"N001", "N001-N030"}
    cumulative_subset = df[
        df["variant"].isin(cumulative_variants)
        & df["workload"].isin(cumulative_workloads)
    ]
    if not cumulative_subset.empty:
        for backend in sorted(cumulative_subset["backend"].unique()):
            backend_subset = cumulative_subset[cumulative_subset["backend"] == backend]
            _plot_with_optional_warmup(
                backend_subset,
                f"Mixed workload cumulative ({backend})",
                f"mixed_workload_cumulative_{backend}.png",
                include_variant_label=True,
                figsize=(16, 9),
            )


# Build payloads with feature toggles for percentiles/cumulative/top.
def _build_payload(
    backend: str,
    node_ids: list[str],
    metrics: list[str],
    percentiles: list[int],
    include_quantiles: bool,
    include_cumulative: bool,
    include_top: bool,
) -> dict:
    if backend == "es":
        payload = build_es_node_metrics_payload(
            node_ids, metrics=metrics, percentiles=percentiles
        )
        node_aggs = payload.get("aggs", {}).get("nodes_metrics", {}).get("aggs", {})
        if not include_quantiles:
            for key in list(node_aggs.keys()):
                if key.startswith("p50_"):
                    node_aggs.pop(key, None)
        if not include_cumulative:
            for key in list(node_aggs.keys()):
                if key.startswith("cum_"):
                    node_aggs.pop(key, None)
        if not include_top:
            payload.get("aggs", {}).pop("top_all", None)
        return payload

    payload = build_sketch_node_metrics_payload(
        node_ids, metrics=metrics, percentiles=percentiles
    )
    aggs = payload.get("aggs", {})
    if not include_quantiles:
        for key in list(aggs.keys()):
            if key.startswith("p50_"):
                aggs.pop(key, None)
    if not include_cumulative:
        for key in list(aggs.keys()):
            if key.startswith("cum_"):
                aggs.pop(key, None)
    if not include_top:
        aggs.pop("top_all", None)
    return payload


# Extract node metrics from backend response for comparison.
def _parse_node_metrics_response(
    response: dict,
    node_ids: list[str],
    use_es: bool,
) -> dict[str, NodeMetricsSnapshot]:
    aggregations = response.get("aggregations", {}) if response else {}
    snapshots: dict[str, NodeMetricsSnapshot] = {}
    if use_es:
        buckets = aggregations.get("nodes_metrics", {}).get("buckets", {})
        for node_id in node_ids:
            bucket = buckets.get(node_id, {})
            cpu_values = _extract_percentile_values(bucket.get("p50_cpu", {}))
            mem_values = _extract_percentile_values(bucket.get("p50_mem", {}))
            net_values = _extract_percentile_values(bucket.get("p50_net", {}))
            cpu_p25 = _pick_percentile(cpu_values, 25.0)
            cpu_p50 = _pick_percentile(cpu_values, 50.0)
            cpu_p75 = _pick_percentile(cpu_values, 75.0)
            cpu_p90 = _pick_percentile(cpu_values, 90.0)
            mem_p25 = _pick_percentile(mem_values, 25.0)
            mem_p50 = _pick_percentile(mem_values, 50.0)
            mem_p75 = _pick_percentile(mem_values, 75.0)
            mem_p90 = _pick_percentile(mem_values, 90.0)
            net_p25 = _pick_percentile(net_values, 25.0)
            net_p50 = _pick_percentile(net_values, 50.0)
            net_p75 = _pick_percentile(net_values, 75.0)
            net_p90 = _pick_percentile(net_values, 90.0)

            cum_cpu = _get_agg_value(bucket, "cum_cpu")
            cum_mem = _get_agg_value(bucket, "cum_mem")
            cum_net = _get_agg_value(bucket, "cum_net")
            cumulative = None
            if cum_cpu is not None or cum_mem is not None or cum_net is not None:
                cumulative = CumulativeResult(
                    cpu_cores=float(cum_cpu or 0.0),
                    memory_gb=float(cum_mem or 0.0),
                    network_mbps=float(cum_net or 0.0),
                )
            snapshots[node_id] = NodeMetricsSnapshot(
                node_id=node_id,
                cpu_p25=cpu_p25,
                cpu_p50=cpu_p50,
                cpu_p75=cpu_p75,
                cpu_p90=cpu_p90,
                memory_p25=mem_p25,
                memory_p50=mem_p50,
                memory_p75=mem_p75,
                memory_p90=mem_p90,
                network_p25=net_p25,
                network_p50=net_p50,
                network_p75=net_p75,
                network_p90=net_p90,
                cumulative=cumulative,
            )
        return snapshots

    for node_id in node_ids:
        cpu_values = _extract_percentile_values(
            aggregations.get(f"p50_cpu_{node_id}", {})
        )
        mem_values = _extract_percentile_values(
            aggregations.get(f"p50_mem_{node_id}", {})
        )
        net_values = _extract_percentile_values(
            aggregations.get(f"p50_net_{node_id}", {})
        )
        cpu_p25 = _pick_percentile(cpu_values, 25.0)
        cpu_p50 = _pick_percentile(cpu_values, 50.0)
        cpu_p75 = _pick_percentile(cpu_values, 75.0)
        cpu_p90 = _pick_percentile(cpu_values, 90.0)
        mem_p25 = _pick_percentile(mem_values, 25.0)
        mem_p50 = _pick_percentile(mem_values, 50.0)
        mem_p75 = _pick_percentile(mem_values, 75.0)
        mem_p90 = _pick_percentile(mem_values, 90.0)
        net_p25 = _pick_percentile(net_values, 25.0)
        net_p50 = _pick_percentile(net_values, 50.0)
        net_p75 = _pick_percentile(net_values, 75.0)
        net_p90 = _pick_percentile(net_values, 90.0)

        cum_cpu = _get_agg_value(aggregations, f"cum_cpu_{node_id}")
        cum_mem = _get_agg_value(aggregations, f"cum_mem_{node_id}")
        cum_net = _get_agg_value(aggregations, f"cum_net_{node_id}")
        cumulative = None
        if cum_cpu is not None or cum_mem is not None or cum_net is not None:
            cumulative = CumulativeResult(
                cpu_cores=float(cum_cpu or 0.0),
                memory_gb=float(cum_mem or 0.0),
                network_mbps=float(cum_net or 0.0),
            )
        snapshots[node_id] = NodeMetricsSnapshot(
            node_id=node_id,
            cpu_p25=cpu_p25,
            cpu_p50=cpu_p50,
            cpu_p75=cpu_p75,
            cpu_p90=cpu_p90,
            memory_p25=mem_p25,
            memory_p50=mem_p50,
            memory_p75=mem_p75,
            memory_p90=mem_p90,
            network_p25=net_p25,
            network_p50=net_p50,
            network_p75=net_p75,
            network_p90=net_p90,
            cumulative=cumulative,
        )
    return snapshots


# Pull a numeric value out of an aggregation entry.
def _get_agg_value(container: dict, key: str) -> float | None:
    agg = container.get(key)
    if not isinstance(agg, dict):
        return None
    value = agg.get("value")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# Normalize a percentile aggregation entry into a values container.
def _extract_percentile_values(agg: dict) -> dict | list | float | int | None:
    if not isinstance(agg, dict):
        return agg
    if "values" in agg:
        return agg.get("values")
    if "value" in agg:
        return agg.get("value")
    return agg


# Resolve percentile values that may use stringified numeric keys.
def _pick_percentile(values: dict | list | float | int | None, percentile: float) -> float | None:
    if values is None:
        return None
    if isinstance(values, (int, float)):
        return float(values)
    if isinstance(values, list):
        for item in values:
            if not isinstance(item, dict):
                continue
            key = item.get("key")
            if key is None:
                continue
            if str(key) in (str(percentile), f"{percentile:.1f}"):
                value = item.get("value")
                return float(value) if value is not None else None
    direct = values.get(str(percentile))
    if direct is not None:
        return direct
    if percentile.is_integer():
        int_key = str(int(percentile))
        direct_int = values.get(int_key)
        if direct_int is not None:
            return direct_int
    alt = values.get(f"{percentile:.1f}")
    if alt is not None:
        return alt
    numeric = values.get(percentile)
    if numeric is not None:
        return numeric
    if percentile.is_integer():
        numeric_int = values.get(int(percentile))
        if numeric_int is not None:
            return numeric_int
    return None


# Entry point: run benchmark variants and write CSV + plot.
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run query variants and plot RTT results."
    )
    parser.add_argument(
        "--node-path",
        default="dummy_data/nodes.csv",
        help="Path to nodes CSV/JSONL used to build node_ids.",
    )
    parser.add_argument(
        "--node-ids",
        default=None,
        help="Comma-separated node IDs (overrides --node-path).",
    )
    parser.add_argument(
        "--backend",
        choices=["sketch", "es", "both"],
        default="sketch",
        help="Backend to benchmark.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of runs per variant/backend.",
    )
    parser.add_argument(
        "--variant",
        action="append",
        default=None,
        help="Variant name(s) to run (overrides automatic variant set).",
    )
    parser.add_argument(
        "--workload-sizes",
        default="1,4,10,30",
        help="Comma-separated node counts for workload scaling.",
    )
    parser.add_argument(
        "--percentiles",
        default="25,50,75,90",
        help="Comma-separated percentiles for quantile queries.",
    )
    parser.add_argument(
        "--out-csv",
        default="bench_queries_rtt.csv",
        help="CSV path to store benchmark RTTs.",
    )
    parser.add_argument(
        "--out-plot-dir",
        default="bench_plots",
        help="Directory to store RTT plots.",
    )
    parser.add_argument(
        "--compare-values",
        action="store_true",
        help="Compare extracted metrics when --backend is both.",
    )
    parser.add_argument(
        "--compare-tolerance",
        type=float,
        default=0.01,
        help="Relative tolerance for value comparisons.",
    )
    parser.add_argument(
        "--compare-out",
        default="bench_queries_compare.csv",
        help="CSV path for value comparison discrepancies.",
    )
    parser.add_argument(
        "--manifest-out",
        default="bench_queries_manifest.txt",
        help="Path to write the query manifest before running benchmarks.",
    )
    parser.add_argument(
        "--dump-response-dir",
        default=None,
        help="Directory to write raw response JSON for debugging.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete the output CSV before writing new results.",
    )
    args = parser.parse_args()

    node_ids = _sort_node_ids(_resolve_node_ids(args.node_ids, args.node_path))

    percentiles = [int(item) for item in args.percentiles.split(",") if item.strip()]
    percentile_sets = [[25], [50], [75], [90], percentiles]
    metric_groups = [
        ("cpu", ["cpu"]),
        ("mem", ["mem"]),
        ("net", ["net"]),
        ("cpu_mem_net", ["cpu", "mem", "net"]),
    ]
    variants: dict[str, dict] = {}
    for metric_label, metrics in metric_groups:
        for pct_list in percentile_sets:
            pct_label = (
                "p" + "_p".join(str(p) for p in pct_list)
                if len(pct_list) > 1
                else f"p{pct_list[0]}"
            )
            variant_name = f"{pct_label}_{metric_label}"
            variants[variant_name] = {
                "metrics": metrics,
                "percentiles": pct_list,
                "include_quantiles": True,
                "include_cumulative": False,
                "include_top": False,
            }
        cumulative_name = f"cumulative_{metric_label}"
        variants[cumulative_name] = {
            "metrics": metrics,
            "percentiles": percentiles,
            "include_quantiles": False,
            "include_cumulative": True,
            "include_top": False,
        }
    for metric_label, metrics in metric_groups[:3]:
        variants[f"top_{metric_label}"] = {
            "metrics": metrics,
            "percentiles": percentiles,
            "include_quantiles": False,
            "include_cumulative": False,
            "include_top": True,
        }
    if args.variant:
        variants = {name: variants[name] for name in args.variant if name in variants}
        if not variants:
            raise ValueError("No valid variants selected.")

    workload_sizes = [
        int(item) for item in args.workload_sizes.split(",") if item.strip()
    ]
    workloads = []
    for size in workload_sizes:
        if size <= 0:
            continue
        subset = _build_synthetic_node_ids(size)
        label = (
            f"{subset[0]}-{subset[-1]}" if len(subset) > 1 else subset[0]
        )
        workloads.append({"label": label, "node_ids": subset})
    if not workloads:
        raise ValueError("No valid workload sizes selected.")

    backends = ["sketch", "es"] if args.backend == "both" else [args.backend]
    out_csv = Path(args.out_csv)
    if args.reset and out_csv.exists():
        out_csv.unlink()
    compare_out = Path(args.compare_out)
    if args.reset and args.compare_values and compare_out.exists():
        compare_out.unlink()
    if args.reset:
        plot_dir = Path(args.out_plot_dir)
        if plot_dir.exists():
            for path in plot_dir.glob("*.png"):
                path.unlink()
    dump_dir = Path(args.dump_response_dir) if args.dump_response_dir else None
    if dump_dir:
        dump_dir.mkdir(parents=True, exist_ok=True)

    if args.manifest_out:
        manifest_lines = []
        for workload in workloads:
            for variant_name, spec in variants.items():
                line = (
                    f"workload={workload['label']} "
                    f"nodes={len(workload['node_ids'])} "
                    f"variant={variant_name} "
                    f"metrics={','.join(spec['metrics'])} "
                    f"percentiles={','.join(str(p) for p in spec['percentiles'])} "
                    f"quantiles={spec['include_quantiles']} "
                    f"cumulative={spec['include_cumulative']} "
                    f"top={spec['include_top']}"
                )
                manifest_lines.append(line)
        Path(args.manifest_out).write_text("\n".join(manifest_lines) + "\n")

    with requests.Session() as session:
        for workload in workloads:
            for variant_name, spec in variants.items():
                for run_idx in range(args.runs):
                    if args.backend == "both":
                        payload_sketch = _build_payload(
                            "sketch",
                            workload["node_ids"],
                            spec["metrics"],
                            spec["percentiles"],
                            spec["include_quantiles"],
                            spec["include_cumulative"],
                            spec["include_top"],
                        )
                        sketch_start = time.perf_counter()
                        sketch_ok, sketch_status, sketch_data = _post_payload(
                            session=session,
                            url=SKETCH_URL,
                            index_name=ES_INDEX_NAME,
                            api_key=SKETCH_API_KEY,
                            payload=payload_sketch,
                            request_type="bench_node_metrics",
                        )
                        if dump_dir and sketch_data is not None:
                            sketch_path = (
                                dump_dir
                                / f"sketch_{variant_name}_run{run_idx + 1}.json"
                            )
                            sketch_path.write_text(
                                json.dumps(sketch_data, indent=2, sort_keys=True)
                            )
                        sketch_ms = (time.perf_counter() - sketch_start) * 1000.0
                        _write_row(
                            out_csv,
                            {
                                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                                "backend": "sketch",
                                "variant": variant_name,
                                "workload": workload["label"],
                                "node_count": len(workload["node_ids"]),
                                "run": run_idx + 1,
                                "duration_ms": sketch_ms,
                                "ok": sketch_ok,
                                "status": sketch_status,
                            },
                        )
                        print(
                            f"sketch/{variant_name} run {run_idx + 1}: "
                            f"{sketch_ms:.2f} ms (ok={sketch_ok}, status={sketch_status})"
                        )

                        payload_es = _build_payload(
                            "es",
                            workload["node_ids"],
                            spec["metrics"],
                            spec["percentiles"],
                            spec["include_quantiles"],
                            spec["include_cumulative"],
                            spec["include_top"],
                        )
                        es_start = time.perf_counter()
                        es_ok, es_status, es_data = _post_payload(
                            session=session,
                            url=ES_URL,
                            index_name=ES_INDEX_NAME,
                            api_key=ES_API_KEY,
                            payload=payload_es,
                            request_type="bench_node_metrics",
                        )
                        if dump_dir and es_data is not None:
                            es_path = (
                                dump_dir / f"es_{variant_name}_run{run_idx + 1}.json"
                            )
                            es_path.write_text(
                                json.dumps(es_data, indent=2, sort_keys=True)
                            )
                        es_ms = (time.perf_counter() - es_start) * 1000.0
                        _write_row(
                            out_csv,
                            {
                                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                                "backend": "es",
                                "variant": variant_name,
                                "workload": workload["label"],
                                "node_count": len(workload["node_ids"]),
                                "run": run_idx + 1,
                                "duration_ms": es_ms,
                                "ok": es_ok,
                                "status": es_status,
                            },
                        )
                        print(
                            f"es/{variant_name} run {run_idx + 1}: "
                            f"{es_ms:.2f} ms (ok={es_ok}, status={es_status})"
                        )

                        if args.compare_values and sketch_ok and es_ok:
                            sketch_metrics = _parse_node_metrics_response(
                                sketch_data or {}, workload["node_ids"], use_es=False
                            )
                            es_metrics = _parse_node_metrics_response(
                                es_data or {}, workload["node_ids"], use_es=True
                            )
                            discrepancies = compare_node_metrics(
                                sketch_metrics,
                                es_metrics,
                                tolerance=args.compare_tolerance,
                            )
                            _write_compare_rows(
                                compare_out,
                                variant_name,
                                run_idx + 1,
                                workload["label"],
                                len(workload["node_ids"]),
                                discrepancies,
                            )
                            if discrepancies:
                                print(
                                    f"compare/{variant_name} run {run_idx + 1}: "
                                    f"{len(discrepancies)} mismatches"
                                )
                            else:
                                print(
                                    f"compare/{variant_name} run {run_idx + 1}: ok"
                                )
                        continue

                    for backend in backends:
                        if backend == "es":
                            payload = _build_payload(
                                "es",
                                workload["node_ids"],
                                spec["metrics"],
                                spec["percentiles"],
                                spec["include_quantiles"],
                                spec["include_cumulative"],
                                spec["include_top"],
                            )
                            url = ES_URL
                            api_key = ES_API_KEY
                        else:
                            payload = _build_payload(
                                "sketch",
                                workload["node_ids"],
                                spec["metrics"],
                                spec["percentiles"],
                                spec["include_quantiles"],
                                spec["include_cumulative"],
                                spec["include_top"],
                            )
                            url = SKETCH_URL
                            api_key = SKETCH_API_KEY

                        start_t = time.perf_counter()
                        ok, status, _ = _post_payload(
                            session=session,
                            url=url,
                            index_name=ES_INDEX_NAME,
                            api_key=api_key,
                            payload=payload,
                            request_type="bench_node_metrics",
                        )
                        duration_ms = (time.perf_counter() - start_t) * 1000.0
                        _write_row(
                            out_csv,
                            {
                                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                                "backend": backend,
                                "variant": variant_name,
                                "workload": workload["label"],
                                "node_count": len(workload["node_ids"]),
                                "run": run_idx + 1,
                                "duration_ms": duration_ms,
                                "ok": ok,
                                "status": status,
                            },
                        )
                        print(
                            f"{backend}/{variant_name} run {run_idx + 1}: "
                            f"{duration_ms:.2f} ms (ok={ok}, status={status})"
                        )

    _plot_results(out_csv, Path(args.out_plot_dir))
    print(f"Wrote {out_csv} and plots in {args.out_plot_dir}")


if __name__ == "__main__":
    # Allow running as a script.
    main()
