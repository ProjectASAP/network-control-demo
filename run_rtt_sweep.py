#!/usr/bin/env python3
"""Run ingestion/query RTT sweep for server + Elasticsearch."""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import requests


DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_ES_INDEX = "cluster-metrics"
DEFAULT_ES_API_KEY = os.getenv(
    "ES_API_KEY",
    "TWg0S01wc0JhR1AxOFVUcUY5N2w6bGR0TjIySHRZTHVwdmZLTmtqcGtGQQ==",
)
DEFAULT_SERVER_URL = "http://localhost:10101"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_INGEST_TIMEOUT = 60.0
DEFAULT_QUERY_TIMEOUT = 60.0
DEFAULT_ES_TIMEOUT = 60.0
DEFAULT_SERVER_READY_TIMEOUT = 30.0
DEFAULT_INGEST_RETRIES = 2
DEFAULT_INGEST_RETRY_BACKOFF = 2.0
DEFAULT_SERVER_LOG = "logs/server.log"
DEFAULT_TRUNCATE_CSV = False
DEFAULT_TRUNCATE_SERVER_LOG = False


@dataclass
class SweepResult:
    rows: int
    server_rtt_ms: float
    es_rtt_ms: float
    max_pct_diff: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=10_000, help="Start row count")
    parser.add_argument("--end", type=int, default=1_000_000, help="End row count")
    parser.add_argument("--step", type=int, default=10_000, help="Row count step")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Rows per ingest batch")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--server-url", type=str, default=DEFAULT_SERVER_URL, help="Server base URL")
    parser.add_argument(
        "--server-log",
        type=str,
        default=DEFAULT_SERVER_LOG,
        help="Server stdout/stderr log file (use '-' to disable)",
    )
    parser.add_argument(
        "--truncate-csv",
        action="store_true",
        default=DEFAULT_TRUNCATE_CSV,
        help="Truncate output CSV before writing",
    )
    parser.add_argument(
        "--truncate-server-log",
        action="store_true",
        default=DEFAULT_TRUNCATE_SERVER_LOG,
        help="Truncate server log before writing",
    )
    parser.add_argument("--connect-timeout", type=float, default=DEFAULT_CONNECT_TIMEOUT, help="HTTP connect timeout (s)")
    parser.add_argument("--ingest-timeout", type=float, default=DEFAULT_INGEST_TIMEOUT, help="Server ingest read timeout (s)")
    parser.add_argument("--query-timeout", type=float, default=DEFAULT_QUERY_TIMEOUT, help="Server query read timeout (s)")
    parser.add_argument("--es-timeout", type=float, default=DEFAULT_ES_TIMEOUT, help="Elasticsearch read timeout (s)")
    parser.add_argument(
        "--server-ready-timeout",
        type=float,
        default=DEFAULT_SERVER_READY_TIMEOUT,
        help="Wait for server readiness (s)",
    )
    parser.add_argument(
        "--ingest-retries",
        type=int,
        default=DEFAULT_INGEST_RETRIES,
        help="Retries for server ingest on timeout/connection error",
    )
    parser.add_argument(
        "--ingest-retry-backoff",
        type=float,
        default=DEFAULT_INGEST_RETRY_BACKOFF,
        help="Base backoff (s) between ingest retries",
    )
    parser.add_argument("--es-url", type=str, default=DEFAULT_ES_URL, help="Elasticsearch URL")
    parser.add_argument("--es-index", type=str, default=DEFAULT_ES_INDEX, help="Elasticsearch index")
    parser.add_argument("--es-api-key", type=str, default=DEFAULT_ES_API_KEY, help="Elasticsearch API key")
    parser.add_argument(
        "--nodes-config",
        type=str,
        default="single_node_server/network-control-server/nodes-config.yaml",
        help="Path to nodes-config.yaml",
    )
    parser.add_argument(
        "--out-csv",
        type=str,
        default="query_rtt.csv",
        help="Output CSV filename",
    )
    parser.add_argument(
        "--out-plot",
        type=str,
        default="query_rtt_plot.png",
        help="Output plot filename",
    )
    return parser.parse_args()


def parse_nodes_config(path: str) -> List[str]:
    start = None
    end = None
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line.startswith("start:"):
                start = line.split(":", 1)[1].strip()
            elif line.startswith("end:"):
                end = line.split(":", 1)[1].strip()
    if not start or not end:
        raise ValueError("nodes-config.yaml missing start/end")
    prefix_start, start_num = start[:-3], int(start[-3:])
    prefix_end, end_num = end[:-3], int(end[-3:])
    if prefix_start != prefix_end:
        raise ValueError("node id prefixes do not match")
    nodes = [f"{prefix_start}{i:03d}" for i in range(start_num, end_num + 1)]
    return nodes


def iter_batches(
    total_rows: int, nodes: List[str], rng: random.Random, batch_size: int
) -> Iterable[List[Dict[str, object]]]:
    tasks = [f"T{i:03d}" for i in range(1, 201)]
    for start in range(0, total_rows, batch_size):
        end = min(total_rows, start + batch_size)
        batch: List[Dict[str, object]] = []
        for i in range(start, end):
            node = nodes[i % len(nodes)]
            task = tasks[i % len(tasks)]
            cpu = rng.uniform(0.1, 64.0)
            mem = rng.uniform(0.1, 256.0)
            net = rng.uniform(0.1, 10_000.0)
            batch.append(
                {
                    "epoch": 0,
                    "node": node,
                    "task": task,
                    "cpu": cpu,
                    "mem": mem,
                    "net": net,
                }
            )
        yield batch


def es_headers(api_key: str | None) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    return headers


def reset_es_index(
    es_url: str,
    es_index: str,
    api_key: str | None,
    connect_timeout: float,
    read_timeout: float,
) -> None:
    headers = es_headers(api_key)
    requests.delete(
        f"{es_url}/{es_index}",
        headers=headers,
        timeout=(connect_timeout, read_timeout),
    )
    mapping = {
        "mappings": {
            "properties": {
                "epoch": {"type": "long"},
                "node": {"type": "keyword"},
                "task": {"type": "keyword"},
                "cpu": {"type": "float"},
                "mem": {"type": "float"},
                "net": {"type": "float"},
            }
        }
    }
    resp = requests.put(
        f"{es_url}/{es_index}",
        headers=headers,
        json=mapping,
        timeout=(connect_timeout, read_timeout),
    )
    resp.raise_for_status()


def bulk_ingest_es(
    es_url: str,
    es_index: str,
    api_key: str | None,
    batch: List[Dict[str, object]],
    connect_timeout: float,
    read_timeout: float,
    refresh: str | None,
) -> None:
    headers = es_headers(api_key)
    bulk_url = f"{es_url}/{es_index}/_bulk"
    lines = []
    for row in batch:
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(row))
    payload = "\n".join(lines) + "\n"
    params = {"refresh": refresh} if refresh else None
    resp = requests.post(
        bulk_url,
        headers=headers,
        data=payload,
        params=params,
        timeout=(connect_timeout, read_timeout),
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("errors"):
        raise RuntimeError("Elasticsearch bulk ingestion reported errors")
    items = data.get("items", [])
    if len(items) != len(batch):
        raise RuntimeError(
            f"Elasticsearch bulk ingestion count mismatch: {len(items)} != {len(batch)}"
        )


def ingest_server(
    server_url: str,
    batch: List[Dict[str, object]],
    connect_timeout: float,
    read_timeout: float,
    retries: int,
    retry_backoff_s: float,
) -> None:
    ingest_url = f"{server_url}/"
    payload = {
        "epoch": 0,
        "task": [row["task"] for row in batch],
        "cluster": [row["node"] for row in batch],
        "cpu_cores": [row["cpu"] for row in batch],
        "memory_gb": [row["mem"] for row in batch],
        "network_mbps": [row["net"] for row in batch],
    }
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = requests.post(
                ingest_url,
                json=payload,
                timeout=(connect_timeout, read_timeout),
            )
            resp.raise_for_status()
            data = resp.json()
            inserted = data.get("inserted")
            if inserted is None:
                raise RuntimeError("Server ingest response missing 'inserted'")
            if inserted != len(batch):
                raise RuntimeError(f"Server ingest count mismatch: {inserted} != {len(batch)}")
            return
        except (requests.ReadTimeout, requests.ConnectionError) as err:
            last_err = err
            if attempt >= retries:
                raise
            sleep_s = retry_backoff_s * (2 ** attempt)
            time.sleep(sleep_s)
    if last_err:
        raise last_err


def start_server(log_path: Path | None = None, truncate_log: bool = False) -> subprocess.Popen:
    server_dir = Path("single_node_server/network-control-server")
    stdout_target = None
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "w" if truncate_log else "a"
        stdout_target = open(log_path, mode, encoding="utf-8")
    proc = subprocess.Popen(
        ["cargo", "run"],
        cwd=server_dir,
        stdout=stdout_target or subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        text=True,
    )
    if stdout_target is not None:
        proc._log_fh = stdout_target
    return proc


def wait_for_server(
    server_url: str,
    timeout_s: float,
    connect_timeout: float,
    read_timeout: float,
) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            resp = requests.get(
                server_url,
                timeout=(connect_timeout, read_timeout),
            )
            if resp.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise RuntimeError("server did not become ready")


def stop_server(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        log_fh = getattr(proc, "_log_fh", None)
        if log_fh:
            log_fh.close()
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        log_fh = getattr(proc, "_log_fh", None)
        if log_fh:
            log_fh.close()
        return
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    log_fh = getattr(proc, "_log_fh", None)
    if log_fh:
        log_fh.close()


def query_server_batch(
    server_url: str,
    nodes: List[str],
    connect_timeout: float,
    read_timeout: float,
) -> Tuple[dict, float]:
    url = f"{server_url}/cluster-metrics/_batch"
    payload = {
        "keys": nodes,
        "fields": ["cpu_cores", "memory_gb", "network_mbps"],
        "aggs": ["percentiles", "cumulative"],
        "percents": [0, 50, 90, 100],
    }
    t0 = time.perf_counter()
    resp = requests.post(
        url,
        json=payload,
        timeout=(connect_timeout, read_timeout),
    )
    resp.raise_for_status()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return resp.json(), elapsed_ms


def query_es_nodes(
    es_url: str,
    es_index: str,
    api_key: str | None,
    nodes: List[str],
    connect_timeout: float,
    read_timeout: float,
) -> Tuple[dict, float]:
    headers = es_headers(api_key)
    url = f"{es_url}/{es_index}/_search"
    results: Dict[str, Dict[str, object]] = {}
    t0 = time.perf_counter()
    for node in nodes:
        payload = {
            "size": 0,
            "query": {"term": {"node": node}},
            "aggs": {
                "cpu_pct": {"percentiles": {"field": "cpu", "percents": [0, 50, 90, 100]}},
                "mem_pct": {"percentiles": {"field": "mem", "percents": [0, 50, 90, 100]}},
                "net_pct": {"percentiles": {"field": "net", "percents": [0, 50, 90, 100]}},
                "cpu_sum": {"sum": {"field": "cpu"}},
                "mem_sum": {"sum": {"field": "mem"}},
                "net_sum": {"sum": {"field": "net"}},
            },
        }
        resp = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=(connect_timeout, read_timeout),
        )
        resp.raise_for_status()
        results[node] = resp.json()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return results, elapsed_ms


def compare_results(server_json: dict, es_json: dict) -> float:
    max_pct = 0.0
    server_results = {item["key"]: item for item in server_json.get("results", [])}
    for node_id, es_result in es_json.items():
        server_entry = server_results.get(node_id)
        if not server_entry:
            continue
        server_pct = server_entry.get("percentiles") or {}
        server_cum = server_entry.get("cumulative") or {}
        aggs = es_result.get("aggregations", {})
        for field_key, es_name in [
            ("cpu_cores", "cpu"),
            ("memory_gb", "mem"),
            ("network_mbps", "net"),
        ]:
            pct_values = server_pct.get(field_key, {})
            es_pct = aggs.get(f"{es_name}_pct", {}).get("values", {})
            for pct in [0, 50, 90, 100]:
                s_val = pct_values.get(str(pct))
                e_val = es_pct.get(str(float(pct)))
                if s_val is None or e_val is None:
                    continue
                pct_diff = _pct_diff(float(s_val), float(e_val))
                if pct_diff >= 2.0 and pct_diff > max_pct:
                    max_pct = pct_diff
            s_cum = server_cum.get(field_key)
            e_cum = aggs.get(f"{es_name}_sum", {}).get("value")
            if s_cum is not None and e_cum is not None:
                pct_diff = _pct_diff(float(s_cum), float(e_cum))
                if pct_diff >= 2.0 and pct_diff > max_pct:
                    max_pct = pct_diff
    return max_pct


def _pct_diff(server_val: float, es_val: float) -> float:
    denom = abs(es_val) if abs(es_val) > 1e-9 else 1e-9
    return abs(server_val - es_val) / denom * 100.0


def format_compact(
    server_json: dict, es_json: dict, nodes: List[str]
) -> List[str]:
    lines: List[str] = []
    server_results = {item["key"]: item for item in server_json.get("results", [])}
    fields = [
        ("cpu_cores", "cpu"),
        ("memory_gb", "mem"),
        ("network_mbps", "net"),
    ]
    percents = [0, 50, 90, 100]
    for node_id in nodes:
        server_entry = server_results.get(node_id)
        es_entry = es_json.get(node_id)
        if not server_entry or not es_entry:
            lines.append(f"{node_id}: missing data in server or ES")
            continue
        server_pct = server_entry.get("percentiles") or {}
        server_cum = server_entry.get("cumulative") or {}
        aggs = es_entry.get("aggregations", {})

        node_lines: List[str] = []
        for field_key, es_name in fields:
            pct_values = server_pct.get(field_key, {})
            es_pct = aggs.get(f"{es_name}_pct", {}).get("values", {})
            pieces = []
            for pct in percents:
                s_val = pct_values.get(str(pct))
                e_val = es_pct.get(str(float(pct)))
                if s_val is None or e_val is None:
                    continue
                else:
                    pct_diff = _pct_diff(float(s_val), float(e_val))
                    if pct_diff >= 2.0:
                        pieces.append(
                            f"p{pct}:{float(s_val):.3f}/{float(e_val):.3f}({pct_diff:.2f}%)"
                        )
            s_cum = server_cum.get(field_key)
            e_cum = aggs.get(f"{es_name}_sum", {}).get("value")
            cum_piece = ""
            if s_cum is not None and e_cum is not None:
                pct_diff = _pct_diff(float(s_cum), float(e_cum))
                if pct_diff >= 2.0:
                    cum_piece = f"sum:{float(s_cum):.3f}/{float(e_cum):.3f}({pct_diff:.2f}%)"
            if pieces or cum_piece:
                detail = " ".join(pieces)
                if cum_piece:
                    detail = f"{detail} {cum_piece}".strip()
                node_lines.append(f"  {field_key}: {detail}")
        if node_lines:
            lines.append(f"{node_id}:")
            lines.extend(node_lines)
    return lines


def plot_results(results: List[SweepResult], out_path: Path) -> None:
    import matplotlib.pyplot as plt

    xs = [r.rows for r in results]
    server = [r.server_rtt_ms for r in results]
    es = [r.es_rtt_ms for r in results]

    plt.figure(figsize=(10, 6))
    plt.plot(xs, server, label="Server RTT (ms)")
    plt.plot(xs, es, label="ES RTT (ms)")
    plt.xlabel("Rows")
    plt.ylabel("RTT (ms)")
    plt.title("Query RTT vs Data Size")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path)


def main() -> None:
    args = parse_args()
    nodes = parse_nodes_config(args.nodes_config)
    rng = random.Random(args.seed)

    out_csv = Path(args.out_csv)
    out_plot = Path(args.out_plot)

    results: List[SweepResult] = []

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    csv_exists = out_csv.exists()
    csv_mode = "w" if args.truncate_csv else "a"
    csv_file = open(out_csv, csv_mode, newline="")
    writer = csv.writer(csv_file)
    if args.truncate_csv or not csv_exists:
        writer.writerow(["timestamp_utc", "rows", "server_rtt_ms", "es_rtt_ms"])
        csv_file.flush()

    for rows in range(args.start, args.end + 1, args.step):
        print(f"\n=== Sweep rows={rows} ===")
        reset_es_index(
            args.es_url,
            args.es_index,
            args.es_api_key,
            args.connect_timeout,
            args.es_timeout,
        )

        server_log_path = None if args.server_log == "-" else Path(args.server_log)
        proc = start_server(server_log_path, truncate_log=args.truncate_server_log)
        try:
            wait_for_server(
                args.server_url,
                args.server_ready_timeout,
                args.connect_timeout,
                args.query_timeout,
            )
            total_batches = (rows + args.batch_size - 1) // args.batch_size
            log_every = max(1, total_batches // 10)
            for batch_idx, batch in enumerate(
                iter_batches(rows, nodes, rng, args.batch_size), start=1
            ):
                ingest_server(
                    args.server_url,
                    batch,
                    args.connect_timeout,
                    args.ingest_timeout,
                    args.ingest_retries,
                    args.ingest_retry_backoff,
                )
                is_last_batch = batch_idx == total_batches
                bulk_ingest_es(
                    args.es_url,
                    args.es_index,
                    args.es_api_key,
                    batch,
                    args.connect_timeout,
                    args.es_timeout,
                    "wait_for" if is_last_batch else None,
                )
                if batch_idx % log_every == 0 or batch_idx == total_batches:
                    print(
                        f"  ingest progress: {batch_idx}/{total_batches} batches "
                        f"({batch_idx * 100 // total_batches}%)"
                    )

            server_json, server_rtt = query_server_batch(
                args.server_url,
                nodes,
                args.connect_timeout,
                args.query_timeout,
            )
            es_json, es_rtt = query_es_nodes(
                args.es_url,
                args.es_index,
                args.es_api_key,
                nodes,
                args.connect_timeout,
                args.es_timeout,
            )

            max_diff = compare_results(server_json, es_json)
            results.append(
                SweepResult(rows=rows, server_rtt_ms=server_rtt, es_rtt_ms=es_rtt, max_pct_diff=max_diff)
            )
            print(f"server RTT: {server_rtt:.2f} ms | ES RTT: {es_rtt:.2f} ms | max diff >=2%: {max_diff:.2f}%")
            print("comparisons:")
            for line in format_compact(server_json, es_json, nodes):
                print(line)
            writer.writerow(
                [
                    datetime.now(timezone.utc).isoformat(),
                    rows,
                    f"{server_rtt:.4f}",
                    f"{es_rtt:.4f}",
                ]
            )
            csv_file.flush()
        finally:
            stop_server(proc)

    csv_file.close()

    plot_results(results, out_plot)
    print(f"\nWrote {out_csv} and {out_plot}")


if __name__ == "__main__":
    main()
