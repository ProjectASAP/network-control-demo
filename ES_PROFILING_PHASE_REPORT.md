# Elasticsearch Profiling — Phase Report

**Date:** 2026-04-28
**Target:** Elasticsearch 8.17.2 (single-node, localhost:9200, API key auth disabled)
**Scope:** Where does time go in the cluster-metrics ingest + query workload used by `scripts/rtt_sweep_common.py`, and what optimizations are available without changing ES configuration.

---

## 1. Setup

### Index

```
index:    cluster-metrics
shards:   1 primary, 0 replicas (default for single-node)
mapping:
  epoch: long
  node:  keyword
  task:  keyword
  cpu:   float
  mem:   float
  net:   float
```

### Workload

| | value |
|---|---|
| Nodes | 30 (`N001`–`N030`) |
| Tasks | 200 (`T001`–`T200`) |
| Rows per epoch | 1,000,000 |
| Epochs profiled | 5 |
| Total docs | 5,000,000 |
| Docs per `(node, epoch)` partition | ~33,333 |
| Bulk batch size | 1,000 rows |

### Production query (verbatim from `rtt_sweep_common.py:531-577`)

`POST /cluster-metrics/_search` per node, `size: 0`, no fetch:

```json
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        {"term": {"node": "<node_id>"}},
        {"term": {"epoch": <epoch>}}
      ]
    }
  },
  "aggs": {
    "cpu_pct": {"percentiles": {"field": "cpu", "percents": [0, 50, 90, 100]}},
    "mem_pct": {"percentiles": {"field": "mem", "percents": [0, 50, 90, 100]}},
    "net_pct": {"percentiles": {"field": "net", "percents": [0, 50, 90, 100]}},
    "cpu_sum": {"sum": {"field": "cpu"}},
    "mem_sum": {"sum": {"field": "mem"}},
    "net_sum": {"sum": {"field": "net"}}
  }
}
```

The `percentiles` aggregation uses **t-digest** internally (ES default, no `tdigest`/`hdr` field is required to enable it). ES has no exact-percentile aggregation; the only alternative is to set `"hdr": {...}` to switch the underlying algorithm to HDR histogram.

### Run

A "30-node coverage" unit = querying all 30 nodes for one epoch. The production code does this serially (30 separate `_search` HTTP calls).

Profiling scripts:
- `scripts/profile_es_query.py` — ingests 5 epochs and runs the production query with ES `profile: true` for fine-grained breakdown.
- `scripts/profile_es_query_decomp.py` — decomposes the query into variants (which aggs are present) and transports (serial vs `_msearch`), without `profile: true`, with cache isolation. Outputs `data/es_profile_decomp.csv`.

---

## 2. Methodology Notes

### Profile API distorts absolute timing

ES's `profile: true` inserts `System.nanoTime()` probes into per-doc inner loops. On this workload (33k docs × 6 aggs per call, very light per-doc work), the probe overhead inflates `took` by ~75–80%:

| measurement | profile=ON | profile=OFF |
|---|---|---|
| D_td took (avg, ms / single call) | 74.8 | 40.8 |

**Profile-ON breakdown is only useful for relative ratios** (which agg costs more than which). Absolute numbers come from profile-OFF runs.

### Cache isolation in the decomposition run

To prevent ES caches from making one variant artificially fast at another's expense:

- Every query sent with `request_cache=false` (URL param for `_search`, header field for `_msearch` sub-searches).
- One full warmup pass before measurement (60 tasks across all variants × transports × epochs).
- 3 measurement passes; tasks shuffled within each pass so variants interleave (any OS page cache state is shared evenly).
- Each `(variant, transport)` cell has 15 samples (3 passes × 5 epochs).
- D_td serial sanity check: this run = 1199 ms / 30-node coverage; previous full-run profile-OFF baseline = 1225 ms. Drift = **2.1%** ✓.

---

## 3. Ingest Results

5 epochs × 1M rows each, 1000-row bulk batches, refresh after each epoch.

### Per-epoch breakdown

| epoch | wall (ms) | took_sum (ms) | index (ms) | refresh (ms) | merge (ms) | flush (ms) | merge MB |
|---|---|---|---|---|---|---|---|
| 1 | 75,651 | 39,803 | 32,622 | 2,892 | 2,575 | 1,626 | 31.6 |
| 2 | 78,071 | 40,425 | 34,701 | 1,347 | 0 | 2,296 | 0 |
| 3 | 80,588 | 41,820 | 35,397 | 3,028 | 0 | 1,307 | 0 |
| 4 | 73,398 | 39,001 | 32,698 | 408 | 0 | 3,318 | 0 |
| 5 | 76,628 | 38,241 | 34,344 | 1,168 | 565 | 2,629 | 16.3 |
| **avg** | **76,867** | **39,858** | **33,952** | — | — | — | — |

- `wall` = client wall-clock for the epoch.
- `took_sum` = sum of `took` across the 1000 `_bulk` responses (server-side handler time).
- `index/refresh/merge/flush` = `/_stats` deltas (ES-internal timing).

### Where the time goes (per 1M-row epoch)

| segment | time (s) | share | what it is |
|---|---|---|---|
| Client + network | ~37 | **47%** | NDJSON encoding (~2M `json.dumps` calls), response parsing (~200 KB per response), serial HTTP |
| ES indexing | ~34 | **42%** | Per-doc work: parse, analysis, postings update, BKD-tree insert, doc values, translog append |
| ES other (fsync, refresh, merge, flush, response build) | ~6 | 11% | Translog `request` durability fsync per bulk dominates this |

### Why ES (76s) is so much slower than the in-memory sketch server (~10s) for the same 1M rows / 1000 batches

Two structural differences in the wire protocol:

1. **NDJSON vs columnar JSON.**
   - Sketch server: 1 columnar JSON object per batch (1 `json.dumps` call per batch × 1000 batches = **1,000** Python-side serializations).
   - ES `_bulk`: NDJSON, action-line + doc-line per row (2 `json.dumps` per row × 1M rows = **2,000,000** Python-side serializations).
   - At ~5–10 µs per `json.dumps`, this alone accounts for ~10–20s of client overhead.

2. **Per-doc response items.**
   - Sketch server response: `{"inserted": 1000}` ≈ 20 bytes.
   - ES `_bulk` response: 1000 entries with `_index`, `_id`, `_version`, `_shards`, `_seq_no`, `_primary_term`, `status` ≈ **200 KB**. Parsing that in Python adds 5–10s across 1000 calls.

The remaining ~15–20s of ES indexing time is genuine per-document work that an in-memory KLL sketch update simply does not do (postings list, BKD tree, doc values, translog fsync).

### Available ingest optimizations (not run here)

| change | expected savings | trade-off |
|---|---|---|
| `requests.Session()` for keep-alive | seconds | none |
| Replace `json` with `orjson` | ~10× faster encode → can save ~10–15s | none |
| Bulk batch 1000 → 10,000 | fewer fsyncs/round-trips, ~5–10s | larger memory per batch |
| Concurrent bulk requests (worker pool) | parallel use of ES indexing threads | more client complexity |
| `index.refresh_interval: -1` during bulk | ~1–5s | data not visible until manual refresh |
| `index.translog.durability: async` | ~5–10s | risk of last-window data loss on crash |

---

## 4. Query Results

### 4.1 Phase decomposition (production query, no profile)

For one `_search` call (33k matched docs), wall = 45.7 ms, server `took` = 40.8 ms.

| phase | est. ms | share | what it is |
|---|---|---|---|
| Network + JSON parse (client) | ~5 | 11% | Wall − took |
| Filter / postings intersection | ~7 | 17% | `term node` + `term epoch` postings → ~33k doc IDs |
| Collection + agg `collect()` — 3× percentiles | ~22 | **54%** | t-digest update per doc, three fields |
| Collection + agg `collect()` — 3× sum | ~6 | 15% | doc-value read + accumulate, three fields |
| Build / reduce / serialize | ~5 | 12% | Finalize sketches → percentiles, JSON response |

The filter + per-agg ratios above were derived by combining (a) the absolute total from profile-OFF, with (b) the relative agg ratios from profile-ON. Section 4.2 below provides cleaner absolute attribution.

### 4.2 Variant decomposition (clean, no profile)

Six query variants run with cache isolation. `took_avg` is server-side time per 30-node coverage (one epoch's worth of work, summed across the 30 serial calls).

| variant | aggs included | took_avg (ms) |
|---|---|---|
| A | none (filter only) | 19 |
| B | 3× sum | 192 |
| C_td | 3× percentiles (t-digest) | 1,070 |
| C_hdr | 3× percentiles (HDR) | 597 |
| **D_td** (= production) | 3× sum + 3× percentiles t-digest | **1,199** |
| D_hdr | 3× sum + 3× percentiles HDR | 712 |

Subtractive cost attribution:

| component | cost per 30-node coverage |
|---|---|
| Filter + framework overhead (A) | 19 ms |
| 3× sum aggs (B − A) | 173 ms (~58 ms each) |
| 3× t-digest percentile aggs (C_td − A) | **1,051 ms** (~350 ms each) |
| 3× HDR percentile aggs (C_hdr − A) | 578 ms (~193 ms each) |

**Percentile aggregations are 88% of the production query's time.** Sum aggs are 14%. Filter + framework < 2%.

### 4.3 Transport effect: serial vs `_msearch`

For each variant, "serial" sends 30 individual `_search` HTTP calls; "msearch" sends 1 `_msearch` HTTP call bundling 30 sub-searches. ES dispatches each sub-search to its own search thread, so the agg pipeline (collect / build / reduce) runs in parallel as well as the filter.

| variant | serial wall (ms) | msearch wall (ms) | savings |
|---|---|---|---|
| A | 135 | 13 | 90.7% |
| B | 310 | 33 | 89.5% |
| C_td | 1,193 | 138 | 88.4% |
| C_hdr | 730 | 86 | 88.2% |
| **D_td** | **1,323** | **150** | **88.7%** |
| D_hdr | 845 | 101 | 88.0% |

The `took_sum` across 30 sub-searches under msearch is roughly equal to (slightly higher than) serial's `took_sum` — server does the same work — but real wall-clock collapses because work runs concurrently.

For D_td: msearch wall = 150 ms vs aggregated took = 1,277 ms → effective parallelism ≈ **8.5×**, bounded by the search thread pool size (default ~13 threads on an 8-core host).

### 4.4 Combined optimization

| configuration | wall per 30-node coverage | speedup vs production |
|---|---|---|
| Production: D_td serial | 1,323 ms | 1.0× (baseline) |
| D_td via `_msearch` | 150 ms | 8.8× |
| D_td via `_msearch` + switch to HDR | 101 ms | **13.1×** |

Both changes are **client-side only** — no ES config change, no mapping change, no re-ingest.

---

## 5. Caveats

- **Single-shard index.** Within a single `_search`, no shard-level parallelism is available; all parallelism here comes from msearch dispatching multiple sub-searches concurrently. Multi-shard indexes would behave differently.
- **Localhost networking.** Real-network deployments will see additional per-call RTT cost on serial transport, making msearch's relative win even larger.
- **HDR accuracy.** HDR histogram is configured with `number_of_significant_value_digits: 3` (default). For cpu/mem/net (continuous floats with bounded ranges), this is more than sufficient for percentiles at p0/p50/p90/p100. If a use case needs higher precision at the tails, t-digest remains preferable.
- **Cold cache not measured.** All measurements are warm; data is in OS page cache after warmup. Cold-start query latency would be higher (first-touch I/O).
- **Variance.** D_td serial p90 is 19% above its average; ES background activity (GC, merges) adds noise. 15-sample averages here are stable but not noise-free.

---

## 6. Generated Artifacts

| path | contents |
|---|---|
| `scripts/profile_es_query.py` | Ingest + profile-API run (initial breakdown) |
| `scripts/profile_es_query_decomp.py` | Variant × transport decomposition |
| `data/es_profile_ingest_batches.csv` | Per-bulk-batch ingest timing |
| `data/es_profile_ingest_epoch.csv` | Per-epoch ingest with `/_stats` deltas |
| `data/es_profile_query_calls.csv` | Per-call query profile (all/single, on/off) |
| `data/es_profile_query_aggs.csv` | Per-call × per-agg profile breakdown |
| `data/es_profile_decomp.csv` | Variant × transport decomposition results |
| `logs/profile_es_decomp_full.log` | Decomposition run log |
