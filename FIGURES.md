# Figure map — which data and which plot, per backend

Every paper figure exists twice: once with the **KLL** quantile backend and once
with **DDSketch (alpha = 1e-3)**. On this branch (`raw_data_plot`) *both*
datasets live in one checkout, split by backend, so a figure can be redrawn or
compared without reaching into another worktree.

```
data/
  kll/                     measured against the KLL server
  dd/                      measured against the DDSketch server
  combined/                cross-backend summaries
  raw_topology/            shared input (topology + workload) -- identical for both
  raw_topology_completion/ shared input, heavier workload
plots/
  kll/  dd/  combined/     same split
```

`data/kll/` and `data/dd/` hold the **same filenames**, so switching backend is
just swapping one path segment. (`data/kll/` additionally has
`raw_data_completion_fig9_seed20260904.csv`, a second-seed repeat that only the
KLL run has.)

## Where the data came from

The experiments still run in the two backend worktrees; this branch only draws
from their output. The only source difference between them is
`single_node_server/network-control-server/src/metrics/store.rs`.

| | checkout | branch | quantile backend |
|---|---|---|---|
| **KLL** | `/users/yuanyc/network-control-demo` | `feat/raw-data-experiments` | `asap_sketchlib::KLL`, default k=200 |
| **DD** | `/users/yuanyc/network-control-demo-dd` | `feat/ddsketch-variant` | `asap_sketchlib::DDSketch`, alpha=1e-3 (`DDSKETCH_ALPHA`) |

All figures were produced with the **same seed and the same ingested rows** in
both trees, so the Elasticsearch arms are near-identical across them — that
agreement is the check that a KLL-vs-DD difference is real.

Note the `run_*` experiment scripts still default to writing `data/<name>.csv`
(no backend segment), because they run on the experiment branches. Re-running an
experiment therefore means copying its output into `data/kll/` or `data/dd/`
here by hand.

---

## The map

| Fig | What it shows | Data (CSV) | Plot (PNG) | Regenerate the plot with |
|---|---|---|---|---|
| **4** | Query latency, sketch vs ES | `data/<b>/raw_data_assignment.csv` | `plots/<b>/raw_data/paper_style_2/fig4_query_latency.png` | `scripts/plot_raw_data_paper_style.py` |
| **5** | CPU + RSS per query and per ingest | `data/<b>/resource_benchmark.csv`, `data/<b>/resource_ingestion.csv`, sidecars in `data/<b>/resource_benchmark_raw/` | `plots/<b>/resource/*.png` (headline: `query_cpu_headline.png`, `sketch_resource.png`) | `scripts/plot_resource_benchmark.py` |
| **6** | Quantile error vs ground truth | `data/kll/raw_data_accuracy.csv`, `data/dd/raw_data_accuracy.csv` | combined (4 series): `plots/combined/fig6_accuracy_kll_vs_dd{,_log}.png`<br>DD only: `plots/dd/raw_data/fig6_accuracy{,_log}.png` | `scripts/plot_raw_data_accuracy.py` |
| **7** | Solver runtime, sketch- vs ES-fed | `data/<b>/raw_data_assignment.csv` | `plots/<b>/raw_data/paper_style_2/fig7_solver_runtime.png` | `scripts/plot_raw_data_paper_style.py` |
| **8** | Completions: static / reassign / dynamic | `data/<b>/raw_data_completion_fig810.csv` | `plots/<b>/raw_data/paper_style_2/fig8_completion.png`<br>alt: `plots/<b>/raw_data/completion_fig8.png` | `scripts/plot_raw_data_paper_style.py`<br>alt: `scripts/plot_raw_data_completion.py` |
| **9** | Sketch vs ES vs static, 10 runs | `data/<b>/raw_data_completion_fig9.csv` | `plots/<b>/raw_data/paper_style_2/fig9_sketch_vs_es.png`<br>alt: `plots/<b>/raw_data/completion_fig9.png` | same as Fig 8 |
| **10** | Telemetry update rules | `data/<b>/raw_data_completion_fig810.csv` (same CSV as Fig 8) | `plots/<b>/raw_data/paper_style_2/fig10_update_rules.png`<br>alt: `plots/<b>/raw_data/completion_fig10.png` | same as Fig 8 |

`<b>` is `kll` or `dd`. **Every plot script defaults to `kll`**; to draw the DD
version, pass the same flags with `kll` swapped for `dd`.

Two extra plots that are not paper figures but come from the same runs:
`plots/<b>/raw_data/assignment.png` and `plots/<b>/raw_data/query_solver.png`
(`scripts/plot_raw_data_assignment.py`, fed by `data/<b>/raw_data_assignment.csv`).

### Fig 6, the combined KLL-vs-DD figure

Both CSVs are now in this checkout, so it is one command with ordinary relative
paths:

```bash
cd solver_experimental
uv run python ../scripts/plot_raw_data_accuracy.py \
    --csv data/kll/raw_data_accuracy.csv \
    --sketch-label "Approximate (KLL, k=200)" \
    --extra-sketch-csv data/dd/raw_data_accuracy.csv \
    --extra-sketch-label "Approximate (DDSketch, alpha=1e-3)" \
    --out plots/combined/fig6_accuracy_kll_vs_dd.png \
    --summary-csv data/combined/raw_data_accuracy_kll_vs_dd_summary.csv --log-y
```

The script warns if the two CSVs' Elasticsearch arms disagree — that warning
means the two runs did not see the same values and the figure is not valid.

---

## Things that will bite you

- **`--log-y` exists for a reason.** Fig 6's arms span five orders of magnitude
  (0.0001% to 8.5%); on a linear axis DDSketch's bar is invisible in the CPU
  panel. Both versions are on disk; the `_log` one is the readable one.

- **Relative `--csv` paths resolve against the repo root, not the CWD.** Every
  plot script does this, so it behaves the same from the repo root and from
  `solver_experimental/` (where the uv env lives). A path like
  `../data/kll/x.csv` typed from `solver_experimental/` is therefore *wrong* —
  write `data/kll/x.csv`.

- **A figure is only drawn when every series it names is present.** Feeding the
  Fig 9 CSV to `plot_raw_data_completion.py` used to silently overwrite Fig 10
  with a two-series subset.

- **`plot_resource_benchmark.py` prunes.** It deletes plots it no longer
  produces unless you pass `--no-prune`.

- **ES RSS is not a memory result.** It is the fixed pre-allocated JVM heap
  (~5.4 GB here, `-Xms=-Xmx`), not per-query usage. The resource plots
  deliberately do not present it as a baseline.

---

## Numbers as they stand (so a redraw can be sanity-checked)

Fig 6, mean relative error, 10 runs x 10 epochs x 30 keys x 900k rows/epoch:

| | KLL k=200 | DD alpha=1e-3 | ES default | ES compression 1000 |
|---|---|---|---|---|
| CPU p50 | 0.621% | 0.050% | 5.753% | 0.115% |
| CPU p90 | 4.938% (sd 3.01) | 0.053% (sd 0.013) | 8.527% | 1.032% |
| Network p50 | 0.477% | 0.051% | 0.275% | 0.021% |
| Network p90 | 1.257% | 0.049% | 0.327% | 0.028% |
| Memory p50 | 0.0018% | 0.0458% | 0.0007% | 0.0001% |
| Memory p90 | 0.0043% | 0.0500% | 0.0007% | 0.0002% |

DDSketch's error is flat at alpha/2 regardless of metric or quantile; KLL's is
rank-based, so it degrades — and becomes erratic — in the long tail, and is
near-exact on the nearly-constant memory metric.

Fig 4 / Fig 5, sketch server only:

| | KLL | DD |
|---|---|---|
| Query latency (assignment run, 43 epochs) | 5.46 ms | 5.38 ms |
| Query latency (resource run) | 4.09 ms | 3.49 ms |
| CPU per query | 8.80 ms | 5.23 ms |
| RSS mean / VmHWM | 12.2 / 12.4 MB | 14.9 / 15.1 MB |
| Ingest CPU per 1M rows | 3840 ms | 3840 ms |

Fig 8 / 9 / 10 are indistinguishable between backends (Fig 8 final completions
3915 KLL vs 3901 DD; Fig 9 506.2 DD vs 505.0 KLL, sd ~2) — both estimators are
far more accurate than the margin that would flip a placement.

The Fig 9 seed check: the first seed's 1.6-task Elasticsearch lead reverses to a
1.0-task sketch lead under the second seed (|t| < 2 both times), so that gap is
noise. The +11.7% / +11.9% gain of dynamic telemetry over static reproduces.

---

## Archived earlier state

`results/seed20260903/` holds the KLL first seed's completion CSVs, the plots drawn
from them, and the run logs, from before the second-seed re-run.
