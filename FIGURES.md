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
  kll/  dd/                exactly the seven paper figures, per backend
```

`plots/` holds **only** the paper figures -- `fig4` through `fig10`, one flat
file each, in `plots/kll/` and `plots/dd/`, as **vector PDF** for `\includegraphics`.
Fonts are embedded as TrueType (`pdf.fonttype = 42`), not Type 3, which IEEE and
ACM PDF checkers reject. `--format png` (or a `.png` `--out` for Fig 6) still works. Everything else the plot scripts can
emit (per-run diagnostics, the resource script's 13-file family, the alternate
completion renderings) is regenerable and is not kept.

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
| **2** | ES query share of the control loop (motivation) | `data/es_ingest_query_sweep_summary.csv` (no backend split -- Elasticsearch only) | `plots/dd/fig2_query_ratio.pdf` | `scripts/plot_es_sweep_query_ratio.py` |
| **4** | Query latency, sketch vs ES | `data/<b>/raw_data_assignment.csv` | `plots/<b>/fig4_query_latency.pdf` | `scripts/plot_raw_data_paper_style.py` |
| **5** | CPU + RSS per query and per ingest | `data/<b>/resource_benchmark.csv`, `data/<b>/resource_ingestion.csv`, sidecars in `data/<b>/resource_benchmark_raw/` | `plots/<b>/fig5_resource_usage.pdf` | `scripts/plot_resource_benchmark.py` (see caveat below) |
| **6** | Quantile error vs ground truth | `data/<b>/raw_data_accuracy.csv` | `plots/<b>/fig6_accuracy.pdf` | `scripts/plot_raw_data_accuracy.py` |
| **7** | Solver runtime, sketch- vs ES-fed | `data/<b>/raw_data_assignment.csv` | `plots/<b>/fig7_solver_runtime.pdf` | `scripts/plot_raw_data_paper_style.py` |
| **8** | Completions: static / reassign / dynamic | `data/<b>/raw_data_completion_fig810.csv` (`--completion-fig8-csv`) | `plots/<b>/fig8_completion.pdf` | `scripts/plot_raw_data_paper_style.py` |
| **9** | Sketch vs ES vs static, 10 runs | `data/<b>/raw_data_completion_fig9.csv` | `plots/<b>/fig9_sketch_vs_es.pdf` | same as Fig 8 |
| **10** | Telemetry update rules | `data/dd/raw_data_completion_fig10.csv` (`--completion-fig10-csv`) | `plots/<b>/fig10_update_rules.pdf` | same as Fig 8 |

**`data/dd/raw_data_assignment.csv` is the current Fig 4 / Fig 7 data**
(regenerated 2026-09-07): 10 independent runs x 10 epochs, **150 s epochs**,
**996,800 telemetry rows per epoch**, and a timed query matching the paper's
Fig 4 caption -- p50/p90/p100 percentiles **plus a cumulative sum** over CPU and
memory, one batched request to the sketch layer and one request per node to
Elasticsearch. Each run re-ingests with a fresh jitter draw and a restarted
server, so the error bars are a real run-to-run spread. Reproduce with the 150 s
workload in `data/raw_topology_150s/` (~1h40m):

```bash
uv run python ../scripts/raw_data_prep.py --epoch-length-s 150 \
    --out-dir data/raw_topology_150s
uv run python ../scripts/run_raw_data_assignment.py --runs 10 --epochs 10 \
    --epoch-length-s 150 --topology-dir data/raw_topology_150s
```

**`data/kll/raw_data_assignment.csv` is superseded**: 300 s epochs and a single
p50 query. The paper uses the DD figures, so KLL was deliberately not re-run --
do not compare the two backends on Fig 4 or Fig 7.

`<b>` is `kll` or `dd`. **Every plot script defaults to `kll`**; to draw the DD
version, pass the same flags with `kll` swapped for `dd`.

Fig 4/7/8/9/10 come out of one `plot_raw_data_paper_style.py` run, already named
correctly, so `--out-dir plots/<b>` is all it needs. Fig 6 is one
`plot_raw_data_accuracy.py` run per backend, with `--log-y`:

```bash
cd solver_experimental
uv run python ../scripts/plot_raw_data_accuracy.py \
    --csv data/dd/raw_data_accuracy.csv \
    --sketch-label "Approximate (DDSketch, alpha=1e-3)" \
    --out plots/dd/fig6_accuracy.pdf --log-y
```

**Three scripts still write outside `plots/<b>/` and will re-create pruned
directories if run as-is:**

- `plot_resource_benchmark.py` writes a 13-file family into
  `plots/<b>/resource/`; Fig 5 is its `sketch_resource.pdf`. It also **prunes**.
  Send it to a scratch dir and copy `sketch_resource.pdf` to
  `plots/<b>/fig5_resource_usage.pdf`, or trim the script when Fig 5 is reworked.
- `plot_raw_data_assignment.py` and `plot_raw_data_completion.py` default to
  `plots/<b>/raw_data/` and produce diagnostics and alternate renderings of Fig
  8/9/10 that are deliberately not kept.

**Fig 2 has no backend variant.** It measures Elasticsearch's query time as a
share of query + solver across ingest row counts, so there is nothing for a
sketch backend to change; it lives in `plots/dd/` only because that is where the
paper's figure set is assembled. Its data was recovered from the
`archive/data-plots` branch. Note its caption's "approaches 40% even for just a
few thousand data points" overstates the data: the ratio is 23-24% at 2k-8k rows
and reaches 40% only past 64k, topping out at 53.9% at 1.02M rows.

**Fig 8 and Fig 10 take separate CSVs on purpose.** Fig 8 asks whether
refreshing the estimates helps at all, so it runs on the faithful workload and
should not move. Fig 10 is a sensitivity study over update rules, so it may run
under different assumptions -- notably bursty task usage, which is the regime
the paper's own quantile argument (Sec. II-B, "CPU utilization is often bursty
and highly volatile") targets, and which `--burst-prob` disables by default
because `raw_data/README.md` says task CPU does not spike in that testbed.
Passing no `--completion-fig10-csv` makes Fig 10 reuse Fig 8's file and prints a
warning; that is only correct while the two genuinely share a workload.

### The combined KLL-vs-DD Fig 6

A single Fig 6 carrying all four arms (KLL, DDSketch, ES default, ES 1000) is not
one of the kept figures, but both accuracy CSVs are in this checkout, so it is one
command with ordinary relative paths:

```bash
cd solver_experimental
uv run python ../scripts/plot_raw_data_accuracy.py \
    --csv data/kll/raw_data_accuracy.csv \
    --sketch-label "Approximate (KLL, k=200)" \
    --extra-sketch-csv data/dd/raw_data_accuracy.csv \
    --extra-sketch-label "Approximate (DDSketch, alpha=1e-3)" \
    --out <somewhere outside plots/> --log-y
```

The script warns if the two CSVs' Elasticsearch arms disagree -- that warning
means the two runs did not see the same values and the figure is not valid.

---

## Open items

- **Fig 7 splits into two regimes, and the paper's "rarely exceeds 1 s" only
  covers one.** In the **7 of 10** epochs where the pending batch fits the
  cluster, solver time is **98-296 ms** (sketch-fed) and every solve is proven
  optimal -- well under the 810 ms an Elasticsearch telemetry query costs. In
  epochs 5, 6 and 7 the batch does **not** fit (`assigned < pending_before`), so
  the solver must also prove which subset to admit: 19-42 s, and 15 of those 30
  solves reach the 60 s deadline. The split tracks whether the batch fits, not
  which backend supplied the telemetry (mean 10.9 s sketch-fed vs 9.5 s ES-fed
  overall). Check the `assigned` and `pending_before` columns before reading
  anything else into a slow epoch.

- **Fig 6's network arm is synthetic.** raw_data has no per-node network metric
  (`bw.csv` is per-edge), so `run_raw_data_accuracy.py` generates one. It is
  measured for accuracy only -- `network_mbps` is dropped in both
  `raw-data-config.yaml` and `raw-data-full-config.yaml`, so no assignment or
  completion result depends on it. Decide whether that arm belongs in the paper.

---

**Fig 10 now has its own run, under within-epoch spike bursts.** With the
original `uniform` burst mode a burst scaled a whole epoch, so p50, p90 and the
mean all moved together and no burst setting changed the ranking of the update
rules -- window averaging tied p50 exactly (476 vs 476 on a 20-epoch probe).
`--burst-mode spike` makes `--burst-prob` the share of an epoch's samples that
spike, which is the "outlier distortion" of Sec. II-B and the only regime in
which a quantile can beat an average.

The committed run: DDSketch alpha=0.001, `--burst-mode spike --burst-prob 0.20
--burst-factor 1.6`, `--usage-base-lo 0.4464 --usage-base-hi 0.8482` (chosen so
the mean load stays at 90%, matching Fig 8, so the only difference is the
*shape* of task usage), 150 epochs, 1 run, the paper's five series (~31 min):

| series | completed | vs static | est. CPU error |
|---|---|---|---|
| p50 | 4061 | **+25.46%** | 16.23% |
| p50 + 1.2x alloc | 3984 | +23.08% | 8.60% |
| avg (window averaging) | 3930 | +21.41% | 8.72% |
| avg(p50, p75) + 1.2x alloc | 3794 | +17.21% | 3.49% |
| no rule | 3237 | -- | 45.12% |

**What this does and does not support.** The two plain quantile rules now beat
recent window averaging, by 4.1 and 1.7 points -- so "the best quantile rule
outperforms window averaging" holds. The paper's stronger claim, quantile
statistics *as a class*, does not: `avg(p50, p75) + 1.2x alloc` loses to window
averaging by 4.2 points, and window averaging lands third of five, not last.
Window averaging's own number never moved (+21.4% with and without spikes);
spikes changed the p50 end.

Note also that estimate accuracy does not order the completions: p50 has the
worst error of any dynamic rule (16.23%) and the most completions, while
`avg(p50, p75) + 1.2x alloc` has the best error (3.49%) and the fewest.

`--burst-prob 0.20`, `--burst-factor 1.6` and the spike mode itself are **our
choices** -- the paper gives no burst numbers, only the qualitative "often
bursty and highly volatile". They belong in the paper's text.

Fig 8 is untouched by all of this: it keeps `raw_data_completion_fig810.csv`,
and `--burst-mode` defaults to `uniform` with an identical RNG stream.

## Things that will bite you

- **Fig 4 and Fig 7 are truncated to 10 epochs** (`--latency-epochs`,
  default 10; `0` plots all 43). The assignment run is much longer than
  either figure needs, and drawing all of it just shrinks the bars.

- **`--log-y` exists for a reason.** Fig 6's arms span five orders of magnitude
  (0.0001% to 8.5%); on a linear axis DDSketch's bar is invisible in the CPU
  panel. Both versions are on disk; the `_log` one is the readable one.

- **Relative `--csv` paths resolve against the repo root, not the CWD.** Every
  plot script does this, so it behaves the same from the repo root and from
  `solver_experimental/` (where the uv env lives). A path like
  `../data/kll/x.csv` typed from `solver_experimental/` is therefore *wrong* —
  write `data/kll/x.csv`.

- **A figure is only drawn when every series it names is present, silently.**
  Fig 10's `p50` series was mapped to the scenario name `dynamic+reassign`,
  which is what the *shared* CSV called it after dedup; the standalone Fig 10 run
  names it `p50`, so the figure was skipped with no error until the mapping was
  fixed. Check the `[plot] wrote` lines actually list every figure you expect. Feeding the
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
| Query latency (Fig 4) | *superseded, 300 s + p50 only* | **5.61 +- 0.70 ms** |
| Elasticsearch, same queries | | **810.1 +- 240.3 ms** |
| Latency reduction | | **99.31%** (144x) |
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
