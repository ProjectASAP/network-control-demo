//! Compare four approaches for storing timestamped metric values that arrive
//! out of order, and answering time-bounded queries (earliest/latest, "last N
//! minutes" percentile):
//!
//!   1. `RawVec`     — append-only Vec; queries scan everything.
//!   2. `SortedBTree`— BTreeMap keyed by timestamp; queries use range().
//!   3. `KllOnly`    — KLL on timestamps + KLL on values; cheap but cannot
//!                     answer per-time-range percentiles (returns global p).
//!   4. `BucketedKll`— window split into fixed-width time buckets, one value
//!                     KLL per bucket. On query, KLL.merge() the relevant
//!                     buckets and read the percentile. This is the form
//!                     where KLL is actually competitive on the same query
//!                     RawVec/SortedBTree answer exactly.
//!
//! Comparison axes: memory footprint, insert throughput, query latency.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::time::Instant;

use asap_sketchlib::KLL;
use rand::Rng;

#[derive(Debug, Clone, Copy)]
struct TimestampedValue {
    timestamp_ms: u64,
    value: f64,
}

// ---------- Approach 1: RawVec (append-only, scan on query) ----------

struct RawVec {
    entries: Vec<TimestampedValue>,
}

impl RawVec {
    fn new() -> Self {
        Self { entries: Vec::new() }
    }

    fn insert(&mut self, ts: u64, value: f64) {
        self.entries.push(TimestampedValue { timestamp_ms: ts, value });
    }

    fn earliest(&self) -> Option<u64> {
        self.entries.iter().map(|e| e.timestamp_ms).min()
    }

    fn latest(&self) -> Option<u64> {
        self.entries.iter().map(|e| e.timestamp_ms).max()
    }

    /// Exact percentile of values whose timestamp >= cutoff_ms.
    fn percentile_since(&self, cutoff_ms: u64, p: f64) -> Option<f64> {
        let mut vals: Vec<f64> = self
            .entries
            .iter()
            .filter(|e| e.timestamp_ms >= cutoff_ms)
            .map(|e| e.value)
            .collect();
        if vals.is_empty() {
            return None;
        }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p * (vals.len() - 1) as f64).round() as usize).min(vals.len() - 1);
        Some(vals[idx])
    }

    fn approx_bytes(&self) -> usize {
        self.entries.capacity() * size_of::<TimestampedValue>()
    }
}

// ---------- Approach 2: SortedBTree (BTreeMap on timestamp) ----------
//
// Multiple values may share a timestamp, so we use Vec<f64> as the value.
// This keeps timestamps sorted at insertion time and gives O(log n + k)
// range queries plus O(log n) min/max via first_key_value/last_key_value.

struct SortedBTree {
    map: BTreeMap<u64, Vec<f64>>,
    count: usize,
}

impl SortedBTree {
    fn new() -> Self {
        Self { map: BTreeMap::new(), count: 0 }
    }

    fn insert(&mut self, ts: u64, value: f64) {
        self.map.entry(ts).or_default().push(value);
        self.count += 1;
    }

    fn earliest(&self) -> Option<u64> {
        self.map.keys().next().copied()
    }

    fn latest(&self) -> Option<u64> {
        self.map.keys().next_back().copied()
    }

    fn percentile_since(&self, cutoff_ms: u64, p: f64) -> Option<f64> {
        let mut vals: Vec<f64> = Vec::new();
        for (_, v) in self.map.range(cutoff_ms..) {
            vals.extend_from_slice(v);
        }
        if vals.is_empty() {
            return None;
        }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p * (vals.len() - 1) as f64).round() as usize).min(vals.len() - 1);
        Some(vals[idx])
    }

    /// Rough estimate: each entry incurs a BTreeMap node overhead plus the
    /// f64 payload. BTreeMap nodes hold up to B=6 key/value pairs, so
    /// per-key overhead is ~ size_of::<(u64, Vec<f64>)>() / B + pointer
    /// overhead. We use a conservative estimate.
    fn approx_bytes(&self) -> usize {
        // Per-key: key (u64) + Vec header (24B) + node pointers (~32B amortised).
        let per_key = size_of::<u64>() + 24 + 32;
        // Per-value: f64.
        per_key * self.map.len() + size_of::<f64>() * self.count
    }
}

// ---------- Approach 3: KllOnly (timestamp KLL + value KLL) ----------

struct KllOnly {
    ts_sketch: KLL<u64>,
    val_sketch: KLL,
    count: usize,
}

impl KllOnly {
    fn new() -> Self {
        Self {
            ts_sketch: KLL::<u64>::init_kll(200),
            val_sketch: KLL::init_kll(200),
            count: 0,
        }
    }

    fn insert(&mut self, ts: u64, value: f64) {
        self.ts_sketch.update(&ts);
        self.val_sketch.update(&value);
        self.count += 1;
    }

    /// Approximate earliest timestamp (KLL min).
    fn earliest(&self) -> f64 {
        self.ts_sketch.cdf().query(0.0)
    }

    /// Approximate latest timestamp (KLL max).
    fn latest(&self) -> f64 {
        self.ts_sketch.cdf().query(1.0)
    }

    /// Approximate "fraction of points since cutoff" combined with the
    /// global value-sketch percentile. This is the cheap, approximate
    /// answer KLL can offer — it cannot precisely filter values by time.
    fn percentile_since(&self, cutoff_ms: u64, p: f64) -> (f64, f64) {
        let ts_cdf = self.ts_sketch.cdf();
        let cutoff_rank = ts_cdf.quantile(cutoff_ms as f64);
        let fraction_in_range = (1.0 - cutoff_rank).clamp(0.0, 1.0);
        let val_cdf = self.val_sketch.cdf();
        let value = val_cdf.query(p);
        (value, fraction_in_range)
    }

    /// KLL sketches with k=200 are roughly bounded; we approximate as
    /// O(k * log(n/k)) entries. For this benchmark we report the rough
    /// upper bound 200 * log2(n/200) * size_of::<T>() per sketch.
    fn approx_bytes(&self) -> usize {
        let k = 200usize;
        let log_levels = ((self.count.max(k) as f64 / k as f64).log2().ceil() as usize).max(1);
        let ts_bytes = k * log_levels * size_of::<u64>();
        let val_bytes = k * log_levels * size_of::<f64>();
        ts_bytes + val_bytes
    }
}

// ---------- Approach 4: BucketedKll (one value KLL per fixed time bucket) ----------
//
// The window is split into `n_buckets` equal-width buckets. Each ingested
// point goes to the bucket containing its timestamp. On query, we identify
// which buckets fall (entirely or partially) in the requested time range,
// merge their KLLs into a fresh sketch, and read the percentile.
//
// Trade-off: the boundary buckets are queried whole (we cannot sub-filter
// inside a bucket), so the time resolution of queries is the bucket width.
// Memory grows with `n_buckets * k` rather than with the data size.

struct BucketedKll {
    buckets: Vec<KLL>,
    /// Tracks earliest/latest timestamps cheaply (without scanning data).
    min_ts: Option<u64>,
    max_ts: Option<u64>,
    window_start_ms: u64,
    bucket_width_ms: u64,
    count: usize,
}

impl BucketedKll {
    fn new(window_start_ms: u64, window_size_ms: u64, n_buckets: usize) -> Self {
        let bucket_width_ms = window_size_ms.div_ceil(n_buckets as u64);
        let _ = window_size_ms;
        let buckets = (0..n_buckets).map(|_| KLL::init_kll(200)).collect();
        Self {
            buckets,
            min_ts: None,
            max_ts: None,
            window_start_ms,
            bucket_width_ms,
            count: 0,
        }
    }

    fn bucket_idx(&self, ts: u64) -> usize {
        let offset = ts.saturating_sub(self.window_start_ms);
        let idx = (offset / self.bucket_width_ms) as usize;
        idx.min(self.buckets.len() - 1)
    }

    fn insert(&mut self, ts: u64, value: f64) {
        let idx = self.bucket_idx(ts);
        self.buckets[idx].update(&value);
        self.min_ts = Some(self.min_ts.map_or(ts, |m| m.min(ts)));
        self.max_ts = Some(self.max_ts.map_or(ts, |m| m.max(ts)));
        self.count += 1;
    }

    fn earliest(&self) -> Option<u64> {
        self.min_ts
    }
    fn latest(&self) -> Option<u64> {
        self.max_ts
    }

    /// Approximate percentile of values whose bucket overlaps [cutoff_ms, ∞).
    /// Buckets are inclusive at boundary: any bucket whose end > cutoff_ms is
    /// merged in whole. Returns None if no bucket qualifies.
    fn percentile_since(&self, cutoff_ms: u64, p: f64) -> Option<f64> {
        let mut start_idx = 0;
        if cutoff_ms > self.window_start_ms {
            start_idx = ((cutoff_ms - self.window_start_ms) / self.bucket_width_ms) as usize;
        }
        if start_idx >= self.buckets.len() {
            return None;
        }
        let mut merged = KLL::init_kll(200);
        let mut any = false;
        for b in &self.buckets[start_idx..] {
            if b.count() > 0 {
                merged.merge(b);
                any = true;
            }
        }
        if !any {
            return None;
        }
        Some(merged.cdf().query(p))
    }

    /// Rough memory estimate: per-bucket KLL ~ k * log2(n_per_bucket / k) * 8B.
    fn approx_bytes(&self) -> usize {
        let k = 200usize;
        let per_bucket = (self.count / self.buckets.len().max(1)).max(k);
        let log_levels = ((per_bucket as f64 / k as f64).log2().ceil() as usize).max(1);
        let per_kll = k * log_levels * size_of::<f64>();
        per_kll * self.buckets.len()
    }
}

// ---------- Driver ----------

fn humanize_bytes(b: usize) -> String {
    if b >= 1 << 20 {
        format!("{:.2} MiB", b as f64 / (1 << 20) as f64)
    } else if b >= 1 << 10 {
        format!("{:.2} KiB", b as f64 / (1 << 10) as f64)
    } else {
        format!("{} B", b)
    }
}

fn main() {
    println!("=== Raw storage vs KLL time management ===\n");

    let window_size_ms: u64 = 100 * 60 * 1000; // 100 minutes
    let window_start_ms: u64 = 0;
    let n_points: usize = 1_000_000;
    let n_queries: usize = 1_000;

    // Pre-generate the input set so all three structures see the same data
    // and we don't pay RNG cost inside the timed inserts.
    println!("Generating {n_points} out-of-order timestamped values...");
    let mut rng = rand::rng();
    let mut data: Vec<(u64, f64)> = Vec::with_capacity(n_points);
    for _ in 0..n_points {
        let ts = rng.random_range(window_start_ms..window_start_ms + window_size_ms);
        let base = (ts as f64 / window_size_ms as f64) * 80.0;
        let noise: f64 = rng.random_range(-10.0..10.0);
        let value = (base + noise).clamp(0.0, 100.0);
        data.push((ts, value));
    }

    // Pre-generate query parameters.
    let query_durations_ms: Vec<u64> = (0..n_queries)
        .map(|_| rng.random_range(1..=100) as u64 * 60 * 1000)
        .collect();
    let percentiles: Vec<f64> = (0..n_queries)
        .map(|_| {
            let pct = [0.50, 0.90, 0.95, 0.99];
            pct[rng.random_range(0..pct.len())]
        })
        .collect();
    let now_ms = window_start_ms + window_size_ms;

    // ---- Insert benchmarks ----
    println!("\n--- Insert throughput (n = {n_points}) ---");

    let mut raw = RawVec::new();
    let t0 = Instant::now();
    for &(ts, v) in &data {
        raw.insert(ts, v);
    }
    let raw_insert = t0.elapsed();

    let mut btree = SortedBTree::new();
    let t0 = Instant::now();
    for &(ts, v) in &data {
        btree.insert(ts, v);
    }
    let btree_insert = t0.elapsed();

    let mut kll = KllOnly::new();
    let t0 = Instant::now();
    for &(ts, v) in &data {
        kll.insert(ts, v);
    }
    let kll_insert = t0.elapsed();

    // 100 buckets => 1-minute resolution over a 100-minute window.
    let n_buckets = 100;
    let mut bucketed = BucketedKll::new(window_start_ms, window_size_ms, n_buckets);
    let t0 = Instant::now();
    for &(ts, v) in &data {
        bucketed.insert(ts, v);
    }
    let bucketed_insert = t0.elapsed();

    let report_insert = |name: &str, dur: std::time::Duration| {
        let secs = dur.as_secs_f64();
        let throughput = n_points as f64 / secs;
        println!(
            "  {:<13} total={:>9.3} ms   per-insert={:>7.0} ns   {:>10.0} ops/s",
            name,
            secs * 1000.0,
            dur.as_nanos() as f64 / n_points as f64,
            throughput,
        );
    };
    report_insert("RawVec", raw_insert);
    report_insert("SortedBTree", btree_insert);
    report_insert("KllOnly", kll_insert);
    report_insert("BucketedKll", bucketed_insert);

    // ---- Memory ----
    println!("\n--- Approximate memory footprint ---");
    println!("  RawVec       {}", humanize_bytes(raw.approx_bytes()));
    println!("  SortedBTree  {}", humanize_bytes(btree.approx_bytes()));
    println!("  KllOnly      {}", humanize_bytes(kll.approx_bytes()));
    println!(
        "  BucketedKll  {}  ({} buckets x {}min)",
        humanize_bytes(bucketed.approx_bytes()),
        n_buckets,
        bucketed.bucket_width_ms / 60_000
    );

    // ---- Min/max query latency ----
    println!("\n--- earliest()/latest() latency (avg over {n_queries} calls) ---");

    let t0 = Instant::now();
    let mut sink: u64 = 0;
    for _ in 0..n_queries {
        sink ^= raw.earliest().unwrap();
        sink ^= raw.latest().unwrap();
    }
    let raw_minmax = t0.elapsed();

    let t0 = Instant::now();
    for _ in 0..n_queries {
        sink ^= btree.earliest().unwrap();
        sink ^= btree.latest().unwrap();
    }
    let btree_minmax = t0.elapsed();

    let t0 = Instant::now();
    let mut fsink: f64 = 0.0;
    for _ in 0..n_queries {
        fsink += kll.earliest();
        fsink += kll.latest();
    }
    let kll_minmax = t0.elapsed();

    let t0 = Instant::now();
    for _ in 0..n_queries {
        sink ^= bucketed.earliest().unwrap();
        sink ^= bucketed.latest().unwrap();
    }
    let bucketed_minmax = t0.elapsed();

    let avg = |d: std::time::Duration| d.as_nanos() as f64 / n_queries as f64;
    println!("  RawVec        avg = {:>10.0} ns/query", avg(raw_minmax));
    println!("  SortedBTree   avg = {:>10.0} ns/query", avg(btree_minmax));
    println!("  KllOnly       avg = {:>10.0} ns/query", avg(kll_minmax));
    println!("  BucketedKll   avg = {:>10.0} ns/query", avg(bucketed_minmax));

    // ---- "Last N minutes" percentile query latency ----
    println!("\n--- last-N-minutes percentile query latency (avg over {n_queries}) ---");

    let t0 = Instant::now();
    for i in 0..n_queries {
        let cutoff = now_ms.saturating_sub(query_durations_ms[i]);
        if let Some(v) = raw.percentile_since(cutoff, percentiles[i]) {
            fsink += v;
        }
    }
    let raw_q = t0.elapsed();

    let t0 = Instant::now();
    for i in 0..n_queries {
        let cutoff = now_ms.saturating_sub(query_durations_ms[i]);
        if let Some(v) = btree.percentile_since(cutoff, percentiles[i]) {
            fsink += v;
        }
    }
    let btree_q = t0.elapsed();

    let t0 = Instant::now();
    for i in 0..n_queries {
        let cutoff = now_ms.saturating_sub(query_durations_ms[i]);
        let (v, frac) = kll.percentile_since(cutoff, percentiles[i]);
        fsink += v + frac;
    }
    let kll_q = t0.elapsed();

    let t0 = Instant::now();
    for i in 0..n_queries {
        let cutoff = now_ms.saturating_sub(query_durations_ms[i]);
        if let Some(v) = bucketed.percentile_since(cutoff, percentiles[i]) {
            fsink += v;
        }
    }
    let bucketed_q = t0.elapsed();

    println!("  RawVec        avg = {:>10.0} ns/query", avg(raw_q));
    println!("  SortedBTree   avg = {:>10.0} ns/query", avg(btree_q));
    println!("  KllOnly       avg = {:>10.0} ns/query", avg(kll_q));
    println!("  BucketedKll   avg = {:>10.0} ns/query", avg(bucketed_q));

    // Side-by-side spot check on a single query.
    let dur_ms = 5 * 60 * 1000;
    let cutoff = now_ms.saturating_sub(dur_ms);
    let p = 0.95;
    println!("\n--- Spot check: last 5 min, p95 ---");
    if let Some(v) = raw.percentile_since(cutoff, p) {
        println!("  RawVec       value = {:.3}", v);
    }
    if let Some(v) = btree.percentile_since(cutoff, p) {
        println!("  SortedBTree  value = {:.3}", v);
    }
    let (v, frac) = kll.percentile_since(cutoff, p);
    println!(
        "  KllOnly      value = {:.3} (global p95)   fraction-in-range = {:.4}",
        v, frac
    );
    println!(
        "  KllOnly      earliest~={:.0}  latest~={:.0}",
        kll.earliest(),
        kll.latest()
    );
    if let Some(v) = bucketed.percentile_since(cutoff, p) {
        println!("  BucketedKll  value = {:.3} (approx, bucket-resolution)", v);
    }

    // Prevent the optimizer from removing all the work.
    std::hint::black_box(sink);
    std::hint::black_box(fsink);
}
