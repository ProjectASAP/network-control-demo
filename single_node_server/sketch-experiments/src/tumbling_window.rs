//! Tumbling window with KLL-based time filtering.
//!
//! Idea: Instead of maintaining precise per-timestamp buckets, use a single
//! large tumbling window (e.g. 100 minutes) and a KLL sketch over timestamps.
//! When a query asks for "last 5 minutes", use the KLL to find the quantile
//! cutoff for that time range, then only consider values whose timestamps
//! fall within that range.
//!
//! Key property: timestamps do NOT need to arrive in order — the KLL sketch
//! handles out-of-order insertion naturally.

use rand::Rng;
use asap_sketchlib::{KLL, SketchInput};

/// A metric value paired with its timestamp.
#[derive(Debug, Clone)]
struct TimestampedValue {
    timestamp_ms: u64,
    value: f64,
}

/// A large tumbling window that uses a KLL sketch on timestamps to support
/// time-range queries without requiring ordered insertion.
struct KllTumblingWindow {
    /// KLL sketch tracking the distribution of timestamps in this window.
    timestamp_sketch: KLL,
    /// KLL sketch tracking the distribution of metric values in this window.
    value_sketch: KLL,
    /// All ingested (timestamp, value) pairs in the current window.
    /// In a production system you'd want a more compact representation,
    /// but for this experiment we keep them to support re-filtering.
    entries: Vec<TimestampedValue>,
    /// Window parameters.
    window_size_ms: u64,
    window_start_ms: u64,
    /// Total number of data points ingested.
    count: usize,
}

impl KllTumblingWindow {
    fn new(window_size_ms: u64, window_start_ms: u64) -> Self {
        Self {
            timestamp_sketch: KLL::init_kll(200),
            value_sketch: KLL::init_kll(200),
            entries: Vec::new(),
            window_size_ms,
            window_start_ms,
            count: 0,
        }
    }

    /// Ingest a data point. Timestamps can arrive out of order.
    fn insert(&mut self, timestamp_ms: u64, value: f64) {
        self.timestamp_sketch
            .update(&SketchInput::U64(timestamp_ms))
            .unwrap();
        self.value_sketch
            .update(&SketchInput::F64(value))
            .unwrap();
        self.entries.push(TimestampedValue {
            timestamp_ms,
            value,
        });
        self.count += 1;
    }

    /// Query: "what is the p-th percentile of values in the last `duration_ms`?"
    ///
    /// Strategy:
    /// 1. Use the timestamp KLL to figure out what fraction of data falls
    ///    in the requested time range.
    /// 2. Filter entries to that range and build a fresh KLL for the values.
    ///
    /// The timestamp KLL lets us cheaply estimate the cutoff without sorting.
    fn query_last_duration(&self, duration_ms: u64, percentile: f64) -> QueryResult {
        let now_ms = self.window_start_ms + self.window_size_ms;
        let cutoff_ms = now_ms.saturating_sub(duration_ms);

        // Use timestamp KLL to estimate what rank fraction the cutoff represents.
        let ts_cdf = self.timestamp_sketch.cdf();
        let cutoff_rank = ts_cdf.quantile(cutoff_ms as f64);

        // The fraction of data points that fall AFTER the cutoff.
        let fraction_in_range = 1.0 - cutoff_rank;
        let estimated_count_in_range = (fraction_in_range * self.count as f64).round() as usize;

        // Now actually filter and compute the exact answer for comparison.
        let mut filtered_kll = KLL::init_kll(200);
        let mut exact_count = 0;
        for entry in &self.entries {
            if entry.timestamp_ms >= cutoff_ms {
                filtered_kll
                    .update(&SketchInput::F64(entry.value))
                    .unwrap();
                exact_count += 1;
            }
        }

        let filtered_cdf = filtered_kll.cdf();
        let result_value = filtered_cdf.query(percentile);

        QueryResult {
            value: result_value,
            estimated_points_in_range: estimated_count_in_range,
            exact_points_in_range: exact_count,
            cutoff_rank,
        }
    }

    /// Use only the timestamp KLL to estimate the percentile of the full window's
    /// values — no filtering at all. This is the "approximate shortcut":
    /// if the window is big and the query range covers most of it, this is
    /// nearly free.
    fn query_full_window_percentile(&self, percentile: f64) -> f64 {
        let cdf = self.value_sketch.cdf();
        cdf.query(percentile)
    }
}

#[derive(Debug)]
struct QueryResult {
    value: f64,
    estimated_points_in_range: usize,
    exact_points_in_range: usize,
    cutoff_rank: f64,
}

fn main() {
    println!("=== KLL Tumbling Window Experiment ===\n");

    let window_size_ms: u64 = 100 * 60 * 1000; // 100 minutes
    let window_start_ms: u64 = 0;
    let mut window = KllTumblingWindow::new(window_size_ms, window_start_ms);

    let mut rng = rand::rng();

    // Simulate ingesting data points with out-of-order timestamps.
    let n_points = 100_000;
    println!("Ingesting {n_points} data points with out-of-order timestamps...");

    let mut timestamps: Vec<u64> = (0..n_points)
        .map(|_| rng.random_range(window_start_ms..window_start_ms + window_size_ms))
        .collect();

    // Shuffle to ensure out-of-order arrival.
    for i in (1..timestamps.len()).rev() {
        let j = rng.random_range(0..=i);
        timestamps.swap(i, j);
    }

    for &ts in &timestamps {
        // Value correlates somewhat with time (later = higher load).
        let base_value = (ts as f64 / window_size_ms as f64) * 80.0;
        let noise: f64 = rng.random_range(-10.0..10.0);
        let value = (base_value + noise).max(0.0).min(100.0);
        window.insert(ts, value);
    }

    println!("  Total ingested: {}", window.count);

    // Query for different time ranges.
    let query_durations_min = [5, 10, 30, 50, 100];
    let percentiles = [0.50, 0.90, 0.95, 0.99];

    println!("\n--- Querying 'last N minutes' at various percentiles ---\n");

    for &dur_min in &query_durations_min {
        let dur_ms = dur_min as u64 * 60 * 1000;
        println!("  Last {} minutes (cutoff at {}ms before window end):", dur_min, dur_ms);

        for &p in &percentiles {
            let result = window.query_last_duration(dur_ms, p);
            println!(
                "    p{:02}: value={:.2}  |  est_points={}  exact_points={}  cutoff_rank={:.4}",
                (p * 100.0) as u32,
                result.value,
                result.estimated_points_in_range,
                result.exact_points_in_range,
                result.cutoff_rank,
            );
        }
        println!();
    }

    // Compare: full-window percentile vs filtered.
    println!("--- Full-window percentiles (no time filtering) ---\n");
    for &p in &percentiles {
        let full_val = window.query_full_window_percentile(p);
        println!("    p{:02}: {:.2}", (p * 100.0) as u32, full_val);
    }

    println!("\n--- Key takeaway ---");
    println!("The timestamp KLL lets us estimate how much data falls in a time range");
    println!("without sorting or indexing. Combined with the value KLL, we get approximate");
    println!("quantiles over arbitrary time sub-ranges of a large tumbling window.");
    println!("Timestamps can arrive completely out of order.");
}
