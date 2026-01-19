use std::{env, error::Error, path::PathBuf, time::Instant};

use csv::StringRecord;

use crate::metrics::{MetricPreAggregation, MetricStore};

/// Accumulated timing for ingestion steps (in nanoseconds for precision)
struct IngestTiming {
    enabled: bool,
    parse_row_ns: u64,
    build_key_ns: u64,
    insert_kll_ns: u64,
    insert_hydra_ns: u64,
    insert_freq_hydra_ns: u64,
    insert_countmin_ns: u64,
}

impl IngestTiming {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            parse_row_ns: 0,
            build_key_ns: 0,
            insert_kll_ns: 0,
            insert_hydra_ns: 0,
            insert_freq_hydra_ns: 0,
            insert_countmin_ns: 0,
        }
    }

    fn reset(&mut self) {
        self.parse_row_ns = 0;
        self.build_key_ns = 0;
        self.insert_kll_ns = 0;
        self.insert_hydra_ns = 0;
        self.insert_freq_hydra_ns = 0;
        self.insert_countmin_ns = 0;
    }

    fn print_checkpoint(&self, rows: u64) {
        if !self.enabled || rows == 0 {
            return;
        }
        let total_ns = self.parse_row_ns
            + self.build_key_ns
            + self.insert_kll_ns
            + self.insert_hydra_ns
            + self.insert_freq_hydra_ns
            + self.insert_countmin_ns;
        let to_ms = |ns: u64| ns as f64 / 1_000_000.0;
        let to_us_per_row = |ns: u64| (ns as f64 / rows as f64) / 1000.0;

        eprintln!(
            "[INGEST TIMING] rows={} total={:.2}ms | parse_row={:.2}ms ({:.3}us/row) build_key={:.2}ms ({:.3}us/row) kll={:.2}ms ({:.3}us/row) hydra={:.2}ms ({:.3}us/row) freq_hydra={:.2}ms ({:.3}us/row) countmin={:.2}ms ({:.3}us/row)",
            rows,
            to_ms(total_ns),
            to_ms(self.parse_row_ns),
            to_us_per_row(self.parse_row_ns),
            to_ms(self.build_key_ns),
            to_us_per_row(self.build_key_ns),
            to_ms(self.insert_kll_ns),
            to_us_per_row(self.insert_kll_ns),
            to_ms(self.insert_hydra_ns),
            to_us_per_row(self.insert_hydra_ns),
            to_ms(self.insert_freq_hydra_ns),
            to_us_per_row(self.insert_freq_hydra_ns),
            to_ms(self.insert_countmin_ns),
            to_us_per_row(self.insert_countmin_ns),
        );
    }
}

pub fn load_metric_store(
    timing_enabled: bool,
) -> Result<MetricStore, Box<dyn Error + Send + Sync>> {
    let start = Instant::now();
    let mut checkpoint_start = Instant::now();
    let mut checkpoint_processed: u64 = 0;
    let mut timing = IngestTiming::new(timing_enabled);
    let csv_path = build_dataset_path();
    let mut reader = csv::Reader::from_path(&csv_path)?;
    let headers = reader.headers()?.clone();

    let cluster_idx = find_column(&headers, &["cluster"])
        .ok_or_else(|| format!("missing 'cluster' column in {}", csv_path.display()))?;
    let task_idx = find_column(&headers, &["task"])
        .ok_or_else(|| format!("missing 'task' column in {}", csv_path.display()))?;
    let cpu_idx = find_column(&headers, &["cpu_cores", "cpu-cores"])
        .ok_or_else(|| format!("missing 'cpu_cores' column in {}", csv_path.display()))?;
    let mem_idx = find_column(&headers, &["memory_gb", "memory-gb"])
        .ok_or_else(|| format!("missing 'memory_gb' column in {}", csv_path.display()))?;
    let net_idx = find_column(&headers, &["network_mbps", "network-mbps"])
        .ok_or_else(|| format!("missing 'network_mbps' column in {}", csv_path.display()))?;

    let mut builder = MetricPreAggregation::new();
    let mut processed: u64 = 0;

    for (row_idx, record) in reader.records().enumerate() {
        let parse_start = if timing.enabled {
            Some(Instant::now())
        } else {
            None
        };

        let record = match record {
            Ok(rec) => rec,
            Err(err) => {
                eprintln!("failed to read row {}: {err}", row_idx + 2);
                continue;
            }
        };

        let cluster = record.get(cluster_idx).unwrap_or("").trim();
        let task = record.get(task_idx).unwrap_or("").trim();
        if cluster.is_empty() || task.is_empty() {
            continue;
        }

        let cpu_raw = record.get(cpu_idx).unwrap_or("").trim();
        let mem_raw = record.get(mem_idx).unwrap_or("").trim();
        let net_raw = record.get(net_idx).unwrap_or("").trim();

        let cpu_value = match cpu_raw.parse::<f64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        let mem_value = match mem_raw.parse::<f64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        let net_value = match net_raw.parse::<f64>() {
            Ok(value) => value,
            Err(_) => continue,
        };

        if let Some(t) = parse_start {
            timing.parse_row_ns += t.elapsed().as_nanos() as u64;
        }

        if timing.enabled {
            let insert_timing =
                builder.insert_timed(cluster, task, cpu_value, mem_value, net_value);
            timing.build_key_ns += insert_timing.build_key_ns;
            timing.insert_kll_ns += insert_timing.kll_ns;
            timing.insert_hydra_ns += insert_timing.hydra_ns;
            timing.insert_freq_hydra_ns += insert_timing.freq_hydra_ns;
            timing.insert_countmin_ns += insert_timing.countmin_ns;
        } else {
            builder.insert(cluster, task, cpu_value, mem_value, net_value);
        }
        processed += 1;

        if processed % 1_000_000 == 0 {
            let elapsed = checkpoint_start.elapsed();
            let delta = processed - checkpoint_processed;
            let rows_per_sec = if elapsed.as_secs_f64() > 0.0 {
                (delta as f64) / elapsed.as_secs_f64()
            } else {
                0.0
            };
            eprintln!(
                "ingested {processed} rows ({rows_per_sec:.2} rows/sec since last checkpoint)"
            );
            timing.print_checkpoint(delta);
            timing.reset();
            checkpoint_start = Instant::now();
            checkpoint_processed = processed;
        }
    }

    let store = builder.finish();
    eprintln!(
        "processed {processed} rows into 3 metric sketches in {:.2?}",
        start.elapsed()
    );

    Ok(store)
}

fn build_dataset_path() -> PathBuf {
    let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join("cluster-metrics.csv")
}

fn find_column(headers: &StringRecord, candidates: &[&str]) -> Option<usize> {
    let lowered: Vec<String> = headers
        .iter()
        .map(|h| h.trim().to_ascii_lowercase())
        .collect();

    lowered.iter().position(|header| {
        candidates
            .iter()
            .any(|candidate| header == candidate.trim().to_ascii_lowercase().as_str())
    })
}
