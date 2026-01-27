use std::{collections::HashMap, env, error::Error, path::PathBuf, time::Instant};

use chrono::DateTime;
use csv::StringRecord;

use crate::metrics::{MetricPreAggregation, MetricStore};

struct BatchInput {
    cpu: Vec<f64>,
    mem: Vec<f64>,
    net: Vec<f64>,
    start_times: Vec<u64>,
    end_times: Vec<u64>,
}

impl BatchInput {
    #[allow(dead_code)]
    fn new() -> Self {
        let cpu_values = Vec::new();
        let mem_values = Vec::new();
        let net_values = Vec::new();
        let start_values = Vec::new();
        let end_values = Vec::new();
        Self {
            cpu: cpu_values,
            mem: mem_values,
            net: net_values,
            start_times: start_values,
            end_times: end_values,
        }
    }

    fn new_with_val(cpu: f64, mem: f64, net: f64, start_time_ms: u64, end_time_ms: u64) -> Self {
        let mut cpu_values = Vec::with_capacity(8);
        let mut mem_values = Vec::with_capacity(8);
        let mut net_values = Vec::with_capacity(8);
        let mut start_values = Vec::with_capacity(8);
        let mut end_values = Vec::with_capacity(8);
        cpu_values.push(cpu);
        mem_values.push(mem);
        net_values.push(net);
        start_values.push(start_time_ms);
        end_values.push(end_time_ms);
        Self {
            cpu: cpu_values,
            mem: mem_values,
            net: net_values,
            start_times: start_values,
            end_times: end_values,
        }
    }

    fn push(&mut self, cpu: f64, mem: f64, net: f64, start_time_ms: u64, end_time_ms: u64) {
        self.cpu.push(cpu);
        self.mem.push(mem);
        self.net.push(net);
        self.start_times.push(start_time_ms);
        self.end_times.push(end_time_ms);
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.cpu.len()
    }

    fn clear(&mut self) {
        self.cpu.clear();
        self.mem.clear();
        self.net.clear();
        self.start_times.clear();
        self.end_times.clear();
    }
}

/// Accumulated timing for ingestion steps (in nanoseconds for precision)
struct IngestTiming {
    enabled: bool,
    parse_row_ns: u64,
    build_key_ns: u64,
    insert_kll_ns: u64,
    insert_hydra_ns: u64,
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
            insert_countmin_ns: 0,
        }
    }

    fn reset(&mut self) {
        self.parse_row_ns = 0;
        self.build_key_ns = 0;
        self.insert_kll_ns = 0;
        self.insert_hydra_ns = 0;
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
            + self.insert_countmin_ns;
        let to_ms = |ns: u64| ns as f64 / 1_000_000.0;
        let to_us_per_row = |ns: u64| (ns as f64 / rows as f64) / 1000.0;

        eprintln!(
            "[INGEST TIMING] rows={} total={:.2}ms | parse_row={:.2}ms ({:.3}us/row) build_key={:.2}ms ({:.3}us/row) kll={:.2}ms ({:.3}us/row) hydra={:.2}ms ({:.3}us/row) countmin={:.2}ms ({:.3}us/row)",
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
            to_ms(self.insert_countmin_ns),
            to_us_per_row(self.insert_countmin_ns),
        );
    }
}

fn flush_batch(
    batches: &mut HashMap<(String, String), BatchInput>,
    builder: &mut MetricPreAggregation,
    timing: &mut IngestTiming,
) {
    for (key, batch) in batches.iter_mut() {
        let cluster = key.0.as_str();
        let task = key.1.as_str();
        let len = batch
            .cpu
            .len()
            .min(batch.mem.len())
            .min(batch.net.len())
            .min(batch.start_times.len())
            .min(batch.end_times.len());
        // Update KLL and CountMin (CMS) sketches per sample.
        if timing.enabled {
            for idx in 0..len {
                timing.insert_kll_ns += builder.insert_kll_timed(
                    batch.cpu[idx],
                    batch.mem[idx],
                    batch.net[idx],
                );
                let (build_key_ns, countmin_ns) = builder.insert_cms_timed(
                    cluster,
                    task,
                    batch.cpu[idx],
                    batch.mem[idx],
                    batch.net[idx],
                );
                timing.build_key_ns += build_key_ns;
                timing.insert_countmin_ns += countmin_ns;
                builder.insert_time_window(
                    batch.start_times[idx],
                    batch.end_times[idx],
                    cluster,
                    task,
                    batch.cpu[idx],
                    batch.mem[idx],
                    batch.net[idx],
                );
            }
        } else {
            for idx in 0..len {
                builder.insert_kll(batch.cpu[idx], batch.mem[idx], batch.net[idx]);
                builder.insert_cms(
                    cluster,
                    task,
                    batch.cpu[idx],
                    batch.mem[idx],
                    batch.net[idx],
                );
                builder.insert_time_window(
                    batch.start_times[idx],
                    batch.end_times[idx],
                    cluster,
                    task,
                    batch.cpu[idx],
                    batch.mem[idx],
                    batch.net[idx],
                );
            }
        }

        // Update Hydra label sketches in batch.
        let start = if timing.enabled {
            Some(Instant::now())
        } else {
            None
        };
        builder.insert_hydra_batch(cluster, task, &batch.cpu, &batch.mem, &batch.net);
        if let Some(t) = start {
            timing.insert_hydra_ns += t.elapsed().as_nanos() as u64;
        }
        batch.clear();
    }
}

// read in csv into batch, and process the batch accordingly
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
    let timestamp_idx = find_column(&headers, &["timestamp", "time"])
        .ok_or_else(|| format!("missing 'timestamp' column in {}", csv_path.display()))?;
    let duration_idx = find_column(&headers, &["estimated_duration", "duration"])
        .ok_or_else(|| format!("missing 'estimated_duration' column in {}", csv_path.display()))?;
    let cpu_idx = find_column(&headers, &["cpu_cores", "cpu-cores"])
        .ok_or_else(|| format!("missing 'cpu_cores' column in {}", csv_path.display()))?;
    let mem_idx = find_column(&headers, &["memory_gb", "memory-gb"])
        .ok_or_else(|| format!("missing 'memory_gb' column in {}", csv_path.display()))?;
    let net_idx = find_column(&headers, &["network_mbps", "network-mbps"])
        .ok_or_else(|| format!("missing 'network_mbps' column in {}", csv_path.display()))?;

    let mut builder = MetricPreAggregation::new();
    let mut input_batch: HashMap<(String, String), BatchInput> = HashMap::new();
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
        let time_raw = record.get(timestamp_idx).unwrap_or("").trim();
        let duration_raw = record.get(duration_idx).unwrap_or("").trim();

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
        let time_ms = match DateTime::parse_from_rfc3339(time_raw) {
            Ok(value) => match value.timestamp_millis() {
                millis if millis >= 0 => millis as u64,
                _ => continue,
            },
            Err(_) => continue,
        };
        let duration_ms = match duration_raw.parse::<f64>() {
            Ok(value) if value.is_finite() && value >= 0.0 => (value * 1000.0).round() as u64,
            _ => continue,
        };
        let end_time_ms = time_ms.saturating_add(duration_ms);

        if let Some(t) = parse_start {
            timing.parse_row_ns += t.elapsed().as_nanos() as u64;
        }

        let key = (cluster.to_string(), task.to_string());
        if let Some(batch) = input_batch.get_mut(&key) {
            batch.push(cpu_value, mem_value, net_value, time_ms, end_time_ms);
        } else {
            input_batch.insert(
                key,
                BatchInput::new_with_val(cpu_value, mem_value, net_value, time_ms, end_time_ms),
            );
        }
        processed += 1;

        if processed % 1_000_000 == 0 {
            flush_batch(&mut input_batch, &mut builder, &mut timing);

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

    // flush the leftover if any
    flush_batch(&mut input_batch, &mut builder, &mut timing);

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
