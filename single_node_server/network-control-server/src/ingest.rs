use std::{env, error::Error, path::PathBuf, time::Instant};

use csv::StringRecord;

use crate::metrics::MetricStore;
use crate::metrics::MetricStoreBuilder;

pub fn load_metric_store() -> Result<MetricStore, Box<dyn Error + Send + Sync>> {
    let start = Instant::now();
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

    let mut builder = MetricStoreBuilder::new();
    let mut processed: u64 = 0;

    for (row_idx, record) in reader.records().enumerate() {
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

        // println!("cluster: {} and task: {} and values: {} {} {}", cluster, task, cpu_value, mem_value, net_value);

        builder.insert(cluster, task, cpu_value, mem_value, net_value);
        processed += 1;

        if processed % 1_000_000 == 0 {
            eprintln!("ingested {processed} rows...");
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
