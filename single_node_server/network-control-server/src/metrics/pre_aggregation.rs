use std::sync::Mutex;
use std::time::Instant;

use serde::Serialize;

use super::cms_cumulative::MetricCumulativeAndTop;
use super::hydra_labels::MetricHydra;
use super::key::hash_key_128;
use super::kll_quantiles::MetricQuantiles;
use super::minute_window::MetricMinuteWindow;
use super::util::{clamp_i128_to_i32, round_to_i32};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum MetricField {
    CpuCores,
    MemoryGb,
    NetworkMbps,
}

impl MetricField {
    pub fn from_spec(spec: &str) -> Option<Self> {
        let normalized = spec
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .replace(' ', "_");
        match normalized.as_str() {
            "cpu_cores" | "cpucores" => Some(Self::CpuCores),
            "memory_gb" | "memorygb" => Some(Self::MemoryGb),
            "network_mbps" | "networkmbps" => Some(Self::NetworkMbps),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct EntityEstimate {
    pub key: String,
    pub value: i32,
}

pub struct MetricStore {
    klls: Mutex<MetricQuantiles>,
    countmins: MetricCumulativeAndTop,
    hydra_by_label: Mutex<MetricHydra>,
    minute_window: Mutex<MetricMinuteWindow>,
}

impl MetricStore {
    pub fn query_percentile(&self, field: MetricField, percent: f64) -> Option<f64> {
        if !(0.0..=100.0).contains(&percent) {
            return None;
        }
        let quantile = percent / 100.0;
        let klls = self.klls.lock().ok()?;
        let value = match field {
            MetricField::CpuCores => {
                let cpu = match klls.cpu_cores.read() {
                    Ok(c) => c,
                    Err(p) => p.into_inner(),
                };
                cpu.quantile(quantile)
            }
            MetricField::MemoryGb => {
                let mem = match klls.memory_gb.read() {
                    Ok(m) => m,
                    Err(p) => p.into_inner(),
                };
                mem.quantile(quantile)
            }
            MetricField::NetworkMbps => {
                let net = match klls.network_mbps.read() {
                    Ok(n) => n,
                    Err(p) => p.into_inner(),
                };
                net.quantile(quantile)
            }
        };
        Some(value)
    }

    #[allow(dead_code)]
    pub fn query_percentile_by_key(
        &self,
        field: MetricField,
        key: &str,
        percent: f64,
    ) -> Option<f64> {
        if !(0.0..=100.0).contains(&percent) {
            return None;
        }
        let quantile = percent / 100.0;
        let hydra = self.hydra_by_label.lock().ok()?;
        hydra.query_quantile(field, key, quantile)
    }

    pub fn query_percentiles(
        &self,
        field: MetricField,
        percents: &[f64],
    ) -> Option<Vec<Option<f64>>> {
        let klls = self.klls.lock().ok()?;
        let results = match field {
            MetricField::CpuCores => {
                let cpu = match klls.cpu_cores.read() {
                    Ok(c) => c,
                    Err(p) => p.into_inner(),
                };
                percents
                    .iter()
                    .map(|percent| {
                        if !(0.0..=100.0).contains(percent) {
                            None
                        } else {
                            Some(cpu.quantile(percent / 100.0))
                        }
                    })
                    .collect()
            }
            MetricField::MemoryGb => {
                let mem = match klls.memory_gb.read() {
                    Ok(m) => m,
                    Err(p) => p.into_inner(),
                };
                percents
                    .iter()
                    .map(|percent| {
                        if !(0.0..=100.0).contains(percent) {
                            None
                        } else {
                            Some(mem.quantile(percent / 100.0))
                        }
                    })
                    .collect()
            }
            MetricField::NetworkMbps => {
                let net = match klls.network_mbps.read() {
                    Ok(n) => n,
                    Err(p) => p.into_inner(),
                };
                percents
                    .iter()
                    .map(|percent| {
                        if !(0.0..=100.0).contains(percent) {
                            None
                        } else {
                            Some(net.quantile(percent / 100.0))
                        }
                    })
                    .collect()
            }
        };
        Some(results)
    }

    pub fn query_percentiles_time(
        &self,
        field: MetricField,
        percents: &[f64],
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<Vec<Option<f64>>> {
        let mut window = self.minute_window.lock().ok()?;
        window.query_percentiles(field, percents, current_time_ms, time_range_ms)
    }

    pub fn query_percentiles_by_key(
        &self,
        field: MetricField,
        key: &str,
        percents: &[f64],
    ) -> Option<Vec<Option<f64>>> {
        let hydra = self.hydra_by_label.lock().ok()?;
        let mut results = Vec::with_capacity(percents.len());
        for percent in percents {
            if !(0.0..=100.0).contains(percent) {
                results.push(None);
                continue;
            }
            let quantile = percent / 100.0;
            results.push(hydra.query_quantile(field, key, quantile));
        }
        Some(results)
    }

    pub fn query_percentiles_by_key_time(
        &self,
        field: MetricField,
        key: &str,
        percents: &[f64],
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<Vec<Option<f64>>> {
        let mut window = self.minute_window.lock().ok()?;
        window.query_percentiles_by_key(field, key, percents, current_time_ms, time_range_ms)
    }

    pub fn top_entity(&self, field: MetricField) -> Option<EntityEstimate> {
        self.countmins.top_entity(field)
    }

    pub fn top_entity_time(
        &self,
        field: MetricField,
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<EntityEstimate> {
        let mut window = self.minute_window.lock().ok()?;
        window.top_entity(field, current_time_ms, time_range_ms)
    }

    pub fn cumulative_value(&self, field: MetricField, key: &str) -> i32 {
        let key_hash = hash_key_128(key);
        clamp_i128_to_i32(self.countmins.cumulative_estimate(field, key_hash))
    }

    pub fn cumulative_value_time(
        &self,
        field: MetricField,
        key: &str,
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<i32> {
        let mut window = self.minute_window.lock().ok()?;
        window.cumulative_value(field, key, current_time_ms, time_range_ms)
    }

    pub fn insert(
        &self,
        cluster: &str,
        task: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) -> Result<(), String> {
        let cluster = cluster.trim();
        let task = task.trim();
        if cluster.is_empty() || task.is_empty() {
            return Ok(());
        }

        // Label key order is always "cluster;task" (same order used for count-min updates).
        let mut key = String::with_capacity(cluster.len() + task.len() + 1);
        key.push_str(cluster);
        key.push(';');
        key.push_str(task);

        {
            let mut klls = self.klls.lock().map_err(|_| "failed to lock klls")?;
            klls.insert_samples(cpu_value, memory_value, network_value);
        }
        let cpu_rounded = round_to_i32(cpu_value);
        let mem_rounded = round_to_i32(memory_value);
        let net_rounded = round_to_i32(network_value);
        {
            let mut hydra = self
                .hydra_by_label
                .lock()
                .map_err(|_| "failed to lock label sketches")?;
            hydra.update(&key, cpu_value, memory_value, network_value);
        }
        update_countmins(
            &self.countmins,
            cluster,
            task,
            &key,
            cpu_rounded.map(|value| value as i128).unwrap_or(0),
            mem_rounded.map(|value| value as i128).unwrap_or(0),
            net_rounded.map(|value| value as i128).unwrap_or(0),
        );

        Ok(())
    }
}

/// Timing data for a single insert operation (in nanoseconds)
#[derive(Default)]
pub struct InsertTiming {
    pub build_key_ns: u64,
    pub kll_ns: u64,
    #[allow(dead_code)]
    pub hydra_ns: u64,
    pub countmin_ns: u64,
}

pub struct MetricPreAggregation {
    klls: MetricQuantiles,
    countmins: MetricCumulativeAndTop,
    hydra_by_label: MetricHydra,
    minute_window: MetricMinuteWindow,
    key_buffer: String,
}

impl MetricPreAggregation {
    pub fn new() -> Self {
        Self {
            klls: MetricQuantiles::default(),
            countmins: MetricCumulativeAndTop::default(),
            hydra_by_label: MetricHydra::new(),
            minute_window: MetricMinuteWindow::default(),
            key_buffer: String::with_capacity(128),
        }
    }

    fn build_key_buffer(&mut self, cluster: &str, task: &str) {
        // Label key order is always "cluster;task" (same order used for count-min updates).
        self.key_buffer.clear();
        self.key_buffer.push_str(cluster);
        self.key_buffer.push(';');
        self.key_buffer.push_str(task);
    }

    fn update_countmins(
        &mut self,
        cluster: &str,
        task: &str,
        cpu_value: i128,
        memory_value: i128,
        network_value: i128,
    ) {
        if cpu_value <= 0 && memory_value <= 0 && network_value <= 0 {
            return;
        }

        let full_hash = hash_key_128(&self.key_buffer);
        self.countmins.update(
            &self.key_buffer,
            full_hash,
            cpu_value,
            memory_value,
            network_value,
        );

        let cluster_hash = hash_key_128(cluster);
        self.countmins.update(
            cluster,
            cluster_hash,
            cpu_value,
            memory_value,
            network_value,
        );

        let task_hash = hash_key_128(task);
        self.countmins
            .update(task, task_hash, cpu_value, memory_value, network_value);
    }

    pub fn insert_time_window(
        &mut self,
        start_time_ms: u64,
        end_time_ms: u64,
        cluster: &str,
        task: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) {
        self.build_key_buffer(cluster, task);
        let full_key = self.key_buffer.as_str();
        self.minute_window.insert_range(
            start_time_ms,
            end_time_ms,
            cluster,
            task,
            full_key,
            cpu_value,
            memory_value,
            network_value,
        );
    }

    pub fn insert_kll(&mut self, cpu_value: f64, memory_value: f64, network_value: f64) {
        self.klls
            .insert_samples(cpu_value, memory_value, network_value);
    }

    pub fn insert_kll_timed(
        &mut self,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) -> u64 {
        let t0 = Instant::now();
        self.insert_kll(cpu_value, memory_value, network_value);
        t0.elapsed().as_nanos() as u64
    }

    pub fn insert_cms(
        &mut self,
        cluster: &str,
        task: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) {
        self.build_key_buffer(cluster, task);

        let cpu_rounded = round_to_i32(cpu_value);
        let mem_rounded = round_to_i32(memory_value);
        let net_rounded = round_to_i32(network_value);
        self.update_countmins(
            cluster,
            task,
            cpu_rounded.map(|value| value as i128).unwrap_or(0),
            mem_rounded.map(|value| value as i128).unwrap_or(0),
            net_rounded.map(|value| value as i128).unwrap_or(0),
        );
    }

    pub fn insert_cms_timed(
        &mut self,
        cluster: &str,
        task: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) -> (u64, u64) {
        let t0 = Instant::now();
        self.build_key_buffer(cluster, task);
        let build_key_ns = t0.elapsed().as_nanos() as u64;

        let cpu_rounded = round_to_i32(cpu_value);
        let mem_rounded = round_to_i32(memory_value);
        let net_rounded = round_to_i32(network_value);

        let t1 = Instant::now();
        self.update_countmins(
            cluster,
            task,
            cpu_rounded.map(|value| value as i128).unwrap_or(0),
            mem_rounded.map(|value| value as i128).unwrap_or(0),
            net_rounded.map(|value| value as i128).unwrap_or(0),
        );
        let countmin_ns = t1.elapsed().as_nanos() as u64;

        (build_key_ns, countmin_ns)
    }

    #[allow(dead_code)]
    pub fn insert_hydra(
        &mut self,
        cluster: &str,
        task: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) {
        self.build_key_buffer(cluster, task);
        self.hydra_by_label
            .update(&self.key_buffer, cpu_value, memory_value, network_value);
    }

    pub fn insert_hydra_batch(
        &mut self,
        cluster: &str,
        task: &str,
        cpu_values: &[f64],
        memory_values: &[f64],
        network_values: &[f64],
    ) {
        let len = cpu_values
            .len()
            .min(memory_values.len())
            .min(network_values.len());
        if len == 0 {
            return;
        }
        self.build_key_buffer(cluster, task);
        for idx in 0..len {
            self.hydra_by_label.update(
                &self.key_buffer,
                cpu_values[idx],
                memory_values[idx],
                network_values[idx],
            );
        }
    }

    #[allow(dead_code)]
    pub fn insert(
        &mut self,
        cluster: &str,
        task: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) {
        self.insert_kll(cpu_value, memory_value, network_value);

        // Label key order is always "cluster;task" (same order used for count-min updates).
        self.build_key_buffer(cluster, task);

        let cpu_rounded = round_to_i32(cpu_value);
        let mem_rounded = round_to_i32(memory_value);
        let net_rounded = round_to_i32(network_value);
        self.hydra_by_label
            .update(&self.key_buffer, cpu_value, memory_value, network_value);
        self.update_countmins(
            cluster,
            task,
            cpu_rounded.map(|value| value as i128).unwrap_or(0),
            mem_rounded.map(|value| value as i128).unwrap_or(0),
            net_rounded.map(|value| value as i128).unwrap_or(0),
        );
    }

    /// Insert with timing - returns timing data for each step
    #[allow(dead_code)]
    pub fn insert_timed(
        &mut self,
        cluster: &str,
        task: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) -> InsertTiming {
        let mut timing = InsertTiming::default();

        // KLL insertion
        timing.kll_ns = self.insert_kll_timed(cpu_value, memory_value, network_value);

        // Build key
        let t1 = Instant::now();
        self.build_key_buffer(cluster, task);
        timing.build_key_ns = t1.elapsed().as_nanos() as u64;

        let cpu_rounded = round_to_i32(cpu_value);
        let mem_rounded = round_to_i32(memory_value);
        let net_rounded = round_to_i32(network_value);

        // Hydra insertion
        let t2 = Instant::now();
        self.hydra_by_label
            .update(&self.key_buffer, cpu_value, memory_value, network_value);
        timing.hydra_ns = t2.elapsed().as_nanos() as u64;

        // CountMin insertion
        let t4 = Instant::now();
        self.update_countmins(
            cluster,
            task,
            cpu_rounded.map(|value| value as i128).unwrap_or(0),
            mem_rounded.map(|value| value as i128).unwrap_or(0),
            net_rounded.map(|value| value as i128).unwrap_or(0),
        );
        timing.countmin_ns = t4.elapsed().as_nanos() as u64;

        timing
    }

    pub fn finish(self) -> MetricStore {
        MetricStore {
            klls: Mutex::new(self.klls),
            countmins: self.countmins,
            hydra_by_label: Mutex::new(self.hydra_by_label),
            minute_window: Mutex::new(self.minute_window),
        }
    }
}

fn update_countmins(
    countmins: &MetricCumulativeAndTop,
    cluster: &str,
    task: &str,
    full_key: &str,
    cpu_value: i128,
    memory_value: i128,
    network_value: i128,
) {
    if cpu_value <= 0 && memory_value <= 0 && network_value <= 0 {
        return;
    }

    let full_hash = hash_key_128(full_key);
    countmins.update(full_key, full_hash, cpu_value, memory_value, network_value);

    let cluster_hash = hash_key_128(cluster);
    countmins.update(
        cluster,
        cluster_hash,
        cpu_value,
        memory_value,
        network_value,
    );

    let task_hash = hash_key_128(task);
    countmins.update(task, task_hash, cpu_value, memory_value, network_value);
}
