use std::sync::Mutex;
use std::time::Instant;

use serde::Serialize;

use super::cms_cumulative::{MetricCumulativeAndTop, clamp_i128_to_i32};
use super::hydra_labels::{MetricFrequencyHydra, clamp_frequency_estimate, round_to_i32};
use super::key::hash_key_128;
use super::kll_quantiles::{MetricHydra, MetricQuantiles};

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
    frequency_by_label: Mutex<MetricFrequencyHydra>,
    quantile_by_label: Mutex<MetricHydra>,
}

impl MetricStore {
    pub fn query_percentile(&self, field: MetricField, percent: f64) -> Option<f64> {
        if !(0.0..=100.0).contains(&percent) {
            return None;
        }
        let quantile = percent / 100.0;
        let klls = self.klls.lock().ok()?;
        let value = match field {
            MetricField::CpuCores => klls.cpu_cores.quantile(quantile),
            MetricField::MemoryGb => klls.memory_gb.quantile(quantile),
            MetricField::NetworkMbps => klls.network_mbps.quantile(quantile),
        };
        Some(value)
    }

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
        let hydra = self.quantile_by_label.lock().ok()?;
        hydra.query_quantile(field, key, quantile)
    }

    pub fn top_entity(&self, field: MetricField) -> Option<EntityEstimate> {
        self.countmins.top_entity(field)
    }

    pub fn cumulative_value(&self, field: MetricField, key: &str) -> i32 {
        let key_hash = hash_key_128(key);
        clamp_i128_to_i32(self.countmins.cumulative_estimate(field, key_hash))
    }

    pub fn frequency_estimate(&self, field: MetricField, key: &str, value: f64) -> Option<i32> {
        let rounded = round_to_i32(value)?;
        let hydra = self.frequency_by_label.lock().ok()?;
        let estimate = hydra.query_frequency(field, key, rounded)?;
        Some(clamp_frequency_estimate(estimate))
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

        let mut key = String::with_capacity(cluster.len() + task.len() + 1);
        key.push_str(cluster);
        key.push(';');
        key.push_str(task);

        {
            let mut klls = self.klls.lock().map_err(|_| "failed to lock klls")?;
            klls.insert_samples(cpu_value, memory_value, network_value);
        }
        {
            let mut hydra = self
                .quantile_by_label
                .lock()
                .map_err(|_| "failed to lock quantile sketches")?;
            hydra.update(&key, cpu_value, memory_value, network_value);
        }

        let mut freq_hydra = self
            .frequency_by_label
            .lock()
            .map_err(|_| "failed to lock frequency sketches")?;

        let cpu_rounded = round_to_i32(cpu_value);
        let mem_rounded = round_to_i32(memory_value);
        let net_rounded = round_to_i32(network_value);
        update_countmins(
            &self.countmins,
            cluster,
            task,
            &key,
            cpu_rounded.map(|value| value as i128).unwrap_or(0),
            mem_rounded.map(|value| value as i128).unwrap_or(0),
            net_rounded.map(|value| value as i128).unwrap_or(0),
        );

        if let Some(value) = cpu_rounded {
            freq_hydra.update(MetricField::CpuCores, &key, value);
        }
        if let Some(value) = mem_rounded {
            freq_hydra.update(MetricField::MemoryGb, &key, value);
        }
        if let Some(value) = net_rounded {
            freq_hydra.update(MetricField::NetworkMbps, &key, value);
        }

        Ok(())
    }
}

/// Timing data for a single insert operation (in nanoseconds)
#[derive(Default)]
pub struct InsertTiming {
    pub build_key_ns: u64,
    pub kll_ns: u64,
    pub hydra_ns: u64,
    pub freq_hydra_ns: u64,
    pub countmin_ns: u64,
}

pub struct MetricPreAggregation {
    klls: MetricQuantiles,
    countmins: MetricCumulativeAndTop,
    frequency_hydra: MetricFrequencyHydra,
    hydra: MetricHydra,
    key_buffer: String,
}

impl MetricPreAggregation {
    pub fn new() -> Self {
        Self {
            klls: MetricQuantiles::default(),
            countmins: MetricCumulativeAndTop::default(),
            frequency_hydra: MetricFrequencyHydra::new(),
            hydra: MetricHydra::new(),
            key_buffer: String::with_capacity(128),
        }
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
        self.countmins
            .update(cluster, cluster_hash, cpu_value, memory_value, network_value);

        let task_hash = hash_key_128(task);
        self.countmins
            .update(task, task_hash, cpu_value, memory_value, network_value);
    }

    pub fn insert(
        &mut self,
        cluster: &str,
        task: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) {
        self.klls
            .insert_samples(cpu_value, memory_value, network_value);

        self.key_buffer.clear();
        self.key_buffer.push_str(cluster);
        self.key_buffer.push(';');
        self.key_buffer.push_str(task);

        self.hydra
            .update(&self.key_buffer, cpu_value, memory_value, network_value);

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

        if let Some(value) = cpu_rounded {
            self.frequency_hydra
                .update(MetricField::CpuCores, &self.key_buffer, value);
        }
        if let Some(value) = mem_rounded {
            self.frequency_hydra
                .update(MetricField::MemoryGb, &self.key_buffer, value);
        }
        if let Some(value) = net_rounded {
            self.frequency_hydra
                .update(MetricField::NetworkMbps, &self.key_buffer, value);
        }
    }

    /// Insert with timing - returns timing data for each step
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
        let t0 = Instant::now();
        self.klls
            .insert_samples(cpu_value, memory_value, network_value);
        timing.kll_ns = t0.elapsed().as_nanos() as u64;

        // Build key
        let t1 = Instant::now();
        self.key_buffer.clear();
        self.key_buffer.push_str(cluster);
        self.key_buffer.push(';');
        self.key_buffer.push_str(task);
        timing.build_key_ns = t1.elapsed().as_nanos() as u64;

        // Hydra insertion
        let t2 = Instant::now();
        self.hydra
            .update(&self.key_buffer, cpu_value, memory_value, network_value);
        timing.hydra_ns = t2.elapsed().as_nanos() as u64;

        let cpu_rounded = round_to_i32(cpu_value);
        let mem_rounded = round_to_i32(memory_value);
        let net_rounded = round_to_i32(network_value);

        // Frequency Hydra insertion
        let t3 = Instant::now();
        if let Some(value) = cpu_rounded {
            self.frequency_hydra
                .update(MetricField::CpuCores, &self.key_buffer, value);
        }
        if let Some(value) = mem_rounded {
            self.frequency_hydra
                .update(MetricField::MemoryGb, &self.key_buffer, value);
        }
        if let Some(value) = net_rounded {
            self.frequency_hydra
                .update(MetricField::NetworkMbps, &self.key_buffer, value);
        }
        timing.freq_hydra_ns = t3.elapsed().as_nanos() as u64;

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
            frequency_by_label: Mutex::new(self.frequency_hydra),
            quantile_by_label: Mutex::new(self.hydra),
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
    countmins.update(cluster, cluster_hash, cpu_value, memory_value, network_value);

    let task_hash = hash_key_128(task);
    countmins.update(task, task_hash, cpu_value, memory_value, network_value);
}
