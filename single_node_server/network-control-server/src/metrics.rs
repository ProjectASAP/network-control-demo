use std::sync::Mutex;

use serde::Serialize;
use sketchlib_rust::{
    CountMin, FastPath, FixedMatrix, Hydra, KLL, SketchInput, Vector2D,
    common::input::{HydraCounter, HydraQuery},
    sketches::kll::CDF,
};

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

#[derive(Default)]
struct MetricSketches {
    cpu_cores: KLL,
    memory_gb: KLL,
    network_mbps: KLL,
}

impl MetricSketches {
    fn insert_samples(&mut self, cpu_value: f64, memory_value: f64, network_value: f64) {
        let cpu = SketchInput::F64(cpu_value);
        let memory = SketchInput::F64(memory_value);
        let network = SketchInput::F64(network_value);

        self.cpu_cores
            .update(&cpu)
            .expect("cpu_cores values should be numeric");
        self.memory_gb
            .update(&memory)
            .expect("memory_gb values should be numeric");
        self.network_mbps
            .update(&network)
            .expect("network_mbps values should be numeric");
    }
}

struct MetricCdfs {
    cpu_cores: CDF,
    memory_gb: CDF,
    network_mbps: CDF,
}

impl MetricCdfs {
    fn from_sketches(sketches: MetricSketches) -> Self {
        Self {
            cpu_cores: sketches.cpu_cores.cdf(),
            memory_gb: sketches.memory_gb.cdf(),
            network_mbps: sketches.network_mbps.cdf(),
        }
    }

    fn query_percentile(&self, field: MetricField, percent: f64) -> Option<f64> {
        if !(0.0..=100.0).contains(&percent) {
            return None;
        }
        let quantile = percent / 100.0;
        match field {
            MetricField::CpuCores => Some(self.cpu_cores.query(quantile)),
            MetricField::MemoryGb => Some(self.memory_gb.query(quantile)),
            MetricField::NetworkMbps => Some(self.network_mbps.query(quantile)),
        }
    }
}

struct MetricHydra {
    kll_cpu: Hydra,
    kll_memory: Hydra,
    kll_network: Hydra,
    cm_cpu: Hydra,
    cm_memory: Hydra,
    cm_network: Hydra,
}

impl MetricHydra {
    fn new() -> Self {
        let kll_template = HydraCounter::KLL(KLL::default());
        let cm_template = HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default());

        Self {
            kll_cpu: Hydra::with_dimensions(3, 64, kll_template.clone()),
            kll_memory: Hydra::with_dimensions(3, 64, kll_template.clone()),
            kll_network: Hydra::with_dimensions(3, 64, kll_template),
            cm_cpu: Hydra::with_dimensions(3, 64, cm_template.clone()),
            cm_memory: Hydra::with_dimensions(3, 64, cm_template.clone()),
            cm_network: Hydra::with_dimensions(3, 64, cm_template),
        }
    }

    fn update(&mut self, key: &str, cpu_value: f64, memory_value: f64, network_value: f64) {
        let cpu_input = SketchInput::F64(cpu_value);
        let memory_input = SketchInput::F64(memory_value);
        let network_input = SketchInput::F64(network_value);

        self.kll_cpu.update(key, &cpu_input, None);
        self.kll_memory.update(key, &memory_input, None);
        self.kll_network.update(key, &network_input, None);

        if let Some(value) = round_to_i32(cpu_value) {
            let input = SketchInput::I32(value);
            self.cm_cpu.update(key, &input, None);
        }
        if let Some(value) = round_to_i32(memory_value) {
            let input = SketchInput::I32(value);
            self.cm_memory.update(key, &input, None);
        }
        if let Some(value) = round_to_i32(network_value) {
            let input = SketchInput::I32(value);
            self.cm_network.update(key, &input, None);
        }
    }

    fn query_quantile(&self, field: MetricField, key: &str, quantile: f64) -> Option<f64> {
        let parts = split_key(key)?;
        let query = HydraQuery::Quantile(quantile);
        Some(match field {
            MetricField::CpuCores => self.kll_cpu.query_key(parts, &query),
            MetricField::MemoryGb => self.kll_memory.query_key(parts, &query),
            MetricField::NetworkMbps => self.kll_network.query_key(parts, &query),
        })
    }

    fn query_frequency(&self, field: MetricField, key: &str, value: i32) -> Option<f64> {
        let parts = split_key(key)?;
        let input = SketchInput::I32(value);
        Some(match field {
            MetricField::CpuCores => self.cm_cpu.query_frequency(parts, &input),
            MetricField::MemoryGb => self.cm_memory.query_frequency(parts, &input),
            MetricField::NetworkMbps => self.cm_network.query_frequency(parts, &input),
        })
    }
}

fn split_key(key: &str) -> Option<Vec<&str>> {
    let parts: Vec<&str> = key.split(';').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() { None } else { Some(parts) }
}

#[derive(Clone)]
struct CountMinPair {
    top_entities: CountMin<FixedMatrix, FastPath>,
    cumulative: CountMin<FixedMatrix, FastPath>,
    top_key: Option<String>,
    top_value: i32,
}

impl Default for CountMinPair {
    fn default() -> Self {
        Self {
            top_entities: CountMin::default(),
            cumulative: CountMin::default(),
            top_key: None,
            top_value: 0,
        }
    }
}

impl CountMinPair {
    fn update_top_entities(&mut self, key: &str, key_input: &SketchInput, value: i32) {
        if value <= 0 {
            return;
        }
        let current = self.top_entities.estimate(key_input);
        if value > current {
            let delta = value - current;
            self.top_entities.insert_many(key_input, delta);
            if self.top_key.as_deref() != Some(key) {
                self.top_key = Some(key.to_string());
            }
            self.top_value = value;
        }
    }

    fn update_cumulative(&mut self, key_input: &SketchInput, value: i32) {
        if value <= 0 {
            return;
        }
        self.cumulative.insert_many(key_input, value);
    }

    fn top_entity(&self) -> Option<EntityEstimate> {
        self.top_key.as_ref().map(|key| EntityEstimate {
            key: key.clone(),
            value: self.top_value,
        })
    }

    fn estimate_cumulative(&self, key_input: &SketchInput) -> i32 {
        self.cumulative.estimate(key_input)
    }
}

#[derive(Default)]
struct MetricCountMins {
    cpu_cores: CountMinPair,
    memory_gb: CountMinPair,
    network_mbps: CountMinPair,
}

impl MetricCountMins {
    fn update(&mut self, field: MetricField, key: &str, key_input: &SketchInput, value: i32) {
        let pair = match field {
            MetricField::CpuCores => &mut self.cpu_cores,
            MetricField::MemoryGb => &mut self.memory_gb,
            MetricField::NetworkMbps => &mut self.network_mbps,
        };

        pair.update_top_entities(key, key_input, value);
        pair.update_cumulative(key_input, value);
    }

    fn top_entity(&self, field: MetricField) -> Option<EntityEstimate> {
        match field {
            MetricField::CpuCores => self.cpu_cores.top_entity(),
            MetricField::MemoryGb => self.memory_gb.top_entity(),
            MetricField::NetworkMbps => self.network_mbps.top_entity(),
        }
    }

    fn cumulative_estimate(&self, field: MetricField, key_input: &SketchInput) -> i32 {
        match field {
            MetricField::CpuCores => self.cpu_cores.estimate_cumulative(key_input),
            MetricField::MemoryGb => self.memory_gb.estimate_cumulative(key_input),
            MetricField::NetworkMbps => self.network_mbps.estimate_cumulative(key_input),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct EntityEstimate {
    pub key: String,
    pub value: i32,
}

pub struct MetricStore {
    cdfs: MetricCdfs,
    countmins: MetricCountMins,
    hydra: Mutex<MetricHydra>,
}

impl MetricStore {
    pub fn query_percentile(&self, field: MetricField, percent: f64) -> Option<f64> {
        self.cdfs.query_percentile(field, percent)
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
        let hydra = self.hydra.lock().ok()?;
        hydra.query_quantile(field, key, quantile)
    }

    pub fn query_frequency_by_key(&self, field: MetricField, key: &str, value: i32) -> Option<f64> {
        let hydra = self.hydra.lock().ok()?;
        hydra.query_frequency(field, key, value)
    }

    pub fn top_entity(&self, field: MetricField) -> Option<EntityEstimate> {
        self.countmins.top_entity(field)
    }

    pub fn cumulative_value(&self, field: MetricField, key: &str) -> i32 {
        let key_input = SketchInput::Str(key);
        self.countmins.cumulative_estimate(field, &key_input)
    }
}

pub struct MetricStoreBuilder {
    sketches: MetricSketches,
    countmins: MetricCountMins,
    hydra: MetricHydra,
    key_buffer: String,
}

impl MetricStoreBuilder {
    pub fn new() -> Self {
        Self {
            sketches: MetricSketches::default(),
            countmins: MetricCountMins::default(),
            hydra: MetricHydra::new(),
            key_buffer: String::with_capacity(128),
        }
    }

    pub fn insert(
        &mut self,
        cluster: &str,
        task: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) {
        self.sketches
            .insert_samples(cpu_value, memory_value, network_value);

        self.key_buffer.clear();
        self.key_buffer.push_str(cluster);
        self.key_buffer.push(';');
        self.key_buffer.push_str(task);

        self.hydra
            .update(&self.key_buffer, cpu_value, memory_value, network_value);

        let key_input = SketchInput::Str(&self.key_buffer);
        if let Some(value) = round_to_i32(cpu_value) {
            self.countmins
                .update(MetricField::CpuCores, &self.key_buffer, &key_input, value);
        }
        if let Some(value) = round_to_i32(memory_value) {
            self.countmins
                .update(MetricField::MemoryGb, &self.key_buffer, &key_input, value);
        }
        if let Some(value) = round_to_i32(network_value) {
            self.countmins.update(
                MetricField::NetworkMbps,
                &self.key_buffer,
                &key_input,
                value,
            );
        }
    }

    pub fn finish(self) -> MetricStore {
        MetricStore {
            cdfs: MetricCdfs::from_sketches(self.sketches),
            countmins: self.countmins,
            hydra: Mutex::new(self.hydra),
        }
    }
}

fn round_to_i32(value: f64) -> Option<i32> {
    if !value.is_finite() {
        return None;
    }
    let rounded = value.round();
    if rounded < i32::MIN as f64 || rounded > i32::MAX as f64 {
        return None;
    }
    let as_i32 = rounded as i32;
    if as_i32 <= 0 { None } else { Some(as_i32) }
}
