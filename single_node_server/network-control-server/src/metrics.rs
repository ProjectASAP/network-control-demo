use std::sync::Mutex;
use std::time::Instant;

use serde::Serialize;
use sketchlib_rust::{
    CountMin, FastPath, Hydra, KLL, SketchInput, Vector2D, XLCountMin,
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
struct MetricKll {
    cpu_cores: KLL,
    memory_gb: KLL,
    network_mbps: KLL,
}

impl MetricKll {
    fn insert_samples(&mut self, cpu_value: f64, memory_value: f64, network_value: f64) {
        self.cpu_cores
            .update(&SketchInput::F64(cpu_value))
            .expect("cpu_cores values should be numeric");
        self.memory_gb
            .update(&SketchInput::F64(memory_value))
            .expect("memory_gb values should be numeric");
        self.network_mbps
            .update(&SketchInput::F64(network_value))
            .expect("network_mbps values should be numeric");
    }
}

struct MetricCdfs {
    cpu_cores: CDF,
    memory_gb: CDF,
    network_mbps: CDF,
}

impl MetricCdfs {
    fn from_sketches(sketches: MetricKll) -> Self {
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
    cpu_quantile: Hydra,
    mem_quantile: Hydra,
    net_quantile: Hydra,
}

impl MetricHydra {
    fn new() -> Self {
        let kll_template = HydraCounter::KLL(KLL::default());

        Self {
            cpu_quantile: Hydra::with_dimensions(3, 64, kll_template.clone()),
            mem_quantile: Hydra::with_dimensions(3, 64, kll_template.clone()),
            net_quantile: Hydra::with_dimensions(3, 64, kll_template),
        }
    }

    fn update(&mut self, key: &str, cpu_value: f64, memory_value: f64, network_value: f64) {
        let cpu_input = SketchInput::F64(cpu_value);
        let memory_input = SketchInput::F64(memory_value);
        let network_input = SketchInput::F64(network_value);

        self.cpu_quantile.update(key, &cpu_input, None);
        self.mem_quantile.update(key, &memory_input, None);
        self.net_quantile.update(key, &network_input, None);
    }

    fn query_quantile(&self, field: MetricField, key: &str, quantile: f64) -> Option<f64> {
        let parts = split_key(key)?;
        let query = HydraQuery::Quantile(quantile);
        Some(match field {
            MetricField::CpuCores => self.cpu_quantile.query_key(parts, &query),
            MetricField::MemoryGb => self.mem_quantile.query_key(parts, &query),
            MetricField::NetworkMbps => self.net_quantile.query_key(parts, &query),
        })
    }
}

fn split_key(key: &str) -> Option<Vec<&str>> {
    let parts: Vec<&str> = key.split(';').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() { None } else { Some(parts) }
}

#[derive(Clone)]
struct CountMinPair {
    top_entities: XLCountMin<FastPath>,
    cumulative: XLCountMin<FastPath>,
    top_key: Option<String>,
    top_value: i128,
}

impl Default for CountMinPair {
    fn default() -> Self {
        Self {
            top_entities: XLCountMin::default(),
            cumulative: XLCountMin::default(),
            top_key: None,
            top_value: 0,
        }
    }
}

impl CountMinPair {
    fn update_top_entities(&mut self, key: &str, key_input: &SketchInput, value: i128) {
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

    fn update_cumulative(&mut self, key_input: &SketchInput, value: i128) {
        if value <= 0 {
            return;
        }
        self.cumulative.insert_many(key_input, value);
    }

    fn top_entity(&self) -> Option<EntityEstimate> {
        self.top_key.as_ref().map(|key| EntityEstimate {
            key: key.clone(),
            value: clamp_i128_to_i32(self.top_value),
        })
    }

    fn estimate_cumulative(&self, key_input: &SketchInput) -> i128 {
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
    fn update(&mut self, field: MetricField, key: &str, key_input: &SketchInput, value: i128) {
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

    fn cumulative_estimate(&self, field: MetricField, key_input: &SketchInput) -> i128 {
        match field {
            MetricField::CpuCores => self.cpu_cores.estimate_cumulative(key_input),
            MetricField::MemoryGb => self.memory_gb.estimate_cumulative(key_input),
            MetricField::NetworkMbps => self.network_mbps.estimate_cumulative(key_input),
        }
    }
}

struct MetricFrequencyHydra {
    cpu_frequency: Hydra,
    mem_frequency: Hydra,
    net_frequency: Hydra,
}

impl MetricFrequencyHydra {
    fn new() -> Self {
        let cm_template = HydraCounter::CM(
            CountMin::<Vector2D<i32>, FastPath>::default(),
        );

        Self {
            cpu_frequency: Hydra::with_dimensions(3, 64, cm_template.clone()),
            mem_frequency: Hydra::with_dimensions(3, 64, cm_template.clone()),
            net_frequency: Hydra::with_dimensions(3, 64, cm_template),
        }
    }

    fn update(&mut self, field: MetricField, key: &str, value: i32) {
        let input = SketchInput::I64(value as i64);
        match field {
            MetricField::CpuCores => self.cpu_frequency.update(key, &input, None),
            MetricField::MemoryGb => self.mem_frequency.update(key, &input, None),
            MetricField::NetworkMbps => self.net_frequency.update(key, &input, None),
        }
    }

    fn query_frequency(&self, field: MetricField, key: &str, value: i32) -> Option<f64> {
        let parts = split_key(key)?;
        let input = SketchInput::I64(value as i64);
        Some(match field {
            MetricField::CpuCores => self.cpu_frequency.query_frequency(parts, &input),
            MetricField::MemoryGb => self.mem_frequency.query_frequency(parts, &input),
            MetricField::NetworkMbps => self.net_frequency.query_frequency(parts, &input),
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct EntityEstimate {
    pub key: String,
    pub value: i32,
}

pub struct MetricStore {
    klls: Mutex<MetricKll>,
    countmins: Mutex<MetricCountMins>,
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
        let cdf = match field {
            MetricField::CpuCores => klls.cpu_cores.cdf(),
            MetricField::MemoryGb => klls.memory_gb.cdf(),
            MetricField::NetworkMbps => klls.network_mbps.cdf(),
        };
        Some(cdf.query(quantile))
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
        let countmins = self.countmins.lock().ok()?;
        countmins.top_entity(field)
    }

    pub fn cumulative_value(&self, field: MetricField, key: &str) -> i32 {
        let key_input = SketchInput::Str(key);
        let countmins = match self.countmins.lock() {
            Ok(guard) => guard,
            Err(_) => return 0,
        };
        clamp_i128_to_i32(countmins.cumulative_estimate(field, &key_input))
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

        let mut countmins = self
            .countmins
            .lock()
            .map_err(|_| "failed to lock countmin sketches")?;
        let mut freq_hydra = self
            .frequency_by_label
            .lock()
            .map_err(|_| "failed to lock frequency sketches")?;

        if let Some(value) = round_to_i32(cpu_value) {
            let value_i128 = value as i128;
            update_countmins_for_value(
                &mut countmins,
                MetricField::CpuCores,
                cluster,
                task,
                &key,
                value_i128,
            );
            freq_hydra.update(MetricField::CpuCores, &key, value);
        }
        if let Some(value) = round_to_i32(memory_value) {
            let value_i128 = value as i128;
            update_countmins_for_value(
                &mut countmins,
                MetricField::MemoryGb,
                cluster,
                task,
                &key,
                value_i128,
            );
            freq_hydra.update(MetricField::MemoryGb, &key, value);
        }
        if let Some(value) = round_to_i32(network_value) {
            let value_i128 = value as i128;
            update_countmins_for_value(
                &mut countmins,
                MetricField::NetworkMbps,
                cluster,
                task,
                &key,
                value_i128,
            );
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
    klls: MetricKll,
    countmins: MetricCountMins,
    frequency_hydra: MetricFrequencyHydra,
    hydra: MetricHydra,
    key_buffer: String,
}

impl MetricPreAggregation {
    pub fn new() -> Self {
        Self {
            klls: MetricKll::default(),
            countmins: MetricCountMins::default(),
            frequency_hydra: MetricFrequencyHydra::new(),
            hydra: MetricHydra::new(),
            key_buffer: String::with_capacity(128),
        }
    }

    fn update_countmins_for_value(
        &mut self,
        field: MetricField,
        cluster: &str,
        task: &str,
        value: i128,
    ) {
        let full_input = SketchInput::Str(&self.key_buffer);
        self.countmins
            .update(field, &self.key_buffer, &full_input, value);

        let cluster_input = SketchInput::Str(cluster);
        self.countmins
            .update(field, cluster, &cluster_input, value);

        let task_input = SketchInput::Str(task);
        self.countmins
            .update(field, task, &task_input, value);
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

        if let Some(value) = round_to_i32(cpu_value) {
            let value_i128 = value as i128;
            self.update_countmins_for_value(MetricField::CpuCores, cluster, task, value_i128);
            self.frequency_hydra
                .update(MetricField::CpuCores, &self.key_buffer, value);
        }
        if let Some(value) = round_to_i32(memory_value) {
            let value_i128 = value as i128;
            self.update_countmins_for_value(MetricField::MemoryGb, cluster, task, value_i128);
            self.frequency_hydra
                .update(MetricField::MemoryGb, &self.key_buffer, value);
        }
        if let Some(value) = round_to_i32(network_value) {
            let value_i128 = value as i128;
            self.update_countmins_for_value(MetricField::NetworkMbps, cluster, task, value_i128);
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
        if let Some(value) = cpu_rounded {
            self.update_countmins_for_value(MetricField::CpuCores, cluster, task, value as i128);
        }
        if let Some(value) = mem_rounded {
            self.update_countmins_for_value(MetricField::MemoryGb, cluster, task, value as i128);
        }
        if let Some(value) = net_rounded {
            self.update_countmins_for_value(MetricField::NetworkMbps, cluster, task, value as i128);
        }
        timing.countmin_ns = t4.elapsed().as_nanos() as u64;

        timing
    }

    pub fn finish(self) -> MetricStore {
        MetricStore {
            klls: Mutex::new(self.klls),
            countmins: Mutex::new(self.countmins),
            frequency_by_label: Mutex::new(self.frequency_hydra),
            quantile_by_label: Mutex::new(self.hydra),
        }
    }
}

fn update_countmins_for_value(
    countmins: &mut MetricCountMins,
    field: MetricField,
    cluster: &str,
    task: &str,
    full_key: &str,
    value: i128,
) {
    let full_input = SketchInput::Str(full_key);
    countmins.update(field, full_key, &full_input, value);

    let cluster_input = SketchInput::Str(cluster);
    countmins.update(field, cluster, &cluster_input, value);

    let task_input = SketchInput::Str(task);
    countmins.update(field, task, &task_input, value);
}

#[inline(always)]
fn clamp_frequency_estimate(value: f64) -> i32 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    if value >= i32::MAX as f64 {
        return i32::MAX;
    }
    value.round() as i32
}

#[inline(always)]
fn clamp_i128_to_i32(value: i128) -> i32 {
    if value > i32::MAX as i128 {
        i32::MAX
    } else if value < i32::MIN as i128 {
        i32::MIN
    } else {
        value as i32
    }
}

#[inline(always)]
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
