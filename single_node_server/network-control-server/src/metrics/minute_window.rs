use std::collections::HashMap;

use sketchlib_rust::{
    FastPath, KLL, SketchInput, XLCountMin,
    common::input::{HydraCounter, HydraQuery},
    hydra::MultiHeadHydra,
};

use super::{EntityEstimate, MetricField};
use super::key::{hash_key_128, split_key};
use super::util::{clamp_i128_to_i32, round_to_i32};

const BUCKET_COUNT: usize = 60;
const BUCKET_MS: u64 = 60_000;

#[derive(Clone, Debug, Default)]
struct TopEntityState {
    key: Option<String>,
    value: i128,
}

#[derive(Clone, Debug)]
struct MetricCumulativeSketch {
    cpu_top: XLCountMin<FastPath>,
    cpu_cumulative: XLCountMin<FastPath>,
    cpu_top_state: TopEntityState,
    mem_top: XLCountMin<FastPath>,
    mem_cumulative: XLCountMin<FastPath>,
    mem_top_state: TopEntityState,
    net_top: XLCountMin<FastPath>,
    net_cumulative: XLCountMin<FastPath>,
    net_top_state: TopEntityState,
}

impl Default for MetricCumulativeSketch {
    fn default() -> Self {
        Self {
            cpu_top: XLCountMin::default(),
            cpu_cumulative: XLCountMin::default(),
            cpu_top_state: TopEntityState::default(),
            mem_top: XLCountMin::default(),
            mem_cumulative: XLCountMin::default(),
            mem_top_state: TopEntityState::default(),
            net_top: XLCountMin::default(),
            net_cumulative: XLCountMin::default(),
            net_top_state: TopEntityState::default(),
        }
    }
}

impl MetricCumulativeSketch {
    fn update(
        &mut self,
        cluster: &str,
        task: &str,
        full_key: &str,
        cpu_value: i128,
        mem_value: i128,
        net_value: i128,
    ) {
        if cpu_value <= 0 && mem_value <= 0 && net_value <= 0 {
            return;
        }

        let full_hash = hash_key_128(full_key);
        {
            let (top, cumulative, state) =
                (&mut self.cpu_top, &mut self.cpu_cumulative, &mut self.cpu_top_state);
            update_field(top, cumulative, state, full_key, full_hash, cpu_value);
        }
        {
            let (top, cumulative, state) =
                (&mut self.mem_top, &mut self.mem_cumulative, &mut self.mem_top_state);
            update_field(top, cumulative, state, full_key, full_hash, mem_value);
        }
        {
            let (top, cumulative, state) =
                (&mut self.net_top, &mut self.net_cumulative, &mut self.net_top_state);
            update_field(top, cumulative, state, full_key, full_hash, net_value);
        }

        let cluster_hash = hash_key_128(cluster);
        {
            let (top, cumulative, state) =
                (&mut self.cpu_top, &mut self.cpu_cumulative, &mut self.cpu_top_state);
            update_field(top, cumulative, state, cluster, cluster_hash, cpu_value);
        }
        {
            let (top, cumulative, state) =
                (&mut self.mem_top, &mut self.mem_cumulative, &mut self.mem_top_state);
            update_field(top, cumulative, state, cluster, cluster_hash, mem_value);
        }
        {
            let (top, cumulative, state) =
                (&mut self.net_top, &mut self.net_cumulative, &mut self.net_top_state);
            update_field(top, cumulative, state, cluster, cluster_hash, net_value);
        }

        let task_hash = hash_key_128(task);
        {
            let (top, cumulative, state) =
                (&mut self.cpu_top, &mut self.cpu_cumulative, &mut self.cpu_top_state);
            update_field(top, cumulative, state, task, task_hash, cpu_value);
        }
        {
            let (top, cumulative, state) =
                (&mut self.mem_top, &mut self.mem_cumulative, &mut self.mem_top_state);
            update_field(top, cumulative, state, task, task_hash, mem_value);
        }
        {
            let (top, cumulative, state) =
                (&mut self.net_top, &mut self.net_cumulative, &mut self.net_top_state);
            update_field(top, cumulative, state, task, task_hash, net_value);
        }
    }

    fn merge(&mut self, other: &Self) {
        self.cpu_top.merge(&other.cpu_top);
        self.cpu_cumulative.merge(&other.cpu_cumulative);
        self.mem_top.merge(&other.mem_top);
        self.mem_cumulative.merge(&other.mem_cumulative);
        self.net_top.merge(&other.net_top);
        self.net_cumulative.merge(&other.net_cumulative);

        merge_top_state(&self.cpu_top, &mut self.cpu_top_state, &other.cpu_top_state);
        merge_top_state(&self.mem_top, &mut self.mem_top_state, &other.mem_top_state);
        merge_top_state(&self.net_top, &mut self.net_top_state, &other.net_top_state);
    }

    fn top_entity(&self, field: MetricField) -> Option<EntityEstimate> {
        let state = match field {
            MetricField::CpuCores => &self.cpu_top_state,
            MetricField::MemoryGb => &self.mem_top_state,
            MetricField::NetworkMbps => &self.net_top_state,
        };
        state.key.as_ref().map(|key| EntityEstimate {
            key: key.clone(),
            value: clamp_i128_to_i32(state.value),
        })
    }

    fn cumulative_estimate(&self, field: MetricField, key_hash: u128) -> i128 {
        match field {
            MetricField::CpuCores => estimate_with_hash(&self.cpu_cumulative, key_hash),
            MetricField::MemoryGb => estimate_with_hash(&self.mem_cumulative, key_hash),
            MetricField::NetworkMbps => estimate_with_hash(&self.net_cumulative, key_hash),
        }
    }
}

#[derive(Clone, Debug)]
struct HydraSketch {
    hydra: MultiHeadHydra,
}

impl HydraSketch {
    fn new() -> Self {
        let kll_template = HydraCounter::KLL(KLL::default());
        let dimensions = vec![
            ("cpu_cores_quantile".to_string(), kll_template.clone()),
            ("memory_gb_quantile".to_string(), kll_template.clone()),
            ("network_mbps_quantile".to_string(), kll_template.clone()),
        ];

        Self {
            hydra: MultiHeadHydra::with_dimensions(3, 64, dimensions),
        }
    }

    fn update(&mut self, key: &str, cpu_value: f64, memory_value: f64, network_value: f64) {
        let cpu_quantile = SketchInput::F64(cpu_value);
        let mem_quantile = SketchInput::F64(memory_value);
        let net_quantile = SketchInput::F64(network_value);

        let cpu_quantile_dims = ["cpu_cores_quantile"];
        let mem_quantile_dims = ["memory_gb_quantile"];
        let net_quantile_dims = ["network_mbps_quantile"];

        let mut values: Vec<(&SketchInput, &[&str])> = Vec::with_capacity(3);
        values.push((&cpu_quantile, &cpu_quantile_dims));
        values.push((&mem_quantile, &mem_quantile_dims));
        values.push((&net_quantile, &net_quantile_dims));

        self.hydra.update(key, &values, None);
    }

    fn merge(&mut self, other: &Self) -> Result<(), String> {
        self.hydra.merge(&other.hydra)
    }

    fn query_quantile(&self, field: MetricField, key: &str, quantile: f64) -> Option<f64> {
        let parts = split_key(key)?;
        let query = HydraQuery::Quantile(quantile);
        let dimension = match field {
            MetricField::CpuCores => "cpu_cores_quantile",
            MetricField::MemoryGb => "memory_gb_quantile",
            MetricField::NetworkMbps => "network_mbps_quantile",
        };
        Some(self.hydra.query_key(parts, dimension, &query))
    }
}

#[derive(Clone, Debug)]
struct MinuteBucket {
    minute: u64,
    cpu_kll: KLL,
    mem_kll: KLL,
    net_kll: KLL,
    hydra: HydraSketch,
    cumulative: MetricCumulativeSketch,
}

impl MinuteBucket {
    fn new(minute: u64) -> Self {
        Self {
            minute,
            cpu_kll: KLL::default(),
            mem_kll: KLL::default(),
            net_kll: KLL::default(),
            hydra: HydraSketch::new(),
            cumulative: MetricCumulativeSketch::default(),
        }
    }

    fn update(
        &mut self,
        cluster: &str,
        task: &str,
        full_key: &str,
        cpu_value: f64,
        mem_value: f64,
        net_value: f64,
    ) {
        let _ = self.cpu_kll.update(&SketchInput::F64(cpu_value));
        let _ = self.mem_kll.update(&SketchInput::F64(mem_value));
        let _ = self.net_kll.update(&SketchInput::F64(net_value));
        self.hydra.update(full_key, cpu_value, mem_value, net_value);

        let cpu_rounded = round_to_i32(cpu_value).map(|value| value as i128).unwrap_or(0);
        let mem_rounded = round_to_i32(mem_value).map(|value| value as i128).unwrap_or(0);
        let net_rounded = round_to_i32(net_value).map(|value| value as i128).unwrap_or(0);
        self.cumulative
            .update(cluster, task, full_key, cpu_rounded, mem_rounded, net_rounded);
    }
}

#[derive(Clone, Debug)]
pub(super) struct MetricMinuteWindow {
    buckets: HashMap<u64, MinuteBucket>,
}

impl Default for MetricMinuteWindow {
    fn default() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }
}

impl MetricMinuteWindow {
    pub(super) fn insert_range(
        &mut self,
        start_time_ms: u64,
        end_time_ms: u64,
        cluster: &str,
        task: &str,
        full_key: &str,
        cpu_value: f64,
        mem_value: f64,
        net_value: f64,
    ) {
        let (start_ms, end_ms) = if end_time_ms >= start_time_ms {
            (start_time_ms, end_time_ms)
        } else {
            (end_time_ms, start_time_ms)
        };
        let start_min = start_ms / BUCKET_MS;
        let end_min = end_ms / BUCKET_MS;
        let window_start = end_min.saturating_sub((BUCKET_COUNT - 1) as u64);
        let effective_start = start_min.max(window_start);

        for minute in effective_start..=end_min {
            self.insert_minute(
                minute,
                cluster,
                task,
                full_key,
                cpu_value,
                mem_value,
                net_value,
            );
        }
    }

    pub(super) fn query_percentiles(
        &mut self,
        field: MetricField,
        percents: &[f64],
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<Vec<Option<f64>>> {
        let (start_min, end_min) = resolve_time_range_minutes(current_time_ms, time_range_ms);
        let effective_start = start_min.max(end_min.saturating_sub((BUCKET_COUNT - 1) as u64));
        self.cleanup(end_min);
        let mut merged = KLL::default();
        let mut seen = false;
        for minute in effective_start..=end_min {
            if let Some(bucket) = self.bucket_for_minute(minute) {
                seen = true;
                match field {
                    MetricField::CpuCores => merged.merge(&bucket.cpu_kll),
                    MetricField::MemoryGb => merged.merge(&bucket.mem_kll),
                    MetricField::NetworkMbps => merged.merge(&bucket.net_kll),
                }
            }
        }
        if !seen {
            return None;
        }
        Some(
            percents
                .iter()
                .map(|percent| {
                    if !(0.0..=100.0).contains(percent) {
                        None
                    } else {
                        Some(merged.quantile(percent / 100.0))
                    }
                })
                .collect(),
        )
    }

    pub(super) fn query_percentiles_by_key(
        &mut self,
        field: MetricField,
        key: &str,
        percents: &[f64],
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<Vec<Option<f64>>> {
        let (start_min, end_min) = resolve_time_range_minutes(current_time_ms, time_range_ms);
        let effective_start = start_min.max(end_min.saturating_sub((BUCKET_COUNT - 1) as u64));
        self.cleanup(end_min);
        let mut merged = HydraSketch::new();
        let mut seen = false;
        for minute in effective_start..=end_min {
            if let Some(bucket) = self.bucket_for_minute(minute) {
                seen = true;
                let _ = merged.merge(&bucket.hydra);
            }
        }
        if !seen {
            return None;
        }
        Some(
            percents
                .iter()
                .map(|percent| {
                    if !(0.0..=100.0).contains(percent) {
                        None
                    } else {
                        merged.query_quantile(field, key, percent / 100.0)
                    }
                })
                .collect(),
        )
    }

    pub(super) fn cumulative_value(
        &mut self,
        field: MetricField,
        key: &str,
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<i32> {
        let (start_min, end_min) = resolve_time_range_minutes(current_time_ms, time_range_ms);
        let effective_start = start_min.max(end_min.saturating_sub((BUCKET_COUNT - 1) as u64));
        self.cleanup(end_min);
        let mut merged = MetricCumulativeSketch::default();
        let mut seen = false;
        for minute in effective_start..=end_min {
            if let Some(bucket) = self.bucket_for_minute(minute) {
                seen = true;
                merged.merge(&bucket.cumulative);
            }
        }
        if !seen {
            return None;
        }
        let value = merged.cumulative_estimate(field, hash_key_128(key));
        Some(clamp_i128_to_i32(value))
    }

    pub(super) fn top_entity(
        &mut self,
        field: MetricField,
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<EntityEstimate> {
        let (start_min, end_min) = resolve_time_range_minutes(current_time_ms, time_range_ms);
        let effective_start = start_min.max(end_min.saturating_sub((BUCKET_COUNT - 1) as u64));
        self.cleanup(end_min);
        let mut merged = MetricCumulativeSketch::default();
        let mut seen = false;
        for minute in effective_start..=end_min {
            if let Some(bucket) = self.bucket_for_minute(minute) {
                seen = true;
                merged.merge(&bucket.cumulative);
            }
        }
        if !seen {
            return None;
        }
        merged.top_entity(field)
    }

    fn insert_minute(
        &mut self,
        minute: u64,
        cluster: &str,
        task: &str,
        full_key: &str,
        cpu_value: f64,
        mem_value: f64,
        net_value: f64,
    ) {
        let bucket = self
            .buckets
            .entry(minute)
            .or_insert_with(|| MinuteBucket::new(minute));
        bucket.update(cluster, task, full_key, cpu_value, mem_value, net_value);
    }

    fn bucket_for_minute(&self, minute: u64) -> Option<&MinuteBucket> {
        self.buckets.get(&minute)
    }

    fn cleanup(&mut self, current_minute: u64) {
        let cutoff = current_minute.saturating_sub((BUCKET_COUNT - 1) as u64);
        self.buckets.retain(|minute, _| *minute >= cutoff);
    }
}

fn resolve_time_range_minutes(current_time_ms: u64, time_range_ms: u64) -> (u64, u64) {
    let end_ms = current_time_ms;
    let start_ms = current_time_ms.saturating_sub(time_range_ms);
    let start_min = start_ms / BUCKET_MS;
    let end_min = end_ms / BUCKET_MS;
    (start_min, end_min)
}

fn update_field(
    top: &mut XLCountMin<FastPath>,
    cumulative: &mut XLCountMin<FastPath>,
    top_state: &mut TopEntityState,
    key: &str,
    key_hash: u128,
    value: i128,
) {
    if value <= 0 {
        return;
    }
    let current = estimate_with_hash(top, key_hash);
    if value > current {
        update_max_with_hash(top, key_hash, value);
        if top_state.key.as_deref() != Some(key) {
            top_state.key = Some(key.to_string());
        }
        top_state.value = value;
    }

    insert_many_with_hash(cumulative, key_hash, value);
}

fn merge_top_state(
    top: &XLCountMin<FastPath>,
    state: &mut TopEntityState,
    other: &TopEntityState,
) {
    if let Some(key) = other.key.as_deref() {
        let estimate = estimate_with_hash(top, hash_key_128(key));
        if estimate > state.value {
            state.key = Some(key.to_string());
            state.value = estimate;
        }
    }
}

#[inline(always)]
fn insert_many_with_hash(inner: &mut XLCountMin<FastPath>, hashed_val: u128, many: i128) {
    if many == 0 {
        return;
    }
    inner.fast_insert_many_with_hash_value(hashed_val, many);
}

#[inline(always)]
fn estimate_with_hash(inner: &XLCountMin<FastPath>, hashed_val: u128) -> i128 {
    inner.fast_estimate_with_hash(hashed_val)
}

#[inline(always)]
fn update_max_with_hash(inner: &mut XLCountMin<FastPath>, hashed_val: u128, next: i128) {
    inner.as_storage_mut().fast_insert(
        |counter, value, _| {
            if *value > *counter {
                *counter = *value;
            }
        },
        next,
        hashed_val,
    );
}
