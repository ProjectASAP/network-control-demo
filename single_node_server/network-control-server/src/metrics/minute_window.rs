use sketchlib_rust::{CountMin, FastPath, MatrixHashType, Vector2D};

type XLCountMin = CountMin<Vector2D<i128>, FastPath>;

use super::{EntityEstimate, MetricField};
use super::key::hash_key_128;
use super::util::{clamp_i128_to_i32, round_to_i32};

const BUCKET_COUNT: usize = 100;
const BUCKET_MS: u64 = 60_000;

#[derive(Clone, Debug)]
struct BucketCumulative {
    cpu: XLCountMin,
    mem: XLCountMin,
    net: XLCountMin,
}

impl Default for BucketCumulative {
    fn default() -> Self {
        Self {
            cpu: XLCountMin::default(),
            mem: XLCountMin::default(),
            net: XLCountMin::default(),
        }
    }
}

impl BucketCumulative {
    fn update(&mut self, cluster: &str, cpu_value: i128, mem_value: i128, net_value: i128) {
        if cpu_value <= 0 && mem_value <= 0 && net_value <= 0 {
            return;
        }
        let cluster_hash = hash_key_128(cluster);
        if cpu_value > 0 {
            insert_many_with_hash(&mut self.cpu, cluster_hash, cpu_value);
        }
        if mem_value > 0 {
            insert_many_with_hash(&mut self.mem, cluster_hash, mem_value);
        }
        if net_value > 0 {
            insert_many_with_hash(&mut self.net, cluster_hash, net_value);
        }
    }

    fn merge(&mut self, other: &Self) {
        self.cpu.merge(&other.cpu);
        self.mem.merge(&other.mem);
        self.net.merge(&other.net);
    }

    fn cumulative_estimate(&self, field: MetricField, key_hash: u128) -> i128 {
        match field {
            MetricField::CpuCores => estimate_with_hash(&self.cpu, key_hash),
            MetricField::MemoryGb => estimate_with_hash(&self.mem, key_hash),
            MetricField::NetworkMbps => estimate_with_hash(&self.net, key_hash),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct MetricMinuteWindow {
    buckets: Vec<BucketSlot>,
}

#[derive(Clone, Debug, Default)]
struct BucketSlot {
    bucket: BucketCumulative,
}

impl Default for MetricMinuteWindow {
    fn default() -> Self {
        let mut buckets = Vec::with_capacity(BUCKET_COUNT);
        for _ in 0..BUCKET_COUNT {
            buckets.push(BucketSlot::default());
        }
        Self { buckets }
    }
}

impl MetricMinuteWindow {
    pub(super) fn insert_range(
        &mut self,
        start_time_ms: u64,
        end_time_ms: u64,
        cluster: &str,
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
        let span = end_min.saturating_sub(start_min);
        let cpu_rounded = round_to_i32(cpu_value).map(|value| value as i128).unwrap_or(0);
        let mem_rounded = round_to_i32(mem_value).map(|value| value as i128).unwrap_or(0);
        let net_rounded = round_to_i32(net_value).map(|value| value as i128).unwrap_or(0);

        // Assumption: timestamps progress without large gaps; we only clear
        // the opposite epoch at boundaries (index 0 or 50).
        if span >= BUCKET_COUNT as u64 {
            for slot in &mut self.buckets {
                slot.bucket
                    .update(cluster, cpu_rounded, mem_rounded, net_rounded);
            }
            return;
        }

        for minute in start_min..=end_min {
            let idx = (minute % BUCKET_COUNT as u64) as usize;
            if idx == 0 {
                self.clear_epoch(50, 99);
            } else if idx == 50 {
                self.clear_epoch(0, 49);
            }
            self.buckets[idx]
                .bucket
                .update(cluster, cpu_rounded, mem_rounded, net_rounded);
        }
    }

    pub(super) fn query_percentiles(
        &mut self,
        field: MetricField,
        percents: &[f64],
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<Vec<Option<f64>>> {
        let _ = (field, percents, current_time_ms, time_range_ms);
        None
    }

    pub(super) fn query_percentiles_by_key(
        &mut self,
        field: MetricField,
        key: &str,
        percents: &[f64],
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<Vec<Option<f64>>> {
        let _ = (field, key, percents, current_time_ms, time_range_ms);
        None
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
        let mut merged = BucketCumulative::default();
        for minute in effective_start..=end_min {
            let idx = (minute % BUCKET_COUNT as u64) as usize;
            merged.merge(&self.buckets[idx].bucket);
        }
        let value = merged.cumulative_estimate(field, hash_key_128(key));
        Some(clamp_i128_to_i32(value))
    }

    pub(super) fn cumulative_value_at(
        &mut self,
        field: MetricField,
        key: &str,
        current_time_ms: u64,
    ) -> Option<i32> {
        let minute = current_time_ms / BUCKET_MS;
        let idx = (minute % BUCKET_COUNT as u64) as usize;
        let value = self.buckets[idx]
            .bucket
            .cumulative_estimate(field, hash_key_128(key));
        Some(clamp_i128_to_i32(value))
    }

    pub(super) fn top_entity(
        &mut self,
        field: MetricField,
        current_time_ms: u64,
        time_range_ms: u64,
    ) -> Option<EntityEstimate> {
        let _ = (field, current_time_ms, time_range_ms);
        None
    }
    fn clear_epoch(&mut self, start_idx: usize, end_idx: usize) {
        for idx in start_idx..=end_idx {
            self.buckets[idx] = BucketSlot::default();
        }
    }
}

fn resolve_time_range_minutes(current_time_ms: u64, time_range_ms: u64) -> (u64, u64) {
    let end_ms = current_time_ms;
    let start_ms = current_time_ms.saturating_sub(time_range_ms);
    let start_min = start_ms / BUCKET_MS;
    let end_min = end_ms / BUCKET_MS;
    (start_min, end_min)
}

#[inline(always)]
fn insert_many_with_hash(inner: &mut XLCountMin, hashed_val: u128, many: i128) {
    if many == 0 {
        return;
    }
    let hashed_val = MatrixHashType::Packed128(hashed_val);
    inner.fast_insert_many_with_hash_value(&hashed_val, many);
}

#[inline(always)]
fn estimate_with_hash(inner: &XLCountMin, hashed_val: u128) -> i128 {
    let hashed_val = MatrixHashType::Packed128(hashed_val);
    inner.fast_estimate_with_hash(&hashed_val)
}
