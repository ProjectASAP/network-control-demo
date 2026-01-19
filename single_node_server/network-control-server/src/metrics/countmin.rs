use std::sync::RwLock;

use sketchlib_rust::{FastPath, XLCountMin};

use super::{EntityEstimate, MetricField};

#[inline(always)]
fn clamp_i128_to_i64(value: i128) -> i64 {
    if value > i64::MAX as i128 {
        i64::MAX
    } else if value < i64::MIN as i128 {
        i64::MIN
    } else {
        value as i64
    }
}

struct AtomicCountMin {
    inner: RwLock<XLCountMin<FastPath>>,
}

impl Default for AtomicCountMin {
    fn default() -> Self {
        Self {
            inner: RwLock::new(XLCountMin::default()),
        }
    }
}

impl AtomicCountMin {
    fn with_dimensions(rows: usize, cols: usize) -> Self {
        Self {
            inner: RwLock::new(XLCountMin::with_dimensions(rows, cols)),
        }
    }

    #[inline(always)]
    fn insert_many_with_hash(&self, hashed_val: u128, many: i128) {
        if many == 0 {
            return;
        }
        let many = i128::from(clamp_i128_to_i64(many));
        let mut inner = match self.inner.write() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
        inner.fast_insert_many_with_hash_value(hashed_val, many);
    }

    #[inline(always)]
    fn estimate_with_hash(&self, hashed_val: u128) -> i128 {
        let inner = match self.inner.read() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
        inner.fast_estimate_with_hash(hashed_val)
    }

    #[inline(always)]
    fn update_max_with_hash(&self, hashed_val: u128, next: i128) {
        let next = i128::from(clamp_i128_to_i64(next));
        let mut inner = match self.inner.write() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
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
}

#[derive(Default)]
struct TopEntityState {
    key: Option<String>,
    value: i128,
}

struct CountMinPair {
    top_entities: AtomicCountMin,
    cumulative: AtomicCountMin,
    top_state: RwLock<TopEntityState>,
}

impl Default for CountMinPair {
    fn default() -> Self {
        Self {
            top_entities: AtomicCountMin::default(),
            cumulative: AtomicCountMin::default(),
            top_state: RwLock::new(TopEntityState::default()),
        }
    }
}

impl CountMinPair {
    fn update_top_entities(&self, key: &str, key_hash: u128, value: i128) {
        if value <= 0 {
            return;
        }
        let current = self.top_entities.estimate_with_hash(key_hash);
        if value > current {
            self.top_entities.update_max_with_hash(key_hash, value);
            if let Ok(mut state) = self.top_state.write() {
                if state.key.as_deref() != Some(key) {
                    state.key = Some(key.to_string());
                }
                state.value = value;
            }
        }
    }

    fn update_cumulative(&self, key_hash: u128, value: i128) {
        if value <= 0 {
            return;
        }
        self.cumulative.insert_many_with_hash(key_hash, value);
    }

    fn top_entity(&self) -> Option<EntityEstimate> {
        let state = self.top_state.read().ok()?;
        state.key.as_ref().map(|key| EntityEstimate {
            key: key.clone(),
            value: clamp_i128_to_i32(state.value),
        })
    }

    fn estimate_cumulative(&self, key_hash: u128) -> i128 {
        self.cumulative.estimate_with_hash(key_hash)
    }
}

#[derive(Default)]
pub(super) struct MetricCountMins {
    cpu_cores: CountMinPair,
    memory_gb: CountMinPair,
    network_mbps: CountMinPair,
}

impl MetricCountMins {
    pub(super) fn update(&self, field: MetricField, key: &str, key_hash: u128, value: i128) {
        let pair = match field {
            MetricField::CpuCores => &self.cpu_cores,
            MetricField::MemoryGb => &self.memory_gb,
            MetricField::NetworkMbps => &self.network_mbps,
        };

        pair.update_top_entities(key, key_hash, value);
        pair.update_cumulative(key_hash, value);
    }

    pub(super) fn top_entity(&self, field: MetricField) -> Option<EntityEstimate> {
        match field {
            MetricField::CpuCores => self.cpu_cores.top_entity(),
            MetricField::MemoryGb => self.memory_gb.top_entity(),
            MetricField::NetworkMbps => self.network_mbps.top_entity(),
        }
    }

    pub(super) fn cumulative_estimate(&self, field: MetricField, key_hash: u128) -> i128 {
        match field {
            MetricField::CpuCores => self.cpu_cores.estimate_cumulative(key_hash),
            MetricField::MemoryGb => self.memory_gb.estimate_cumulative(key_hash),
            MetricField::NetworkMbps => self.network_mbps.estimate_cumulative(key_hash),
        }
    }
}

#[inline(always)]
pub(super) fn clamp_i128_to_i32(value: i128) -> i32 {
    if value > i32::MAX as i128 {
        i32::MAX
    } else if value < i32::MIN as i128 {
        i32::MIN
    } else {
        value as i32
    }
}
