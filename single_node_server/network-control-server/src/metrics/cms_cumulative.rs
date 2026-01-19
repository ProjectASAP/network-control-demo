use std::sync::RwLock;

use sketchlib_rust::{FastPath, XLCountMin};

use super::util::clamp_i128_to_i32;
use super::{EntityEstimate, MetricField};

#[derive(Default)]
struct TopEntityState {
    key: Option<String>,
    value: i128,
}

pub(super) struct MetricCumulativeAndTop {
    cpu_top: RwLock<XLCountMin<FastPath>>,
    cpu_cumulative: RwLock<XLCountMin<FastPath>>,
    cpu_top_state: RwLock<TopEntityState>,
    mem_top: RwLock<XLCountMin<FastPath>>,
    mem_cumulative: RwLock<XLCountMin<FastPath>>,
    mem_top_state: RwLock<TopEntityState>,
    net_top: RwLock<XLCountMin<FastPath>>,
    net_cumulative: RwLock<XLCountMin<FastPath>>,
    net_top_state: RwLock<TopEntityState>,
}

impl Default for MetricCumulativeAndTop {
    fn default() -> Self {
        Self {
            cpu_top: RwLock::new(XLCountMin::default()),
            cpu_cumulative: RwLock::new(XLCountMin::default()),
            cpu_top_state: RwLock::new(TopEntityState::default()),
            mem_top: RwLock::new(XLCountMin::default()),
            mem_cumulative: RwLock::new(XLCountMin::default()),
            mem_top_state: RwLock::new(TopEntityState::default()),
            net_top: RwLock::new(XLCountMin::default()),
            net_cumulative: RwLock::new(XLCountMin::default()),
            net_top_state: RwLock::new(TopEntityState::default()),
        }
    }
}

impl MetricCumulativeAndTop {
    pub(super) fn update(
        &self,
        key: &str,
        key_hash: u128,
        cpu_value: i128,
        mem_value: i128,
        net_value: i128,
    ) {
        let update_field = |top: &RwLock<XLCountMin<FastPath>>,
                            cumulative: &RwLock<XLCountMin<FastPath>>,
                            top_state: &RwLock<TopEntityState>,
                            value: i128| {
            if value <= 0 {
                return;
            }
            let current = estimate_with_hash(top, key_hash);
            if value > current {
                update_max_with_hash(top, key_hash, value);
                if let Ok(mut state) = top_state.write() {
                    if state.key.as_deref() != Some(key) {
                        state.key = Some(key.to_string());
                    }
                    state.value = value;
                }
            }

            insert_many_with_hash(cumulative, key_hash, value);
        };

        update_field(
            &self.cpu_top,
            &self.cpu_cumulative,
            &self.cpu_top_state,
            cpu_value,
        );
        update_field(
            &self.mem_top,
            &self.mem_cumulative,
            &self.mem_top_state,
            mem_value,
        );
        update_field(
            &self.net_top,
            &self.net_cumulative,
            &self.net_top_state,
            net_value,
        );
    }

    pub(super) fn top_entity(&self, field: MetricField) -> Option<EntityEstimate> {
        match field {
            MetricField::CpuCores => self.top_entity_for_field(&self.cpu_top_state),
            MetricField::MemoryGb => self.top_entity_for_field(&self.mem_top_state),
            MetricField::NetworkMbps => self.top_entity_for_field(&self.net_top_state),
        }
    }

    pub(super) fn cumulative_estimate(&self, field: MetricField, key_hash: u128) -> i128 {
        match field {
            MetricField::CpuCores => estimate_with_hash(&self.cpu_cumulative, key_hash),
            MetricField::MemoryGb => estimate_with_hash(&self.mem_cumulative, key_hash),
            MetricField::NetworkMbps => estimate_with_hash(&self.net_cumulative, key_hash),
        }
    }

    fn top_entity_for_field(&self, top_state: &RwLock<TopEntityState>) -> Option<EntityEstimate> {
        let state = top_state.read().ok()?;
        state.key.as_ref().map(|key| EntityEstimate {
            key: key.clone(),
            value: clamp_i128_to_i32(state.value),
        })
    }
}

#[inline(always)]
fn insert_many_with_hash(inner: &RwLock<XLCountMin<FastPath>>, hashed_val: u128, many: i128) {
    if many == 0 {
        return;
    }
    let mut inner = match inner.write() {
        Ok(inner) => inner,
        Err(poisoned) => poisoned.into_inner(),
    };
    inner.fast_insert_many_with_hash_value(hashed_val, many);
}

#[inline(always)]
fn estimate_with_hash(inner: &RwLock<XLCountMin<FastPath>>, hashed_val: u128) -> i128 {
    let inner = match inner.read() {
        Ok(inner) => inner,
        Err(poisoned) => poisoned.into_inner(),
    };
    inner.fast_estimate_with_hash(hashed_val)
}

#[inline(always)]
fn update_max_with_hash(inner: &RwLock<XLCountMin<FastPath>>, hashed_val: u128, next: i128) {
    let mut inner = match inner.write() {
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
