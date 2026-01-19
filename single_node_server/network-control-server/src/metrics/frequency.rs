use sketchlib_rust::{
    CountMin, FastPath, Hydra, SketchInput, Vector2D, common::input::HydraCounter,
};

use super::MetricField;
use super::key::split_key;

pub(super) struct MetricFrequencyHydra {
    cpu_frequency: Hydra,
    mem_frequency: Hydra,
    net_frequency: Hydra,
}

impl MetricFrequencyHydra {
    pub(super) fn new() -> Self {
        let cm_template = HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default());

        Self {
            cpu_frequency: Hydra::with_dimensions(3, 64, cm_template.clone()),
            mem_frequency: Hydra::with_dimensions(3, 64, cm_template.clone()),
            net_frequency: Hydra::with_dimensions(3, 64, cm_template),
        }
    }

    pub(super) fn update(&mut self, field: MetricField, key: &str, value: i32) {
        let input = SketchInput::I64(value as i64);
        match field {
            MetricField::CpuCores => self.cpu_frequency.update(key, &input, None),
            MetricField::MemoryGb => self.mem_frequency.update(key, &input, None),
            MetricField::NetworkMbps => self.net_frequency.update(key, &input, None),
        }
    }

    pub(super) fn query_frequency(&self, field: MetricField, key: &str, value: i32) -> Option<f64> {
        let parts = split_key(key)?;
        let input = SketchInput::I64(value as i64);
        Some(match field {
            MetricField::CpuCores => self.cpu_frequency.query_frequency(parts, &input),
            MetricField::MemoryGb => self.mem_frequency.query_frequency(parts, &input),
            MetricField::NetworkMbps => self.net_frequency.query_frequency(parts, &input),
        })
    }
}

#[inline(always)]
pub(super) fn clamp_frequency_estimate(value: f64) -> i32 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    if value >= i32::MAX as f64 {
        return i32::MAX;
    }
    value.round() as i32
}

#[inline(always)]
pub(super) fn round_to_i32(value: f64) -> Option<i32> {
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
