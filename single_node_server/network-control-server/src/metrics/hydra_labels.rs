use sketchlib_rust::{
    KLL, SketchInput,
    common::input::{HydraCounter, HydraQuery},
    hydra::MultiHeadHydra,
};

use super::MetricField;
use super::key::split_key;

const CPU_QUANTILE_DIM: &str = "cpu_cores_quantile";
const MEM_QUANTILE_DIM: &str = "memory_gb_quantile";
const NET_QUANTILE_DIM: &str = "network_mbps_quantile";

pub(super) struct MetricHydra {
    hydra: MultiHeadHydra,
}

impl MetricHydra {
    pub(super) fn new() -> Self {
        let kll_template = HydraCounter::KLL(KLL::default());
        let dimensions = vec![
            (CPU_QUANTILE_DIM.to_string(), kll_template.clone()),
            (MEM_QUANTILE_DIM.to_string(), kll_template.clone()),
            (NET_QUANTILE_DIM.to_string(), kll_template.clone()),
        ];

        Self {
            hydra: MultiHeadHydra::with_dimensions(3, 64, dimensions),
        }
    }

    pub(super) fn update(
        &mut self,
        key: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) {
        self.update_with_count(key, cpu_value, memory_value, network_value, None);
    }

    pub(super) fn update_with_count(
        &mut self,
        key: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
        count: Option<i32>,
    ) {
        let cpu_quantile = SketchInput::F64(cpu_value);
        let mem_quantile = SketchInput::F64(memory_value);
        let net_quantile = SketchInput::F64(network_value);

        let cpu_quantile_dims = [CPU_QUANTILE_DIM];
        let mem_quantile_dims = [MEM_QUANTILE_DIM];
        let net_quantile_dims = [NET_QUANTILE_DIM];

        let mut values: Vec<(&SketchInput, &[&str])> = Vec::with_capacity(3);
        values.push((&cpu_quantile, &cpu_quantile_dims));
        values.push((&mem_quantile, &mem_quantile_dims));
        values.push((&net_quantile, &net_quantile_dims));

        self.hydra.update(key, &values, count);
    }

    pub(super) fn query_quantile(
        &self,
        field: MetricField,
        key: &str,
        quantile: f64,
    ) -> Option<f64> {
        let parts = split_key(key)?;
        let query = HydraQuery::Quantile(quantile);
        let dimension = match field {
            MetricField::CpuCores => CPU_QUANTILE_DIM,
            MetricField::MemoryGb => MEM_QUANTILE_DIM,
            MetricField::NetworkMbps => NET_QUANTILE_DIM,
        };
        Some(self.hydra.query_key(parts, dimension, &query))
    }

    // Frequency queries are intentionally unsupported here.
}
