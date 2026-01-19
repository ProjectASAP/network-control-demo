use sketchlib_rust::{
    Hydra, KLL, SketchInput,
    common::input::{HydraCounter, HydraQuery},
};

use super::MetricField;
use super::key::split_key;

#[derive(Default)]
pub(super) struct MetricQuantiles {
    pub(super) cpu_cores: KLL,
    pub(super) memory_gb: KLL,
    pub(super) network_mbps: KLL,
}

impl MetricQuantiles {
    pub(super) fn insert_samples(&mut self, cpu_value: f64, memory_value: f64, network_value: f64) {
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

// struct MetricCdfs {
//     cpu_cores: CDF,
//     memory_gb: CDF,
//     network_mbps: CDF,
// }

// impl MetricCdfs {
//     fn from_sketches(sketches: MetricKll) -> Self {
//         Self {
//             cpu_cores: sketches.cpu_cores.cdf(),
//             memory_gb: sketches.memory_gb.cdf(),
//             network_mbps: sketches.network_mbps.cdf(),
//         }
//     }

//     fn query_percentile(&self, field: MetricField, percent: f64) -> Option<f64> {
//         if !(0.0..=100.0).contains(&percent) {
//             return None;
//         }
//         let quantile = percent / 100.0;
//         match field {
//             MetricField::CpuCores => Some(self.cpu_cores.query(quantile)),
//             MetricField::MemoryGb => Some(self.memory_gb.query(quantile)),
//             MetricField::NetworkMbps => Some(self.network_mbps.query(quantile)),
//         }
//     }
// }

pub(super) struct MetricHydra {
    cpu_quantile: Hydra,
    mem_quantile: Hydra,
    net_quantile: Hydra,
}

impl MetricHydra {
    pub(super) fn new() -> Self {
        let kll_template = HydraCounter::KLL(KLL::default());

        Self {
            cpu_quantile: Hydra::with_dimensions(3, 64, kll_template.clone()),
            mem_quantile: Hydra::with_dimensions(3, 64, kll_template.clone()),
            net_quantile: Hydra::with_dimensions(3, 64, kll_template),
        }
    }

    pub(super) fn update(
        &mut self,
        key: &str,
        cpu_value: f64,
        memory_value: f64,
        network_value: f64,
    ) {
        let cpu_input = SketchInput::F64(cpu_value);
        let memory_input = SketchInput::F64(memory_value);
        let network_input = SketchInput::F64(network_value);

        self.cpu_quantile.update(key, &cpu_input, None);
        self.mem_quantile.update(key, &memory_input, None);
        self.net_quantile.update(key, &network_input, None);
    }

    pub(super) fn query_quantile(
        &self,
        field: MetricField,
        key: &str,
        quantile: f64,
    ) -> Option<f64> {
        let parts = split_key(key)?;
        let query = HydraQuery::Quantile(quantile);
        Some(match field {
            MetricField::CpuCores => self.cpu_quantile.query_key(parts, &query),
            MetricField::MemoryGb => self.mem_quantile.query_key(parts, &query),
            MetricField::NetworkMbps => self.net_quantile.query_key(parts, &query),
        })
    }
}
