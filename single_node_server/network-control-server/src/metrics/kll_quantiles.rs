use sketchlib_rust::{KLL, SketchInput};
use std::sync::RwLock;

#[derive(Default)]
pub(super) struct MetricQuantiles {
    pub(super) cpu_cores: RwLock<KLL>,
    pub(super) memory_gb: RwLock<KLL>,
    pub(super) network_mbps: RwLock<KLL>,
}

impl MetricQuantiles {
    pub(super) fn insert_samples(&mut self, cpu_value: f64, memory_value: f64, network_value: f64) {
        let mut cpu = match self.cpu_cores.write() {
            Ok(c) => c,
            Err(p) => p.into_inner(),
        };
        cpu.update(&SketchInput::F64(cpu_value))
            .expect("cpu_cores values should be numeric");
        let mut mem = match self.memory_gb.write() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        mem.update(&SketchInput::F64(memory_value))
            .expect("cpu_cores values should be numeric");
        let mut net = match self.network_mbps.write() {
            Ok(n) => n,
            Err(p) => p.into_inner(),
        };
        net.update(&SketchInput::F64(network_value))
            .expect("cpu_cores values should be numeric");
    }
}
