use sketchlib_rust::{KLL, SketchInput};
use std::collections::HashMap;
use std::error::Error;
use std::sync::RwLock;

use crate::config::NodesConfig;
use crate::metrics::MetricField;

pub struct NodeStore {
    pub nodes: HashMap<String, NodeData>,
}

pub struct NodeData {
    pub cpu_kll: RwLock<KLL>,
    pub mem_kll: RwLock<KLL>,
    pub net_kll: RwLock<KLL>,
    pub cpu_cumulative: RwLock<f64>,
    pub mem_cumulative: RwLock<f64>,
    pub net_cumulative: RwLock<f64>,
}

impl NodeStore {
    pub fn from_config(config: NodesConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let count = config.nodes.count;
        let range = config.nodes.range;
        let (prefix, start_num, width) = split_node_id(&range.start)?;
        let (end_prefix, end_num, end_width) = split_node_id(&range.end)?;

        if prefix != end_prefix {
            return Err(format!(
                "node id prefixes do not match: '{}' vs '{}'",
                prefix, end_prefix
            )
            .into());
        }
        if width != end_width {
            return Err(format!("node id width does not match: {} vs {}", width, end_width).into());
        }
        if end_num < start_num {
            return Err(format!("node range end before start: {}..{}", start_num, end_num).into());
        }

        let expected = (end_num - start_num + 1) as usize;
        if count != expected {
            return Err(format!(
                "node count {} does not match range size {}",
                count, expected
            )
            .into());
        }

        let mut nodes = HashMap::with_capacity(count);
        for num in start_num..=end_num {
            let id = format!("{prefix}{:0width$}", num, width = width);
            nodes.insert(id, NodeData::new());
        }

        Ok(Self { nodes })
    }

    pub fn insert_sample(
        &self,
        node_id: &str,
        cpu_value: f64,
        mem_value: f64,
        net_value: f64,
    ) -> Result<(), String> {
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("node id '{}' not found", node_id))?;

        {
            let mut cpu = node.cpu_kll.write().map_err(|_| "failed to lock cpu kll")?;
            cpu.update(&SketchInput::F64(cpu_value))
                .map_err(|_| "cpu values should be numeric")?;
        }
        {
            let mut mem = node.mem_kll.write().map_err(|_| "failed to lock mem kll")?;
            mem.update(&SketchInput::F64(mem_value))
                .map_err(|_| "mem values should be numeric")?;
        }
        {
            let mut net = node.net_kll.write().map_err(|_| "failed to lock net kll")?;
            net.update(&SketchInput::F64(net_value))
                .map_err(|_| "net values should be numeric")?;
        }
        {
            let mut cpu = node
                .cpu_cumulative
                .write()
                .map_err(|_| "failed to lock cpu cumulative")?;
            *cpu += cpu_value;
        }
        {
            let mut mem = node
                .mem_cumulative
                .write()
                .map_err(|_| "failed to lock mem cumulative")?;
            *mem += mem_value;
        }
        {
            let mut net = node
                .net_cumulative
                .write()
                .map_err(|_| "failed to lock net cumulative")?;
            *net += net_value;
        }

        Ok(())
    }

    pub fn cumulative_value(&self, node_id: &str, field: MetricField) -> Result<f64, String> {
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("node id '{}' not found", node_id))?;
        let value = match field {
            MetricField::CpuCores => node
                .cpu_cumulative
                .read()
                .map_err(|_| "failed to lock cpu cumulative")?,
            MetricField::MemoryGb => node
                .mem_cumulative
                .read()
                .map_err(|_| "failed to lock mem cumulative")?,
            MetricField::NetworkMbps => node
                .net_cumulative
                .read()
                .map_err(|_| "failed to lock net cumulative")?,
        };
        Ok(*value)
    }

    pub fn query_percentiles(
        &self,
        node_id: &str,
        field: MetricField,
        percents: &[f64],
    ) -> Result<Vec<Option<f64>>, String> {
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("node id '{}' not found", node_id))?;
        let kll = match field {
            MetricField::CpuCores => node.cpu_kll.read().map_err(|_| "failed to lock cpu kll")?,
            MetricField::MemoryGb => node.mem_kll.read().map_err(|_| "failed to lock mem kll")?,
            MetricField::NetworkMbps => {
                node.net_kll.read().map_err(|_| "failed to lock net kll")?
            }
        };
        let mut results = Vec::with_capacity(percents.len());
        for percent in percents {
            if !(0.0..=100.0).contains(percent) {
                results.push(None);
                continue;
            }
            results.push(Some(kll.quantile(*percent / 100.0)));
        }
        Ok(results)
    }
}

impl NodeData {
    fn new() -> Self {
        Self {
            cpu_kll: RwLock::new(KLL::default()),
            mem_kll: RwLock::new(KLL::default()),
            net_kll: RwLock::new(KLL::default()),
            cpu_cumulative: RwLock::new(0.0),
            mem_cumulative: RwLock::new(0.0),
            net_cumulative: RwLock::new(0.0),
        }
    }
}

fn split_node_id(id: &str) -> Result<(String, u32, usize), Box<dyn Error + Send + Sync>> {
    let mut digit_idx = None;
    for (idx, ch) in id.char_indices() {
        if ch.is_ascii_digit() {
            digit_idx = Some(idx);
            break;
        }
    }
    let digit_idx = digit_idx.ok_or_else(|| format!("node id '{id}' has no digits"))?;
    let (prefix, number_str) = id.split_at(digit_idx);
    if number_str.is_empty() {
        return Err(format!("node id '{id}' missing numeric suffix").into());
    }
    if !number_str.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("node id '{id}' has non-numeric suffix").into());
    }
    let number: u32 = number_str.parse()?;
    Ok((prefix.to_string(), number, number_str.len()))
}
