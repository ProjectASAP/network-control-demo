use std::collections::HashMap;
use std::error::Error;
use std::sync::RwLock;

use asap_sketchlib::{KLL, SketchInput};

use crate::config::NodeCatalogConfig;

use super::MetricField;

pub trait KeyCatalog: Send + Sync {
    fn keys(&self) -> Vec<String>;
    fn contains(&self, key: &str) -> bool;
}

pub trait MetricStore: Send + Sync {
    fn insert_sample(
        &self,
        node_id: &str,
        metrics: &HashMap<String, f64>,
    ) -> Result<(), String>;
    fn cumulative_value(&self, node_id: &str, field: &MetricField) -> Result<f64, String>;
    fn query_percentiles(
        &self,
        node_id: &str,
        field: &MetricField,
        percents: &[f64],
    ) -> Result<Vec<Option<f64>>, String>;
    fn clear_all(&self) -> Result<(), String>;
    fn contains_key(&self, key: &str) -> bool;
}

pub struct RangeKeyCatalog {
    keys: Vec<String>,
}

pub struct InMemoryKeyStore {
    pub key_data: HashMap<String, PerKeyData>,
    pub allowed_metrics: Vec<String>,
}

pub struct PerKeyData {
    /// Per-metric KLL sketch and cumulative value, keyed by metric storage_field name.
    pub metrics: HashMap<String, MetricData>,
}

pub struct MetricData {
    pub kll: RwLock<KLL>,
    pub cumulative: RwLock<f64>,
}

impl RangeKeyCatalog {
    pub fn from_config(config: &NodeCatalogConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let count = config.count;
        let range = &config.range;
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

        let mut keys = Vec::with_capacity(count);
        for num in start_num..=end_num {
            keys.push(format!("{prefix}{:0width$}", num, width = width));
        }

        Ok(Self { keys })
    }
}

impl KeyCatalog for RangeKeyCatalog {
    fn keys(&self) -> Vec<String> {
        self.keys.clone()
    }

    fn contains(&self, key: &str) -> bool {
        self.keys.iter().any(|candidate| candidate == key)
    }
}

impl InMemoryKeyStore {
    pub fn from_catalog(catalog: &dyn KeyCatalog, metric_names: &[String]) -> Self {
        let mut nodes = HashMap::new();
        for key in catalog.keys() {
            nodes.insert(key, PerKeyData::new(metric_names));
        }
        Self { key_data: nodes, allowed_metrics: metric_names.to_vec() }
    }
}

impl MetricStore for InMemoryKeyStore {
    fn insert_sample(
        &self,
        key: &str,
        metrics: &HashMap<String, f64>,
    ) -> Result<(), String> {
        let keyed_data = self
            .key_data
            .get(key)
            .ok_or_else(|| format!("key '{}' not found in store", key))?;
        
        for (name, value) in metrics {
            let metric_data = keyed_data
                .metrics
                .get(name)
                .ok_or_else(|| format!("unknown metric '{}' for key '{}'", name, key))?;
            {
                let mut kll = metric_data
                    .kll
                    .write()
                    .map_err(|_| format!("failed to lock kll for {}", name))?;
                kll.update(&SketchInput::F64(*value))
                    .map_err(|_| format!("{} values should be numeric", name))?;
            }
            {
                let mut cum = metric_data
                    .cumulative
                    .write()
                    .map_err(|_| format!("failed to lock cumulative for {}", name))?;
                *cum += value;
            }
        }

        Ok(())
    }

    fn cumulative_value(&self, key: &str, field: &MetricField) -> Result<f64, String> {
        let keyed_data = self
            .key_data
            .get(key)
            .ok_or_else(|| format!("sum statistics for key '{}' not found", key))?;
        let metric_data = keyed_data
            .metrics
            .get(field.as_storage_field())
            .ok_or_else(|| format!("unknown metric '{}'", field.as_storage_field()))?;
        let value = metric_data
            .cumulative
            .read()
            .map_err(|_| format!("failed to lock cumulative for {}", field.as_storage_field()))?;
        Ok(*value)
    }

    fn query_percentiles(
        &self,
        node_id: &str,
        field: &MetricField,
        percents: &[f64],
    ) -> Result<Vec<Option<f64>>, String> {
        let keyed_data = self
            .key_data
            .get(node_id)
            .ok_or_else(|| format!("quantile statistics for key '{}' not found", node_id))?;
        let metric_data = keyed_data
            .metrics
            .get(field.as_storage_field())
            .ok_or_else(|| format!("unknown metric '{}'", field.as_storage_field()))?;
        let kll = metric_data
            .kll
            .read()
            .map_err(|_| format!("failed to lock kll for {}", field.as_storage_field()))?;
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

    fn clear_all(&self) -> Result<(), String> {
        for keyed_data in self.key_data.values() {
            for (name, metric_data) in &keyed_data.metrics {
                {
                    let mut kll = metric_data
                        .kll
                        .write()
                        .map_err(|_| format!("failed to lock kll for {}", name))?;
                    *kll = KLL::default();
                }
                {
                    let mut cum = metric_data
                        .cumulative
                        .write()
                        .map_err(|_| format!("failed to lock cumulative for {}", name))?;
                    *cum = 0.0;
                }
            }
        }
        Ok(())
    }

    fn contains_key(&self, key: &str) -> bool {
        self.key_data.contains_key(key)
    }
}

impl PerKeyData {
    fn new(metric_names: &[String]) -> Self {
        let mut metrics = HashMap::new();
        for name in metric_names {
            metrics.insert(
                name.clone(),
                MetricData {
                    kll: RwLock::new(KLL::default()),
                    cumulative: RwLock::new(0.0),
                },
            );
        }
        Self { metrics }
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
