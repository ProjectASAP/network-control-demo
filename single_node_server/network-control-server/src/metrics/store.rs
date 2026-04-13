use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, RwLock};

use asap_sketchlib::{KLL, SketchInput};

use crate::config::RangeKeyCatalogConfig;

use super::MetricField;

pub trait MetricStore: Send + Sync {
    fn insert_sample(&self, key: &str, metrics: &HashMap<String, f64>) -> Result<(), String>;
    fn cumulative_value(&self, key: &str, field: &MetricField) -> Result<f64, String>;
    fn query_percentiles(
        &self,
        key: &str,
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
    pub key_data: RwLock<HashMap<String, Arc<PerKeyData>>>,
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
    pub fn from_config(
        config: &RangeKeyCatalogConfig,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let start = config.start;
        let end = config.end;
        if end < start {
            return Err(format!("key range end before start: {}..{}", start, end).into());
        }

        let mut keys = Vec::with_capacity((end - start + 1) as usize);
        for idx in start..=end {
            keys.push(format_key_index(&config.format, idx)?);
        }

        Ok(Self { keys })
    }

    pub fn keys(&self) -> Vec<String> {
        self.keys.clone()
    }
}

fn format_key_index(format: &str, idx: u32) -> Result<String, Box<dyn Error + Send + Sync>> {
    if let Some((prefix, suffix)) = format.split_once("{}") {
        if prefix.contains('{')
            || prefix.contains('}')
            || suffix.contains('{')
            || suffix.contains('}')
        {
            return Err("range key format supports exactly one placeholder".into());
        }
        return Ok(format!("{prefix}{idx}{suffix}"));
    }

    let Some(open) = format.find("{:0") else {
        return Err("range key format must contain {} or {:0N} placeholder".into());
    };
    let width_start = open + 3;
    let Some(close_rel) = format[width_start..].find('}') else {
        return Err("range key format has '{' without matching '}'".into());
    };
    let close = width_start + close_rel;

    if format[close + 1..].contains('{') {
        return Err("range key format supports exactly one placeholder".into());
    }

    let width_digits = &format[width_start..close];
    if width_digits.is_empty() || !width_digits.chars().all(|ch| ch.is_ascii_digit()) {
        return Err("range key format width must be numeric, e.g. {:03}".into());
    }
    let width: usize = width_digits.parse()?;
    let prefix = &format[..open];
    let suffix = &format[close + 1..];
    Ok(format!("{prefix}{:0width$}{suffix}", idx, width = width))
}

impl InMemoryKeyStore {
    pub fn new(metric_names: &[String]) -> Self {
        Self {
            key_data: RwLock::new(HashMap::new()),
            allowed_metrics: metric_names.to_vec(),
        }
    }

    pub fn with_keys(keys: &[String], metric_names: &[String]) -> Self {
        let mut seeded = HashMap::with_capacity(2 * keys.len());
        for key in keys {
            let normalized = key.trim();
            if normalized.is_empty() {
                continue;
            }
            seeded
                .entry(normalized.to_string())
                .or_insert_with(|| Arc::new(PerKeyData::new(metric_names)));
        }
        Self {
            key_data: RwLock::new(seeded),
            allowed_metrics: metric_names.to_vec(),
        }
    }

    fn get_or_create_key_data(&self, key: &str) -> Result<Arc<PerKeyData>, String> {
        if let Ok(guard) = self.key_data.read() {
            if let Some(existing) = guard.get(key) {
                return Ok(Arc::clone(existing));
            }
        }

        let mut guard = self
            .key_data
            .write()
            .map_err(|_| "failed to lock key map for write".to_string())?;
        let entry = guard
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(PerKeyData::new(&self.allowed_metrics)));
        Ok(Arc::clone(entry))
    }

    fn get_key_data(&self, key: &str) -> Result<Option<Arc<PerKeyData>>, String> {
        let guard = self
            .key_data
            .read()
            .map_err(|_| "failed to lock key map for read".to_string())?;
        Ok(guard.get(key).cloned())
    }
}

impl MetricStore for InMemoryKeyStore {
    fn insert_sample(&self, key: &str, metrics: &HashMap<String, f64>) -> Result<(), String> {
        let keyed_data = self.get_or_create_key_data(key)?;

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
            .get_key_data(key)?
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
        key: &str,
        field: &MetricField,
        percents: &[f64],
    ) -> Result<Vec<Option<f64>>, String> {
        let keyed_data = self
            .get_key_data(key)?
            .ok_or_else(|| format!("quantile statistics for key '{}' not found", key))?;
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
        let guard = self
            .key_data
            .read()
            .map_err(|_| "failed to lock key map for read".to_string())?;
        for keyed_data in guard.values() {
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
        self.key_data
            .read()
            .map(|guard| guard.contains_key(key))
            .unwrap_or(false)
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

#[cfg(test)]
mod tests {
    use super::format_key_index;

    #[test]
    fn formats_simple_and_zero_padded_indices() {
        assert_eq!(format_key_index("N{}", 7).unwrap(), "N7");
        assert_eq!(format_key_index("N{:03}", 7).unwrap(), "N007");
        assert_eq!(
            format_key_index("cluster-{:02}-x", 12).unwrap(),
            "cluster-12-x"
        );
    }

    #[test]
    fn rejects_invalid_format_templates() {
        assert!(format_key_index("N", 7).is_err());
        assert!(format_key_index("N{abc}", 7).is_err());
        assert!(format_key_index("N{:x}", 7).is_err());
        assert!(format_key_index("N{}-{}", 7).is_err());
    }
}
