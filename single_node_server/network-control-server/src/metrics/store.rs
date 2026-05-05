use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, RwLock};

use asap_sketchlib::KLL;

use crate::config::RangeKeyCatalogConfig;

use super::MetricField;

pub trait MetricStore: Send + Sync {
    /// Insert a single sample into the store. `metrics` is a slice of `(metric_idx, value)`
    /// pairs where `metric_idx` is the metric's position in the index schema. The caller
    /// is responsible for resolving metric names to indices once per request via
    /// `metric_index`, so the hot loop avoids HashMap lookups and string allocations.
    fn insert_sample(&self, key: &str, metrics: &[(usize, f64)]) -> Result<(), String>;
    fn cumulative_value(&self, key: &str, field: &MetricField) -> Result<f64, String>;
    fn query_percentiles(
        &self,
        key: &str,
        field: &MetricField,
        percents: &[f64],
    ) -> Result<Vec<Option<f64>>, String>;
    fn clear_all(&self) -> Result<(), String>;
    fn contains_key(&self, key: &str) -> bool;
    /// Resolve a metric storage_field name to its positional index within this store's
    /// schema, or `None` if the name is not configured for this index.
    fn metric_index(&self, name: &str) -> Option<usize>;
}

pub struct RangeKeyCatalog {
    keys: Vec<String>,
}

pub struct InMemoryKeyStore {
    pub key_data: RwLock<HashMap<String, Arc<PerKeyData>>>,
    pub metric_names: Vec<String>,
    pub metric_idx: HashMap<String, usize>,
}

pub struct PerKeyData {
    /// Per-metric KLL sketch and cumulative value, indexed positionally by the metric's
    /// position in the configured schema metric list.
    pub metrics: Vec<MetricData>,
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
            metric_names: metric_names.to_vec(),
            metric_idx: build_metric_idx(metric_names),
        }
    }

    pub fn with_keys(keys: &[String], metric_names: &[String]) -> Self {
        let metric_count = metric_names.len();
        let mut seeded = HashMap::with_capacity(2 * keys.len());
        for key in keys {
            let normalized = key.trim();
            if normalized.is_empty() {
                continue;
            }
            seeded
                .entry(normalized.to_string())
                .or_insert_with(|| Arc::new(PerKeyData::new(metric_count)));
        }
        Self {
            key_data: RwLock::new(seeded),
            metric_names: metric_names.to_vec(),
            metric_idx: build_metric_idx(metric_names),
        }
    }

    fn get_or_create_key_data(&self, key: &str) -> Result<Arc<PerKeyData>, String> {
        if let Ok(guard) = self.key_data.read() {
            if let Some(existing) = guard.get(key) {
                return Ok(Arc::clone(existing));
            }
        }

        let metric_count = self.metric_names.len();
        let mut guard = self
            .key_data
            .write()
            .map_err(|_| "failed to lock key map for write".to_string())?;
        let entry = guard
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(PerKeyData::new(metric_count)));
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
    fn insert_sample(&self, key: &str, metrics: &[(usize, f64)]) -> Result<(), String> {
        let keyed_data = self.get_or_create_key_data(key)?;

        for (idx, value) in metrics {
            let metric_data = keyed_data.metrics.get(*idx).ok_or_else(|| {
                format!("metric idx {} out of range for key '{}'", idx, key)
            })?;
            {
                let mut kll = metric_data
                    .kll
                    .write()
                    .map_err(|_| format!("failed to lock kll for idx {}", idx))?;
                kll.update(value);
            }
            {
                let mut cum = metric_data
                    .cumulative
                    .write()
                    .map_err(|_| format!("failed to lock cumulative for idx {}", idx))?;
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
            .get(field.idx())
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
            .get(field.idx())
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
            for (idx, metric_data) in keyed_data.metrics.iter().enumerate() {
                {
                    let mut kll = metric_data
                        .kll
                        .write()
                        .map_err(|_| format!("failed to lock kll for idx {}", idx))?;
                    kll.clear();
                }
                {
                    let mut cum = metric_data
                        .cumulative
                        .write()
                        .map_err(|_| format!("failed to lock cumulative for idx {}", idx))?;
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

    fn metric_index(&self, name: &str) -> Option<usize> {
        self.metric_idx.get(name).copied()
    }
}

impl PerKeyData {
    fn new(metric_count: usize) -> Self {
        let mut metrics = Vec::with_capacity(metric_count);
        for _ in 0..metric_count {
            metrics.push(MetricData {
                kll: RwLock::new(KLL::default()),
                cumulative: RwLock::new(0.0),
            });
        }
        Self { metrics }
    }
}

fn build_metric_idx(metric_names: &[String]) -> HashMap<String, usize> {
    let mut map = HashMap::with_capacity(metric_names.len());
    for (idx, name) in metric_names.iter().enumerate() {
        map.insert(name.clone(), idx);
    }
    map
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
