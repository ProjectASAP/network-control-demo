use std::collections::HashMap;
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

pub struct InMemoryNodeStore {
    pub nodes: HashMap<String, NodeData>,
}

pub struct NodeData {
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

impl InMemoryNodeStore {
    pub fn from_catalog(catalog: &dyn KeyCatalog, metric_names: &[String]) -> Self {
        let mut nodes = HashMap::new();
        for key in catalog.keys() {
            nodes.insert(key, NodeData::new(metric_names));
        }
        Self { nodes }
    }
}

impl MetricStore for InMemoryNodeStore {
    fn insert_sample(
        &self,
        node_id: &str,
        metrics: &HashMap<String, f64>,
    ) -> Result<(), String> {
        if metrics.is_empty() {
            return Ok(());
        }

        for (name, value) in metrics {
            let metric_data = node
                .metrics
                .get(name)
                .ok_or_else(|| format!("unknown metric '{}' for node '{}'", name, node_id))?;
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

    fn cumulative_value(&self, node_id: &str, field: &MetricField) -> Result<f64, String> {
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("node id '{}' not found", node_id))?;
        let metric_data = node
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
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("node id '{}' not found", node_id))?;
        let metric_data = node
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
            results.push(Some(metric_data.quantile(*percent / 100.0)));
        }

        Ok(results)
    }

    fn clear_all(&self) -> Result<(), String> {
        for node in self.nodes.values() {
            for (name, metric_data) in &node.metrics {
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
        self.nodes.contains_key(key)
    }
}

impl NodeData {
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
    use std::collections::HashMap;

    use super::MetricStore;

    #[test]
    fn insert_metrics_allows_any_group_keys() {
        let store = MetricStore::new();
        let mut metrics = HashMap::new();
        metrics.insert("cpu_cores".to_string(), 10.0);

        store
            .insert_metrics(&["any-key".to_string()], &metrics)
            .expect("arbitrary key should succeed");
    }

    #[test]
    fn insert_and_query_grouped_and_global_values() {
        let store = MetricStore::new();

        let mut sample_a = HashMap::new();
        sample_a.insert("cpu_cores".to_string(), 10.0);
        sample_a.insert("memory_gb".to_string(), 4.0);

        let mut sample_b = HashMap::new();
        sample_b.insert("cpu_cores".to_string(), 30.0);
        sample_b.insert("memory_gb".to_string(), 6.0);

        let group_keys = vec!["N001".to_string(), "task-a".to_string(), "N001;task-a".to_string()];

        store
            .insert_metrics(&group_keys, &sample_a)
            .expect("first insert should succeed");
        store
            .insert_metrics(&group_keys, &sample_b)
            .expect("second insert should succeed");

        let global_cpu = store
            .cumulative_value(None, "cpu-cores")
            .expect("global cumulative should exist");
        assert!((global_cpu - 40.0).abs() < f64::EPSILON);

        let grouped_cpu = store
            .cumulative_value(Some("N001;task-a"), "cpu_cores")
            .expect("group cumulative should exist");
        assert!((grouped_cpu - 40.0).abs() < f64::EPSILON);

        let grouped_mem = store
            .cumulative_value(Some("task-a"), "memory gb")
            .expect("group cumulative should exist");
        assert!((grouped_mem - 10.0).abs() < f64::EPSILON);

        let pct = store
            .query_percentiles(Some("N001"), "cpu_cores", &[50.0, -1.0, 101.0])
            .expect("percentile query should succeed");
        assert_eq!(pct.len(), 3);
        assert!(pct[0].is_some());
        assert!(pct[1].is_none());
        assert!(pct[2].is_none());

        let p50 = pct[0].expect("p50 value");
        assert!((10.0..=30.0).contains(&p50));
    }

    #[test]
    fn clear_all_removes_all_metrics() {
        let store = MetricStore::new();
        let mut metrics = HashMap::new();
        metrics.insert("cpu_cores".to_string(), 7.0);

        store
            .insert_metrics(&["N001".to_string()], &metrics)
            .expect("insert should succeed");

        store.clear_all().expect("clear should succeed");

        let err = store
            .cumulative_value(None, "cpu_cores")
            .expect_err("metrics should be empty after clear");
        assert!(err.contains("metric 'cpu_cores' not found"));
    }
}
