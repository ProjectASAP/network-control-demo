use sketchlib_rust::{KLL, SketchInput};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Default)]
struct MetricData {
    kll: KLL,
    cumulative: f64,
}

pub struct MetricStore {
    groups: RwLock<HashMap<String, HashMap<String, MetricData>>>,
    global_metrics: RwLock<HashMap<String, MetricData>>,
}

impl Default for MetricStore {
    fn default() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            global_metrics: RwLock::new(HashMap::new()),
        }
    }
}

impl MetricStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_metrics(
        &self,
        group_keys: &[String],
        metrics: &HashMap<String, f64>,
    ) -> Result<(), String> {
        if metrics.is_empty() {
            return Ok(());
        }

        {
            let mut global = self
                .global_metrics
                .write()
                .map_err(|_| "failed to lock global metrics")?;
            for (metric, value) in metrics {
                let metric_entry = global.entry(metric.clone()).or_default();
                metric_entry
                    .kll
                    .update(&SketchInput::F64(*value))
                    .map_err(|_| format!("{} values should be numeric", metric))?;
                metric_entry.cumulative += *value;
            }
        }

        if group_keys.is_empty() {
            return Ok(());
        }

        let mut groups = self.groups.write().map_err(|_| "failed to lock grouped metrics")?;
        for group_key in group_keys {
            let per_group = groups.entry(group_key.clone()).or_insert_with(HashMap::new);
            for (metric, value) in metrics {
                let metric_entry = per_group.entry(metric.clone()).or_default();
                metric_entry
                    .kll
                    .update(&SketchInput::F64(*value))
                    .map_err(|_| format!("{} values should be numeric", metric))?;
                metric_entry.cumulative += *value;
            }
        }

        Ok(())
    }

    pub fn cumulative_value(&self, key: Option<&str>, field: &str) -> Result<f64, String> {
        let normalized_field = normalize_metric_name(field);
        if let Some(group_key) = key {
            let groups = self.groups.read().map_err(|_| "failed to lock grouped metrics")?;
            let per_group = groups
                .get(group_key)
                .ok_or_else(|| format!("key '{}' not found", group_key))?;
            let value = per_group
                .get(&normalized_field)
                .ok_or_else(|| format!("metric '{}' not found for key '{}'", field, group_key))?;
            return Ok(value.cumulative);
        }

        let global = self
            .global_metrics
            .read()
            .map_err(|_| "failed to lock global metrics")?;
        let value = global
            .get(&normalized_field)
            .ok_or_else(|| format!("metric '{}' not found", field))?;
        Ok(value.cumulative)
    }

    pub fn query_percentiles(
        &self,
        key: Option<&str>,
        field: &str,
        percents: &[f64],
    ) -> Result<Vec<Option<f64>>, String> {
        let normalized_field = normalize_metric_name(field);

        let metric_data = if let Some(group_key) = key {
            let groups = self.groups.read().map_err(|_| "failed to lock grouped metrics")?;
            let per_group = groups
                .get(group_key)
                .ok_or_else(|| format!("key '{}' not found", group_key))?;
            per_group
                .get(&normalized_field)
                .ok_or_else(|| format!("metric '{}' not found for key '{}'", field, group_key))?
                .kll
                .clone()
        } else {
            let global = self
                .global_metrics
                .read()
                .map_err(|_| "failed to lock global metrics")?;
            global
                .get(&normalized_field)
                .ok_or_else(|| format!("metric '{}' not found", field))?
                .kll
                .clone()
        };

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

    pub fn clear_all(&self) -> Result<(), String> {
        {
            let mut groups = self.groups.write().map_err(|_| "failed to lock grouped metrics")?;
            groups.clear();
        }
        {
            let mut global = self
                .global_metrics
                .write()
                .map_err(|_| "failed to lock global metrics")?;
            global.clear();
        }
        Ok(())
    }
}

fn normalize_metric_name(name: &str) -> String {
    name.trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace(' ', "_")
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
