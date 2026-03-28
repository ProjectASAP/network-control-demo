use std::{collections::HashSet, env, error::Error, fs};

use serde::Deserialize;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct AggregationConfig {
    // Used only for query validation, not for ingestion-time enforcement.
    pub percentile_fields: HashSet<String>,
    pub percentile_label_fields: HashSet<String>,
    pub percentile_labels: HashSet<String>,
    pub cumulative_metrics: HashSet<String>,
    pub cumulative_label_metrics: HashSet<String>,
    pub cumulative_labels: HashSet<String>,
    pub label_combinations: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct RawAggregationConfig {
    supported_aggs: SupportedAggs,
}

#[derive(Debug, Deserialize)]
struct SupportedAggs {
    percentiles: AggSupport,
    cumulative: AggSupport,
}

#[derive(Debug, Deserialize)]
struct AggSupport {
    metrics: Vec<String>,
    #[serde(default)]
    metrics_with_labels: MetricsWithLabelsSupport,
}

#[derive(Debug, Deserialize, Default)]
struct MetricsWithLabelsSupport {
    #[serde(default)]
    labels: Vec<String>,
    metrics: Vec<String>,
}

impl AggregationConfig {
    pub fn load() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let path = env::var("AGG_CONFIG_PATH").unwrap_or_else(|_| "agg-config.yaml".to_string());
        let contents = fs::read_to_string(&path)?;
        let raw: RawAggregationConfig = serde_yaml::from_str(&contents)?;
        let percentiles = raw.supported_aggs.percentiles;
        let cumulative = raw.supported_aggs.cumulative;

        let percentile_metrics = percentiles.metrics;
        let percentile_label_metrics = percentiles.metrics_with_labels.metrics;
        let percentile_labels = percentiles.metrics_with_labels.labels;

        let cumulative_metrics = cumulative.metrics;
        let cumulative_label_metrics = cumulative.metrics_with_labels.metrics;
        let cumulative_labels = cumulative.metrics_with_labels.labels;

        Ok(Self {
            percentile_fields: normalize_vec(percentile_metrics),
            percentile_label_fields: normalize_vec(percentile_label_metrics),
            percentile_labels: normalize_vec(percentile_labels.clone()),
            cumulative_metrics: normalize_vec(cumulative_metrics),
            cumulative_label_metrics: normalize_vec(cumulative_label_metrics),
            cumulative_labels: normalize_vec(cumulative_labels.clone()),
            label_combinations: parse_label_combinations(
                percentile_labels
                    .into_iter()
                    .chain(cumulative_labels.into_iter())
                    .collect(),
            ),
        })
    }

    pub fn supports_percentile_field(&self, field: &str, has_key: bool) -> bool {
        if has_key {
            self.percentile_label_fields.contains(field) || self.percentile_fields.contains(field)
        } else {
            self.percentile_fields.contains(field)
        }
    }

    pub fn supports_cumulative_field(&self, field: &str) -> bool {
        self.cumulative_metrics.contains(field) || self.cumulative_label_metrics.contains(field)
    }

    pub fn supported_metric_fields(&self) -> HashSet<String> {
        self.percentile_fields
            .iter()
            .chain(self.percentile_label_fields.iter())
            .chain(self.cumulative_metrics.iter())
            .chain(self.cumulative_label_metrics.iter())
            .cloned()
            .collect()
    }

    pub fn supported_label_names(&self) -> HashSet<String> {
        self.label_combinations
            .iter()
            .flat_map(|parts| parts.iter().cloned())
            .collect()
    }
}

fn normalize_vec(items: Vec<String>) -> HashSet<String> {
    items
        .into_iter()
        .map(|item| item.trim().to_ascii_lowercase())
        .collect()
}

fn parse_label_combinations(raw_labels: Vec<String>) -> Vec<Vec<String>> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();

    for raw in raw_labels {
        let normalized_parts: Vec<String> = raw
            .split(';')
            .map(|part| part.trim().to_ascii_lowercase())
            .filter(|part| !part.is_empty())
            .collect();

        if normalized_parts.is_empty() {
            continue;
        }

        let dedupe_key = normalized_parts.join(";");
        if seen.insert(dedupe_key) {
            result.push(normalized_parts);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{AggregationConfig, parse_label_combinations};

    #[test]
    fn parse_label_combinations_normalizes_and_dedupes() {
        let parsed = parse_label_combinations(vec![
            " cluster ; task ".to_string(),
            "cluster;task".to_string(),
            "task".to_string(),
            " ; ".to_string(),
            "INSTANCE ; JOB".to_string(),
        ]);

        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0], vec!["cluster".to_string(), "task".to_string()]);
        assert_eq!(parsed[1], vec!["task".to_string()]);
        assert_eq!(parsed[2], vec!["instance".to_string(), "job".to_string()]);
    }

    #[test]
    fn support_helpers_match_expected_fields() {
        let cfg = AggregationConfig {
            percentile_fields: HashSet::from(["cpu_cores".to_string()]),
            percentile_label_fields: HashSet::from(["memory_gb".to_string()]),
            percentile_labels: HashSet::new(),
            cumulative_metrics: HashSet::from(["network_mbps".to_string()]),
            cumulative_label_metrics: HashSet::from(["cpu_cores".to_string()]),
            cumulative_labels: HashSet::new(),
            label_combinations: vec![
                vec!["cluster".to_string()],
                vec!["cluster".to_string(), "task".to_string()],
            ],
        };

        assert!(cfg.supports_percentile_field("cpu_cores", false));
        assert!(!cfg.supports_percentile_field("memory_gb", false));
        assert!(cfg.supports_percentile_field("memory_gb", true));

        assert!(cfg.supports_cumulative_field("network_mbps"));
        assert!(cfg.supports_cumulative_field("cpu_cores"));
        assert!(!cfg.supports_cumulative_field("disk_io"));

        let fields = cfg.supported_metric_fields();
        assert!(fields.contains("cpu_cores"));
        assert!(fields.contains("memory_gb"));
        assert!(fields.contains("network_mbps"));

        let labels = cfg.supported_label_names();
        assert!(labels.contains("cluster"));
        assert!(labels.contains("task"));
    }
}
