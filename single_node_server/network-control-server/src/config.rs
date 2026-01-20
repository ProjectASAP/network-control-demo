use std::{collections::HashSet, env, error::Error, fs};

use serde::Deserialize;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct AggregationConfig {
    // Used only for query validation, not for ingestion-time enforcement.
    pub percentile_fields: HashSet<String>,
    pub percentile_label_fields: HashSet<String>,
    pub percentile_labels: HashSet<String>,
    pub top_entities_metrics: HashSet<String>,
    pub top_entities_label_metrics: HashSet<String>,
    pub top_entities_labels: HashSet<String>,
    pub cumulative_metrics: HashSet<String>,
    pub cumulative_label_metrics: HashSet<String>,
    pub cumulative_labels: HashSet<String>,
}

#[derive(Debug, Deserialize)]
struct RawAggregationConfig {
    supported_aggs: SupportedAggs,
}

#[derive(Debug, Deserialize)]
struct SupportedAggs {
    percentiles: AggSupport,
    top_entities: AggSupport,
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

        Ok(Self {
            percentile_fields: normalize_vec(raw.supported_aggs.percentiles.metrics),
            percentile_label_fields: normalize_vec(
                raw.supported_aggs.percentiles.metrics_with_labels.metrics,
            ),
            percentile_labels: normalize_vec(
                raw.supported_aggs.percentiles.metrics_with_labels.labels,
            ),
            top_entities_metrics: normalize_vec(raw.supported_aggs.top_entities.metrics),
            top_entities_label_metrics: normalize_vec(
                raw.supported_aggs.top_entities.metrics_with_labels.metrics,
            ),
            top_entities_labels: normalize_vec(
                raw.supported_aggs.top_entities.metrics_with_labels.labels,
            ),
            cumulative_metrics: normalize_vec(raw.supported_aggs.cumulative.metrics),
            cumulative_label_metrics: normalize_vec(
                raw.supported_aggs.cumulative.metrics_with_labels.metrics,
            ),
            cumulative_labels: normalize_vec(
                raw.supported_aggs.cumulative.metrics_with_labels.labels,
            ),
        })
    }
}

fn normalize_vec(items: Vec<String>) -> HashSet<String> {
    items
        .into_iter()
        .map(|item| item.trim().to_ascii_lowercase())
        .collect()
}
