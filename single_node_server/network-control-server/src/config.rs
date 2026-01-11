use std::{collections::HashSet, env, error::Error, fs};

use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct AggregationConfig {
    pub percentile_fields: HashSet<String>,
    pub top_entities_metrics: HashSet<String>,
    pub cumulative_metrics: HashSet<String>,
}

#[derive(Debug, Deserialize)]
struct RawAggregationConfig {
    supported_aggs: SupportedAggs,
}

#[derive(Debug, Deserialize)]
struct SupportedAggs {
    percentiles: PercentileSupport,
    top_entities: EntitySupport,
    cumulative: EntitySupport,
}

#[derive(Debug, Deserialize)]
struct PercentileSupport {
    fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EntitySupport {
    metrics: Vec<String>,
}

impl AggregationConfig {
    pub fn load() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let path = env::var("AGG_CONFIG_PATH").unwrap_or_else(|_| "agg-config.yaml".to_string());
        let contents = fs::read_to_string(&path)?;
        let raw: RawAggregationConfig = serde_yaml::from_str(&contents)?;

        Ok(Self {
            percentile_fields: normalize_vec(raw.supported_aggs.percentiles.fields),
            top_entities_metrics: normalize_vec(raw.supported_aggs.top_entities.metrics),
            cumulative_metrics: normalize_vec(raw.supported_aggs.cumulative.metrics),
        })
    }
}

fn normalize_vec(items: Vec<String>) -> HashSet<String> {
    items
        .into_iter()
        .map(|item| item.trim().to_ascii_lowercase())
        .collect()
}
