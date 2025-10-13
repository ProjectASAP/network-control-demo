use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::fs::File;
use std::io::BufReader;

use crate::data_model::{MetricConfig, QueryConfig};
use promql_utilities::data_model::KeyByLabelNames;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceConfig {
    pub metric_config: MetricConfig,
    pub query_configs: Vec<QueryConfig>,
}

impl InferenceConfig {
    pub fn new() -> Self {
        Self {
            metric_config: MetricConfig::new(),
            query_configs: Vec::new(),
        }
    }

    pub fn from_yaml_file(yaml_file: &str) -> Result<Self> {
        let file = File::open(yaml_file)?;
        let reader = BufReader::new(file);
        let data: Value = serde_yaml::from_reader(reader)?;

        Self::from_yaml_data(&data)
    }

    pub fn from_yaml_data(data: &Value) -> Result<Self> {
        // Handle metrics field -> metric_config
        let mut metric_config = MetricConfig::new();
        if let Some(metrics) = data.get("metrics") {
            if let Some(metrics_map) = metrics.as_mapping() {
                for (metric_name_val, labels_val) in metrics_map {
                    if let (Some(metric_name), Some(labels_seq)) =
                        (metric_name_val.as_str(), labels_val.as_sequence())
                    {
                        let labels: Vec<String> = labels_seq
                            .iter()
                            .filter_map(|v| v.as_str())
                            .map(|s| s.to_string())
                            .collect();
                        let key_by_label_names = KeyByLabelNames::new(labels);
                        metric_config =
                            metric_config.add_metric(metric_name.to_string(), key_by_label_names);
                    }
                }
            }
        }

        // Handle queries field -> query_configs
        let query_configs = if let Some(queries) = data.get("queries").and_then(|v| v.as_sequence())
        {
            let mut configs = Vec::new();
            for query_data in queries {
                let query = query_data
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Missing query field"))?
                    .to_string();

                // Parse aggregations if present
                let aggregations = if let Some(aggregations_data) =
                    query_data.get("aggregations").and_then(|v| v.as_sequence())
                {
                    let mut agg_refs = Vec::new();
                    for agg_data in aggregations_data {
                        let aggregation_id = agg_data
                            .get("aggregation_id")
                            .and_then(|v| v.as_u64())
                            .ok_or_else(|| {
                                anyhow::anyhow!("Missing aggregation_id in aggregation")
                            })?;

                        let num_aggregates_to_retain = agg_data
                            .get("num_aggregates_to_retain")
                            .and_then(|v| v.as_u64());

                        agg_refs.push(crate::data_model::AggregationReference::new(
                            aggregation_id,
                            num_aggregates_to_retain,
                        ));
                    }
                    agg_refs
                } else {
                    Vec::new()
                };

                let config = QueryConfig::new(query).with_aggregations(aggregations);
                configs.push(config);
            }
            configs
        } else {
            Vec::new()
        };

        Ok(Self {
            metric_config,
            query_configs,
        })
    }
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self::new()
    }
}
