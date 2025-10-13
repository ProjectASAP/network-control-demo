use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_yaml;
use std::collections::HashMap;

use crate::data_model::traits::SerializableToSink;
use crate::utils::promql::normalize_spatial_filter;
use promql_utilities::data_model::KeyByLabelNames;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationConfig {
    pub aggregation_id: u64,
    pub aggregation_type: String,
    pub aggregation_sub_type: String,
    pub parameters: HashMap<String, Value>,
    pub grouping_labels: KeyByLabelNames,
    pub aggregated_labels: KeyByLabelNames,
    pub rollup_labels: KeyByLabelNames,
    pub original_yaml: String,
    pub tumbling_window_size: u64,
    pub spatial_filter: String,
    pub spatial_filter_normalized: String,
    pub metric: String,
    pub num_aggregates_to_retain: Option<u64>,
}

// TODO: need to implement deserialization methods

impl AggregationConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        aggregation_id: u64,
        aggregation_type: String,
        aggregation_sub_type: String,
        parameters: HashMap<String, Value>,
        grouping_labels: KeyByLabelNames,
        aggregated_labels: KeyByLabelNames,
        rollup_labels: KeyByLabelNames,
        original_yaml: String,
        tumbling_window_size: u64,
        spatial_filter: String,
        metric: String,
        num_aggregates_to_retain: Option<u64>,
    ) -> Self {
        // Generate normalized spatial filter (placeholder implementation)
        let spatial_filter_normalized = normalize_spatial_filter(&spatial_filter);

        Self {
            aggregation_id,
            aggregation_type,
            aggregation_sub_type,
            parameters,
            grouping_labels,
            aggregated_labels,
            rollup_labels,
            original_yaml,
            tumbling_window_size,
            spatial_filter,
            spatial_filter_normalized,
            metric,
            num_aggregates_to_retain,
        }
    }

    // pub fn with_sub_type(mut self, sub_type: String) -> Self {
    //     self.aggregation_sub_type = Some(sub_type);
    //     self
    // }

    // pub fn with_parameters(mut self, parameters: HashMap<String, String>) -> Self {
    //     self.parameters = parameters;
    //     self
    // }

    pub fn with_original_yaml(mut self, yaml: String) -> Self {
        self.original_yaml = yaml;
        self
    }

    pub fn deserialize_from_json(
        data: &Value,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let aggregation_id = data["aggregationId"]
            .as_u64()
            .ok_or("Missing aggregationId")?;

        let aggregation_type = data["aggregationType"]
            .as_str()
            .ok_or("Missing aggregationType")?
            .to_string();

        let aggregation_sub_type = data["aggregationSubType"]
            .as_str()
            .ok_or("Missing aggregationSubType")?
            .to_string();

        let parameters = data["parameters"]
            .as_object()
            .ok_or("Missing parameters")?
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Note: In Python, eval(data["originalYaml"]) is used, but this is unsafe
        // Using the string value directly instead
        let original_yaml = data["originalYaml"].as_str().unwrap_or("").to_string();

        // Deserialize KeyByLabelNames - assuming they have deserialize_from_json methods
        let grouping_labels = KeyByLabelNames::deserialize_from_json(&data["groupingLabels"])?;
        let aggregated_labels = KeyByLabelNames::deserialize_from_json(&data["aggregatedLabels"])?;
        let rollup_labels = KeyByLabelNames::deserialize_from_json(&data["rollupLabels"])?;

        let tumbling_window_size = data["tumblingWindowSize"]
            .as_u64()
            .ok_or("Missing tumblingWindowSize")?;

        let spatial_filter = data["spatialFilter"].as_str().unwrap_or("").to_string();

        let metric = data["metric"].as_str().ok_or("Missing metric")?.to_string();

        let num_aggregates_to_retain = data
            .get("numAggregatesToRetain")
            .and_then(|v| v.as_u64())
            .ok_or("Missing numAggregatesToRetain")?;

        Ok(Self::new(
            aggregation_id,
            aggregation_type,
            aggregation_sub_type,
            parameters,
            grouping_labels,
            aggregated_labels,
            rollup_labels,
            original_yaml,
            tumbling_window_size,
            spatial_filter,
            metric,
            Some(num_aggregates_to_retain),
        ))
    }

    pub fn deserialize_from_bytes(
        bytes: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let data_str = std::str::from_utf8(bytes)?.trim();
        let data: Value = serde_json::from_str(data_str)?;
        Self::deserialize_from_json(&data)
    }

    pub fn from_yaml_data(
        aggregation_data: &serde_yaml::Value,
        num_aggregates_to_retain: Option<u64>,
    ) -> Result<Self, anyhow::Error> {
        let aggregation_id = aggregation_data["aggregationId"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Missing aggregationId"))?;

        let labels = &aggregation_data["labels"];
        let grouping_labels = KeyByLabelNames::new(
            labels["grouping"]
                .as_sequence()
                .ok_or_else(|| anyhow::anyhow!("Missing grouping labels"))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect(),
        );
        let aggregated_labels = KeyByLabelNames::new(
            labels["aggregated"]
                .as_sequence()
                .ok_or_else(|| anyhow::anyhow!("Missing aggregated labels"))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect(),
        );
        let rollup_labels = KeyByLabelNames::new(
            labels["rollup"]
                .as_sequence()
                .ok_or_else(|| anyhow::anyhow!("Missing rollup labels"))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect(),
        );

        let aggregation_type = aggregation_data["aggregationType"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing aggregationType"))?
            .to_string();

        let aggregation_sub_type = aggregation_data["aggregationSubType"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing aggregationSubType"))?
            .to_string();

        // Convert serde_yaml::Value to serde_json::Value for parameters
        let parameters: HashMap<String, Value> = aggregation_data["parameters"]
            .as_mapping()
            .ok_or_else(|| anyhow::anyhow!("Missing parameters"))?
            .iter()
            .map(|(k, v)| {
                let key = k.as_str().unwrap_or("").to_string();
                let value = serde_json::to_value(v).unwrap_or(Value::Null);
                (key, value)
            })
            .collect();

        let tumbling_window_size = aggregation_data["tumblingWindowSize"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Missing tumblingWindowSize"))?;

        let spatial_filter = aggregation_data["spatialFilter"]
            .as_str()
            .unwrap_or("")
            .to_string();

        let metric = aggregation_data["metric"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing metric"))?
            .to_string();

        Ok(Self::new(
            aggregation_id,
            aggregation_type,
            aggregation_sub_type,
            parameters,
            grouping_labels,
            aggregated_labels,
            rollup_labels,
            String::new(), // original_yaml - empty as in Python
            tumbling_window_size,
            spatial_filter,
            metric,
            num_aggregates_to_retain,
        ))
    }
}

impl SerializableToSink for AggregationConfig {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "aggregationId": self.aggregation_id,
            "aggregationType": self.aggregation_type,
            "aggregationSubType": self.aggregation_sub_type,
            "parameters": self.parameters,
            "originalYaml": self.original_yaml,
            "tumblingWindowSize": self.tumbling_window_size,
            "spatialFilter": self.spatial_filter,
            "metric": self.metric,
            "numAggregatesToRetain": self.num_aggregates_to_retain
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.original_yaml.as_bytes().to_vec()
    }
}
