use crate::data_model::{InferenceConfig, StreamingConfig};
use anyhow::{Context, Result};

/// Read inference configuration from a YAML file
pub fn read_inference_config(yaml_file: &str) -> Result<InferenceConfig> {
    let config = InferenceConfig::from_yaml_file(yaml_file)
        .with_context(|| format!("Failed to parse YAML config from: {yaml_file}"))?;

    Ok(config)
}

pub fn read_streaming_config(
    yaml_file: &str,
    inference_config: &InferenceConfig,
) -> Result<StreamingConfig> {
    let yaml_data = std::fs::read_to_string(yaml_file)
        .with_context(|| format!("Failed to read YAML file: {yaml_file}"))?;
    let yaml_data: serde_yaml::Value = serde_yaml::from_str(&yaml_data)
        .with_context(|| format!("Failed to parse YAML file: {yaml_file}"))?;

    let config = StreamingConfig::from_yaml_data(&yaml_data, Some(inference_config))
        .with_context(|| format!("Failed to parse YAML config from: {yaml_file}"))?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_read_streaming_config() {
        let streaming_yaml_content = r#"
aggregations:
- aggregationId: 1
  aggregationSubType: ''
  aggregationType: DatasketchesKLL
  labels:
    aggregated: []
    grouping:
    - instance
    - job
    - label_0
    - label_1
    - label_2
    rollup: []

  metric: fake_metric_total
  parameters:
    K: 200
  spatialFilter: ''
  tumblingWindowSize: 10
metrics:
  fake_metric_total:
  - instance
  - job
  - label_0
  - label_1
  - label_2
"#;

        let inference_yaml_content = r#"
metrics:
  fake_metric_total:
  - instance
  - job
  - label_0
  - label_1
  - label_2
queries:
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.5, fake_metric_total[1m])
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.95, fake_metric_total[1m])
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.99, fake_metric_total[1m])
        "#;

        let mut inference_temp_file = NamedTempFile::new().unwrap();
        write!(inference_temp_file, "{inference_yaml_content}").unwrap();
        let inference_config =
            read_inference_config(inference_temp_file.path().to_str().unwrap()).unwrap();
        assert!(!inference_config.query_configs.is_empty());

        let mut streaming_temp_file = NamedTempFile::new().unwrap();
        write!(streaming_temp_file, "{streaming_yaml_content}").unwrap();

        let config = read_streaming_config(
            streaming_temp_file.path().to_str().unwrap(),
            &inference_config,
        )
        .unwrap();
        assert!(!config.aggregation_configs.is_empty());
    }

    #[test]

    fn test_read_inference_config() {
        let yaml_content = r#"
metrics:
  fake_metric_total:
  - instance
  - job
  - label_0
  - label_1
  - label_2
queries:
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.5, fake_metric_total[1m])
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.95, fake_metric_total[1m])
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.99, fake_metric_total[1m])
        "#;

        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, "{yaml_content}").unwrap();

        let config = read_inference_config(temp_file.path().to_str().unwrap()).unwrap();
        assert!(!config.query_configs.is_empty());
    }
}
