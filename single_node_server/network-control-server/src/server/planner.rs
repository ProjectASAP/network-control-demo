use std::collections::HashSet;

use serde_json::Value;

use crate::config::ServerRuntimeConfig;

use super::types::{
    AggregationKind, AppState, LocalAggregationPlan, QueryContext, QueryExecutionPlan,
    RequestPlanner, SearchRequest, UnsupportedFeature,
};

pub struct DefaultRequestPlanner;

impl RequestPlanner for DefaultRequestPlanner {
    fn plan_search(
        &self,
        state: &AppState,
        request: &SearchRequest,
        index_name: &str,
    ) -> Result<QueryExecutionPlan, String> {
        let mut unsupported_features = Vec::new();
        let context = parse_query_context(
            &state.runtime_config,
            index_name,
            request,
            &mut unsupported_features,
        );
        let allowed_aggs: HashSet<String> = state.runtime_config.aggregation_names();
        let mut local_aggs = Vec::new();
        let mut forwarded_aggs = HashSet::new();

        if let Some(size) = request.size {
            if size != 0 {
                unsupported_features.push(UnsupportedFeature {
                    code: "unsupported_size".to_string(),
                    message: "only size=0 is supported for local search execution".to_string(),
                    details: vec![format!("received size={size}")],
                });
            }
        }

        if let Some(aggs) = request.aggs.as_ref() {
            for (name, agg) in aggs {
                let Some(kind) = agg.kind() else {
                    forwarded_aggs.insert(name.clone());
                    continue;
                };
                let registration = match &kind {
                    AggregationKind::Percentiles(_) => {
                        state.aggregation_engine.registration("percentiles")
                    }
                    AggregationKind::Cumulative(_) => {
                        state.aggregation_engine.registration("cumulative")
                    }
                };
                let Some(registration) = registration else {
                    forwarded_aggs.insert(name.clone());
                    continue;
                };
                if !allowed_aggs.contains(registration.name) || !registration.supports_search {
                    forwarded_aggs.insert(name.clone());
                    continue;
                }
                local_aggs.push(LocalAggregationPlan {
                    name: name.clone(),
                    kind,
                });
            }
        }

        let has_other_fields = !request.other.is_empty();
        Ok(QueryExecutionPlan {
            context,
            local_aggs,
            forwarded_aggs,
            unsupported_features,
            has_other_fields,
        })
    }
}

fn parse_query_context(
    config: &ServerRuntimeConfig,
    index_name: &str,
    request: &SearchRequest,
    unsupported_features: &mut Vec<UnsupportedFeature>,
) -> QueryContext {
    let mut context = QueryContext {
        index_name: Some(index_name.to_string()),
        ..QueryContext::default()
    };
    let Some(query) = request.query.as_ref() else {
        return context;
    };
    let Some(bool_obj) = query.get("bool").and_then(Value::as_object) else {
        unsupported_features.push(UnsupportedFeature {
            code: "unsupported_query".to_string(),
            message: "only bool.filter.term queries are supported locally".to_string(),
            details: vec!["expected query.bool.filter".to_string()],
        });
        return context;
    };
    let Some(filters) = bool_obj.get("filter").and_then(Value::as_array) else {
        unsupported_features.push(UnsupportedFeature {
            code: "unsupported_query".to_string(),
            message: "only bool.filter.term queries are supported locally".to_string(),
            details: vec!["query.bool.filter must be an array".to_string()],
        });
        return context;
    };
    for filter in filters {
        let Some(term_obj) = filter.get("term").and_then(Value::as_object) else {
            unsupported_features.push(UnsupportedFeature {
                code: "unsupported_filter".to_string(),
                message: "only term filters are supported locally".to_string(),
                details: vec![filter.to_string()],
            });
            continue;
        };
        if term_obj.len() != 1 {
            unsupported_features.push(UnsupportedFeature {
                code: "invalid_term_filter".to_string(),
                message: "term filter must contain exactly one field".to_string(),
                details: vec![filter.to_string()],
            });
            continue;
        }
        for (field, value) in term_obj {
            let normalized_field = field.trim().to_ascii_lowercase();
            if config.key_fields_for_index(index_name).contains(&normalized_field) {
                let Some(key) = extract_scalar_string(value) else {
                    unsupported_features.push(UnsupportedFeature {
                        code: "invalid_term_value".to_string(),
                        message: "key term filter value must be a scalar".to_string(),
                        details: vec![filter.to_string()],
                    });
                    continue;
                };
                if context
                    .key
                    .as_ref()
                    .map(|current| current != &key)
                    .unwrap_or(false)
                {
                    unsupported_features.push(UnsupportedFeature {
                        code: "multiple_keys".to_string(),
                        message: "multiple key filters are not supported locally".to_string(),
                        details: vec![filter.to_string()],
                    });
                    continue;
                }
                context.key = Some(key);
                continue;
            }
            if normalized_field == "epoch" {
                let epoch = value.as_u64().or_else(|| {
                    value
                        .as_str()
                        .and_then(|candidate| candidate.parse::<u64>().ok())
                });
                if let Some(epoch) = epoch {
                    context.epoch = Some(epoch);
                } else {
                    unsupported_features.push(UnsupportedFeature {
                        code: "invalid_epoch".to_string(),
                        message: "epoch term filter must be an integer".to_string(),
                        details: vec![filter.to_string()],
                    });
                }
                continue;
            }
            unsupported_features.push(UnsupportedFeature {
                code: "unsupported_term_field".to_string(),
                message: format!("unsupported term filter field '{field}'"),
                details: vec![filter.to_string()],
            });
        }
    }
    context
}

fn extract_scalar_string(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        return Some(value.to_string());
    }
    value.as_i64().map(|value| value.to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::config::ServerRuntimeConfig;

    use super::{QueryContext, parse_query_context};

    fn config() -> ServerRuntimeConfig {
        serde_yaml::from_str(
            r#"
server:
  host: "0.0.0.0"
  port: 10101
  body_limit_mb: 50
  request_log_buffer: 1000
  enable_timing: false
  timing_csv_path: "server_request_timing.csv"
api:
  index_name: "cluster-metrics"
  enable_batch_endpoint: true
  enable_metrics_endpoint: true
  strict_mode: false
upstream:
  mode: "disabled"
  search_url:
  forward_headers: []
storage:
    backend: "in_memory_key_store"
    range_key_catalog:
        format: "N{:03}"
        start: 1
        end: 1
schema:
  metrics:
    - name: "cpu_cores"
      aliases: ["cpucores"]
      storage_field: "cpu_cores"
    - name: "memory_gb"
      aliases: ["memorygb"]
      storage_field: "memory_gb"
    - name: "network_mbps"
      aliases: ["networkmbps"]
      storage_field: "network_mbps"
  key_fields: ["cluster"]
  ingest_field_mapping:
    key_field: "cluster"
    epoch_field: "epoch"
    task_field: "task"
    metric_fields:
      cpu_cores: "cpu_cores"
      memory_gb: "memory_gb"
      network_mbps: "network_mbps"
query_support:
  aggregations: ["percentiles", "cumulative"]
  supported_filter_types: ["term"]
  default_batch_fields: ["cpu_cores"]
  default_batch_percents: [50.0]
"#,
        )
        .unwrap()
    }

    #[test]
    fn parses_term_filter_into_context() {
        let request = crate::server::types::SearchRequest {
            size: Some(0),
            query: Some(json!({
                "bool": {
                    "filter": [
                        {"term": {"cluster": "N001"}},
                        {"term": {"epoch": 7}}
                    ]
                }
            })),
            aggs: None,
            other: Default::default(),
        };
        let mut unsupported = Vec::new();
        let context = parse_query_context(&config(), "cluster-metrics", &request, &mut unsupported);
        assert!(unsupported.is_empty());
        assert_eq!(context.key, Some("N001".to_string()));
        assert_eq!(context.epoch, Some(7));
    }

    #[test]
    fn empty_query_yields_default_context() {
        let request = crate::server::types::SearchRequest {
            size: None,
            query: None,
            aggs: None,
            other: Default::default(),
        };
        let mut unsupported = Vec::new();
        let context = parse_query_context(&config(), "cluster-metrics", &request, &mut unsupported);
        assert_eq!(context.key, QueryContext::default().key);
        assert!(unsupported.is_empty());
    }
}
