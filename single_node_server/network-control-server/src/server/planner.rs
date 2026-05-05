use std::collections::HashSet;

use elasticsearch_dsl_ast::{Aggregation, AggregationName, Query, Term};
use serde_json::Value;

// Workaround: the elasticsearch-dsl-ast crate's `BoolQuery: Deserialize` impl
// is broken (it recurses through the `Deserialize` trait and errors with
// `missing field 'bool'`), so `Query`'s untagged enum falls through and bool
// queries land in `Query::Json(serde_json::Value)`. Until the upstream crate is
// fixed we walk that fallback Value manually for the bool/term shape we
// support. Aggregations are unaffected and use the typed AST normally.

use crate::config::ServerRuntimeConfig;

use super::types::{
    AggregationKind, AppState, LocalAggregationPlan, PercentileAggregation, QueryContext,
    QueryExecutionPlan, RequestPlanner, SearchRequest, SumAggregation, UnsupportedFeature,
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

        for (name, agg) in request.aggs.iter() {
            let name_str = aggregation_name_str(name);
            let kind = match agg {
                Aggregation::Percentiles(pct) => {
                    let inner = &pct.percentiles;
                    let percents = inner.percents.clone().unwrap_or_default();
                    Some((
                        "percentiles",
                        AggregationKind::Percentiles(PercentileAggregation {
                            field: inner.field.clone(),
                            percents,
                        }),
                    ))
                }
                Aggregation::Sum(s) => {
                    let inner = &s.sum;
                    let Some(field) = inner.field.clone() else {
                        forwarded_aggs.insert(name_str.clone());
                        continue;
                    };
                    Some(("sum", AggregationKind::Sum(SumAggregation { field })))
                }
                _ => None,
            };
            let Some((reg_name, kind)) = kind else {
                forwarded_aggs.insert(name_str);
                continue;
            };
            let Some(registration) = state.aggregation_engine.registration(reg_name) else {
                forwarded_aggs.insert(name_str);
                continue;
            };
            if !allowed_aggs.contains(registration.name) || !registration.supports_search {
                forwarded_aggs.insert(name_str);
                continue;
            }
            local_aggs.push(LocalAggregationPlan {
                name: name_str,
                kind,
            });
        }

        Ok(QueryExecutionPlan {
            context,
            local_aggs,
            forwarded_aggs,
            unsupported_features,
            has_other_fields: false,
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
    match query {
        Query::Bool(bool_query) => {
            for filter in &bool_query.filter.0 {
                apply_typed_term_filter(config, index_name, filter, &mut context, unsupported_features);
            }
        }
        Query::Json(raw) => {
            // See module-top comment: bool queries fall through to JsonQuery.
            apply_value_bool_filter(config, index_name, &raw.0, &mut context, unsupported_features);
        }
        _ => {
            unsupported_features.push(UnsupportedFeature {
                code: "unsupported_query".to_string(),
                message: "only bool.filter.term queries are supported locally".to_string(),
                details: vec![format!("got {query:?}")],
            });
        }
    }
    context
}

fn apply_typed_term_filter(
    config: &ServerRuntimeConfig,
    index_name: &str,
    filter: &Query,
    context: &mut QueryContext,
    unsupported_features: &mut Vec<UnsupportedFeature>,
) {
    let Query::Term(term) = filter else {
        unsupported_features.push(UnsupportedFeature {
            code: "unsupported_filter".to_string(),
            message: "only term filters are supported locally".to_string(),
            details: vec![format!("{filter:?}")],
        });
        return;
    };
    let field = term.field.trim().to_ascii_lowercase();
    let Some(value) = term.value.as_ref() else {
        unsupported_features.push(UnsupportedFeature {
            code: "invalid_term_value".to_string(),
            message: "term filter must specify a value".to_string(),
            details: vec![format!("field={field}")],
        });
        return;
    };
    apply_term_assignment(
        config,
        index_name,
        &field,
        term_as_string(value),
        term_as_u64(value),
        context,
        unsupported_features,
    );
}

fn apply_value_bool_filter(
    config: &ServerRuntimeConfig,
    index_name: &str,
    raw_query: &Value,
    context: &mut QueryContext,
    unsupported_features: &mut Vec<UnsupportedFeature>,
) {
    let Some(bool_obj) = raw_query.get("bool").and_then(Value::as_object) else {
        unsupported_features.push(UnsupportedFeature {
            code: "unsupported_query".to_string(),
            message: "only bool.filter.term queries are supported locally".to_string(),
            details: vec!["expected query.bool".to_string()],
        });
        return;
    };
    let Some(filters) = bool_obj.get("filter").and_then(Value::as_array) else {
        unsupported_features.push(UnsupportedFeature {
            code: "unsupported_query".to_string(),
            message: "only bool.filter.term queries are supported locally".to_string(),
            details: vec!["query.bool.filter must be an array".to_string()],
        });
        return;
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
        for (raw_field, value) in term_obj {
            let field = raw_field.trim().to_ascii_lowercase();
            let as_string = match value {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            };
            let as_u64 = value.as_u64().or_else(|| {
                value
                    .as_str()
                    .and_then(|candidate| candidate.parse::<u64>().ok())
            });
            apply_term_assignment(
                config,
                index_name,
                &field,
                as_string,
                as_u64,
                context,
                unsupported_features,
            );
        }
    }
}

fn apply_term_assignment(
    config: &ServerRuntimeConfig,
    index_name: &str,
    field: &str,
    as_string: Option<String>,
    as_u64: Option<u64>,
    context: &mut QueryContext,
    unsupported_features: &mut Vec<UnsupportedFeature>,
) {
    if config.key_fields_for_index(index_name).contains(field) {
        let Some(key) = as_string else {
            unsupported_features.push(UnsupportedFeature {
                code: "invalid_term_value".to_string(),
                message: "key term filter value must be a scalar".to_string(),
                details: vec![format!("field={field}")],
            });
            return;
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
                details: vec![format!("field={field}")],
            });
            return;
        }
        context.key = Some(key);
        return;
    }
    if field == "epoch" {
        if let Some(epoch) = as_u64 {
            context.epoch = Some(epoch);
        } else {
            unsupported_features.push(UnsupportedFeature {
                code: "invalid_epoch".to_string(),
                message: "epoch term filter must be an integer".to_string(),
                details: Vec::new(),
            });
        }
        return;
    }
    unsupported_features.push(UnsupportedFeature {
        code: "unsupported_term_field".to_string(),
        message: format!("unsupported term filter field '{field}'"),
        details: Vec::new(),
    });
}

fn term_as_u64(value: &Term) -> Option<u64> {
    match value {
        Term::PositiveNumber(n) => Some(*n),
        Term::NegativeNumber(n) if *n >= 0 => Some(*n as u64),
        Term::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

/// Extract the underlying string from an `AggregationName`. The crate exposes
/// no accessor, so we go through serde (newtype struct -> JSON string).
fn aggregation_name_str(name: &AggregationName) -> String {
    match serde_json::to_value(name) {
        Ok(Value::String(s)) => s,
        _ => format!("{name:?}"),
    }
}

fn term_as_string(value: &Term) -> Option<String> {
    match value {
        Term::String(s) => Some(s.clone()),
        Term::PositiveNumber(n) => Some(n.to_string()),
        Term::NegativeNumber(n) => Some(n.to_string()),
        _ => None,
    }
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
indices:
  cluster-metrics:
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
      metric_fields:
        cpu_cores: "cpu_cores"
        memory_gb: "memory_gb"
        network_mbps: "network_mbps"
query_support:
  aggregations: ["percentiles", "sum"]
  supported_filter_types: ["term"]
  default_batch_fields: ["cpu_cores"]
  default_batch_percents: [50.0]
"#,
        )
        .unwrap()
    }

    #[test]
    fn parses_term_filter_into_context() {
        let request: super::SearchRequest = serde_json::from_value(json!({
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"cluster": "N001"}},
                        {"term": {"epoch": 7}}
                    ]
                }
            }
        }))
        .unwrap();
        let mut unsupported = Vec::new();
        let context = parse_query_context(&config(), "cluster-metrics", &request, &mut unsupported);
        assert!(unsupported.is_empty(), "unexpected: {unsupported:?}");
        assert_eq!(context.key, Some("N001".to_string()));
        assert_eq!(context.epoch, Some(7));
    }

    #[test]
    fn empty_query_yields_default_context() {
        let request: super::SearchRequest = serde_json::from_value(json!({})).unwrap();
        let mut unsupported = Vec::new();
        let context = parse_query_context(&config(), "cluster-metrics", &request, &mut unsupported);
        assert_eq!(context.key, QueryContext::default().key);
        assert!(unsupported.is_empty());
    }
}
