use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::HeaderMap,
    middleware::from_fn_with_state,
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use chrono::DateTime;

use crate::metrics::MetricField;

use super::logging::log_request_middleware;
use super::query::{
    extract_query_key, handle_cumulative, handle_percentiles, handle_top_entities,
    parse_quantile_spec,
};
use super::timing::{QueryTiming, write_timing_log};
use super::types::{
    AggregationKind, AppState, BatchQueryRequest, BatchQueryResponse, BatchQueryResult,
    IngestRecord, MetricsQuery, PercentileAggregation, QueryKeyStatus, RootResponse, SearchRequest,
    TopEntitiesResult,
};
use super::upstream::{forward_to_upstream, merge_aggregations};

#[derive(Clone, Copy)]
enum BatchAggKind {
    Percentiles,
    Cumulative,
}

pub async fn run_http_server(
    state: AppState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let log_state = state.clone();
    let app = Router::new()
        .route("/", get(root_handler).post(ingest_handler))
        .route("/healthz", get(|| async { "ok" }))
        .route("/cluster-metrics/_search", post(search_handler))
        .route("/cluster-metrics/_batch", post(batch_query_handler))
        .route("/metrics/:field", post(metrics_handler))
        .with_state(state)
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024))
        .layer(from_fn_with_state(log_state, log_request_middleware));

    let listener = TcpListener::bind("0.0.0.0:10101").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn root_handler() -> Json<RootResponse<'static>> {
    Json(RootResponse {
        message: "POST /cluster-metrics/_search with aggs for percentiles, top_entities, or cumulative (cumulative requires a key). Other aggs (e.g. avg) are forwarded to Elasticsearch.",
        examples: [
            "POST /cluster-metrics/_search {\"aggs\":{\"cpu_quantiles\":{\"percentiles\":{\"field\":\"cpu_cores\",\"percents\":[10,50]}}}}",
            "POST /cluster-metrics/_search {\"aggs\":{\"top_cpu\":{\"top_entities\":{\"field\":\"cpu_cores\"}}}}",
            "POST /cluster-metrics/_search {\"aggs\":{\"cpu_cumulative\":{\"cumulative\":{\"field\":\"cpu_cores\",\"key\":\"cluster-c;cache\"}}}}",
        ],
    })
}

async fn ingest_handler(
    State(state): State<AppState>,
    Json(record): Json<IngestRecord>,
) -> impl IntoResponse {
    if state.no_ingest {
        return Json(json!({ "inserted": 0 })).into_response();
    }
    let len = record.cpu_cores.len();
    if len == 0 {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "metrics record must contain at least one sample".to_string(),
        )
            .into_response();
    }
    if record.task.len() != len
        || record.cluster.len() != len
        || record.memory_gb.len() != len
        || record.network_mbps.len() != len
    {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "metrics record fields must have equal lengths".to_string(),
        )
            .into_response();
    }
    if let Some(timestamps) = record.timestamp_ms.as_ref() {
        if timestamps.len() != len {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "timestamp_ms must match metrics length".to_string(),
            )
                .into_response();
        }
    }
    if let Some(timestamps) = record.timestamp.as_ref() {
        if timestamps.len() != len {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "timestamp must match metrics length".to_string(),
            )
                .into_response();
        }
    }
    if let Some(timestamps) = record.at_timestamp.as_ref() {
        if timestamps.len() != len {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "@timestamp must match metrics length".to_string(),
            )
                .into_response();
        }
    }

    let mut inserted = 0usize;
    for idx in 0..len {
        let cluster = record.cluster[idx].trim();
        let task = record.task[idx].trim();
        if cluster.is_empty() || task.is_empty() {
            continue;
        }
        let timestamp_ms = record
            .timestamp_ms
            .as_ref()
            .and_then(|values| values.get(idx).copied())
            .or_else(|| {
                record
                    .at_timestamp
                    .as_ref()
                    .and_then(|values| values.get(idx))
                    .and_then(|value| parse_rfc3339_millis(value))
            })
            .or_else(|| {
                record
                    .timestamp
                    .as_ref()
                    .and_then(|values| values.get(idx))
                    .and_then(|value| parse_rfc3339_millis(value))
            });
        if let Err(message) = state.store.insert(
            cluster,
            task,
            record.cpu_cores[idx],
            record.memory_gb[idx],
            record.network_mbps[idx],
            timestamp_ms,
        ) {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
        }
        inserted += 1;
    }

    Json(json!({ "inserted": 0 })).into_response()
}

fn parse_rfc3339_millis(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let parsed = DateTime::parse_from_rfc3339(trimmed).ok()?;
    let millis = parsed.timestamp_millis();
    if millis < 0 {
        None
    } else {
        Some(millis as u64)
    }
}

async fn search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let mut timing = if state.timing_enabled {
        Some(QueryTiming::new())
    } else {
        None
    };

    // Step 1: Parse JSON
    let request: SearchRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                format!("invalid JSON body: {err}"),
            )
                .into_response();
        }
    };
    if let Some(t) = &mut timing {
        t.step("parse_json");
    }

    // Step 3: Process aggregations
    let mut handled = BTreeMap::new();
    let mut handled_names = HashSet::new();
    let mut has_unhandled = false;
    let query_status = extract_query_key(request._other.get("query"));
    let query_supported = !matches!(query_status, QueryKeyStatus::Unsupported);
    let query_key = match &query_status {
        QueryKeyStatus::Key(key) => Some(key.as_str()),
        _ => None,
    };
    let has_other = request._other.keys().any(|key| key.as_str() != "query")
        || matches!(query_status, QueryKeyStatus::Unsupported);

    if let Some(aggs) = request.aggs.as_ref() {
        for (name, agg) in aggs {
            let result = if !query_supported {
                None
            } else {
                match agg.kind() {
                    Some(kind) => match kind {
                        AggregationKind::Percentiles(pct) => {
                            let t0 = Instant::now();
                            let res = handle_percentiles(&state, &pct, query_key);
                            let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
                            if let Some(t) = &mut timing {
                                t.record("sketch_estimate", elapsed_ms);
                            }
                            match res {
                                Ok(Some(values)) => Some(json!({ "values": values })),
                                Ok(None) => None,
                                Err(message) => {
                                    return (axum::http::StatusCode::BAD_REQUEST, message)
                                        .into_response();
                                }
                            }
                        }

                        AggregationKind::TopEntities(top) => {
                            if query_key.is_some() {
                                None
                            } else {
                                let t0 = Instant::now();
                                let res = handle_top_entities(&state, &top);
                                let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
                                if let Some(t) = &mut timing {
                                    t.record("sketch_estimate", elapsed_ms);
                                }
                                match res {
                                    Ok(TopEntitiesResult::Single(entity)) => Some(json!({
                                        "key": entity.key,
                                        "value": entity.value
                                    })),
                                    Ok(TopEntitiesResult::Multi(entities)) => Some(json!(entities)),
                                    Err(message) => {
                                        if message == "no top entity available" {
                                            Some(json!({}))
                                        } else {
                                            return (axum::http::StatusCode::BAD_REQUEST, message)
                                                .into_response();
                                        }
                                    }
                                }
                            }
                        }
                        AggregationKind::Cumulative(cum) => {
                            let t0 = Instant::now();
                            let res = handle_cumulative(&state, &cum);
                            let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
                            if let Some(t) = &mut timing {
                                t.record("sketch_estimate", elapsed_ms);
                            }
                            match res {
                                Ok(value) => Some(json!({ "key": cum.key, "value": value })),
                                Err(message) => {
                                    return (axum::http::StatusCode::BAD_REQUEST, message)
                                        .into_response();
                                }
                            }
                        }
                    },
                    None => None,
                }
            };

            if let Some(value) = result {
                let name = name.clone();
                handled_names.insert(name.clone());
                handled.insert(name, value);
            } else {
                has_unhandled = true;
            }
        }
    }
    if let Some(t) = &mut timing {
        t.step("aggregations");
    }

    // Step 5: Forward to upstream if needed
    let needs_upstream = has_other || has_unhandled;
    let mut response_value = if needs_upstream {
        let mut upstream_body = match serde_json::to_value(&request) {
            Ok(value) => value,
            Err(err) => {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to build upstream payload: {err}"),
                )
                    .into_response();
            }
        };
        if let Some(t) = &mut timing {
            t.step("deserialize");
        }
        if let Some(aggs_obj) = upstream_body.get_mut("aggs").and_then(Value::as_object_mut) {
            aggs_obj.retain(|name, _| !handled_names.contains(name));
        }
        if let Some(t) = &mut timing {
            t.step("prepare_upstream");
        }
        let response_value = match forward_to_upstream(&state, &headers, &upstream_body).await {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Some(t) = &mut timing {
            t.step("upstream");
        }
        response_value
    } else {
        if let Some(t) = &mut timing {
            t.step("prepare_upstream");
        }
        if let Some(t) = &mut timing {
            t.step("upstream");
        }
        json!({ "aggregations": {} })
    };

    // Step 6: Merge results
    merge_aggregations(&mut response_value, handled);
    if let Some(t) = &mut timing {
        t.step("merge");
    }

    // Build response (with timing if enabled)
    if let Some(t) = &mut timing {
        // Add timing to response before serialization
        if let Some(obj) = response_value.as_object_mut() {
            obj.insert("_timing".to_string(), t.to_json());
        }

        // Serialize to HTTP response (timed)
        let mut response = Json(response_value).into_response();
        t.step("serialize");
        t.log();

        let timing_header = t.to_header();
        response
            .headers_mut()
            .insert("X-Server-Timing", timing_header.parse().unwrap());
        let request_type_header = headers
            .get("x-request-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("unknown");
        let request_type = if request_type_header == "es" {
            if needs_upstream {
                "es(forwarded)"
            } else {
                "es(native)"
            }
        } else {
            request_type_header
        };
        write_timing_log(
            &state,
            &headers,
            request_type,
            "POST",
            "/cluster-metrics/_search",
            response.status(),
            t,
        );
        response
    } else {
        Json(response_value).into_response()
    }
}

async fn batch_query_handler(
    State(state): State<AppState>,
    Json(request): Json<BatchQueryRequest>,
) -> impl IntoResponse {
    if request.keys.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "keys must be a non-empty list".to_string(),
        )
            .into_response();
    }

    let fields = request.fields.unwrap_or_else(|| {
        vec![
            "cpu_cores".to_string(),
            "memory_gb".to_string(),
            "network_mbps".to_string(),
        ]
    });
    let percents = request.percents.unwrap_or_else(|| vec![50.0]);
    let agg_kinds: Vec<BatchAggKind> = request
        .aggs
        .iter()
        .filter_map(|agg| match agg.trim().to_ascii_lowercase().as_str() {
            "percentiles" => Some(BatchAggKind::Percentiles),
            "cumulative" => Some(BatchAggKind::Cumulative),
            _ => None,
        })
        .collect();
    let pct_aggs: Vec<PercentileAggregation> = if percents.is_empty() {
        Vec::new()
    } else {
        fields
            .iter()
            .map(|field_name| PercentileAggregation {
                field: field_name.clone(),
                percents: percents.clone(),
                key: None,
                current_time_ms: None,
                time_range_ms: None,
            })
            .collect()
    };
    let cumulative_fields: Vec<(String, MetricField)> = fields
        .iter()
        .filter_map(|field_name| {
            let trimmed = field_name.trim();
            if trimmed.is_empty() {
                return None;
            }
            if !state
                .agg_config
                .cumulative_metrics
                .contains(&trimmed.to_ascii_lowercase())
            {
                return None;
            }
            let field = MetricField::from_spec(trimmed)?;
            Some((field_name.clone(), field))
        })
        .collect();

    let agg_kinds = Arc::new(agg_kinds);
    let pct_aggs = Arc::new(pct_aggs);
    let cumulative_fields = Arc::new(cumulative_fields);

    let mut join_set = JoinSet::new();
    for (idx, key) in request.keys.iter().cloned().enumerate() {
        let state = state.clone();
        let agg_kinds = Arc::clone(&agg_kinds);
        let pct_aggs = Arc::clone(&pct_aggs);
        let cumulative_fields = Arc::clone(&cumulative_fields);
        join_set.spawn_blocking(move || {
            let key_for_query = {
                let trimmed_key = key.trim();
                if trimmed_key.is_empty() {
                    None
                } else {
                    Some(trimmed_key.to_string())
                }
            };
            let key_for_query_ref = key_for_query.as_deref();
            let mut result = BatchQueryResult {
                key,
                percentiles: None,
                cumulative: None,
            };

            for agg_kind in agg_kinds.iter().copied() {
                match agg_kind {
                    BatchAggKind::Percentiles => {
                        if pct_aggs.is_empty() {
                            continue;
                        }
                        let mut field_percentiles = HashMap::new();
                        for pct in pct_aggs.iter() {
                            match handle_percentiles(&state, pct, key_for_query_ref) {
                                Ok(Some(values)) => {
                                    let pct_map: HashMap<String, f64> = values.into_iter().collect();
                                    if !pct_map.is_empty() {
                                        field_percentiles.insert(pct.field.clone(), pct_map);
                                    }
                                }
                                Ok(None) => {}
                                Err(message) => {
                                    return Err(message);
                                }
                            }
                        }
                        if !field_percentiles.is_empty() {
                            result.percentiles = Some(field_percentiles);
                        }
                    }
                    BatchAggKind::Cumulative => {
                        let Some(key_value) = key_for_query_ref else {
                            continue;
                        };
                        if cumulative_fields.is_empty() {
                            continue;
                        }
                        let mut field_cumulative = HashMap::new();
                        for (field_name, field) in cumulative_fields.iter() {
                            let value = state.store.cumulative_value(*field, key_value);
                            field_cumulative.insert(field_name.clone(), value);
                        }
                        if !field_cumulative.is_empty() {
                            result.cumulative = Some(field_cumulative);
                        }
                    }
                }
            }

            Ok((idx, result))
        });
    }

    let mut results: Vec<Option<BatchQueryResult>> = (0..request.keys.len()).map(|_| None).collect();
    let mut error_message: Option<String> = None;
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok((idx, result))) => {
                results[idx] = Some(result);
            }
            Ok(Err(message)) => {
                if error_message.is_none() {
                    error_message = Some(message);
                }
            }
            Err(err) => {
                if error_message.is_none() {
                    error_message = Some(format!("batch query task failed: {err}"));
                }
            }
        }
    }

    if let Some(message) = error_message {
        return (axum::http::StatusCode::BAD_REQUEST, message).into_response();
    }

    let results: Vec<BatchQueryResult> = results.into_iter().flatten().collect();
    Json(BatchQueryResponse { results }).into_response()
}

async fn metrics_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(field_spec): Path<String>,
    Json(query): Json<MetricsQuery>,
) -> impl IntoResponse {
    let mut timing = if state.timing_enabled {
        Some(QueryTiming::new())
    } else {
        None
    };

    // Step 1: Parse field
    let field = match MetricField::from_spec(&field_spec) {
        Some(field) => field,
        None => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                format!("unsupported metric field: {field_spec}"),
            )
                .into_response();
        }
    };
    if let Some(t) = &mut timing {
        t.step("parse_field");
    }

    // Step 2: Validate query
    if query.quantiles.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "quantiles must be a non-empty list".to_string(),
        )
            .into_response();
    }
    if let Some(t) = &mut timing {
        t.step("validate");
    }

    // Step 3: Query percentiles
    let mut results = BTreeMap::new();
    for spec in query.quantiles {
        let percent = match parse_quantile_spec(&spec) {
            Some(percent) if (0.0..=100.0).contains(&percent) => percent,
            Some(_) => {
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("quantile out of range (0-100): {spec}"),
                )
                    .into_response();
            }
            None => {
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("invalid quantile format: {spec}"),
                )
                    .into_response();
            }
        };

        if let Some(value) = state.store.query_percentile(field, percent) {
            results.insert(format!("p{percent}"), value);
        }
    }
    if let Some(t) = &mut timing {
        t.step("query_percentiles");
    }

    // Build response (with timing if enabled)
    if let Some(t) = &mut timing {
        // Build response JSON (timed)
        let mut response_value = json!({
            "field": field_spec,
            "quantiles": results,
        });
        t.step("build_response");

        // Add timing to the response before serialization
        if let Some(obj) = response_value.as_object_mut() {
            obj.insert("_timing".to_string(), t.to_json());
        }

        // Serialize to HTTP response (timed)
        let mut response = Json(response_value).into_response();
        t.step("serialize");
        t.log();

        let timing_header = t.to_header();
        response
            .headers_mut()
            .insert("X-Server-Timing", timing_header.parse().unwrap());
        let request_type = headers
            .get("x-request-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("unknown");
        write_timing_log(
            &state,
            &headers,
            request_type,
            "POST",
            "/metrics/:field",
            response.status(),
            t,
        );
        response
    } else {
        Json(json!({
            "field": field_spec,
            "quantiles": results
        }))
        .into_response()
    }
}
