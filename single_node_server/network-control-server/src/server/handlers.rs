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

use super::logging::log_request_middleware;
use super::query::{handle_cumulative, handle_percentiles, parse_quantile_spec};
use super::timing::{QueryTiming, write_timing_log};
use super::types::{
    AggregationKind, AppState, BatchQueryRequest, BatchQueryResponse, BatchQueryResult,
    MetricsQuery, PercentileAggregation, RootResponse, SearchRequest,
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
        message: "POST /cluster-metrics/_search with aggs for percentiles or cumulative (cumulative requires a key). Other aggs (e.g. avg) are forwarded to Elasticsearch.",
        examples: [
            "POST /cluster-metrics/_search {\"aggs\":{\"cpu_quantiles\":{\"percentiles\":{\"field\":\"cpu_cores\",\"percents\":[10,50]}}}}",
            "POST /cluster-metrics/_search {\"aggs\":{\"cpu_cumulative\":{\"cumulative\":{\"field\":\"cpu_cores\",\"key\":\"cluster-c;cache\"}}}}",
            "POST /cluster-metrics/_search {\"aggs\":{\"mem_quantiles\":{\"percentiles\":{\"field\":\"memory_gb\",\"percents\":[25,75]}}}}",
        ],
    })
}

async fn ingest_handler(
    State(state): State<AppState>,
    Json(record): Json<Value>,
) -> impl IntoResponse {
    let parsed = match parse_ingest_record(&state, &record) {
        Ok(parsed) => parsed,
        Err(message) => {
            return (axum::http::StatusCode::BAD_REQUEST, message).into_response();
        }
    };

    let len = parsed.len;
    if len == 0 {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "metrics record must contain at least one sample".to_string(),
        )
            .into_response();
    }
    if let Some(epoch) = parsed.epoch {
        let mut should_clear = false;
        match state.current_epoch.lock() {
            Ok(mut guard) => {
                if guard.map_or(true, |current| current != epoch) {
                    *guard = Some(epoch);
                    should_clear = true;
                }
            }
            Err(_) => {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to lock epoch state".to_string(),
                )
                    .into_response();
            }
        }
        if should_clear {
            if let Err(message) = state.metric_store.clear_all() {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    message,
                )
                    .into_response();
            }
            eprintln!("epoch switch detected: cleared in-memory store for epoch {epoch}");
        }
    }

    let mut inserted = 0usize;
    for idx in 0..len {
        let sample_labels: HashMap<String, String> = parsed
            .label_columns
            .iter()
            .map(|(name, values)| (name.clone(), values[idx].clone()))
            .collect();

        let sample_metrics: HashMap<String, f64> = parsed
            .metric_columns
            .iter()
            .map(|(name, values)| (name.clone(), values[idx]))
            .collect();

        if sample_metrics.is_empty() {
            continue;
        }

        let group_keys = build_group_keys(&parsed.label_combinations, &sample_labels);
        if let Err(message) = state
            .metric_store
            .insert_metrics(&group_keys, &sample_metrics)
        {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
        }
        inserted += 1;
    }

    Json(json!({ "inserted": inserted })).into_response()
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
    let query_supported = true;
    let query_key = None;
    let has_other = request._other.keys().any(|key| key.as_str() != "query");

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
        let mut fields: Vec<String> = state
            .agg_config
            .supported_metric_fields()
            .into_iter()
            .collect();
        fields.sort();
        fields
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
            })
            .collect()
    };
    let cumulative_fields: Vec<(String, String)> = fields
        .iter()
        .filter_map(|field_name| {
            let trimmed = field_name.trim();
            if trimmed.is_empty() {
                return None;
            }
            let normalized = normalize_metric_name(trimmed);
            if !state.agg_config.supports_cumulative_field(&normalized) {
                return None;
            }
            Some((field_name.clone(), normalized))
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
                                    let pct_map: HashMap<String, f64> =
                                        values.into_iter().collect();
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
                            let value = state
                                .metric_store
                                .cumulative_value(Some(key_value), field)
                                .map_err(|message| message)?;
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

    let mut results: Vec<Option<BatchQueryResult>> =
        (0..request.keys.len()).map(|_| None).collect();
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
    let field = normalize_metric_name(&field_spec);
    if !state
        .agg_config
        .supports_percentile_field(&field, true)
    {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            format!("unsupported metric field: {field_spec}"),
        )
            .into_response();
    }
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

    let node_id = query
        .node_id
        .as_ref()
        .map(|id| id.trim())
        .filter(|id| !id.is_empty());
    let node_id = match node_id {
        Some(id) => id.to_string(),
        None => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "node_id is required".to_string(),
            )
                .into_response();
        }
    };
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

        let values = match state
            .metric_store
            .query_percentiles(Some(&node_id), &field, &[percent])
        {
            Ok(values) => values,
            Err(message) => {
                return (axum::http::StatusCode::BAD_REQUEST, message).into_response();
            }
        };
        if let Some(Some(value)) = values.get(0) {
            results.insert(format!("p{percent}"), *value);
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

struct ParsedIngestRecord {
    epoch: Option<u64>,
    len: usize,
    label_columns: HashMap<String, Vec<String>>,
    metric_columns: HashMap<String, Vec<f64>>,
    label_combinations: Vec<Vec<String>>,
}

fn parse_ingest_record(state: &AppState, value: &Value) -> Result<ParsedIngestRecord, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "ingest payload must be a JSON object".to_string())?;

    let epoch = object.get("epoch").map(parse_epoch).transpose()?;
    let mut string_columns: HashMap<String, Vec<String>> = HashMap::new();
    let mut numeric_columns: HashMap<String, Vec<f64>> = HashMap::new();
    let mut row_len: Option<usize> = None;

    for (name, col_value) in object {
        if name == "epoch" {
            continue;
        }
        let normalized_name = normalize_metric_name(name);
        let array = col_value
            .as_array()
            .ok_or_else(|| format!("field '{}' must be an array", name))?;

        if let Some(existing) = row_len {
            if existing != array.len() {
                return Err("metrics record fields must have equal lengths".to_string());
            }
        } else {
            row_len = Some(array.len());
        }

        let mut all_numbers = true;
        let mut num_values = Vec::with_capacity(array.len());
        for item in array {
            if let Some(value) = item.as_f64() {
                num_values.push(value);
            } else {
                all_numbers = false;
                break;
            }
        }
        if all_numbers {
            numeric_columns.insert(normalized_name, num_values);
            continue;
        }

        let mut all_strings = true;
        let mut str_values = Vec::with_capacity(array.len());
        for item in array {
            if let Some(value) = item.as_str() {
                str_values.push(value.trim().to_string());
            } else {
                all_strings = false;
                break;
            }
        }
        if all_strings {
            string_columns.insert(normalized_name, str_values);
            continue;
        }

        return Err(format!(
            "field '{}' must be either a numeric array or string array",
            name
        ));
    }

    let len = row_len.unwrap_or(0);
    let mut label_combinations: Vec<Vec<String>> = string_columns
        .keys()
        .cloned()
        .map(|name| vec![name])
        .collect();
    label_combinations.extend(state.agg_config.label_combinations.iter().cloned());

    Ok(ParsedIngestRecord {
        epoch,
        len,
        label_columns: string_columns,
        metric_columns: numeric_columns,
        label_combinations,
    })
}

fn parse_epoch(value: &Value) -> Result<u64, String> {
    value
        .as_u64()
        .ok_or_else(|| "epoch must be an unsigned integer".to_string())
}

fn build_group_keys(
    label_combinations: &[Vec<String>],
    labels: &HashMap<String, String>,
) -> Vec<String> {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();

    for labels_for_key in label_combinations {
        let mut parts = Vec::with_capacity(labels_for_key.len());
        let mut missing = false;

        for label_name in labels_for_key {
            let Some(value) = labels.get(label_name) else {
                missing = true;
                break;
            };
            let trimmed = value.trim();
            if trimmed.is_empty() {
                missing = true;
                break;
            }
            parts.push(trimmed.to_string());
        }

        if missing {
            continue;
        }

        let key = parts.join(";");
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }

    keys
}

fn normalize_metric_name(name: &str) -> String {
    name.trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace(' ', "_")
}
