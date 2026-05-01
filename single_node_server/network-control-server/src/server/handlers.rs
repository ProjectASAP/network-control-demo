use std::collections::{BTreeMap, HashMap};
use std::io::BufRead;
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
use super::query::parse_quantile_spec;
use super::timing::{QueryTiming, write_timing_log};
use super::types::{
    AppState, BatchQueryRequest, BatchQueryResponse, BatchQueryResult, ErrorResponse, IngestRecord,
    MetricsQuery, QueryExecutionPlan, RootResponse, SearchRequest, DocumentAction,
};
use super::upstream::merge_aggregations;

pub async fn run_http_server(
    state: AppState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let log_state = state.clone();
    let metrics_enabled = state.runtime_config.api.enable_metrics_endpoint;
    let batch_enabled = state.runtime_config.api.enable_batch_endpoint;
    let body_limit = state.runtime_config.body_limit_bytes();
    let bind_addr = state.runtime_config.bind_addr();

    // Build routes first (Router<AppState>), then apply layers, then consume state.
    let mut router = Router::new()
        .route("/", get(root_handler).post(ingest_handler_default))
        .route("/:index", post(ingest_handler_with_index))
        .route("/:index/_bulk", post(elasticsearch_bulk_handler_with_index))
        .route("/healthz", get(healthz_handler))
        .route("/:index/_search", post(search_handler));

    if batch_enabled {
        router = router.route("/:index/_batch", post(batch_query_handler));
    }
    if metrics_enabled {
        router = router
            .route("/metrics/:field", post(metrics_handler_default))
            .route("/:index/metrics/:field", post(metrics_handler_with_index));
    }

    // Apply layers before consuming state so from_fn_with_state has access to AppState.
    let app = router
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(from_fn_with_state(log_state, log_request_middleware))
        .with_state(state);

    let listener = TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { eprintln!("received Ctrl+C, shutting down"); },
        _ = terminate => { eprintln!("received SIGTERM, shutting down"); },
    }
}

async fn root_handler(State(state): State<AppState>) -> Json<RootResponse<'static>> {
    let default_index = state
        .runtime_config
        .default_index_name()
        .unwrap_or_else(|| "cluster-metrics".to_string());
    let search_path = state.runtime_config.search_path_for(&default_index);
    Json(RootResponse {
        message: "Portable single-node metrics server. Supports local percentiles and sum aggregations over configured keys; unsupported features are either forwarded upstream or rejected based on runtime config.",
        examples: [
            Box::leak(
                format!(
                    "POST {search_path} {{\"size\":0,\"query\":{{\"bool\":{{\"filter\":[{{\"term\":{{\"cluster\":\"N001\"}}}}]}}}},\"aggs\":{{\"cpu_p50\":{{\"percentiles\":{{\"field\":\"cpu_cores\",\"percents\":[50]}}}}}}}}"
                )
                .into_boxed_str(),
            ),
            Box::leak(
                format!(
                    "POST {search_path} {{\"size\":0,\"query\":{{\"bool\":{{\"filter\":[{{\"term\":{{\"cluster\":\"N001\"}}}}]}}}},\"aggs\":{{\"mem_sum\":{{\"sum\":{{\"field\":\"memory_gb\"}}}}}}}}"
                )
                .into_boxed_str(),
            ),
            "POST /metrics/cpu_cores {\"quantiles\":[\"p50\"],\"node_id\":\"N001\"}",
        ],
    })
}

async fn healthz_handler(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "config_loaded": true,
        "upstream_enabled": state.runtime_config.is_upstream_enabled(),
        "registered_aggregations": ["percentiles", "sum"],
    }))
}

async fn elasticsearch_bulk_handler_with_index(
    State(state): State<AppState>, 
    Path(index): Path<String>,
    _headers: HeaderMap,
    body: Bytes
) -> impl IntoResponse {
    let t0 = Instant::now(); // Track execution time even when timing is disabled to include in response body.
    let mut timing = state.timing_enabled.then(QueryTiming::new);

    // Fetch index-specific config and store before parsing body since bulk endpoint requires index in path and we want to fail fast.
    let index_name = match resolve_index_name(&state, Some(index.as_str())) {
        Ok(index_name) => index_name,
        Err(response) => return response,
    };
    let index_key = AppState::normalize_index_name(&index_name);
    let Some(store) = state.store_for_index(&index_name) else {
        return error_json_response(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorResponse::bad_request(format!("store for index '{index_name}' is not available")),
        );
    };
    let schema = match state.runtime_config.schema_for_index(&index_name) {
        Some(s) => s,
        None => {
            return error_json_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse::bad_request(format!("schema for index '{index_name}' is not available")),
            );
        }
    };
    let ingest_mapping = &schema.ingest_field_mapping;

    if let Some(t) = &mut timing {
        t.step("fetch_index_config");
    }

    // Parse NDJSON body.
    let reader = std::io::BufReader::new(&body[..]);
    let mut action_seen = false;
    let mut inserted = 0usize;
    for line in reader.lines().flatten() {
        let document = match serde_json::from_str::<Value>(&line) {
            Ok(v) => v,
            Err(_) => return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::bad_request("invalid JSON in bulk request body"),
            ),
        };
        if let Some(t) = &mut timing {
            t.step("parse_json");
        }
        // Look for the line before the document source that specifies the action being performed and document ID.
        if !action_seen {
            let action = serde_json::from_value::<DocumentAction>(document);
            match action {
                Ok(DocumentAction::Index(_)) | Ok(DocumentAction::Create(_)) => {
                    action_seen = true;
                }
                Ok(other) => {
                    return error_json_response(
                        axum::http::StatusCode::BAD_REQUEST,
                        ErrorResponse::bad_request(format!("action '{:?}' not supported in bulk endpoint", other)),
                    );
                },
                Err(_) => {
                    return error_json_response(
                        axum::http::StatusCode::BAD_REQUEST,
                        ErrorResponse::bad_request("invalid bulk action in request body"),
                    );
                }
            }
            if let Some(t) = &mut timing {
                t.step("deserialize_action");
            }
            continue;
        }
        match document {
            Value::Object(map) => {
                // eprintln!("Document source for bulk action: {:?}", map);

                // Clear stale data from previous epoch.
                let epoch = map.get(ingest_mapping.epoch_field.as_str()).and_then(|v| v.as_u64());
                if let Some(epoch) = epoch {
                    let mut should_clear = false;
                    match state.current_epoch_by_index.lock() {
                        Ok(mut guard) => {
                            if guard.get(&index_key).copied() != Some(epoch) {
                                guard.insert(index_key.clone(), epoch);
                                should_clear = true;
                            }
                        }
                        Err(_) => {
                            return error_json_response(
                                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                ErrorResponse::bad_request("failed to lock epoch state"),
                            );
                        }
                    }
                    if should_clear {
                        if let Err(message) = store.clear_all() {
                            return error_json_response(
                                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                ErrorResponse::bad_request(message),
                            );
                        }
                    }
                }

                // Built record (row) for this document based on the schema's ingest field mapping. The mapping specifies which JSON fields to extract for each metric, and which JSON field to use as the key.
                let mut record = HashMap::new();
                for (metric_name, json_field) in &ingest_mapping.metric_fields {
                    let value = map.get(json_field).and_then(|v| v.as_f64());
                    if let Some(value) = value {
                        record.insert(metric_name.clone(), value);
                    }
                }
                store.insert_sample(&ingest_mapping.key_field, &record).unwrap_or_else(|err| {
                    eprintln!("Failed to insert sample from bulk document: {err}");
                });
                inserted += 1;
                action_seen = false; // Reset for next action/document pair in the bulk request.
            }
            _ => {
                return error_json_response(
                    axum::http::StatusCode::BAD_REQUEST,
                    ErrorResponse::bad_request("expected document source after bulk action line"),
                );
            }
        }
        if let Some(t) = &mut timing {
            t.step("insert_record");
        }
    }
    if let Some(t) = &mut timing {
        t.log_cumulative();
    }
    Json(json!({
        "took": t0.elapsed().as_millis(),
        "errors": false,
        "items": [],
        "inserted": inserted,
    }))
    .into_response()
}

async fn ingest_handler_default(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    ingest_handler_inner(state, None, body).await
}

async fn ingest_handler_with_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    ingest_handler_inner(state, Some(index), body).await
}

async fn ingest_handler_inner(
    state: AppState,
    index: Option<String>,
    body: Value,
) -> axum::response::Response {
    let index_name = match resolve_index_name(&state, index.as_deref()) {
        Ok(index_name) => index_name,
        Err(response) => return response,
    };
    let index_key = AppState::normalize_index_name(&index_name);
    let Some(store) = state.store_for_index(&index_name) else {
        return error_json_response(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorResponse::bad_request(format!("store for index '{index_name}' is not available")),
        );
    };

    let mut timing = state.timing_enabled.then(QueryTiming::new);
    let schema = match state.runtime_config.schema_for_index(&index_name) {
        Some(s) => s,
        None => {
            return error_json_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse::bad_request(format!("schema for index '{index_name}' is not available")),
            );
        }
    };
    let mapping = &schema.ingest_field_mapping;
    if let Some(t) = &mut timing {
        t.step("parse_json");
    }
    let record = match IngestRecord::from_json(&body, mapping) {
        Ok(record) => record,
        Err(message) => {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::bad_request(message),
            );
        }
    };

    let len = record.len();
    if len == 0 {
        return error_json_response(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse::bad_request("metrics record must contain at least one sample"),
        );
    }
    // Validate all metric arrays have the same length as the key array.
    for (name, values) in &record.metrics {
        if values.len() != len {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::bad_request(format!(
                    "metric '{}' has {} values but key field has {}",
                    name,
                    values.len(),
                    len
                )),
            );
        }
    }

    if let Some(epoch) = record.epoch {
        let mut should_clear = false;
        match state.current_epoch_by_index.lock() {
            Ok(mut guard) => {
                if guard.get(&index_key).copied() != Some(epoch) {
                    guard.insert(index_key.clone(), epoch);
                    should_clear = true;
                }
            }
            Err(_) => {
                return error_json_response(
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorResponse::bad_request("failed to lock epoch state"),
                );
            }
        }
        if should_clear {
            if let Err(message) = store.clear_all() {
                return error_json_response(
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorResponse::bad_request(message),
                );
            }
        }
    }

    let mut inserted = 0usize;
    for idx in 0..len {
        let key = record.key[idx].trim();
        if key.is_empty() {
            continue;
        }
        // Build per-sample metric map for this row.
        let mut sample_metrics = std::collections::HashMap::new();
        for (name, values) in &record.metrics {
            sample_metrics.insert(name.clone(), values[idx]);
        }
        if let Err(message) = store.insert_sample(key, &sample_metrics) {
            return error_json_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse::bad_request(message),
            );
        }
        inserted += 1;
    }
    if let Some(t) = &mut timing {
        t.step("insert_samples");
        t.log();
    }

    Json(json!({ "index": index_name, "inserted": inserted })).into_response()
}

async fn search_handler(
    State(state): State<AppState>,
    Path(index): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let index_name = match resolve_index_name(&state, Some(index.as_str())) {
        Ok(index_name) => index_name,
        Err(response) => return response,
    };
    let Some(store) = state.store_for_index(&index_name) else {
        return error_json_response(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorResponse::bad_request(format!("store for index '{index_name}' is not available")),
        );
    };

    let mut timing = state.timing_enabled.then(QueryTiming::new);
    if let Some(logger) = &state.payload_logger {
        if logger.is_active() {
            let payload = std::str::from_utf8(&body).unwrap_or("<non-utf8>");
            logger.log(&state.runtime_config.search_path_for(&index_name), payload);
        }
    }
    let request: SearchRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::bad_request(format!("invalid JSON body: {err}")),
            );
        }
    };
    if let Some(t) = &mut timing {
        t.step("parse_json");
    }

    let plan = match state.request_planner.plan_search(&state, &request, &index_name) {
        Ok(plan) => plan,
        Err(message) => {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::bad_request(message),
            );
        }
    };

    let can_fallback = state.runtime_config.is_upstream_enabled();
    if !plan.unsupported_features.is_empty()
        && (state.runtime_config.api.strict_mode || !can_fallback)
    {
        let details = plan
            .unsupported_features
            .iter()
            .map(|feature| format!("{}: {}", feature.code, feature.message))
            .collect();
        return error_json_response(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse::unsupported(
                "request contains unsupported local query features",
                details,
                state.aggregation_engine.supported_features(),
            ),
        );
    }
    if (plan.has_other_fields || !plan.forwarded_aggs.is_empty())
        && (state.runtime_config.api.strict_mode || !can_fallback)
    {
        let mut details = Vec::new();
        if plan.has_other_fields {
            details.push(
                "request contains unsupported top-level search fields outside size/query/aggs"
                    .to_string(),
            );
        }
        if !plan.forwarded_aggs.is_empty() {
            details.push(format!(
                "request contains aggregations that are not locally supported: {}",
                plan.forwarded_aggs
                    .iter()
                    .cloned()
                    .collect::<Vec<String>>()
                    .join(", ")
            ));
        }
        return error_json_response(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse::unsupported(
                "request requires unsupported features and cannot be satisfied locally",
                details,
                state.aggregation_engine.supported_features(),
            ),
        );
    }

    let mut handled = BTreeMap::new();
    let can_execute_local = plan.unsupported_features.is_empty();
    if can_execute_local {
        for local_agg in &plan.local_aggs {
            let t0 = Instant::now();
            let result = match state
                .aggregation_engine
                .evaluate(&state, store.as_ref(), &plan.context, local_agg)
            {
                Ok(result) => result,
                Err(message) => {
                    return error_json_response(
                        axum::http::StatusCode::BAD_REQUEST,
                        ErrorResponse::bad_request(message),
                    );
                }
            };
            if let Some(t) = &mut timing {
                t.record("sketch_estimate", t0.elapsed().as_secs_f64() * 1000.0);
            }
            if let Some(value) = result {
                handled.insert(local_agg.name.clone(), value);
            }
        }
    }
    if let Some(t) = &mut timing {
        t.step("aggregations");
    }

    let should_forward = should_forward_request(&state, &plan, can_execute_local);
    let mut response_value = if should_forward {
        let upstream_body = match build_upstream_body(&request, &plan, can_execute_local) {
            Ok(value) => value,
            Err(message) => {
                return error_json_response(
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorResponse::bad_request(message),
                );
            }
        };
        if let Some(t) = &mut timing {
            t.step("prepare_upstream");
        }
        let value = match state
            .upstream_client
            .forward(&state, &index_name, &headers, &upstream_body)
            .await
        {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Some(t) = &mut timing {
            t.step("upstream");
        }
        value
    } else {
        json!({ "aggregations": {} })
    };

    merge_aggregations(&mut response_value, handled);
    if let Some(t) = &mut timing {
        t.step("merge");
    }

    if let Some(t) = &mut timing {
        if let Some(obj) = response_value.as_object_mut() {
            obj.insert("_timing".to_string(), t.to_json());
        }
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
            &state.runtime_config.search_path_for(&index_name),
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
    Path(index): Path<String>,
    Json(request): Json<BatchQueryRequest>,
) -> impl IntoResponse {
    let index_name = match resolve_index_name(&state, Some(index.as_str())) {
        Ok(index_name) => index_name,
        Err(response) => return response,
    };
    let Some(store) = state.store_for_index(&index_name) else {
        return error_json_response(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorResponse::bad_request(format!("store for index '{index_name}' is not available")),
        );
    };

    let mut timing = state.timing_enabled.then(QueryTiming::new);
    if let Some(logger) = &state.payload_logger {
        if logger.is_active() {
            let payload = serde_json::to_string(&request).unwrap_or_default();
            logger.log(&state.runtime_config.batch_path_for(&index_name), &payload);
        }
    }
    if let Some(t) = &mut timing {
        t.step("parse_json");
    }
    if request.keys.is_empty() {
        return error_json_response(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse::bad_request("keys must be a non-empty list"),
        );
    }

    let Some(fields) = request.fields else {
        return error_json_response(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse::bad_request("fields must be specified for batch queries"),
        );
    };
    let percents = request.percents.unwrap_or_else(|| {
        state
            .runtime_config
            .api
            .default_batch_percents
            .clone()
    });

    let supported_aggs = state.runtime_config.aggregation_names();
    let mut requested_aggs = Vec::new();
    for agg in &request.aggs {
        let normalized = agg.trim().to_ascii_lowercase();
        if !supported_aggs.contains(&normalized) {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::unsupported(
                    format!("unsupported batch aggregation '{agg}'"),
                    Vec::new(),
                    state.aggregation_engine.supported_features(),
                ),
            );
        }
        let Some(registration) = state.aggregation_engine.registration(&normalized) else {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::unsupported(
                    format!("aggregation '{agg}' is not registered"),
                    Vec::new(),
                    state.aggregation_engine.supported_features(),
                ),
            );
        };
        if !registration.supports_batch {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::unsupported(
                    format!("aggregation '{agg}' is not supported by the batch endpoint"),
                    Vec::new(),
                    state.aggregation_engine.supported_features(),
                ),
            );
        }
        requested_aggs.push(normalized);
    }

    for key in &request.keys {
        if state.runtime_config.api.strict_mode && !store.contains_key(key.trim()) {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::bad_request(format!("unknown key '{}'", key)),
            );
        }
    }

    let fields: Vec<String> = fields
        .into_iter()
        .filter(|field| {
            super::types::metric_field_for_name(&state.runtime_config, &index_name, field).is_some()
        })
        .collect();
    if fields.is_empty() {
        return error_json_response(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse::bad_request(
                "batch request does not contain any supported metric fields",
            ),
        );
    }

    let requested_aggs = Arc::new(requested_aggs);
    let percents = Arc::new(percents);
    let fields = Arc::new(fields);
    let index_name_for_tasks = index_name.clone();
    let mut join_set = JoinSet::new();
    for (idx, key) in request.keys.iter().cloned().enumerate() {
        let state = state.clone();
        let store = Arc::clone(&store);
        let requested_aggs = Arc::clone(&requested_aggs);
        let percents = Arc::clone(&percents);
        let fields = Arc::clone(&fields);
        let index_name = index_name_for_tasks.clone();
        join_set.spawn_blocking(move || {
            let mut result = BatchQueryResult {
                key: key.clone(),
                percentiles: None,
                sum: None,
            };
            let context = super::types::QueryContext {
                index_name: Some(index_name),
                key: Some(key.clone()),
                epoch: None,
            };
            for agg in requested_aggs.iter() {
                match agg.as_str() {
                    "percentiles" => {
                        let mut field_percentiles = HashMap::new();
                        for field in fields.iter() {
                            let plan = super::types::LocalAggregationPlan {
                                name: field.clone(),
                                kind: super::types::AggregationKind::Percentiles(
                                    super::types::PercentileAggregation {
                                        field: field.clone(),
                                        percents: percents.as_ref().clone(),
                                    },
                                ),
                            };
                            if let Some(value) =
                                state
                                    .aggregation_engine
                                    .evaluate(&state, store.as_ref(), &context, &plan)?
                            {
                                let values = value
                                    .get("values")
                                    .and_then(Value::as_object)
                                    .ok_or_else(|| "invalid percentiles response".to_string())?;
                                let mut percentiles = HashMap::new();
                                for (percent, value) in values {
                                    if let Some(value) = value.as_f64() {
                                        percentiles.insert(percent.clone(), value);
                                    }
                                }
                                if !percentiles.is_empty() {
                                    field_percentiles.insert(field.clone(), percentiles);
                                }
                            }
                        }
                        if !field_percentiles.is_empty() {
                            result.percentiles = Some(field_percentiles);
                        }
                    }
                    "sum" => {
                        let mut field_sum = HashMap::new();
                        for field in fields.iter() {
                            let plan = super::types::LocalAggregationPlan {
                                name: field.clone(),
                                kind: super::types::AggregationKind::Sum(
                                    super::types::SumAggregation {
                                        field: field.clone(),
                                    },
                                ),
                            };
                            if let Some(value) =
                                state
                                    .aggregation_engine
                                    .evaluate(&state, store.as_ref(), &context, &plan)?
                            {
                                if let Some(value) = value.get("value").and_then(Value::as_f64) {
                                    field_sum.insert(field.clone(), value);
                                }
                            }
                        }
                        if !field_sum.is_empty() {
                            result.sum = Some(field_sum);
                        }
                    }
                    _ => {}
                }
            }
            Ok::<(usize, BatchQueryResult), String>((idx, result))
        });
    }

    let mut results: Vec<Option<BatchQueryResult>> =
        (0..request.keys.len()).map(|_| None).collect();
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok((idx, result))) => results[idx] = Some(result),
            Ok(Err(message)) => {
                return error_json_response(
                    axum::http::StatusCode::BAD_REQUEST,
                    ErrorResponse::bad_request(message),
                );
            }
            Err(err) => {
                return error_json_response(
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorResponse::bad_request(format!("batch query task failed: {err}")),
                );
            }
        }
    }

    if let Some(t) = &mut timing {
        t.step("batch_execute");
        t.log();
    }

    Json(BatchQueryResponse {
        results: results.into_iter().flatten().collect(),
    })
    .into_response()
}

async fn metrics_handler_default(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(field_spec): Path<String>,
    Json(query): Json<MetricsQuery>,
) -> impl IntoResponse {
    metrics_handler_inner(state, None, headers, field_spec, query).await
}

async fn metrics_handler_with_index(
    State(state): State<AppState>,
    Path((index, field_spec)): Path<(String, String)>,
    headers: HeaderMap,
    Json(query): Json<MetricsQuery>,
) -> impl IntoResponse {
    metrics_handler_inner(state, Some(index), headers, field_spec, query).await
}

async fn metrics_handler_inner(
    state: AppState,
    index: Option<String>,
    headers: HeaderMap,
    field_spec: String,
    query: MetricsQuery,
) -> axum::response::Response {
    if !state.runtime_config.api.enable_metrics_endpoint {
        return error_json_response(
            axum::http::StatusCode::NOT_FOUND,
            ErrorResponse::bad_request("metrics endpoint is disabled"),
        );
    }

    let index_name = match resolve_index_name(&state, index.as_deref()) {
        Ok(index_name) => index_name,
        Err(response) => return response,
    };
    let Some(store) = state.store_for_index(&index_name) else {
        return error_json_response(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorResponse::bad_request(format!("store for index '{index_name}' is not available")),
        );
    };

    let mut timing = state.timing_enabled.then(QueryTiming::new);
    if let Some(logger) = &state.payload_logger {
        if logger.is_active() {
            let payload = serde_json::to_string(&query).unwrap_or_default();
            logger.log(&format!("/{index_name}/metrics/{field_spec}"), &payload);
        }
    }
    let field = match super::types::metric_field_for_name(&state.runtime_config, &index_name, &field_spec) {
        Some(field) => field,
        None => {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::bad_request(format!("unsupported metric field: {field_spec}")),
            );
        }
    };
    if let Some(t) = &mut timing {
        t.step("parse_field");
    }

    if query.quantiles.is_empty() {
        return error_json_response(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse::bad_request("quantiles must be a non-empty list"),
        );
    }
    if let Some(t) = &mut timing {
        t.step("validate");
    }

    let node_id = query
        .node_id
        .as_ref()
        .map(|id| id.trim())
        .filter(|id| !id.is_empty());
    let Some(node_id) = node_id else {
        return error_json_response(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse::bad_request("node_id is required"),
        );
    };

    let mut results = BTreeMap::new();
    for spec in query.quantiles {
        let percent = match parse_quantile_spec(&spec) {
            Some(percent) if (0.0..=100.0).contains(&percent) => percent,
            Some(_) => {
                return error_json_response(
                    axum::http::StatusCode::BAD_REQUEST,
                    ErrorResponse::bad_request(format!("quantile out of range (0-100): {spec}")),
                );
            }
            None => {
                return error_json_response(
                    axum::http::StatusCode::BAD_REQUEST,
                    ErrorResponse::bad_request(format!("invalid quantile format: {spec}")),
                );
            }
        };
        let values = match store.query_percentiles(node_id, &field, &[percent]) {
            Ok(values) => values,
            Err(message) => {
                return error_json_response(
                    axum::http::StatusCode::BAD_REQUEST,
                    ErrorResponse::bad_request(message),
                );
            }
        };
        if let Some(Some(value)) = values.first() {
            results.insert(format!("p{percent}"), *value);
        }
    }
    if let Some(t) = &mut timing {
        t.step("query_percentiles");
    }

    if let Some(t) = &mut timing {
        let mut response_value = json!({
            "field": field_spec,
            "quantiles": results,
            "deprecated": true,
        });
        t.step("build_response");
        if let Some(obj) = response_value.as_object_mut() {
            obj.insert("_timing".to_string(), t.to_json());
        }
        let mut response = Json(response_value).into_response();
        t.step("serialize");
        t.log();
        response
            .headers_mut()
            .insert("X-Server-Timing", t.to_header().parse().unwrap());
        write_timing_log(
            &state,
            &headers,
            "metrics_compat",
            "POST",
            &format!("/{index_name}/metrics/:field"),
            response.status(),
            t,
        );
        response
    } else {
        Json(json!({
            "field": field_spec,
            "quantiles": results,
            "deprecated": true,
        }))
        .into_response()
    }
}

fn resolve_index_name(
    state: &AppState,
    requested: Option<&str>,
) -> Result<String, axum::response::Response> {
    let resolved = match requested {
        Some(index) if !index.trim().is_empty() => index.trim().to_string(),
        _ => state.runtime_config.default_index_name().ok_or_else(|| {
            error_json_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse::bad_request("no configured indices are available"),
            )
        })?,
    };

    if !state.runtime_config.supports_index(&resolved) {
        return Err(error_json_response(
            axum::http::StatusCode::NOT_FOUND,
            ErrorResponse::bad_request(format!("unsupported index '{resolved}'")),
        ));
    }

    Ok(resolved)
}

fn should_forward_request(
    state: &AppState,
    plan: &QueryExecutionPlan,
    can_execute_local: bool,
) -> bool {
    if !state.runtime_config.is_upstream_enabled() {
        return false;
    }
    if !can_execute_local {
        return true;
    }
    !plan.forwarded_aggs.is_empty() || plan.has_other_fields
}

fn build_upstream_body(
    request: &SearchRequest,
    plan: &QueryExecutionPlan,
    can_execute_local: bool,
) -> Result<Value, String> {
    let mut upstream_body = serde_json::to_value(request)
        .map_err(|err| format!("failed to build upstream payload: {err}"))?;
    prune_nulls(&mut upstream_body);
    if can_execute_local {
        let handled_names: std::collections::HashSet<String> =
            plan.local_aggs.iter().map(|agg| agg.name.clone()).collect();
        if let Some(aggs_obj) = upstream_body.get_mut("aggs").and_then(Value::as_object_mut) {
            aggs_obj.retain(|name, _| !handled_names.contains(name));
            if aggs_obj.is_empty() {
                upstream_body
                    .as_object_mut()
                    .expect("upstream body should be a JSON object")
                    .remove("aggs");
            }
        }
    }
    Ok(upstream_body)
}

fn prune_nulls(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|_, child| {
                prune_nulls(child);
                !child.is_null()
            });
        }
        Value::Array(items) => {
            for item in items {
                prune_nulls(item);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::server::types::QueryExecutionPlan;

    use super::{build_upstream_body, prune_nulls};

    #[test]
    fn prune_nulls_removes_null_object_fields() {
        let mut value = json!({
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"epoch": {"gte": 1}}}
                    ]
                }
            },
            "aggs": null
        });

        prune_nulls(&mut value);

        assert_eq!(
            value,
            json!({
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {"epoch": {"gte": 1}}}
                        ]
                    }
                }
            })
        );
    }

    #[test]
    fn build_upstream_body_omits_empty_aggs() {
        let request: crate::server::types::SearchRequest = serde_json::from_value(json!({
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"epoch": {"gte": 1}}}
                    ]
                }
            }
        }))
        .unwrap();

        let plan = QueryExecutionPlan {
            context: Default::default(),
            local_aggs: Vec::new(),
            forwarded_aggs: Default::default(),
            unsupported_features: Vec::new(),
            has_other_fields: false,
        };

        let body = build_upstream_body(&request, &plan, false).unwrap();

        assert_eq!(
            body,
            json!({
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {"epoch": {"gte": 1}}}
                        ]
                    }
                }
            })
        );
    }
}

fn error_json_response(
    status: axum::http::StatusCode,
    body: ErrorResponse,
) -> axum::response::Response {
    (status, Json(body)).into_response()
}
