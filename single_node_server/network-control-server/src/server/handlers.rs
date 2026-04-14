use std::collections::{BTreeMap, HashMap};
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
    MetricsQuery, QueryExecutionPlan, RootResponse, SearchRequest,
};
use super::upstream::merge_aggregations;

pub async fn run_http_server(
    state: AppState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let log_state = state.clone();
    let search_path = state.runtime_config.search_path();
    let batch_path = state.runtime_config.batch_path();
    let metrics_enabled = state.runtime_config.api.enable_metrics_endpoint;
    let batch_enabled = state.runtime_config.api.enable_batch_endpoint;
    let body_limit = state.runtime_config.body_limit_bytes();
    let bind_addr = state.runtime_config.bind_addr();

    // Build routes first (Router<AppState>), then apply layers, then consume state.
    let mut router = Router::new()
        .route("/", get(root_handler).post(ingest_handler))
        .route("/healthz", get(healthz_handler))
        .route(&search_path, post(search_handler));

    if batch_enabled {
        router = router.route(&batch_path, post(batch_query_handler));
    }
    if metrics_enabled {
        router = router.route("/metrics/:field", post(metrics_handler));
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
    let search_path = state.runtime_config.search_path();
    Json(RootResponse {
        message: "Portable single-node metrics server. Supports local percentiles and cumulative aggregations over configured keys; unsupported features are either forwarded upstream or rejected based on runtime config.",
        examples: [
            Box::leak(
                format!(
                    "POST {search_path} {{\"size\":0,\"query\":{{\"bool\":{{\"filter\":[{{\"term\":{{\"cluster\":\"N001\"}}}}]}}}},\"aggs\":{{\"cpu_p50\":{{\"percentiles\":{{\"field\":\"cpu_cores\",\"percents\":[50]}}}}}}}}"
                )
                .into_boxed_str(),
            ),
            Box::leak(
                format!(
                    "POST {search_path} {{\"size\":0,\"aggs\":{{\"mem_sum\":{{\"cumulative\":{{\"field\":\"memory_gb\",\"key\":\"N001\"}}}}}}}}"
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
        "registered_aggregations": state.runtime_config.query_support.aggregations.clone(),
    }))
}

async fn ingest_handler(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let mut timing = state.timing_enabled.then(QueryTiming::new);
    let mapping = &state.runtime_config.schema.ingest_field_mapping;
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
    if let Some(ref task) = record.task {
        if task.len() != len {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::bad_request("task field length must match key field length"),
            );
        }
    }

    if let Some(epoch) = record.epoch {
        let mut should_clear = false;
        match state.current_epoch.lock() {
            Ok(mut guard) => {
                if guard.map_or(true, |current| current != epoch) {
                    *guard = Some(epoch);
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
            if let Err(message) = state.store.clear_all() {
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
        // If task is configured and present, skip rows with empty task.
        if let Some(ref task) = record.task {
            if task[idx].trim().is_empty() {
                continue;
            }
        }
        // Build per-sample metric map for this row.
        let mut sample_metrics = std::collections::HashMap::new();
        for (name, values) in &record.metrics {
            sample_metrics.insert(name.clone(), values[idx]);
        }
        if let Err(message) = state.store.insert_sample(key, &sample_metrics) {
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

    Json(json!({ "inserted": inserted })).into_response()
}

async fn search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let mut timing = state.timing_enabled.then(QueryTiming::new);
    if let Some(logger) = &state.payload_logger {
        if logger.is_active() {
            let payload = std::str::from_utf8(&body).unwrap_or("<non-utf8>");
            logger.log(&state.runtime_config.search_path(), payload);
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

    let plan = match state.request_planner.plan_search(&state, &request) {
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
                .evaluate(&state, &plan.context, local_agg)
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
            .forward(&state, &headers, &upstream_body)
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
            &state.runtime_config.search_path(),
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
    let mut timing = state.timing_enabled.then(QueryTiming::new);
    if let Some(logger) = &state.payload_logger {
        if logger.is_active() {
            let payload = serde_json::to_string(&request).unwrap_or_default();
            logger.log(&state.runtime_config.batch_path(), &payload);
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

    let fields = request.fields.unwrap_or_else(|| {
        state
            .runtime_config
            .query_support
            .default_batch_fields
            .clone()
    });
    let percents = request.percents.unwrap_or_else(|| {
        state
            .runtime_config
            .query_support
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
        if state.runtime_config.api.strict_mode && !state.store.contains_key(key.trim()) {
            return error_json_response(
                axum::http::StatusCode::BAD_REQUEST,
                ErrorResponse::bad_request(format!("unknown key '{}'", key)),
            );
        }
    }

    let fields: Vec<String> = fields
        .into_iter()
        .filter(|field| super::types::metric_field_for_name(&state.runtime_config, field).is_some())
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
    let mut join_set = JoinSet::new();
    for (idx, key) in request.keys.iter().cloned().enumerate() {
        let state = state.clone();
        let requested_aggs = Arc::clone(&requested_aggs);
        let percents = Arc::clone(&percents);
        let fields = Arc::clone(&fields);
        join_set.spawn_blocking(move || {
            let mut result = BatchQueryResult {
                key: key.clone(),
                percentiles: None,
                cumulative: None,
            };
            let context = super::types::QueryContext {
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
                                        key: None,
                                    },
                                ),
                            };
                            if let Some(value) =
                                state.aggregation_engine.evaluate(&state, &context, &plan)?
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
                    "cumulative" => {
                        let mut field_cumulative = HashMap::new();
                        for field in fields.iter() {
                            let plan = super::types::LocalAggregationPlan {
                                name: field.clone(),
                                kind: super::types::AggregationKind::Cumulative(
                                    super::types::CumulativeAggregation {
                                        field: field.clone(),
                                        key: None,
                                    },
                                ),
                            };
                            if let Some(value) =
                                state.aggregation_engine.evaluate(&state, &context, &plan)?
                            {
                                if let Some(value) = value.get("value").and_then(Value::as_f64) {
                                    field_cumulative.insert(field.clone(), value);
                                }
                            }
                        }
                        if !field_cumulative.is_empty() {
                            result.cumulative = Some(field_cumulative);
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

async fn metrics_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(field_spec): Path<String>,
    Json(query): Json<MetricsQuery>,
) -> impl IntoResponse {
    if !state.runtime_config.api.enable_metrics_endpoint {
        return error_json_response(
            axum::http::StatusCode::NOT_FOUND,
            ErrorResponse::bad_request("metrics endpoint is disabled"),
        );
    }

    let mut timing = state.timing_enabled.then(QueryTiming::new);
    if let Some(logger) = &state.payload_logger {
        if logger.is_active() {
            let payload = serde_json::to_string(&query).unwrap_or_default();
            logger.log(&format!("/metrics/{field_spec}"), &payload);
        }
    }
    let field = match super::types::metric_field_for_name(&state.runtime_config, &field_spec) {
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
        let values = match state.store.query_percentiles(node_id, &field, &[percent]) {
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
            "/metrics/:field",
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
        let request = crate::server::types::SearchRequest {
            size: Some(0),
            query: Some(json!({
                "bool": {
                    "filter": [
                        {"range": {"epoch": {"gte": 1}}}
                    ]
                }
            })),
            aggs: None,
            other: Default::default(),
        };

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
