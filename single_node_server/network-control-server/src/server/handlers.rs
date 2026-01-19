use std::collections::{BTreeMap, HashMap};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
    middleware::from_fn_with_state,
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::metrics::MetricField;

use super::logging::log_request_middleware;
use super::query::{
    extract_query_key, handle_cumulative, handle_frequency, handle_percentiles,
    handle_top_entities, parse_quantile_spec,
};
use super::timing::{QueryTiming, write_timing_log};
use super::types::{
    AggregationKind, AppState, BatchQueryRequest, BatchQueryResponse, BatchQueryResult,
    IngestRecord, MetricsQuery, PercentileAggregation, QueryKeyStatus, RootResponse, SearchRequest,
    TopEntitiesResult,
};
use super::upstream::{forward_to_upstream, merge_aggregations};

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
        .layer(from_fn_with_state(log_state, log_request_middleware));

    let listener = TcpListener::bind("0.0.0.0:10101").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn root_handler() -> Json<RootResponse<'static>> {
    Json(RootResponse {
        message: "POST /cluster-metrics/_search with aggs for percentiles, frequency, top_entities, or cumulative (cumulative requires a key). Other aggs (e.g. avg) are forwarded to Elasticsearch.",
        examples: [
            "POST /cluster-metrics/_search {\"aggs\":{\"cpu_quantiles\":{\"percentiles\":{\"field\":\"cpu_cores\",\"percents\":[10,50]}}}}",
            "POST /cluster-metrics/_search {\"aggs\":{\"cpu_frequency\":{\"frequency\":{\"field\":\"cpu_cores\",\"key\":\"cluster-c;cache\",\"value\":4}}}}",
            "POST /cluster-metrics/_search {\"aggs\":{\"top_cpu\":{\"top_entities\":{\"field\":\"cpu_cores\"}}}}",
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

    let mut inserted = 0usize;
    for idx in 0..len {
        let cluster = record.cluster[idx].trim();
        let task = record.task[idx].trim();
        if cluster.is_empty() || task.is_empty() {
            continue;
        }
        if let Err(message) = state.store.insert(
            cluster,
            task,
            record.cpu_cores[idx],
            record.memory_gb[idx],
            record.network_mbps[idx],
        ) {
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
    let request_value: Value = match serde_json::from_slice(&body) {
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

    // Step 2: Deserialize into SearchRequest
    let request: SearchRequest = match serde_json::from_value(request_value.clone()) {
        Ok(value) => value,
        Err(err) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                format!("invalid search request: {err}"),
            )
                .into_response();
        }
    };
    if let Some(t) = &mut timing {
        t.step("deserialize");
    }

    // Step 3: Process aggregations
    let mut handled = BTreeMap::new();
    let mut handled_names = Vec::new();
    let mut unhandled = BTreeMap::new();
    let query_status = extract_query_key(request._other.get("query"));
    let query_supported = !matches!(query_status, QueryKeyStatus::Unsupported);
    let query_key = match &query_status {
        QueryKeyStatus::Key(key) => Some(key.clone()),
        _ => None,
    };
    let has_other = request._other.keys().any(|key| key.as_str() != "query")
        || matches!(query_status, QueryKeyStatus::Unsupported);

    if let Some(aggs) = request.aggs {
        for (name, agg) in aggs {
            let result = if !query_supported {
                None
            } else {
                match agg.kind() {
                    Some(kind) => match kind {
                        AggregationKind::Percentiles(pct) => {
                            match handle_percentiles(&state, &pct, query_key.as_deref()) {
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
                                match handle_top_entities(&state, &top) {
                                    Ok(TopEntitiesResult::Single(entity)) => Some(json!({
                                        "key": entity.key,
                                        "value": entity.value
                                    })),
                                    Ok(TopEntitiesResult::Multi(entities)) => Some(json!(entities)),
                                    Err(message) => {
                                        return (axum::http::StatusCode::BAD_REQUEST, message)
                                            .into_response();
                                    }
                                }
                            }
                        }
                        AggregationKind::Cumulative(cum) => match handle_cumulative(&state, &cum) {
                            Ok(value) => Some(json!({ "key": cum.key, "value": value })),
                            Err(message) => {
                                return (axum::http::StatusCode::BAD_REQUEST, message)
                                    .into_response();
                            }
                        },
                        AggregationKind::Frequency(freq) => match handle_frequency(&state, &freq) {
                            Ok(count) => Some(json!({
                                "key": freq.key,
                                "value": freq.value,
                                "count": count,
                            })),
                            Err(message) => {
                                return (axum::http::StatusCode::BAD_REQUEST, message)
                                    .into_response();
                            }
                        },
                    },
                    None => None,
                }
            };

            if let Some(value) = result {
                handled_names.push(name.clone());
                handled.insert(name, value);
            } else {
                unhandled.insert(name, agg);
            }
        }
    }
    if let Some(t) = &mut timing {
        t.step("aggregations");
    }

    // Step 4: Prepare upstream body
    let mut upstream_body = request_value;
    if let Some(aggs_obj) = upstream_body
        .get_mut("aggs")
        .and_then(Value::as_object_mut)
    {
        for name in &handled_names {
            aggs_obj.remove(name);
        }
    }
    if let Some(t) = &mut timing {
        t.step("prepare_upstream");
    }

    // Step 5: Forward to upstream if needed
    let needs_upstream = has_other || !unhandled.is_empty();
    let mut response_value = if needs_upstream {
        match forward_to_upstream(&state, &headers, &upstream_body).await {
            Ok(value) => value,
            Err(response) => return response,
        }
    } else {
        json!({ "aggregations": {} })
    };
    if let Some(t) = &mut timing {
        t.step("upstream");
    }

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

    let mut results = Vec::with_capacity(request.keys.len());

    for key in &request.keys {
        let trimmed_key = key.trim();
        let key_for_query = if trimmed_key.is_empty() {
            None
        } else {
            Some(trimmed_key)
        };
        let mut result = BatchQueryResult {
            key: key.clone(),
            percentiles: None,
            cumulative: None,
            frequency: None,
        };

        for agg_type in &request.aggs {
            match agg_type.trim().to_ascii_lowercase().as_str() {
                "percentiles" => {
                    if percents.is_empty() {
                        continue;
                    }
                    let mut field_percentiles = HashMap::new();
                    for field_name in &fields {
                        let pct = PercentileAggregation {
                            field: field_name.clone(),
                            percents: percents.clone(),
                            key: None,
                        };
                        match handle_percentiles(&state, &pct, key_for_query) {
                            Ok(Some(values)) => {
                                let mut pct_map = HashMap::new();
                                for (percent, value) in values {
                                    pct_map.insert(percent, value);
                                }
                                if !pct_map.is_empty() {
                                    field_percentiles.insert(field_name.clone(), pct_map);
                                }
                            }
                            Ok(None) => {}
                            Err(message) => {
                                return (axum::http::StatusCode::BAD_REQUEST, message)
                                    .into_response();
                            }
                        }
                    }
                    if !field_percentiles.is_empty() {
                        result.percentiles = Some(field_percentiles);
                    }
                }
                "cumulative" => {
                    let Some(key_value) = key_for_query else {
                        continue;
                    };
                    let mut field_cumulative = HashMap::new();
                    for field_name in &fields {
                        let trimmed = field_name.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if !state
                            .agg_config
                            .cumulative_metrics
                            .contains(&trimmed.to_ascii_lowercase())
                        {
                            continue;
                        }
                        let Some(field) = MetricField::from_spec(trimmed) else {
                            continue;
                        };
                        let value = state.store.cumulative_value(field, key_value);
                        field_cumulative.insert(field_name.clone(), value);
                    }
                    if !field_cumulative.is_empty() {
                        result.cumulative = Some(field_cumulative);
                    }
                }
                "frequency" => {
                    let Some(freq_value) = request.frequency_value else {
                        continue;
                    };
                    let Some(key_value) = key_for_query else {
                        continue;
                    };
                    let mut field_frequency = HashMap::new();
                    for field_name in &fields {
                        let trimmed = field_name.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        let Some(field) = MetricField::from_spec(trimmed) else {
                            continue;
                        };
                        if let Some(count) =
                            state.store.frequency_estimate(field, key_value, freq_value)
                        {
                            field_frequency.insert(field_name.clone(), count);
                        }
                    }
                    if !field_frequency.is_empty() {
                        result.frequency = Some(field_frequency);
                    }
                }
                _ => {}
            }
        }

        results.push(result);
    }

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
