use std::{collections::BTreeMap, error::Error, sync::Arc};

use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{Path, State},
    http::{HeaderMap, Request},
    middleware::{Next, from_fn},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::config::AggregationConfig;
use crate::metrics::{EntityEstimate, MetricField, MetricStore};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<MetricStore>,
    pub agg_config: Arc<AggregationConfig>,
    pub http_client: Client,
    pub upstream_url: String,
}

#[derive(Serialize)]
struct RootResponse<'a> {
    message: &'a str,
    examples: [&'a str; 3],
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct SearchRequest {
    aggs: Option<BTreeMap<String, AggregationRequest>>,
    #[serde(flatten, default)]
    _other: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct AggregationRequest {
    #[serde(default)]
    percentiles: Option<PercentileAggregation>,
    #[serde(default)]
    frequency: Option<FrequencyAggregation>,
    #[serde(default)]
    top_entities: Option<TopEntitiesAggregation>,
    #[serde(default)]
    cumulative: Option<CumulativeAggregation>,
    #[serde(flatten, default)]
    other: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct PercentileAggregation {
    field: String,
    percents: Vec<f64>,
    #[serde(default)]
    key: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TopEntitiesAggregation {
    field: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct CumulativeAggregation {
    field: String,
    key: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct FrequencyAggregation {
    field: String,
    key: String,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct MetricsQuery {
    quantiles: Vec<String>,
}

#[derive(Serialize)]
struct MetricsResponse {
    field: String,
    quantiles: BTreeMap<String, f64>,
}

enum AggregationKind {
    Percentiles(PercentileAggregation),
    TopEntities(TopEntitiesAggregation),
    Cumulative(CumulativeAggregation),
}

impl AggregationRequest {
    fn kind(&self) -> Option<AggregationKind> {
        let mut kind = None;
        let mut count = 0;
        if let Some(pct) = self.percentiles.clone() {
            kind = Some(AggregationKind::Percentiles(pct));
            count += 1;
        }

        if let Some(top) = self.top_entities.clone() {
            kind = Some(AggregationKind::TopEntities(top));
            count += 1;
        }
        if let Some(cum) = self.cumulative.clone() {
            kind = Some(AggregationKind::Cumulative(cum));
            count += 1;
        }

        if count == 1 && self.other.is_empty() {
            kind
        } else {
            None
        }
    }
}

pub async fn run_http_server(state: AppState) -> Result<(), Box<dyn Error + Send + Sync>> {
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/healthz", get(|| async { "ok" }))
        .route("/cluster-metrics/_search", post(search_handler))
        .route("/metrics/:field", post(metrics_handler))
        .with_state(state)
        .layer(from_fn(log_request_middleware));

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

async fn search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
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

    let mut handled = BTreeMap::new();
    let mut handled_names = Vec::new();
    let mut unhandled = BTreeMap::new();
    let has_other = !request._other.is_empty();

    if let Some(aggs) = request.aggs {
        for (name, agg) in aggs {
            let result = match agg.kind() {
                Some(kind) => match kind {
                    AggregationKind::Percentiles(pct) => match handle_percentiles(&state, &pct) {
                        Ok(Some(values)) => Some(json!({ "values": values })),
                        Ok(None) => None,
                        Err(message) => {
                            return (axum::http::StatusCode::BAD_REQUEST, message).into_response();
                        }
                    },

                    AggregationKind::TopEntities(top) => match handle_top_entities(&state, &top) {
                        Ok(entity) => Some(json!({ "key": entity.key, "value": entity.value })),
                        Err(message) => {
                            return (axum::http::StatusCode::BAD_REQUEST, message).into_response();
                        }
                    },
                    AggregationKind::Cumulative(cum) => match handle_cumulative(&state, &cum) {
                        Ok(value) => Some(json!({ "key": cum.key, "value": value })),
                        Err(message) => {
                            return (axum::http::StatusCode::BAD_REQUEST, message).into_response();
                        }
                    },
                },
                None => None,
            };

            if let Some(value) = result {
                handled_names.push(name.clone());
                handled.insert(name, value);
            } else {
                unhandled.insert(name, agg);
            }
        }
    }

    let mut upstream_body = request_value;
    if let Some(aggs_obj) = upstream_body.get_mut("aggs").and_then(Value::as_object_mut) {
        for name in &handled_names {
            aggs_obj.remove(name);
        }
    }

    let needs_upstream = has_other || !unhandled.is_empty();
    let mut response_value = if needs_upstream {
        match forward_to_upstream(&state, &headers, &upstream_body).await {
            Ok(value) => value,
            Err(resp) => return resp,
        }
    } else {
        json!({ "aggregations": {} })
    };

    merge_aggregations(&mut response_value, handled);
    Json(response_value).into_response()
}

async fn metrics_handler(
    State(state): State<AppState>,
    Path(field_spec): Path<String>,
    Json(query): Json<MetricsQuery>,
) -> impl IntoResponse {
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

    if query.quantiles.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "quantiles must be a non-empty list".to_string(),
        )
            .into_response();
    }

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

    Json(MetricsResponse {
        field: field_spec,
        quantiles: results,
    })
    .into_response()
}

fn handle_percentiles(
    state: &AppState,
    pct: &PercentileAggregation,
) -> Result<Option<BTreeMap<String, f64>>, String> {
    if pct.percents.is_empty() {
        return Ok(None);
    }
    if !state
        .agg_config
        .percentile_fields
        .contains(&pct.field.trim().to_ascii_lowercase())
    {
        return Ok(None);
    }
    let field = MetricField::from_spec(&pct.field)
        .ok_or_else(|| format!("unsupported percentile field: {}", pct.field))?;

    let mut values = BTreeMap::new();
    for percent in &pct.percents {
        let value = if let Some(key) = pct.key.as_ref() {
            let key = key.trim();
            if key.is_empty() {
                return Err("percentiles key is required when provided".to_string());
            }
            state.store.query_percentile_by_key(field, key, *percent)
        } else {
            state.store.query_percentile(field, *percent)
        };

        if let Some(value) = value {
            values.insert(percent.to_string(), value);
        }
    }

    Ok(Some(values))
}

fn handle_top_entities(
    state: &AppState,
    top: &TopEntitiesAggregation,
) -> Result<EntityEstimate, String> {
    if !state
        .agg_config
        .top_entities_metrics
        .contains(&top.field.trim().to_ascii_lowercase())
    {
        return Err(format!("unsupported top_entities field: {}", top.field));
    }
    let field = MetricField::from_spec(&top.field)
        .ok_or_else(|| format!("unsupported top_entities field: {}", top.field))?;
    state
        .store
        .top_entity(field)
        .ok_or_else(|| "no top entity available".to_string())
}

fn handle_cumulative(state: &AppState, cum: &CumulativeAggregation) -> Result<i32, String> {
    if !state
        .agg_config
        .cumulative_metrics
        .contains(&cum.field.trim().to_ascii_lowercase())
    {
        return Err(format!("unsupported cumulative field: {}", cum.field));
    }
    let field = MetricField::from_spec(&cum.field)
        .ok_or_else(|| format!("unsupported cumulative field: {}", cum.field))?;
    if cum.key.trim().is_empty() {
        return Err("cumulative key is required".to_string());
    }
    Ok(state.store.cumulative_value(field, cum.key.trim()))
}

async fn forward_to_upstream(
    state: &AppState,
    headers: &HeaderMap,
    body: &Value,
) -> Result<Value, axum::response::Response> {
    let mut upstream_req = state.http_client.post(&state.upstream_url).json(body);

    for (name, value) in headers.iter() {
        if name == axum::http::header::HOST
            || name == axum::http::header::CONTENT_TYPE
            || name == axum::http::header::CONTENT_LENGTH
        {
            continue;
        }
        upstream_req = upstream_req.header(name, value);
    }

    let upstream_resp = match upstream_req.send().await {
        Ok(resp) => resp,
        Err(err) => {
            return Err((
                axum::http::StatusCode::BAD_GATEWAY,
                format!("failed to contact upstream elasticsearch: {err}"),
            )
                .into_response());
        }
    };

    let body_val: Value = upstream_resp.json().await.unwrap_or_else(|_| Value::Null);
    Ok(body_val)
}

fn merge_aggregations(response: &mut Value, handled: BTreeMap<String, Value>) {
    let obj = match response.as_object_mut() {
        Some(obj) => obj,
        None => {
            *response = json!({ "aggregations": handled });
            return;
        }
    };

    let aggs = obj.entry("aggregations").or_insert_with(|| json!({}));
    if let Some(aggs_obj) = aggs.as_object_mut() {
        for (name, value) in handled {
            aggs_obj.insert(name, value);
        }
    } else {
        *aggs = json!(handled);
    }
}

fn parse_quantile_spec(spec: &str) -> Option<f64> {
    let trimmed = spec.trim();
    let candidate = trimmed
        .strip_prefix('p')
        .or_else(|| trimmed.strip_prefix('P'))
        .unwrap_or(trimmed)
        .trim();
    if candidate.is_empty() {
        return None;
    }
    candidate.parse::<f64>().ok()
}

async fn log_request_middleware(req: Request<Body>, next: Next) -> Response {
    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let uri = parts.uri.clone();
    let headers = parts.headers.clone();

    let body_bytes = match to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("failed to read request body: {err}");
            Bytes::new()
        }
    };

    log_request_details(&method, &uri, &headers, &body_bytes);

    let req = Request::from_parts(parts, Body::from(body_bytes));
    let response = next.run(req).await;
    eprintln!("response status: {}", response.status());
    response
}

fn log_request_details(
    method: &axum::http::Method,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
    body: &Bytes,
) {
    const MAX_LOG_BODY_BYTES: usize = 1024 * 1024;

    eprintln!("incoming request: {method} {uri}");

    let mut header_pairs: Vec<(String, String)> = headers
        .iter()
        .map(|(name, value)| {
            let value_str = value
                .to_str()
                .map(|val| val.to_string())
                .unwrap_or_else(|_| format!("<non-utf8:{} bytes>", value.as_bytes().len()));
            (name.to_string(), value_str)
        })
        .collect();
    header_pairs.sort_by(|a, b| a.0.cmp(&b.0));

    eprintln!("headers:");
    if header_pairs.is_empty() {
        eprintln!("  <none>");
    } else {
        for (name, value) in header_pairs {
            eprintln!("  {name}: {value}");
        }
    }

    if body.is_empty() {
        eprintln!("body (0 bytes): <empty>");
        eprintln!("end request");
        return;
    }

    let total_len = body.len();
    if total_len > MAX_LOG_BODY_BYTES {
        eprintln!(
            "body ({} bytes, showing first {}):",
            total_len, MAX_LOG_BODY_BYTES
        );
        let preview = &body[..MAX_LOG_BODY_BYTES];
        eprintln!("{}", String::from_utf8_lossy(preview));
        eprintln!("body truncated");
        eprintln!("end request");
        return;
    }

    eprintln!("body ({} bytes):", total_len);
    match serde_json::from_slice::<Value>(body) {
        Ok(value) => match serde_json::to_string_pretty(&value) {
            Ok(pretty) => eprintln!("{pretty}"),
            Err(_) => eprintln!("{value}"),
        },
        Err(_) => eprintln!("{}", String::from_utf8_lossy(body)),
    }
    eprintln!("end request");
}
