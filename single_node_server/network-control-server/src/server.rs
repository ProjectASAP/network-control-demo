use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{Path, State},
    http::{HeaderMap, Request},
    middleware::{Next, from_fn_with_state},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::config::AggregationConfig;
use crate::metrics::{EntityEstimate, MetricField, MetricStore};

pub type TimingSender = std::sync::mpsc::Sender<String>;
pub type LogSender = mpsc::Sender<LogEntry>;

/// Tracks timing for each step of query processing
pub struct QueryTiming {
    start: Instant,
    last_step: Instant,
    steps: Vec<(String, f64)>, // (step_name, duration_ms)
}

impl QueryTiming {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            start: now,
            last_step: now,
            steps: Vec::new(),
        }
    }

    /// Record a step with elapsed time since last step (in ms)
    pub fn step(&mut self, name: &str) {
        let now = Instant::now();
        let duration_ms = now.duration_since(self.last_step).as_secs_f64() * 1000.0;
        self.steps.push((name.to_string(), duration_ms));
        self.last_step = now;
    }

    /// Get total elapsed time in ms
    pub fn total_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }

    /// Log timing to stderr
    pub fn log(&self) {
        let steps_str: Vec<String> = self.steps
            .iter()
            .map(|(name, ms)| format!("{}={:.3}ms", name, ms))
            .collect();
        eprintln!(
            "[TIMING] {} total={:.3}ms",
            steps_str.join(" "),
            self.total_ms()
        );
    }

    /// Convert to JSON value for response
    pub fn to_json(&self) -> Value {
        let mut steps_obj = serde_json::Map::new();
        for (name, ms) in &self.steps {
            steps_obj.insert(format!("{}_ms", name), json!(ms));
        }
        json!({
            "total_ms": self.total_ms(),
            "steps": steps_obj
        })
    }

    /// Format as header value
    pub fn to_header(&self) -> String {
        format!("{:.3}", self.total_ms())
    }
}

struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

type PercentileCacheKey = (MetricField, Option<String>, Vec<i32>);

pub struct QueryCache {
    percentiles: RwLock<HashMap<PercentileCacheKey, CacheEntry<Vec<f64>>>>,
    ttl: Duration,
}

impl QueryCache {
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            percentiles: RwLock::new(HashMap::new()),
            ttl: Duration::from_millis(ttl_ms),
        }
    }

    fn cache_key(
        field: MetricField,
        key: Option<&str>,
        percents: &[f64],
    ) -> PercentileCacheKey {
        (
            field,
            key.map(String::from),
            percents.iter().map(|p| *p as i32).collect(),
        )
    }

    pub fn get_percentiles(
        &self,
        field: MetricField,
        key: Option<&str>,
        percents: &[f64],
    ) -> Option<Vec<f64>> {
        if self.ttl.is_zero() {
            return None;
        }
        let cache_key = Self::cache_key(field, key, percents);
        let cache = self.percentiles.read().ok()?;
        let entry = cache.get(&cache_key)?;
        if entry.expires_at > Instant::now() {
            Some(entry.value.clone())
        } else {
            None
        }
    }

    pub fn set_percentiles(
        &self,
        field: MetricField,
        key: Option<&str>,
        percents: &[f64],
        value: Vec<f64>,
    ) {
        if self.ttl.is_zero() {
            return;
        }
        let cache_key = Self::cache_key(field, key, percents);
        if let Ok(mut cache) = self.percentiles.write() {
            cache.insert(
                cache_key,
                CacheEntry {
                    value,
                    expires_at: Instant::now() + self.ttl,
                },
            );
        }
    }
}

#[derive(Debug)]
pub struct LogEntry {
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<MetricStore>,
    pub agg_config: Arc<AggregationConfig>,
    pub http_client: Client,
    pub upstream_url: String,
    pub timing_enabled: bool,
    pub timing_sender: Option<TimingSender>,
    pub no_ingest: bool,
    pub cache: Arc<QueryCache>,
    pub log_tx: Option<LogSender>,
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
    #[serde(default)]
    field: Option<String>,
    #[serde(default)]
    fields: Option<Vec<String>>,
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

#[derive(Debug, Deserialize)]
struct BatchQueryRequest {
    keys: Vec<String>,
    fields: Option<Vec<String>>,
    aggs: Vec<String>,
    percents: Option<Vec<f64>>,
    frequency_value: Option<f64>,
}

#[derive(Debug, Serialize)]
struct BatchQueryResult {
    key: String,
    percentiles: Option<HashMap<String, HashMap<String, f64>>>,
    cumulative: Option<HashMap<String, i32>>,
    frequency: Option<HashMap<String, i32>>,
}

#[derive(Debug, Serialize)]
struct BatchQueryResponse {
    results: Vec<BatchQueryResult>,
}

enum AggregationKind {
    Percentiles(PercentileAggregation),
    TopEntities(TopEntitiesAggregation),
    Cumulative(CumulativeAggregation),
    Frequency(FrequencyAggregation),
}

enum QueryKeyStatus {
    None,
    Key(String),
    Unsupported,
}

enum TopEntitiesResult {
    Single(EntityEstimate),
    Multi(HashMap<String, EntityEstimate>),
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
        if let Some(freq) = self.frequency.clone() {
            kind = Some(AggregationKind::Frequency(freq));
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

#[derive(Debug, Deserialize)]
struct IngestRecord {
    task: Vec<String>,
    cluster: Vec<String>,
    cpu_cores: Vec<f64>,
    memory_gb: Vec<f64>,
    network_mbps: Vec<f64>,
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
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                message,
            )
                .into_response();
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
    if let Some(t) = &mut timing { t.step("parse_json"); }

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
    if let Some(t) = &mut timing { t.step("deserialize"); }

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
    let has_other = request
        ._other
        .keys()
        .any(|key| key.as_str() != "query")
        || matches!(query_status, QueryKeyStatus::Unsupported);

    if let Some(aggs) = request.aggs {
        for (name, agg) in aggs {
            let result = if !query_supported {
                None
            } else {
                match agg.kind() {
                    Some(kind) => match kind {
                        AggregationKind::Percentiles(pct) => match handle_percentiles(
                            &state,
                            &pct,
                            query_key.as_deref(),
                        ) {
                            Ok(Some(values)) => Some(json!({ "values": values })),
                            Ok(None) => None,
                            Err(message) => {
                                return (axum::http::StatusCode::BAD_REQUEST, message)
                                    .into_response();
                            }
                        },

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
    if let Some(t) = &mut timing { t.step("aggregations"); }

    // Step 4: Prepare upstream body (forwarding disabled for now)
    let mut _upstream_body = request_value;
    if let Some(aggs_obj) = _upstream_body.get_mut("aggs").and_then(Value::as_object_mut) {
        for name in &handled_names {
            aggs_obj.remove(name);
        }
    }
    if let Some(t) = &mut timing { t.step("prepare_upstream"); }

    // Step 5: Forward to upstream if needed
    let needs_upstream = has_other || !unhandled.is_empty();
    let mut response_value = if needs_upstream {
        // NOTE: Upstream forwarding disabled for now.
        json!({ "aggregations": {} })
    } else {
        json!({ "aggregations": {} })
    };
    if let Some(t) = &mut timing { t.step("upstream"); }

    // Step 6: Merge results
    merge_aggregations(&mut response_value, handled);
    if let Some(t) = &mut timing { t.step("merge"); }

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
        response.headers_mut().insert(
            "X-Server-Timing",
            timing_header.parse().unwrap(),
        );
        write_timing_log(
            &state,
            &headers,
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
                        if let Some(count) = state.store.frequency_estimate(field, key_value, freq_value) {
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
    if let Some(t) = &mut timing { t.step("parse_field"); }

    // Step 2: Validate query
    if query.quantiles.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "quantiles must be a non-empty list".to_string(),
        )
            .into_response();
    }
    if let Some(t) = &mut timing { t.step("validate"); }

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
    if let Some(t) = &mut timing { t.step("query_percentiles"); }

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
        response.headers_mut().insert(
            "X-Server-Timing",
            timing_header.parse().unwrap(),
        );
        write_timing_log(
            &state,
            &headers,
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
        })).into_response()
    }
}

fn build_percentile_response(percents: &[f64], values: &[f64]) -> BTreeMap<String, f64> {
    let mut response = BTreeMap::new();
    for (percent, value) in percents.iter().zip(values.iter()) {
        response.insert(percent.to_string(), *value);
    }
    response
}

fn handle_percentiles(
    state: &AppState,
    pct: &PercentileAggregation,
    query_key: Option<&str>,
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
    let explicit_key = pct
        .key
        .as_ref()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty());
    if pct.key.is_some() && explicit_key.is_none() {
        return Err("percentiles key is required when provided".to_string());
    }
    let key = explicit_key.or(query_key);
    if let Some(cached) = state.cache.get_percentiles(field, key, &pct.percents) {
        return Ok(Some(build_percentile_response(&pct.percents, &cached)));
    }

    let mut cache_values = Vec::with_capacity(pct.percents.len());
    let mut all_present = true;
    for percent in &pct.percents {
        let value = if let Some(key) = key {
            state.store.query_percentile_by_key(field, key, *percent)
        } else {
            state.store.query_percentile(field, *percent)
        };

        if let Some(value) = value {
            values.insert(percent.to_string(), value);
            cache_values.push(value);
        } else {
            all_present = false;
        }
    }

    if all_present && !cache_values.is_empty() {
        state.cache.set_percentiles(field, key, &pct.percents, cache_values);
    }

    Ok(Some(values))
}

fn handle_multi_top_entities(
    state: &AppState,
    fields: &[String],
) -> Result<HashMap<String, EntityEstimate>, String> {
    let mut results = HashMap::new();

    for field_name in fields {
        let trimmed = field_name.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !state
            .agg_config
            .top_entities_metrics
            .contains(&trimmed.to_ascii_lowercase())
        {
            return Err(format!("unsupported top_entities field: {}", field_name));
        }
        let field = MetricField::from_spec(trimmed)
            .ok_or_else(|| format!("unsupported top_entities field: {}", field_name))?;

        if let Some(entity) = state.store.top_entity(field) {
            results.insert(field_name.clone(), entity);
        }
    }

    if results.is_empty() {
        return Err("no top entity available".to_string());
    }

    Ok(results)
}

fn handle_top_entities(
    state: &AppState,
    top: &TopEntitiesAggregation,
) -> Result<TopEntitiesResult, String> {
    if let Some(fields) = top.fields.as_ref().filter(|fields| !fields.is_empty()) {
        let results = handle_multi_top_entities(state, fields)?;
        return Ok(TopEntitiesResult::Multi(results));
    }

    let field_name = top
        .field
        .as_ref()
        .map(|field| field.trim())
        .filter(|field| !field.is_empty())
        .ok_or_else(|| "top_entities field is required".to_string())?;
    if !state
        .agg_config
        .top_entities_metrics
        .contains(&field_name.to_ascii_lowercase())
    {
        return Err(format!("unsupported top_entities field: {}", field_name));
    }
    let field = MetricField::from_spec(field_name)
        .ok_or_else(|| format!("unsupported top_entities field: {}", field_name))?;
    let entity = state
        .store
        .top_entity(field)
        .ok_or_else(|| "no top entity available".to_string())?;
    Ok(TopEntitiesResult::Single(entity))
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

fn handle_frequency(state: &AppState, freq: &FrequencyAggregation) -> Result<i32, String> {
    let field = MetricField::from_spec(&freq.field)
        .ok_or_else(|| format!("unsupported frequency field: {}", freq.field))?;
    let key = freq.key.trim();
    if key.is_empty() {
        return Err("frequency key is required".to_string());
    }

    state
        .store
        .frequency_estimate(field, key, freq.value)
        .ok_or_else(|| format!("invalid frequency value: {}", freq.value))
}

fn extract_query_key(query_value: Option<&Value>) -> QueryKeyStatus {
    let Some(query_value) = query_value else {
        return QueryKeyStatus::None;
    };
    if query_value.is_null() {
        return QueryKeyStatus::None;
    }

    let query_obj = match query_value.as_object() {
        Some(obj) => obj,
        None => return QueryKeyStatus::Unsupported,
    };

    let mut cluster = None;
    let mut task = None;

    if let Some(term_value) = query_obj.get("term") {
        if parse_term_object(term_value, &mut cluster, &mut task).is_err() {
            return QueryKeyStatus::Unsupported;
        }
    } else if let Some(bool_value) = query_obj.get("bool") {
        let bool_obj = match bool_value.as_object() {
            Some(obj) => obj,
            None => return QueryKeyStatus::Unsupported,
        };
        if bool_obj.len() != 1 || !bool_obj.contains_key("must") {
            return QueryKeyStatus::Unsupported;
        }
        let must_value = match bool_obj.get("must") {
            Some(value) => value,
            None => return QueryKeyStatus::Unsupported,
        };
        let must_items = match must_value.as_array() {
            Some(items) => items,
            None => return QueryKeyStatus::Unsupported,
        };
        for item in must_items {
            let term_value = match item.get("term") {
                Some(value) => value,
                None => return QueryKeyStatus::Unsupported,
            };
            if parse_term_object(term_value, &mut cluster, &mut task).is_err() {
                return QueryKeyStatus::Unsupported;
            }
        }
    } else {
        return QueryKeyStatus::Unsupported;
    }

    match (cluster, task) {
        (None, None) => QueryKeyStatus::None,
        (Some(cluster), Some(task)) => QueryKeyStatus::Key(format!("{cluster};{task}")),
        (Some(cluster), None) => QueryKeyStatus::Key(cluster),
        (None, Some(task)) => QueryKeyStatus::Key(task),
    }
}

fn parse_term_object(
    term_value: &Value,
    cluster: &mut Option<String>,
    task: &mut Option<String>,
) -> Result<(), ()> {
    let term_obj = term_value.as_object().ok_or(())?;
    for (field, value) in term_obj {
        let normalized = field.trim().to_ascii_lowercase();
        let term_value = extract_term_value(value).ok_or(())?;
        let term_value = term_value.trim();
        if term_value.is_empty() {
            return Err(());
        }
        match normalized.as_str() {
            "cluster" | "cluster.keyword" => {
                *cluster = Some(term_value.to_string());
            }
            "task" | "task.keyword" => {
                *task = Some(term_value.to_string());
            }
            _ => return Err(()),
        }
    }
    Ok(())
}

fn extract_term_value(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        return Some(value.to_string());
    }
    if let Some(value) = value.as_i64() {
        return Some(value.to_string());
    }
    if let Some(value) = value.as_u64() {
        return Some(value.to_string());
    }
    if let Some(value) = value.as_f64() {
        return Some(value.to_string());
    }
    if let Some(obj) = value.as_object() {
        if let Some(inner) = obj.get("value") {
            return extract_term_value(inner);
        }
    }
    None
}

fn write_timing_log(
    state: &AppState,
    headers: &HeaderMap,
    _method: &str,
    _path: &str,
    status: axum::http::StatusCode,
    timing: &QueryTiming,
) {
    let Some(sender) = state.timing_sender.as_ref() else {
        return;
    };
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");
    let request_type = headers
        .get("x-request-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");
    let mut steps: BTreeMap<&str, f64> = BTreeMap::new();
    for (name, ms) in &timing.steps {
        steps.insert(name.as_str(), *ms);
    }
    let total_ms: f64 = steps.values().copied().sum();
    let step_names = [
        "parse_json",
        "deserialize",
        "aggregations",
        "prepare_upstream",
        "upstream",
        "merge",
        "serialize",
        "parse_field",
        "validate",
        "query_percentiles",
        "build_response",
    ];
    let format_value = |value: Option<&f64>| -> String {
        value.map(|ms| format!("{ms:.3}")).unwrap_or_default()
    };
    let mut row = Vec::with_capacity(6 + step_names.len());
    row.push(request_id.to_string());
    row.push(request_type.to_string());
    row.push(status.to_string());
    row.push(format!("{total_ms:.3}"));
    for name in step_names {
        row.push(format_value(steps.get(name)));
    }
    let _ = sender.send(row.join(","));
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

async fn log_request_middleware(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
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

    let log_body = body_bytes.clone();
    let log_method = method.clone();
    let log_uri = uri.clone();
    let log_headers = headers.clone();
    if let Some(log_tx) = &state.log_tx {
        let log_entry = LogEntry {
            method: log_method,
            uri: log_uri,
            headers: log_headers,
            body: log_body,
        };
        let _ = log_tx.try_send(log_entry);
    }

    let req = Request::from_parts(parts, Body::from(body_bytes));
    let response = next.run(req).await;
    let status = response.status();
    eprintln!("response status: {}", status);
    response
}

fn log_request_details(
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
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
    match serde_json::from_slice::<Value>(&body) {
        Ok(value) => match serde_json::to_string_pretty(&value) {
            Ok(pretty) => eprintln!("{pretty}"),
            Err(_) => eprintln!("{value}"),
        },
        Err(_) => eprintln!("{}", String::from_utf8_lossy(&body)),
    }
    eprintln!("end request");
}

pub fn start_request_logger(buffer: usize) -> LogSender {
    let (log_tx, mut log_rx) = mpsc::channel::<LogEntry>(buffer);
    tokio::spawn(async move {
        while let Some(entry) = log_rx.recv().await {
            log_request_details(entry.method, entry.uri, entry.headers, entry.body);
        }
    });
    log_tx
}
