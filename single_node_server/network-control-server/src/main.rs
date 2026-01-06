use std::{
    collections::{BTreeMap, HashSet},
    env,
    error::Error,
    fs,
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use csv::StringRecord;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sketchlib_rust::{KLL, SketchInput, sketches::kll::CDF};
use tokio::net::TcpListener;

#[derive(Clone)]
struct AppState {
    cdfs: Arc<MetricCdfs>,
    agg_config: Arc<AggregationConfig>,
    http_client: Client,
}

#[derive(Default)]
struct MetricSketches {
    cpu_cores: KLL,
    memory_gb: KLL,
    network_mbps: KLL,
}

impl MetricSketches {
    fn insert_samples(&mut self, cpu_value: f64, memory_value: f64, network_value: f64) {
        let cpu = SketchInput::F64(cpu_value);
        let memory = SketchInput::F64(memory_value);
        let network = SketchInput::F64(network_value);

        self.cpu_cores
            .update(&cpu)
            .expect("cpu_cores values should be numeric");
        self.memory_gb
            .update(&memory)
            .expect("memory_gb values should be numeric");
        self.network_mbps
            .update(&network)
            .expect("network_mbps values should be numeric");
    }
}

struct MetricCdfs {
    cpu_cores: CDF,
    memory_gb: CDF,
    network_mbps: CDF,
}

impl MetricCdfs {
    fn from_sketches(sketches: MetricSketches) -> Self {
        Self {
            cpu_cores: sketches.cpu_cores.cdf(),
            memory_gb: sketches.memory_gb.cdf(),
            network_mbps: sketches.network_mbps.cdf(),
        }
    }

    fn query_percent(&self, field: &str, percent: f64) -> Option<f64> {
        if !(0.0..=100.0).contains(&percent) {
            return None;
        }
        let quantile = percent / 100.0;
        match field.to_ascii_lowercase().as_str() {
            "cpu_cores" => Some(self.cpu_cores.query(quantile)),
            "memory_gb" => Some(self.memory_gb.query(quantile)),
            "network_mbps" => Some(self.network_mbps.query(quantile)),
            _ => None,
        }
    }
}

#[derive(Serialize)]
struct RootResponse<'a> {
    message: &'a str,
    examples: [&'a str; 3],
}

#[derive(Clone, Debug)]
struct AggregationConfig {
    allowed_percentile_fields: HashSet<String>,
}

#[derive(Debug, Deserialize)]
struct RawAggregationConfig {
    supported_aggs: SupportedAggs,
}

#[derive(Debug, Deserialize)]
struct SupportedAggs {
    percentiles: PercentileSupport,
}

#[derive(Debug, Deserialize)]
struct PercentileSupport {
    fields: Vec<String>,
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
    #[serde(flatten, default)]
    other: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct PercentileAggregation {
    field: String,
    percents: Vec<f64>,
}

#[tokio::main]
async fn main() {
    let agg_config = match load_aggregation_config() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("failed to load aggregation config: {err}");
            return;
        }
    };
    println!("loaded aggregation config: {agg_config:?}");
    let agg_config = Arc::new(agg_config);

    let cdfs = match tokio::task::spawn_blocking(load_metric_cdfs).await {
        Ok(Ok(cdfs)) => cdfs,
        Ok(Err(err)) => {
            eprintln!("failed to load sketches: {err}");
            return;
        }
        Err(join_err) => {
            eprintln!("loader task panicked: {join_err}");
            return;
        }
    };
    println!("metric CDFs ready");
    let cdfs = Arc::new(cdfs);

    let state = AppState {
        cdfs,
        agg_config,
        http_client: Client::new(),
    };

    if let Err(err) = run_http_server(state).await {
        eprintln!("server error: {err}");
    }
}

async fn run_http_server(state: AppState) -> Result<(), Box<dyn Error + Send + Sync>> {
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/healthz", get(|| async { "ok" }))
        .route("/cluster-metrics/_search", post(search_root_post_handler))
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:10101").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn root_handler() -> Json<RootResponse<'static>> {
    Json(RootResponse {
        message: "POST /cluster-metrics/_search with a body containing aggs; the server will parse and log it.",
        examples: [
            "POST /cluster-metrics/_search {\"aggs\":{\"cpu_quantiles\":{\"percentiles\":{\"field\":\"cpu_cores\",\"percents\":[10,50]}}}}",
            "POST /cluster-metrics/_search {\"query\":{\"match_all\":{}},\"aggs\":{}}",
            "POST /cluster-metrics/_search {\"size\":0}",
        ],
    })
}

// The original handlers computed percentiles and forwarded mixed requests.
// For now we only need to parse the incoming JSON and print it.

async fn search_root_post_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    println!("received _search POST request with no query param");
    process_search(body, headers, state).await
}

async fn process_search(body: Bytes, headers: HeaderMap, state: AppState) -> impl IntoResponse {
    // Parse the JSON body into the known shape and log it.
    let parsed: SearchRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(err) => {
            return (StatusCode::BAD_REQUEST, format!("invalid JSON body: {err}")).into_response();
        }
    };

    println!("aggregation config: {:?}", state.agg_config);
    println!("parsed search request: {parsed:#?}");

    // Peel out handled vs unhandled aggregations in the request body.
    let (handled, unhandled) = match parsed.aggs {
        Some(map) => {
            let mut handled = BTreeMap::new();
            let mut unhandled = BTreeMap::new();
            for (name, agg) in map {
                let allowed = agg
                    .percentiles
                    .as_ref()
                    .map(|p| {
                        !p.percents.is_empty()
                            && state
                                .agg_config
                                .allowed_percentile_fields
                                .contains(&p.field.to_ascii_lowercase())
                    })
                    .unwrap_or(false);

                if allowed {
                    handled.insert(name, agg);
                } else {
                    unhandled.insert(name, agg);
                }
            }
            (handled, unhandled)
        }
        None => (BTreeMap::new(), BTreeMap::new()),
    };

    println!("handled aggs: {handled:#?}");
    println!("unhandled aggs: {unhandled:#?}");

    // For handled percentile aggs, query the CDF and build a simple response.
    let mut handled_results: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for (name, agg) in &handled {
        if let Some(pct) = &agg.percentiles {
            let mut values = BTreeMap::new();
            for percent in &pct.percents {
                if let Some(value) = state.cdfs.query_percent(&pct.field, *percent) {
                    values.insert(percent.to_string(), value);
                }
            }
            handled_results.insert(name.clone(), values);
        }
    }

    #[derive(Serialize)]
    struct SearchHandlingResponse {
        handled: BTreeMap<String, BTreeMap<String, f64>>,
        unhandled: BTreeMap<String, AggregationRequest>,
        upstream_status: u16,
        upstream_body: Value,
        forwarded_authorization: bool,
    }

    // Build upstream request body: drop handled aggs.
    let mut upstream_body: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid JSON body when preparing upstream request: {err}"),
            )
                .into_response();
        }
    };

    if let Some(aggs_obj) = upstream_body.get_mut("aggs").and_then(Value::as_object_mut) {
        for name in handled.keys() {
            aggs_obj.remove(name);
        }
    }

    let mut upstream_req = state
        .http_client
        .post("http://localhost:9200/cluster-metrics/_search")
        .json(&upstream_body);

    // Forward relevant headers (e.g., Authorization, Content-Type).
    let mut saw_auth = false;
    for (name, value) in headers.iter() {
        if name == axum::http::header::HOST
            || name == axum::http::header::CONTENT_TYPE
            || name == axum::http::header::CONTENT_LENGTH
        {
            continue;
        }
        if name == axum::http::header::AUTHORIZATION {
            saw_auth = true;
        }
        upstream_req = upstream_req.header(name, value);
    }

    let upstream_resp = upstream_req.send().await;

    let (upstream_status, upstream_body_val) = match upstream_resp {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let body_val: Value = resp.json().await.unwrap_or_else(|_| Value::Null);
            (status, body_val)
        }
        Err(err) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("failed to contact upstream elasticsearch: {err}"),
            )
                .into_response();
        }
    };

    Json(SearchHandlingResponse {
        handled: handled_results,
        unhandled,
        upstream_status,
        upstream_body: upstream_body_val,
        forwarded_authorization: saw_auth,
    })
    .into_response()
}

fn load_aggregation_config() -> Result<AggregationConfig, Box<dyn Error + Send + Sync>> {
    let path = env::var("AGG_CONFIG_PATH").unwrap_or_else(|_| "agg-config.yaml".to_string());
    let contents = fs::read_to_string(&path)?;
    let raw: RawAggregationConfig = serde_yaml::from_str(&contents)?;

    let allowed_percentile_fields = raw
        .supported_aggs
        .percentiles
        .fields
        .into_iter()
        .map(|field| field.trim().to_ascii_lowercase())
        .collect();

    Ok(AggregationConfig {
        allowed_percentile_fields,
    })
}

fn load_metric_cdfs() -> Result<MetricCdfs, Box<dyn Error + Send + Sync>> {
    let start = Instant::now();
    let csv_path = build_dataset_path();
    let mut reader = csv::Reader::from_path(&csv_path)?;
    let headers = reader.headers()?.clone();

    let cpu_idx = find_column(&headers, &["cpu_cores", "cpu-cores"])
        .ok_or_else(|| format!("missing 'cpu_cores' column in {}", csv_path.display()))?;
    let mem_idx = find_column(&headers, &["memory_gb", "memory-gb"])
        .ok_or_else(|| format!("missing 'memory_gb' column in {}", csv_path.display()))?;
    let net_idx = find_column(&headers, &["network_mbps", "network-mbps"])
        .ok_or_else(|| format!("missing 'network_mbps' column in {}", csv_path.display()))?;

    let mut sketches = MetricSketches::default();
    let mut processed: u64 = 0;

    for (row_idx, record) in reader.records().enumerate() {
        let record = match record {
            Ok(rec) => rec,
            Err(err) => {
                eprintln!("failed to read row {}: {err}", row_idx + 2);
                continue;
            }
        };

        let cpu_raw = record.get(cpu_idx).unwrap_or("").trim();
        let mem_raw = record.get(mem_idx).unwrap_or("").trim();
        let net_raw = record.get(net_idx).unwrap_or("").trim();

        let cpu_value = match cpu_raw.parse::<f64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        let mem_value = match mem_raw.parse::<f64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        let net_value = match net_raw.parse::<f64>() {
            Ok(value) => value,
            Err(_) => continue,
        };

        sketches.insert_samples(cpu_value, mem_value, net_value);
        processed += 1;

        if processed % 1_000_000 == 0 {
            eprintln!("ingested {processed} rows...");
        }
    }

    eprintln!(
        "processed {processed} rows into 3 metric sketches in {:.2?}",
        start.elapsed()
    );

    Ok(MetricCdfs::from_sketches(sketches))
}

fn build_dataset_path() -> PathBuf {
    let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join("cluster-metrics.csv")
}

fn find_column(headers: &StringRecord, candidates: &[&str]) -> Option<usize> {
    let lowered: Vec<String> = headers
        .iter()
        .map(|h| h.trim().to_ascii_lowercase())
        .collect();

    lowered.iter().position(|header| {
        candidates
            .iter()
            .any(|candidate| header == candidate.trim().to_ascii_lowercase().as_str())
    })
}
