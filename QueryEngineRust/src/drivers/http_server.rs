use axum::{
    extract::{Form, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use reqwest;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use crate::engines::SimpleEngine;
use crate::stores::Store;
use crate::utils::http::convert_query_result_to_prometheus;

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub port: u16,
    pub handle_http_requests: bool,
    pub prometheus_server_url: String,
    pub forward_unsupported_queries: bool,
}

#[derive(Clone)]
pub struct HttpServer {
    config: HttpServerConfig,
    query_engine: Arc<SimpleEngine>,
    store: Arc<dyn Store>,
}

#[derive(Clone)]
struct AppState {
    config: HttpServerConfig,
    query_engine: Arc<SimpleEngine>,
    store: Arc<dyn Store>,
}

#[derive(Debug, Deserialize, Default)]
struct QueryParams {
    query: Option<String>,
    time: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PrometheusResponse {
    status: String,
    data: Option<Value>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl PrometheusResponse {
    fn success(data: Value) -> Self {
        Self {
            status: "success".to_string(),
            data: Some(data),
            error_type: None,
            error: None,
        }
    }

    fn error(error_type: &str, error: &str) -> Self {
        Self {
            status: "error".to_string(),
            data: None,
            error_type: Some(error_type.to_string()),
            error: Some(error.to_string()),
        }
    }
}

impl HttpServer {
    pub fn new(
        config: HttpServerConfig,
        query_engine: Arc<SimpleEngine>,
        store: Arc<dyn Store>,
    ) -> Self {
        Self {
            config,
            query_engine,
            store,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine,
            store: self.store,
        };

        let app = Router::new()
            .route("/api/v1/query", get(handle_instant_query))
            .route("/api/v1/query", post(handle_instant_query_post))
            .route("/api/v1/status/runtimeinfo", get(handle_runtime_info))
            .with_state(app_state);

        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.config.port)).await?;
        info!("HTTP server listening on port {}", self.config.port);

        axum::serve(listener, app).await?;
        Ok(())
    }

    /// Start server for testing on a random available port
    /// Returns the actual port number used
    #[cfg(test)]
    pub async fn start_test_server(&self) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine.clone(),
            store: self.store.clone(),
        };

        let app = Router::new()
            .route("/api/v1/query", get(handle_instant_query))
            .route("/api/v1/query", post(handle_instant_query_post))
            .route("/api/v1/status/runtimeinfo", get(handle_runtime_info))
            .with_state(app_state);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let actual_port = listener.local_addr()?.port();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give the server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(actual_port)
    }
}

async fn handle_instant_query(
    Query(params): Query<QueryParams>,
    State(state): State<AppState>,
) -> Result<Json<PrometheusResponse>, StatusCode> {
    let start_time = Instant::now();
    debug!("=== INCOMING GET REQUEST ===");
    debug!("Raw query params extracted: {:?}", params);
    debug!("Query param 'query': {:?}", params.query);
    debug!("Query param 'time': {:?}", params.time);
    debug!(
        "State config - handle_http_requests: {}",
        state.config.handle_http_requests
    );
    debug!(
        "State config - forward_unsupported_queries: {}",
        state.config.forward_unsupported_queries
    );
    debug!(
        "State config - prometheus_server_url: {}",
        state.config.prometheus_server_url
    );

    if !state.config.handle_http_requests {
        debug!("HTTP request handling is disabled");
        if state.config.forward_unsupported_queries {
            debug!("Forwarding to Prometheus due to disabled handling");
            return handle_prometheus_forward(&state, &params).await;
        } else {
            debug!("Returning error - both handling and forwarding disabled");
            return Ok(Json(PrometheusResponse::error(
                "bad_data",
                "Query handling is disabled",
            )));
        }
    }

    let query = match &params.query {
        Some(q) => {
            debug!("Extracted query string: '{}'", q);
            q.clone()
        }
        None => {
            debug!("ERROR: Missing query parameter in request");
            return Ok(Json(PrometheusResponse::error(
                "bad_data",
                "Missing query parameter",
            )));
        }
    };

    // time in seconds
    let time = params.time.unwrap_or_else(|| {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        debug!(
            "No time parameter provided, using current time: {}",
            current_time
        );
        current_time
    });
    debug!("Using timestamp: {}", time);

    // Convert to query_dict format expected by SimpleEngine
    // let mut query_dict = HashMap::new();
    // query_dict.insert("query".to_string(), vec![query]);
    // query_dict.insert("time".to_string(), vec![time]);

    let query_start_time = Instant::now();
    debug!(
        "About to call query_engine.handle_query with query='{}' and time={}",
        query, time
    );
    // match state.query_engine.handle_query(query, time).await {
    match state.query_engine.handle_query(query, time) {
        Some((query_output_labels, query_result)) => {
            let query_duration = query_start_time.elapsed();
            debug!("=== QUERY ENGINE SUCCESS ===");
            debug!(
                "Query engine execution took: {:.2}ms",
                query_duration.as_secs_f64() * 1000.0
            );
            debug!("Query output labels: {:?}", query_output_labels);
            debug!("Query result: {:?}", query_result);

            // Convert QueryResult to Prometheus-compatible format
            debug!("Converting query result to Prometheus format...");
            let prometheus_data =
                convert_query_result_to_prometheus(&query_result, &query_output_labels);
            debug!("Prometheus-formatted data: {:?}", prometheus_data);

            let total_duration = start_time.elapsed();
            debug!(
                "Total request processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            debug!("=== RETURNING SUCCESS RESPONSE ===");
            Ok(Json(PrometheusResponse::success(prometheus_data)))
        }
        None => {
            let total_duration = start_time.elapsed();
            debug!("=== QUERY ENGINE RETURNED NONE ===");
            debug!(
                "Request failed after: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            if state.config.forward_unsupported_queries {
                debug!("Query not supported locally, forwarding to Prometheus");
                handle_prometheus_forward(&state, &params).await
            } else {
                debug!("Query not supported and forwarding disabled, returning error");
                Ok(Json(PrometheusResponse::error(
                    "bad_data",
                    "No result for query",
                )))
            }
        }
    }
}

async fn handle_instant_query_post(
    State(state): State<AppState>,
    Form(params): Form<QueryParams>,
) -> Result<Json<PrometheusResponse>, StatusCode> {
    let start_time = Instant::now();
    debug!("=== INCOMING POST REQUEST ===");
    debug!("Form params extracted: {:?}", params);

    debug!("Delegating to handle_instant_query with parsed params");
    let result = handle_instant_query(Query(params), State(state)).await;
    let total_duration = start_time.elapsed();
    debug!(
        "POST request processing took: {:.2}ms",
        total_duration.as_secs_f64() * 1000.0
    );
    debug!("=== POST REQUEST COMPLETE ===");
    result
}

async fn handle_runtime_info(
    State(state): State<AppState>,
) -> Result<Json<PrometheusResponse>, StatusCode> {
    debug!("Handling runtime info request");

    // Get earliest timestamp per aggregation ID from store
    let earliest_timestamps = match state.store.get_earliest_timestamp_per_aggregation_id() {
        Ok(timestamps) => timestamps,
        Err(e) => {
            error!("Error getting earliest timestamps: {}", e);
            HashMap::new()
        }
    };

    // Forward request to Prometheus and append our data
    let prometheus_url = format!(
        "{}/api/v1/status/runtimeinfo",
        state.config.prometheus_server_url.trim_end_matches('/')
    );

    debug!(
        "Forwarding runtime info request to Prometheus at: {}",
        prometheus_url
    );

    let client = reqwest::Client::new();
    match client
        .get(&prometheus_url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
    {
        Ok(prometheus_response) => {
            match prometheus_response.json::<PrometheusResponse>().await {
                Ok(mut prometheus_data) => {
                    // Append our earliest timestamps data to the Prometheus response
                    if let Some(ref mut data) = prometheus_data.data {
                        if let Some(data_obj) = data.as_object_mut() {
                            data_obj.insert(
                                "earliest_timestamp_per_aggregation_id".to_string(),
                                serde_json::to_value(earliest_timestamps).unwrap_or(json!({})),
                            );
                        }
                    }
                    debug!("Successfully merged runtime info from Prometheus with local data");
                    Ok(Json(prometheus_data))
                }
                Err(parse_err) => {
                    error!("Failed to parse Prometheus runtime info response: {parse_err}");
                    // Fallback to just our data if Prometheus response can't be parsed
                    let runtime_info = json!({
                        "earliest_timestamp_per_aggregation_id": earliest_timestamps
                    });
                    Ok(Json(PrometheusResponse::success(runtime_info)))
                }
            }
        }
        Err(req_err) => {
            error!("Failed to forward runtime info request to Prometheus: {req_err}");
            // Fallback to just our data if Prometheus is unreachable
            let runtime_info = json!({
                "earliest_timestamp_per_aggregation_id": earliest_timestamps
            });
            Ok(Json(PrometheusResponse::success(runtime_info)))
        }
    }
}

async fn handle_prometheus_forward(
    state: &AppState,
    params: &QueryParams,
) -> Result<Json<PrometheusResponse>, StatusCode> {
    debug!("=== FORWARDING TO PROMETHEUS ===");
    debug!("Forwarding params: {:?}", params);
    forward_to_prometheus(state, params, "query").await
}

async fn forward_to_prometheus(
    state: &AppState,
    params: &QueryParams,
    endpoint: &str,
) -> Result<Json<PrometheusResponse>, StatusCode> {
    debug!("=== PROMETHEUS FORWARDING SETUP ===");
    debug!("Endpoint: {}", endpoint);
    debug!(
        "Base Prometheus URL: {}",
        state.config.prometheus_server_url
    );

    // Build the full URL for the Prometheus endpoint
    let full_url = format!(
        "{}/api/v1/{}",
        state.config.prometheus_server_url.trim_end_matches('/'),
        endpoint
    );

    debug!("Full forwarding URL: {}", full_url);

    // Create HTTP client
    let client = reqwest::Client::new();

    // Prepare query parameters for forwarding
    let mut query_params: Vec<(&str, String)> = Vec::new();

    if let Some(query) = &params.query {
        debug!("Adding query parameter: '{}'", query);
        query_params.push(("query", query.clone()));
    }

    if let Some(time) = &params.time {
        debug!("Adding time parameter: {}", time);
        query_params.push(("time", format!("{time}")));
    }

    debug!("Final query parameters for forwarding: {:?}", query_params);

    // Forward the request to Prometheus
    debug!("Sending request to Prometheus...");
    match client
        .get(&full_url)
        .query(&query_params)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status();
            debug!("Received response from Prometheus, status: {}", status);
            match response.json::<PrometheusResponse>().await {
                Ok(prometheus_response) => {
                    debug!(
                        "Successfully parsed Prometheus response: {:?}",
                        prometheus_response
                    );
                    debug!("=== PROMETHEUS FORWARD SUCCESS ===");
                    Ok(Json(prometheus_response))
                }
                Err(parse_err) => {
                    error!("Failed to parse Prometheus response: {parse_err}");
                    debug!("=== PROMETHEUS FORWARD PARSE ERROR ===");
                    Ok(Json(PrometheusResponse::error(
                        "internal",
                        "Failed to parse Prometheus response",
                    )))
                }
            }
        }
        Err(req_err) => {
            error!("Failed to forward query to Prometheus: {req_err}");
            debug!("=== PROMETHEUS FORWARD REQUEST ERROR ===");
            Ok(Json(PrometheusResponse::error(
                "internal",
                &format!("Failed to forward query to Prometheus: {req_err}"),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::{InferenceConfig, StreamingConfig};
    use crate::engines::SimpleEngine;
    use crate::stores::simple_map_store::SimpleMapStore;
    use reqwest::Client;
    use std::sync::Arc;

    async fn setup_test_server() -> u16 {
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            prometheus_server_url: "http://127.0.0.1:9999".to_string(), // Unused for this test
            forward_unsupported_queries: false,
        };

        let inference_config = InferenceConfig::default();
        let streaming_config = Arc::new(StreamingConfig::default());
        let store = Arc::new(SimpleMapStore::new(streaming_config.clone()));
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config.clone(),
            15000,
        ));

        let server = HttpServer::new(config, query_engine, store);
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    #[tokio::test]
    async fn test_get_endpoint_plus_symbol_decoding() {
        // Enable debug logging for this test
        // let _ = tracing_subscriber::fmt()
        //     .with_env_filter("debug")
        //     .try_init();

        let server_port = setup_test_server().await;
        let client = Client::new();

        // Test query with + symbols that should become spaces
        let test_query = "quantile by (instance, job) (0.95, fake_metric_total)";

        println!("Sending query: {test_query}");

        let response = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[("query", test_query)])
            .send()
            .await
            .expect("Failed to send request");

        let status = response.status();
        let response_json: serde_json::Value = response.json().await.expect("Failed to parse JSON");

        println!("Response status: {status}");
        println!("Response JSON: {response_json}");

        // The debug logs should show what query was actually parsed
        assert!(status.is_success() || status == reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_post_endpoint_form_decoding() {
        // let _ = tracing_subscriber::fmt()
        //     .with_env_filter("debug")
        //     .try_init();

        let server_port = setup_test_server().await;
        let client = Client::new();

        // Test the same query via POST with form encoding
        let test_query = "quantile+by+(instance,+job)+(0.95,+fake_metric_total)";

        println!("Sending POST with form data: {test_query}");

        let response = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("query={test_query}&time=1758161478.205"))
            .send()
            .await
            .expect("Failed to send request");

        let status = response.status();
        let response_json: serde_json::Value = response.json().await.expect("Failed to parse JSON");

        println!("Response status: {status}");
        println!("Response JSON: {response_json}");

        assert!(status.is_success() || status == reqwest::StatusCode::OK);
    }
}
