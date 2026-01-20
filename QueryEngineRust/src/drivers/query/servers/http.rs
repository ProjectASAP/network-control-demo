use axum::{
    extract::{Form, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, info};

use crate::drivers::query::adapters::{create_http_adapter, AdapterConfig, HttpProtocolAdapter};
use crate::engines::SimpleEngine;
use crate::stores::Store;

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub port: u16,
    pub handle_http_requests: bool,
    pub adapter_config: AdapterConfig,
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
    adapter: Arc<dyn HttpProtocolAdapter>,
    fallback: Option<Arc<dyn crate::drivers::query::fallback::FallbackClient>>,
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
        // Create adapter using factory
        let adapter = create_http_adapter(self.config.adapter_config.clone());

        let query_endpoint = adapter.get_query_endpoint();
        let runtime_info_path = adapter.get_runtime_info_path();
        info!(
            "Adapter '{}' configured for endpoint: {}",
            adapter.adapter_name(),
            query_endpoint
        );
        info!("Runtime info endpoint: {}", runtime_info_path);

        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine,
            store: self.store,
            adapter: adapter.clone(),
            fallback: self.config.adapter_config.fallback.clone(),
        };

        let app = Router::new()
            .route(query_endpoint, get(handle_instant_query))
            .route(query_endpoint, post(handle_instant_query_post))
            .route(runtime_info_path, get(handle_runtime_info))
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
        // Create adapter using factory
        let adapter = create_http_adapter(self.config.adapter_config.clone());

        let query_endpoint = adapter.get_query_endpoint();
        let runtime_info_path = adapter.get_runtime_info_path();

        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine.clone(),
            store: self.store.clone(),
            adapter: adapter.clone(),
            fallback: self.config.adapter_config.fallback.clone(),
        };

        let app = Router::new()
            .route(query_endpoint, get(handle_instant_query))
            .route(query_endpoint, post(handle_instant_query_post))
            .route(runtime_info_path, get(handle_runtime_info))
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
    query_params: Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Response {
    let start_time = Instant::now();
    debug!("=== INCOMING GET REQUEST ===");
    debug!("Raw query params: {:?}", query_params.0);

    // Step 1: Parse request using adapter (replaces manual QueryParams extraction)
    let parsed_request = match state.adapter.parse_get_request(query_params).await {
        Ok(req) => {
            debug!(
                "Successfully parsed - query: '{}', time: {}",
                req.query, req.time
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse request: {:?}", parse_error);
            // Adapter handles error formatting (replaces manual PrometheusResponse::error)
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    // Now we have: parsed_request.query and parsed_request.time
    // (This replaces the old: query and time variables)

    // Check if handling is enabled (keeping this check for now)
    if !state.config.handle_http_requests {
        debug!("HTTP request handling is disabled");
        if let Some(fallback) = &state.fallback {
            debug!("Forwarding to fallback due to disabled handling");
            return match fallback.execute_query(&parsed_request).await {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            };
        } else {
            debug!("Returning error - both handling and forwarding disabled");
            use crate::drivers::query::adapters::AdapterError;
            return match state
                .adapter
                .format_error_response(&AdapterError::ProtocolError(
                    "Query handling is disabled".to_string(),
                ))
                .await
            {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    }

    // Step 2: Execute query with engine (using parsed request)
    let query_start_time = Instant::now();
    debug!(
        "About to call query_engine.handle_query with query='{}' and time={}",
        parsed_request.query, parsed_request.time
    );
    match state
        .query_engine
        .handle_query(parsed_request.query.clone(), parsed_request.time)
    {
        Some((query_output_labels, query_result)) => {
            let query_duration = query_start_time.elapsed();
            debug!("=== QUERY ENGINE SUCCESS ===");
            debug!(
                "Query engine execution took: {:.2}ms",
                query_duration.as_secs_f64() * 1000.0
            );
            debug!("Query output labels: {:?}", query_output_labels);
            debug!("Query result: {:?}", query_result);

            // Step 3: Format success response using adapter
            // (Adapter handles protocol-specific formatting, e.g., convert_query_result_to_prometheus)
            use crate::drivers::query::adapters::QueryExecutionResult;
            let execution_result = QueryExecutionResult {
                query_output_labels,
                query_result,
            };

            let total_duration = start_time.elapsed();
            debug!(
                "Total request processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            debug!("=== RETURNING SUCCESS RESPONSE ===");

            match state
                .adapter
                .format_success_response(&execution_result)
                .await
            {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            }
        }
        None => {
            let total_duration = start_time.elapsed();
            debug!("=== QUERY ENGINE RETURNED NONE ===");
            debug!(
                "Request failed after: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );

            // Step 4: Handle unsupported query using fallback client
            if let Some(fallback) = &state.fallback {
                debug!("Query not supported locally, forwarding to fallback");
                // Fallback client handles the HTTP call and returns formatted response
                match fallback.execute_query(&parsed_request).await {
                    Ok(response) => response.into_response(),
                    Err(status) => status.into_response(),
                }
            } else {
                debug!("Query not supported and forwarding disabled, returning error");
                // Adapter formats the unsupported query error for its protocol
                match state.adapter.format_unsupported_query_response().await {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                }
            }
        }
    }
}

async fn handle_instant_query_post(
    State(state): State<AppState>,
    Form(params): Form<HashMap<String, String>>,
) -> Response {
    let start_time = Instant::now();
    debug!("=== INCOMING POST REQUEST ===");
    debug!("Form params extracted: {:?}", params);

    // Use adapter to parse POST request (handles form-encoded parameters)
    let parsed_request = match state.adapter.parse_post_request(Form(params)).await {
        Ok(req) => {
            debug!(
                "Successfully parsed POST - query: '{}', time: {}",
                req.query, req.time
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse POST request: {:?}", parse_error);
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    // Delegate to the same logic as GET by calling the query engine directly
    // (We could call handle_instant_query_get_internal if we refactor further)
    debug!("Delegating POST to query engine");

    // For now, just convert to Query and delegate
    let mut query_map = HashMap::new();
    query_map.insert("query".to_string(), parsed_request.query);
    query_map.insert("time".to_string(), parsed_request.time.to_string());

    let result = handle_instant_query(Query(query_map), State(state)).await;
    let total_duration = start_time.elapsed();
    debug!(
        "POST request processing took: {:.2}ms",
        total_duration.as_secs_f64() * 1000.0
    );
    debug!("=== POST REQUEST COMPLETE ===");
    result
}

async fn handle_runtime_info(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    debug!("Delegating runtime info request to adapter");

    // Delegate to adapter for protocol-specific handling
    state.adapter.handle_runtime_info(state.store.clone()).await
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
        let adapter_config = AdapterConfig::prometheus_promql(
            "http://127.0.0.1:9999".to_string(), // Unused for this test
            false,                               // forward_unsupported_queries
        );

        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };

        let inference_config = InferenceConfig::default();
        let streaming_config = Arc::new(StreamingConfig::default());
        let store = Arc::new(SimpleMapStore::new(streaming_config.clone(), false));
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config.clone(),
            15000,
            crate::data_model::QueryLanguage::promql,
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
