use super::config::AdapterConfig;
use super::traits::*;
use crate::utils::http::convert_query_result_to_prometheus;
use async_trait::async_trait;
use axum::{
    extract::{Form, Query},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error};

/// Prometheus-compatible response structure
#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusResponse {
    pub status: String,
    pub data: Option<Value>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl PrometheusResponse {
    pub fn success(data: Value) -> Self {
        Self {
            status: "success".to_string(),
            data: Some(data),
            error_type: None,
            error: None,
        }
    }

    pub fn error(error_type: &str, error: &str) -> Self {
        Self {
            status: "error".to_string(),
            data: None,
            error_type: Some(error_type.to_string()),
            error: Some(error.to_string()),
        }
    }
}

/// Prometheus HTTP protocol adapter
pub struct PrometheusHttpAdapter {
    config: AdapterConfig,
}

impl PrometheusHttpAdapter {
    pub fn new(config: AdapterConfig) -> Self {
        Self { config }
    }

    /// Helper to parse query parameters (used by both GET and POST)
    fn parse_params(
        &self,
        params: &HashMap<String, String>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        let query = params
            .get("query")
            .ok_or_else(|| AdapterError::MissingParameter("query".to_string()))?
            .clone();

        let time = if let Some(time_str) = params.get("time") {
            time_str.parse::<f64>().map_err(|e| {
                AdapterError::InvalidParameter(format!("Invalid time parameter: {}", e))
            })?
        } else {
            // Use current time as default
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
        };

        Ok(ParsedQueryRequest { query, time })
    }
}

#[async_trait]
impl QueryRequestAdapter for PrometheusHttpAdapter {
    async fn parse_get_request(
        &self,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        debug!(
            "Prometheus adapter: parsing GET request with params: {:?}",
            params
        );
        self.parse_params(&params)
    }

    async fn parse_post_request(
        &self,
        Form(params): Form<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        debug!(
            "Prometheus adapter: parsing POST request with params: {:?}",
            params
        );
        self.parse_params(&params)
    }

    fn get_query_endpoint(&self) -> &'static str {
        "/api/v1/query"
    }
}

#[async_trait]
impl QueryResponseAdapter for PrometheusHttpAdapter {
    async fn format_success_response(
        &self,
        result: &QueryExecutionResult,
    ) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting success response");

        let prometheus_data =
            convert_query_result_to_prometheus(&result.query_result, &result.query_output_labels);

        let response = PrometheusResponse::success(prometheus_data);
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }

    async fn format_error_response(&self, error: &AdapterError) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting error response: {:?}", error);

        let (error_type, error_msg) = match error {
            AdapterError::MissingParameter(p) => ("bad_data", format!("Missing parameter: {}", p)),
            AdapterError::InvalidParameter(p) => ("bad_data", format!("Invalid parameter: {}", p)),
            AdapterError::ParseError(e) => ("bad_data", format!("Parse error: {}", e)),
            AdapterError::NetworkError(e) => ("internal", format!("Network error: {}", e)),
            AdapterError::ProtocolError(e) => ("internal", format!("Protocol error: {}", e)),
        };

        let response = PrometheusResponse::error(error_type, &error_msg);
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }

    async fn format_unsupported_query_response(&self) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting unsupported query response");

        let response = PrometheusResponse::error("bad_data", "No result for query");
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }
}

#[async_trait]
impl HttpProtocolAdapter for PrometheusHttpAdapter {
    fn adapter_name(&self) -> &'static str {
        "PrometheusHTTP"
    }

    fn get_runtime_info_path(&self) -> &'static str {
        "/api/v1/status/runtimeinfo"
    }

    async fn handle_runtime_info(
        &self,
        store: Arc<dyn crate::stores::Store>,
    ) -> Result<Json<Value>, StatusCode> {
        debug!("Handling runtime info request in Prometheus adapter");

        // Get earliest timestamp per aggregation ID from store
        let earliest_timestamps = match store.get_earliest_timestamp_per_aggregation_id() {
            Ok(timestamps) => timestamps,
            Err(e) => {
                error!("Error getting earliest timestamps: {}", e);
                HashMap::new()
            }
        };

        // Get runtime info from fallback if available
        let mut runtime_data = if let Some(fallback) = &self.config.fallback {
            debug!("Fetching runtime info from fallback");
            match fallback.get_runtime_info().await {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to get runtime info from fallback: {:?}", e);
                    json!({})
                }
            }
        } else {
            json!({})
        };

        // Merge local data with fallback data
        if let Some(data_obj) = runtime_data.as_object_mut() {
            data_obj.insert(
                "earliest_timestamp_per_aggregation_id".to_string(),
                serde_json::to_value(earliest_timestamps).unwrap_or(json!({})),
            );
        } else {
            // If runtime_data is not an object, just create a new one with local data
            runtime_data = json!({
                "earliest_timestamp_per_aggregation_id": earliest_timestamps
            });
        }

        debug!("Successfully merged runtime info with local data");

        // Wrap in Prometheus response format
        let response = PrometheusResponse::success(runtime_data);
        Ok(Json(serde_json::to_value(response).unwrap()))
    }
}
