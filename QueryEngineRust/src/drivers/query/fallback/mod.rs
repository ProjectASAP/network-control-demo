use async_trait::async_trait;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde_json::Value;

use crate::drivers::query::adapters::ParsedQueryRequest;

/// Response format from fallback backend
#[derive(Debug, Clone)]
pub enum FallbackResponse {
    /// JSON response (used by Prometheus, etc.)
    Json(Value),
    /// Plain text response (used by ClickHouse TSV, etc.)
    Text(String),
}

impl IntoResponse for FallbackResponse {
    fn into_response(self) -> Response {
        match self {
            FallbackResponse::Json(value) => Json(value).into_response(),
            FallbackResponse::Text(text) => {
                // Return plain text with appropriate content type
                (
                    [(
                        axum::http::header::CONTENT_TYPE,
                        "text/tab-separated-values",
                    )],
                    text,
                )
                    .into_response()
            }
        }
    }
}

/// Client for forwarding unsupported queries to a fallback backend
#[async_trait]
pub trait FallbackClient: Send + Sync {
    /// Execute a query against the fallback backend
    ///
    /// # Arguments
    /// * `request` - The parsed query request (query string, time, etc.)
    ///
    /// # Returns
    /// Protocol-specific response from the fallback backend (JSON or Text)
    async fn execute_query(
        &self,
        request: &ParsedQueryRequest,
    ) -> Result<FallbackResponse, StatusCode>;

    /// Get runtime info from the fallback backend (optional)
    ///
    /// # Returns
    /// Runtime info as JSON, or empty object if not supported
    async fn get_runtime_info(&self) -> Result<Value, StatusCode> {
        // Default implementation: return empty object
        Ok(serde_json::json!({}))
    }
}

mod clickhouse;
mod prometheus;

pub use clickhouse::ClickHouseHttpFallback;
pub use prometheus::PrometheusHttpFallback;
