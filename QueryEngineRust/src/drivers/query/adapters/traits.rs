use async_trait::async_trait;
use axum::{
    extract::{Form, Query},
    http::StatusCode,
    response::{Json, Response},
};
use promql_utilities::data_model::KeyByLabelNames;
use serde_json::Value;
use std::collections::HashMap;

use crate::engines::QueryResult;

/// Parsed query request data ready for engine processing
#[derive(Debug, Clone)]
pub struct ParsedQueryRequest {
    pub query: String,
    pub time: f64,
}

/// Result of query execution (before formatting for protocol)
#[derive(Debug, Clone)]
pub struct QueryExecutionResult {
    pub query_output_labels: KeyByLabelNames,
    pub query_result: QueryResult,
}

/// Error types for adapters
#[derive(Debug)]
pub enum AdapterError {
    MissingParameter(String),
    InvalidParameter(String),
    ParseError(String),
    NetworkError(String),
    ProtocolError(String),
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdapterError::MissingParameter(p) => write!(f, "Missing parameter: {}", p),
            AdapterError::InvalidParameter(p) => write!(f, "Invalid parameter: {}", p),
            AdapterError::ParseError(e) => write!(f, "Parse error: {}", e),
            AdapterError::NetworkError(e) => write!(f, "Network error: {}", e),
            AdapterError::ProtocolError(e) => write!(f, "Protocol error: {}", e),
        }
    }
}

impl std::error::Error for AdapterError {}

/// Trait for parsing incoming HTTP requests into internal query format
/// Handles Axum extractors directly for different request types (GET/POST)
#[async_trait]
pub trait QueryRequestAdapter: Send + Sync {
    /// Parse a GET request with query parameters
    async fn parse_get_request(
        &self,
        query_params: Query<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError>;

    /// Parse a POST request with form data
    async fn parse_post_request(
        &self,
        form_params: Form<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError>;

    /// Get the HTTP path this adapter handles (e.g., "/api/v1/query")
    fn get_query_endpoint(&self) -> &'static str;
}

/// Trait for formatting query results into protocol-specific HTTP responses
#[async_trait]
pub trait QueryResponseAdapter: Send + Sync {
    /// Format a successful query result into protocol response
    async fn format_success_response(
        &self,
        result: &QueryExecutionResult,
    ) -> Result<Response, StatusCode>;

    /// Format an error into protocol response
    async fn format_error_response(&self, error: &AdapterError) -> Result<Response, StatusCode>;

    /// Format an error when query returns None (unsupported query)
    async fn format_unsupported_query_response(&self) -> Result<Response, StatusCode>;
}

/// Adapter trait for HTTP-based query protocols
/// (Prometheus HTTP, ClickHouse HTTP, etc.)
///
/// For non-HTTP protocols (Flight SQL, native protocols),
/// define separate adapter traits.
///
/// Note: Fallback logic is handled separately via FallbackClient
#[async_trait]
pub trait HttpProtocolAdapter: QueryRequestAdapter + QueryResponseAdapter + Send + Sync {
    /// Get a descriptive name for this adapter (for logging/debugging)
    fn adapter_name(&self) -> &'static str;

    /// Get the path for the runtime info endpoint
    ///
    /// Example: "/api/v1/status/runtimeinfo" for Prometheus
    fn get_runtime_info_path(&self) -> &'static str;

    /// Handle runtime info request
    ///
    /// The adapter can query the store for internal metrics and
    /// optionally forward to fallback backend for additional info.
    async fn handle_runtime_info(
        &self,
        store: std::sync::Arc<dyn crate::stores::Store>,
    ) -> Result<Json<Value>, StatusCode>;
}
