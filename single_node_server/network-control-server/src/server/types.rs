use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use elasticsearch_dsl_ast::{Document, Search};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::ServerRuntimeConfig;
use crate::metrics::{MetricField, MetricStore};

/// The wire-level search request is the typed Elasticsearch DSL `Search` AST.
pub(crate) type SearchRequest = Search;

use super::TimingSender;
use super::logging::LogSender;
use super::payload_log::PayloadLogger;

#[derive(Clone)]
pub struct AppState {
    pub stores_by_index: HashMap<String, Arc<dyn MetricStore>>,
    pub current_epoch_by_index: Arc<Mutex<HashMap<String, u64>>>,
    pub runtime_config: Arc<ServerRuntimeConfig>,
    pub aggregation_engine: Arc<dyn AggregationEngine>,
    pub request_planner: Arc<dyn RequestPlanner>,
    pub upstream_client: Arc<dyn UpstreamClient>,
    pub http_client: Client,
    pub timing_enabled: bool,
    pub timing_sender: Option<TimingSender>,
    pub log_tx: Option<LogSender>,
    pub payload_logger: Option<PayloadLogger>,
}

impl AppState {
    pub(crate) fn normalize_index_name(index_name: &str) -> String {
        index_name.trim().to_ascii_lowercase()
    }

    pub(crate) fn store_for_index(&self, index_name: &str) -> Option<Arc<dyn MetricStore>> {
        self.stores_by_index
            .get(&Self::normalize_index_name(index_name))
            .cloned()
    }
}

#[derive(Serialize)]
pub(crate) struct RootResponse<'a> {
    pub(crate) message: &'a str,
    pub(crate) examples: [&'a str; 3],
}

/// Engine-side description of a `percentiles` aggregation request, extracted
/// from the typed DSL during planning.
#[derive(Debug, Clone)]
pub(crate) struct PercentileAggregation {
    pub(crate) field: String,
    pub(crate) percents: Vec<f64>,
}

/// Engine-side description of a standard ES `sum` aggregation request,
/// extracted from the typed DSL during planning.
#[derive(Debug, Clone)]
pub(crate) struct SumAggregation {
    pub(crate) field: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct MetricsQuery {
    pub(crate) quantiles: Vec<String>,
    pub(crate) node_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct BatchQueryRequest {
    pub(crate) keys: Vec<String>,
    pub(crate) fields: Option<Vec<String>>,
    pub(crate) aggs: Vec<String>,
    pub(crate) percents: Option<Vec<f64>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchQueryResult {
    pub(crate) key: String,
    pub(crate) percentiles: Option<HashMap<String, HashMap<String, f64>>>,
    pub(crate) sum: Option<HashMap<String, f64>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchQueryResponse {
    pub(crate) results: Vec<BatchQueryResult>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum DocumentAction {
    Index(DocumentActionInner),
    Create(DocumentActionInner),
    Update(DocumentActionInner),
    Delete(DocumentActionInner),
}

#[derive(Debug, Serialize, Deserialize)]
pub (crate) struct DocumentActionInner {
    #[serde(rename = "_index")]
    pub(crate) index: Option<String>,
    #[serde(rename = "_id")]
    pub(crate) id: Option<String>,
}

#[derive(Debug)]
pub(crate) struct IngestRecord {
    pub(crate) epoch: Option<u64>,
    pub(crate) key: Vec<String>,
    /// metric storage_field name → values
    pub(crate) metrics: std::collections::HashMap<String, Vec<f64>>,
}

impl IngestRecord {
    /// Parse a raw JSON value into an IngestRecord using the config's field mapping.
    pub(crate) fn from_json(
        value: &Value,
        mapping: &crate::config::IngestFieldMapping,
    ) -> Result<Self, String> {
        let obj = value
            .as_object()
            .ok_or_else(|| "ingest body must be a JSON object".to_string())?;

        let epoch = obj.get(&mapping.epoch_field).and_then(|v| v.as_u64());

        let key = parse_string_array(obj, &mapping.key_field)?;

        let mut metrics = std::collections::HashMap::new();
        for (metric_name, json_field) in &mapping.metric_fields {
            let values = parse_f64_array(obj, json_field)?;
            metrics.insert(metric_name.clone(), values);
        }

        Ok(Self {
            epoch,
            key,
            metrics,
        })
    }

    /// Returns the number of samples (length of the key array).
    pub(crate) fn len(&self) -> usize {
        self.key.len()
    }
}

fn parse_string_array(
    obj: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Vec<String>, String> {
    let arr = obj
        .get(field)
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("field '{}' must be a JSON array", field))?;
    arr.iter()
        .map(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| format!("field '{}' elements must be strings", field))
        })
        .collect()
}

fn parse_f64_array(obj: &serde_json::Map<String, Value>, field: &str) -> Result<Vec<f64>, String> {
    let arr = obj
        .get(field)
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("field '{}' must be a JSON array", field))?;
    arr.iter()
        .map(|v| {
            v.as_f64()
                .ok_or_else(|| format!("field '{}' elements must be numbers", field))
        })
        .collect()
}

#[derive(Clone, Debug)]
pub(crate) enum AggregationKind {
    Percentiles(PercentileAggregation),
    Sum(SumAggregation),
}

#[derive(Clone, Debug)]
pub(crate) struct AggregationRegistration {
    pub(crate) name: &'static str,
    pub(crate) supports_search: bool,
    pub(crate) supports_batch: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct QueryContext {
    pub(crate) index_name: Option<String>,
    pub(crate) key: Option<String>,
    pub(crate) epoch: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct LocalAggregationPlan {
    pub(crate) name: String,
    pub(crate) kind: AggregationKind,
}

#[derive(Clone, Debug)]
pub(crate) struct QueryExecutionPlan {
    pub(crate) context: QueryContext,
    pub(crate) local_aggs: Vec<LocalAggregationPlan>,
    pub(crate) forwarded_aggs: HashSet<String>,
    pub(crate) unsupported_features: Vec<UnsupportedFeature>,
    pub(crate) has_other_fields: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct UnsupportedFeature {
    pub(crate) code: String,
    pub(crate) message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) details: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ErrorResponse {
    pub(crate) code: String,
    pub(crate) message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) details: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) supported_features: Vec<String>,
}

#[async_trait]
pub(crate) trait UpstreamClient: Send + Sync {
    async fn forward(
        &self,
        state: &AppState,
        index_name: &str,
        headers: &axum::http::HeaderMap,
        body: &Value,
    ) -> Result<Value, axum::response::Response>;
}

pub(crate) trait AggregationEngine: Send + Sync {
    fn evaluate(
        &self,
        state: &AppState,
        store: &dyn MetricStore,
        context: &QueryContext,
        plan: &LocalAggregationPlan,
    ) -> Result<Option<Value>, String>;
    fn registration(&self, name: &str) -> Option<AggregationRegistration>;
    fn supported_features(&self) -> Vec<String>;
}

pub(crate) trait RequestPlanner: Send + Sync {
    fn plan_search(
        &self,
        state: &AppState,
        request: &SearchRequest,
        index_name: &str,
    ) -> Result<QueryExecutionPlan, String>;
}

impl ErrorResponse {
    pub(crate) fn unsupported(
        message: impl Into<String>,
        details: Vec<String>,
        supported_features: Vec<String>,
    ) -> Self {
        Self {
            code: "unsupported_request".to_string(),
            message: message.into(),
            details,
            supported_features,
        }
    }

    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            code: "bad_request".to_string(),
            message: message.into(),
            details: Vec::new(),
            supported_features: Vec::new(),
        }
    }
}

pub(crate) fn metric_field_for_name(
    config: &ServerRuntimeConfig,
    index_name: &str,
    name: &str,
) -> Option<MetricField> {
    let normalized = name.trim().to_ascii_lowercase();
    config
        .schema_for_index(index_name)
        .and_then(|schema| {
            schema
                .metrics
                .iter()
                .enumerate()
                .find(|(_, metric)| {
                    metric.name.trim().eq_ignore_ascii_case(&normalized)
                        || metric
                            .aliases
                            .iter()
                            .any(|alias| alias.trim().eq_ignore_ascii_case(&normalized))
                })
                .map(|(idx, metric)| MetricField::new(idx, &metric.storage_field))
        })
}
