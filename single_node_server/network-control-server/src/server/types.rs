use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::AggregationConfig;
use crate::metrics::{EntityEstimate, MetricStore};

use super::TimingSender;
use super::cache::QueryCache;
use super::logging::LogSender;

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
pub(crate) struct RootResponse<'a> {
    pub(crate) message: &'a str,
    pub(crate) examples: [&'a str; 3],
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct SearchRequest {
    pub(crate) aggs: Option<BTreeMap<String, AggregationRequest>>,
    #[serde(flatten, default)]
    pub(crate) _other: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct AggregationRequest {
    #[serde(default)]
    pub(crate) percentiles: Option<PercentileAggregation>,
    #[serde(default)]
    pub(crate) frequency: Option<FrequencyAggregation>,
    #[serde(default)]
    pub(crate) top_entities: Option<TopEntitiesAggregation>,
    #[serde(default)]
    pub(crate) cumulative: Option<CumulativeAggregation>,
    #[serde(flatten, default)]
    pub(crate) other: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct PercentileAggregation {
    pub(crate) field: String,
    pub(crate) percents: Vec<f64>,
    #[serde(default)]
    pub(crate) key: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct TopEntitiesAggregation {
    #[serde(default)]
    pub(crate) field: Option<String>,
    #[serde(default)]
    pub(crate) fields: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct CumulativeAggregation {
    pub(crate) field: String,
    pub(crate) key: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct FrequencyAggregation {
    pub(crate) field: String,
    pub(crate) key: String,
    pub(crate) value: f64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MetricsQuery {
    pub(crate) quantiles: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BatchQueryRequest {
    pub(crate) keys: Vec<String>,
    pub(crate) fields: Option<Vec<String>>,
    pub(crate) aggs: Vec<String>,
    pub(crate) percents: Option<Vec<f64>>,
    pub(crate) frequency_value: Option<f64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchQueryResult {
    pub(crate) key: String,
    pub(crate) percentiles: Option<HashMap<String, HashMap<String, f64>>>,
    pub(crate) cumulative: Option<HashMap<String, i32>>,
    pub(crate) frequency: Option<HashMap<String, i32>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchQueryResponse {
    pub(crate) results: Vec<BatchQueryResult>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct IngestRecord {
    pub(crate) task: Vec<String>,
    pub(crate) cluster: Vec<String>,
    pub(crate) cpu_cores: Vec<f64>,
    pub(crate) memory_gb: Vec<f64>,
    pub(crate) network_mbps: Vec<f64>,
}

pub(crate) enum AggregationKind {
    Percentiles(PercentileAggregation),
    TopEntities(TopEntitiesAggregation),
    Cumulative(CumulativeAggregation),
    Frequency(FrequencyAggregation),
}

pub(crate) enum QueryKeyStatus {
    None,
    Key(String),
    Unsupported,
}

pub(crate) enum TopEntitiesResult {
    Single(EntityEstimate),
    Multi(HashMap<String, EntityEstimate>),
}

impl AggregationRequest {
    pub(crate) fn kind(&self) -> Option<AggregationKind> {
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
