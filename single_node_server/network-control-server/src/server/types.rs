use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::AggregationConfig;
// use crate::metrics::{EntityEstimate, MetricStore, NodeStore};
use crate::metrics::NodeStore;

use super::TimingSender;
use super::logging::LogSender;

#[derive(Clone)]
pub struct AppState {
    // pub store: Arc<MetricStore>,
    pub node_store: Arc<NodeStore>,
    pub current_epoch: Arc<Mutex<Option<u64>>>,
    pub agg_config: Arc<AggregationConfig>,
    pub http_client: Client,
    pub upstream_url: String,
    pub timing_enabled: bool,
    pub timing_sender: Option<TimingSender>,
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
pub(crate) struct CumulativeAggregation {
    pub(crate) field: String,
    pub(crate) key: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MetricsQuery {
    pub(crate) quantiles: Vec<String>,
    pub(crate) node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
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
    pub(crate) cumulative: Option<HashMap<String, f64>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchQueryResponse {
    pub(crate) results: Vec<BatchQueryResult>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct IngestRecord {
    #[serde(default)]
    pub(crate) epoch: Option<u64>,
    pub(crate) task: Vec<String>,
    pub(crate) cluster: Vec<String>,
    pub(crate) cpu_cores: Vec<f64>,
    pub(crate) memory_gb: Vec<f64>,
    pub(crate) network_mbps: Vec<f64>,
}

pub(crate) enum AggregationKind {
    Percentiles(PercentileAggregation),
    Cumulative(CumulativeAggregation),
}

// pub(crate) enum TopEntitiesResult {
//     Single(EntityEstimate),
//     Multi(HashMap<String, EntityEstimate>),
// }

impl AggregationRequest {
    pub(crate) fn kind(&self) -> Option<AggregationKind> {
        let percentiles = self.percentiles.as_ref();
        let cumulative = self.cumulative.as_ref();
        let count = usize::from(percentiles.is_some()) + usize::from(cumulative.is_some());
        if count != 1 || !self.other.is_empty() {
            return None;
        }
        if let Some(pct) = percentiles {
            return Some(AggregationKind::Percentiles(pct.clone()));
        }
        cumulative.map(|cum| AggregationKind::Cumulative(cum.clone()))
    }
}
