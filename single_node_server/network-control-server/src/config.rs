use std::{
    collections::{HashMap, HashSet},
    env,
    error::Error,
    fs,
    path::Path,
};

use serde::Deserialize;

use crate::metrics::MetricField;

const SUPPORTED_AGGREGATIONS: &[&str] = &["percentiles", "cumulative"];
const SUPPORTED_FILTER_TYPES: &[&str] = &["term"];
const SUPPORTED_UPSTREAM_MODES: &[&str] = &["disabled", "fallback"];

#[derive(Clone, Debug, Deserialize)]
pub struct ServerRuntimeConfig {
    pub server: ServerConfig,
    pub api: ApiConfig,
    pub upstream: UpstreamConfig,
    pub storage: StorageConfig,
    pub schema: SchemaConfig,
    pub query_support: QuerySupportConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub body_limit_mb: usize,
    pub request_log_buffer: usize,
    #[serde(default)]
    pub enable_timing: bool,
    pub timing_csv_path: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ApiConfig {
    pub index_name: String,
    #[serde(default = "default_true")]
    pub enable_batch_endpoint: bool,
    #[serde(default = "default_true")]
    pub enable_metrics_endpoint: bool,
    #[serde(default)]
    pub strict_mode: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpstreamConfig {
    pub mode: String,
    pub search_url: Option<String>,
    #[serde(default)]
    pub forward_headers: Vec<String>,
    /// Loaded at runtime from ES_API_KEY env var or a key file; never from YAML.
    #[serde(skip, default)]
    pub es_api_key: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StorageConfig {
    pub backend: String,
    pub node_catalog: NodeCatalogConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeCatalogConfig {
    pub kind: String,
    pub count: usize,
    pub range: NodeCatalogRange,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeCatalogRange {
    pub start: String,
    pub end: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SchemaConfig {
    pub metrics: Vec<MetricConfig>,
    pub key_fields: Vec<String>,
    pub ingest_field_mapping: IngestFieldMapping,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricConfig {
    pub name: String,
    pub aliases: Vec<String>,
    pub storage_field: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct IngestFieldMapping {
    pub key_field: String,
    pub epoch_field: String,
    pub task_field: Option<String>,
    pub metric_fields: HashMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct QuerySupportConfig {
    pub aggregations: Vec<String>,
    pub supported_filter_types: Vec<String>,
    pub default_batch_fields: Vec<String>,
    pub default_batch_percents: Vec<f64>,
}

impl ServerRuntimeConfig {
    pub fn load_from_env_and_args(args: &[String]) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let path = config_path_from_args(args)
            .or_else(|| env::var("NCS_CONFIG_PATH").ok())
            .unwrap_or_else(|| "server-config.yaml".to_string());
        let contents = fs::read_to_string(&path)?;
        let mut config: ServerRuntimeConfig = serde_yaml::from_str(&contents)?;
        config.apply_env_overrides();
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.server.host.trim().is_empty() {
            return Err("server.host must not be empty".into());
        }
        if self.server.body_limit_mb == 0 {
            return Err("server.body_limit_mb must be > 0".into());
        }
        if self.server.request_log_buffer == 0 {
            return Err("server.request_log_buffer must be > 0".into());
        }
        if self.server.timing_csv_path.trim().is_empty() {
            return Err("server.timing_csv_path must not be empty".into());
        }
        if self.api.index_name.trim().is_empty() {
            return Err("api.index_name must not be empty".into());
        }
        if self.storage.backend.trim() != "in_memory_node_store" {
            return Err(format!(
                "unsupported storage.backend '{}'; expected in_memory_node_store",
                self.storage.backend
            )
            .into());
        }
        if self.storage.node_catalog.kind.trim() != "range" {
            return Err(format!(
                "unsupported storage.node_catalog.kind '{}'; expected range",
                self.storage.node_catalog.kind
            )
            .into());
        }
        if self.storage.node_catalog.count == 0 {
            return Err("storage.node_catalog.count must be > 0".into());
        }
        let upstream_mode = self.upstream.mode.trim().to_ascii_lowercase();
        if !SUPPORTED_UPSTREAM_MODES.contains(&upstream_mode.as_str()) {
            return Err(format!(
                "unsupported upstream.mode '{}'; supported: {}",
                self.upstream.mode,
                SUPPORTED_UPSTREAM_MODES.join(", ")
            )
            .into());
        }
        if upstream_mode == "fallback"
            && self
                .upstream
                .search_url
                .as_ref()
                .map(|url| url.trim().is_empty())
                .unwrap_or(true)
        {
            return Err("upstream.search_url is required when upstream.mode=fallback".into());
        }

        let mut metrics = HashSet::new();
        for metric in &self.schema.metrics {
            if metric.name.trim().is_empty() {
                return Err("schema.metrics[].name must not be empty".into());
            }
            if MetricField::from_storage_field(&metric.storage_field).is_none() {
                return Err(format!(
                    "unsupported schema.metrics[].storage_field '{}'",
                    metric.storage_field
                )
                .into());
            }
            if !metrics.insert(metric.name.trim().to_ascii_lowercase()) {
                return Err(format!("duplicate schema metric '{}'", metric.name).into());
            }
        }
        if self.schema.key_fields.is_empty() {
            return Err("schema.key_fields must not be empty".into());
        }
        let allowed_key_fields: HashSet<String> = self
            .schema
            .key_fields
            .iter()
            .map(|item| item.trim().to_ascii_lowercase())
            .collect();
        if !allowed_key_fields.contains(
            &self
                .schema
                .ingest_field_mapping
                .key_field
                .trim()
                .to_ascii_lowercase(),
        ) {
            return Err(
                "schema.ingest_field_mapping.key_field must be listed in schema.key_fields".into(),
            );
        }

        for agg in &self.query_support.aggregations {
            let normalized = agg.trim().to_ascii_lowercase();
            if !SUPPORTED_AGGREGATIONS.contains(&normalized.as_str()) {
                return Err(format!(
                    "unsupported query_support aggregation '{}'; supported: {}",
                    agg,
                    SUPPORTED_AGGREGATIONS.join(", ")
                )
                .into());
            }
        }
        for filter_type in &self.query_support.supported_filter_types {
            let normalized = filter_type.trim().to_ascii_lowercase();
            if !SUPPORTED_FILTER_TYPES.contains(&normalized.as_str()) {
                return Err(format!(
                    "unsupported query_support filter '{}'; supported: {}",
                    filter_type,
                    SUPPORTED_FILTER_TYPES.join(", ")
                )
                .into());
            }
        }
        if self.query_support.default_batch_fields.is_empty() {
            return Err("query_support.default_batch_fields must not be empty".into());
        }
        if self.query_support.default_batch_percents.is_empty() {
            return Err("query_support.default_batch_percents must not be empty".into());
        }
        for percent in &self.query_support.default_batch_percents {
            if !(0.0..=100.0).contains(percent) {
                return Err(format!(
                    "query_support.default_batch_percents contains out-of-range value {percent}"
                )
                .into());
            }
        }

        let metric_names = self.metric_names();
        for field in &self.query_support.default_batch_fields {
            if !metric_names.contains(&field.trim().to_ascii_lowercase()) {
                return Err(format!(
                    "query_support.default_batch_fields contains unknown metric '{}'",
                    field
                )
                .into());
            }
        }

        Ok(())
    }

    pub fn apply_env_overrides(&mut self) {
        if let Ok(host) = env::var("NCS_SERVER_HOST") {
            self.server.host = host;
        }
        if let Ok(port) = env::var("NCS_SERVER_PORT") {
            if let Ok(port) = port.parse::<u16>() {
                self.server.port = port;
            }
        }
        if let Ok(url) = env::var("NCS_UPSTREAM_SEARCH_URL") {
            self.upstream.search_url = Some(url);
        }
        if let Ok(enabled) = env::var("NCS_TIMING_ENABLED") {
            self.server.enable_timing = parse_bool(&enabled);
        }
        if let Ok(path) = env::var("NCS_TIMING_CSV_PATH") {
            self.server.timing_csv_path = path;
        }
        self.upstream.es_api_key = load_es_api_key();
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    pub fn body_limit_bytes(&self) -> usize {
        self.server.body_limit_mb * 1024 * 1024
    }

    pub fn is_upstream_enabled(&self) -> bool {
        self.upstream.mode.eq_ignore_ascii_case("fallback")
    }

    pub fn search_path(&self) -> String {
        format!("/{}/_search", self.api.index_name)
    }

    pub fn batch_path(&self) -> String {
        format!("/{}/_batch", self.api.index_name)
    }

    pub fn metric_names(&self) -> HashSet<String> {
        self.schema
            .metrics
            .iter()
            .map(|metric| metric.name.trim().to_ascii_lowercase())
            .collect()
    }

    pub fn key_fields(&self) -> HashSet<String> {
        self.schema
            .key_fields
            .iter()
            .map(|item| item.trim().to_ascii_lowercase())
            .collect()
    }

    pub fn aggregation_names(&self) -> HashSet<String> {
        self.query_support
            .aggregations
            .iter()
            .map(|item| item.trim().to_ascii_lowercase())
            .collect()
    }
}

/// Load the Elasticsearch API key using the same resolution order as the Python scripts:
/// 1. `ES_API_KEY` environment variable
/// 2. File path from `ES_API_KEY_FILE` environment variable
/// 3. `.es_api_key` file in the current working directory
fn load_es_api_key() -> Option<String> {
    if let Ok(key) = env::var("ES_API_KEY") {
        if !key.trim().is_empty() {
            return Some(key.trim().to_string());
        }
    }
    let key_path = env::var("ES_API_KEY_FILE")
        .map(|p| p.into())
        .unwrap_or_else(|_| Path::new(".es_api_key").to_path_buf());
    if key_path.exists() {
        if let Ok(contents) = fs::read_to_string(&key_path) {
            let trimmed = contents.trim().to_string();
            if !trimmed.is_empty() {
                return Some(trimmed);
            }
        }
    }
    None
}

fn config_path_from_args(args: &[String]) -> Option<String> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--config" {
            return iter.next().cloned();
        }
    }
    None
}

fn parse_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::ServerRuntimeConfig;

    #[test]
    fn validates_minimal_config() {
        let yaml = r#"
server:
  host: "0.0.0.0"
  port: 10101
  body_limit_mb: 50
  request_log_buffer: 1000
  enable_timing: false
  timing_csv_path: "server_request_timing.csv"
api:
  index_name: "cluster-metrics"
  enable_batch_endpoint: true
  enable_metrics_endpoint: true
  strict_mode: false
upstream:
  mode: "fallback"
  search_url: "http://localhost:9200/cluster-metrics/_search"
  forward_headers: ["x-request-id"]
storage:
  backend: "in_memory_node_store"
  node_catalog:
    kind: "range"
    count: 2
    range:
      start: "N001"
      end: "N002"
schema:
  metrics:
    - name: "cpu_cores"
      aliases: ["cpucores"]
      storage_field: "cpu_cores"
    - name: "memory_gb"
      aliases: ["memorygb"]
      storage_field: "memory_gb"
    - name: "network_mbps"
      aliases: ["networkmbps"]
      storage_field: "network_mbps"
  key_fields: ["cluster"]
  ingest_field_mapping:
    key_field: "cluster"
    epoch_field: "epoch"
    task_field: "task"
    metric_fields:
      cpu_cores: "cpu_cores"
      memory_gb: "memory_gb"
      network_mbps: "network_mbps"
query_support:
  aggregations: ["percentiles", "cumulative"]
  supported_filter_types: ["term"]
  default_batch_fields: ["cpu_cores"]
  default_batch_percents: [50.0]
"#;
        let config: ServerRuntimeConfig = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();
    }
}
