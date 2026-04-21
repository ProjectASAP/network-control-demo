use std::{
    collections::{HashMap, HashSet},
    env,
    error::Error,
    fs,
    path::Path,
};

use serde::Deserialize;

const SUPPORTED_AGGREGATIONS: &[&str] = &["percentiles", "cumulative"];
const _SUPPORTED_FILTER_TYPES: &[&str] = &["term"];
const SUPPORTED_UPSTREAM_MODES: &[&str] = &["disabled", "fallback"];

#[derive(Clone, Debug, Deserialize)]
pub struct ServerRuntimeConfig {
    pub server: ServerConfig,
    pub api: ApiConfig,
    pub upstream: UpstreamConfig,
    pub storage: StorageConfig,
    pub indices: HashMap<String, SchemaConfig>,
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
    #[serde(default)]
    pub index_name: Option<String>,
    #[serde(default)]
    pub index_names: Vec<String>,
    #[serde(default = "default_true")]
    pub enable_batch_endpoint: bool,
    #[serde(default = "default_true")]
    pub enable_metrics_endpoint: bool,
    #[serde(default)]
    pub strict_mode: bool,
    #[serde(default)]
    pub default_batch_fields: Vec<String>,
    #[serde(default)]
    pub default_batch_percents: Vec<f64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpstreamConfig {
    pub mode: String,
    pub search_url: Option<String>,
    pub search_url_template: Option<String>,
    #[serde(default)]
    pub forward_headers: Vec<String>,
    /// Loaded at runtime from ES_API_KEY env var or a key file; never from YAML.
    #[serde(skip, default)]
    pub es_api_key: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StorageConfig {
    pub backend: String,
    #[serde(default)]
    pub predefined_keys: Vec<String>,
    #[serde(default, alias = "node_catalog")]
    pub range_key_catalog: Option<RangeKeyCatalogConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RangeKeyCatalogConfig {
    pub format: String,
    pub start: u32,
    pub end: u32,
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
        if self
            .api
            .index_names
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err("api.index_names must not contain empty values".into());
        }
        let index_names = self.index_names();
        if index_names.is_empty() {
            return Err("api.index_names must contain at least one value (or set api.index_name)"
                .into());
        }
        if self.api.default_batch_fields.is_empty() {
            return Err("api.default_batch_fields must not be empty".into());
        }
        if self.api.default_batch_percents.is_empty() {
            return Err("api.default_batch_percents must not be empty".into());
        }
        for percent in &self.api.default_batch_percents {
            if !(0.0..=100.0).contains(percent) {
                return Err(format!(
                    "api.default_batch_percents contains out-of-range value {percent}"
                )
                .into());
            }
        }
        let backend = self.storage.backend.trim().to_ascii_lowercase();
        if backend != "in_memory_key_store" && backend != "in_memory_node_store" {
            return Err(format!(
                "unsupported storage.backend '{}'; expected in_memory_key_store",
                self.storage.backend
            )
            .into());
        }

        for key in &self.storage.predefined_keys {
            if key.trim().is_empty() {
                return Err("storage.predefined_keys must not contain empty values".into());
            }
        }

        if let Some(range_key_catalog) = self.storage.range_key_catalog.as_ref() {
            if range_key_catalog.format.trim().is_empty() {
                return Err("storage.range_key_catalog.format must not be empty".into());
            }
            if range_key_catalog.end < range_key_catalog.start {
                return Err(format!(
                    "storage.range_key_catalog.end {} must be >= start {}",
                    range_key_catalog.end, range_key_catalog.start
                )
                .into());
            }
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
        let has_search_template = self
            .upstream
            .search_url_template
            .as_ref()
            .map(|url| !url.trim().is_empty())
            .unwrap_or(false);
        if upstream_mode == "fallback"
            && !has_search_template
            && self
                .upstream
                .search_url
                .as_ref()
                .map(|url| url.trim().is_empty())
                .unwrap_or(true)
        {
            return Err(
                "upstream.search_url or upstream.search_url_template is required when upstream.mode=fallback"
                    .into(),
            );
        }

        for (idx_name, schema) in &self.indices {
            let mut metrics = HashSet::new();
            for metric in &schema.metrics {
                if metric.name.trim().is_empty() {
                    return Err(format!("indices[{}].metrics[].name must not be empty", idx_name).into());
                }
                if metric.storage_field.trim().is_empty() {
                    return Err(format!("indices[{}].metrics[].storage_field must not be empty", idx_name).into());
                }
                if !metrics.insert(metric.name.trim().to_ascii_lowercase()) {
                    return Err(format!("duplicate metric '{}' in indices[{}]", metric.name, idx_name).into());
                }
            }
            if schema.key_fields.is_empty() {
                return Err(format!("indices[{}].key_fields must not be empty", idx_name).into());
            }
            let allowed_key_fields: HashSet<String> = schema
                .key_fields
                .iter()
                .map(|item| item.trim().to_ascii_lowercase())
                .collect();
            if !allowed_key_fields.contains(
                &schema
                    .ingest_field_mapping
                    .key_field
                    .trim()
                    .to_ascii_lowercase(),
            ) {
                return Err(
                    format!("indices[{}].ingest_field_mapping.key_field must be listed in key_fields", idx_name).into(),
                );
            }
        }

        // Validate that default_batch_fields exist in at least one index's metrics
        let all_metric_names: HashSet<String> = self.indices
            .values()
            .flat_map(|schema| {
                schema.metrics.iter()
                    .map(|m| m.name.trim().to_ascii_lowercase())
            })
            .collect();
        for field in &self.api.default_batch_fields {
            if !all_metric_names.contains(&field.trim().to_ascii_lowercase()) {
                return Err(format!(
                    "api.default_batch_fields contains unknown metric '{}'",
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
        if let Ok(url) = env::var("NCS_UPSTREAM_SEARCH_URL_TEMPLATE") {
            self.upstream.search_url_template = Some(url);
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

    pub fn index_names(&self) -> Vec<String> {
        let mut dedup = HashSet::new();
        let mut ordered = Vec::new();

        for value in &self.api.index_names {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }
            let normalized = trimmed.to_ascii_lowercase();
            if dedup.insert(normalized) {
                ordered.push(trimmed.to_string());
            }
        }

        if let Some(single) = &self.api.index_name {
            let trimmed = single.trim();
            if !trimmed.is_empty() {
                let normalized = trimmed.to_ascii_lowercase();
                if dedup.insert(normalized) {
                    ordered.push(trimmed.to_string());
                }
            }
        }

        ordered
    }

    pub fn default_index_name(&self) -> Option<String> {
        self.index_names().into_iter().next()
    }

    pub fn supports_index(&self, index_name: &str) -> bool {
        let candidate = index_name.trim().to_ascii_lowercase();
        self.index_names()
            .iter()
            .any(|name| name.trim().eq_ignore_ascii_case(&candidate))
    }

    pub fn search_path_for(&self, index_name: &str) -> String {
        format!("/{}/_search", index_name)
    }

    pub fn batch_path_for(&self, index_name: &str) -> String {
        format!("/{}/_batch", index_name)
    }

    pub fn upstream_search_url_for(&self, index_name: &str) -> Option<String> {
        if let Some(template) = self.upstream.search_url_template.as_ref() {
            let trimmed = template.trim();
            if trimmed.is_empty() {
                return None;
            }
            if trimmed.contains("{index}") {
                return Some(trimmed.replace("{index}", index_name));
            }
            return Some(trimmed.to_string());
        }
        self.upstream.search_url.as_ref().and_then(|url| {
            let trimmed = url.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
    }

    pub fn schema_for_index(&self, index_name: &str) -> Option<&SchemaConfig> {
        self.indices.get(&index_name.trim().to_ascii_lowercase())
    }

    pub fn metric_storage_fields_for_index(&self, index_name: &str) -> Vec<String> {
        self.schema_for_index(index_name)
            .map(|schema| {
                schema
                    .metrics
                    .iter()
                    .map(|metric| metric.storage_field.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn key_fields_for_index(&self, index_name: &str) -> HashSet<String> {
        self.schema_for_index(index_name)
            .map(|schema| {
                schema
                    .key_fields
                    .iter()
                    .map(|item| item.trim().to_ascii_lowercase())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn aggregation_names(&self) -> HashSet<String> {
        SUPPORTED_AGGREGATIONS
            .iter()
            .map(|s| s.to_string())
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
        use std::collections::HashMap;

        use super::{
                ApiConfig, IngestFieldMapping, MetricConfig, RangeKeyCatalogConfig, SchemaConfig,
                ServerConfig, ServerRuntimeConfig, StorageConfig, UpstreamConfig,
        };

    #[test]
    fn validates_minimal_config() {
                let mut indices = HashMap::new();
                indices.insert(
                        "cluster-metrics".to_string(),
                        SchemaConfig {
                                metrics: vec![
                                        MetricConfig {
                                                name: "cpu_cores".to_string(),
                                                aliases: vec!["cpucores".to_string()],
                                                storage_field: "cpu_cores".to_string(),
                                        },
                                        MetricConfig {
                                                name: "memory_gb".to_string(),
                                                aliases: vec!["memorygb".to_string()],
                                                storage_field: "memory_gb".to_string(),
                                        },
                                        MetricConfig {
                                                name: "network_mbps".to_string(),
                                                aliases: vec!["networkmbps".to_string()],
                                                storage_field: "network_mbps".to_string(),
                                        },
                                ],
                                key_fields: vec!["cluster".to_string()],
                                ingest_field_mapping: IngestFieldMapping {
                                        key_field: "cluster".to_string(),
                                        epoch_field: "epoch".to_string(),
                                        task_field: Some("task".to_string()),
                                        metric_fields: HashMap::from([
                                                ("cpu_cores".to_string(), "cpu_cores".to_string()),
                                                ("memory_gb".to_string(), "memory_gb".to_string()),
                                                ("network_mbps".to_string(), "network_mbps".to_string()),
                                        ]),
                                },
                        },
                );

                let config = ServerRuntimeConfig {
                        server: ServerConfig {
                                host: "0.0.0.0".to_string(),
                                port: 10101,
                                body_limit_mb: 50,
                                request_log_buffer: 1000,
                                enable_timing: false,
                                timing_csv_path: "server_request_timing.csv".to_string(),
                        },
                        api: ApiConfig {
                                index_name: Some("cluster-metrics".to_string()),
                                index_names: Vec::new(),
                                enable_batch_endpoint: true,
                                enable_metrics_endpoint: true,
                                strict_mode: false,
                                default_batch_fields: vec!["cpu_cores".to_string()],
                                default_batch_percents: vec![50.0],
                        },
                        upstream: UpstreamConfig {
                                mode: "fallback".to_string(),
                                search_url: Some("http://localhost:9200/cluster-metrics/_search".to_string()),
                                search_url_template: None,
                                forward_headers: vec!["x-request-id".to_string()],
                                es_api_key: None,
                        },
                        storage: StorageConfig {
                                backend: "in_memory_key_store".to_string(),
                                predefined_keys: Vec::new(),
                                range_key_catalog: Some(RangeKeyCatalogConfig {
                                        format: "N{:03}".to_string(),
                                        start: 1,
                                        end: 2,
                                }),
                        },
                        indices,
                };
        config.validate().unwrap();
    }
}
