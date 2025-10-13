# Rust Implementation Roadmap: Missing Functionality

## Overview

This document provides a detailed implementation roadmap to bring the Rust query engine to feature parity with the Python implementation. It includes specific code changes, implementation examples, and priority ordering.

## Phase 1: Critical Infrastructure (Days 1-10)

### 1.1 Create Utils Module Foundation

**Priority**: CRITICAL
**Estimated Time**: 2-3 days

#### Create Module Structure
```bash
# Terminal commands to run:
mkdir -p src/utils
```

#### Files to Create:

**src/utils/mod.rs**
```rust
pub mod http;
pub mod promql;
pub mod file_io;

pub use http::*;
pub use promql::*;
pub use file_io::*;
```

**src/utils/http.rs** - Based on Python `utils/http.py`
```rust
use crate::data_model::precomputed_output::PrecomputedOutput;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusResponse {
    pub status: String,
    pub data: PrometheusData,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusData {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub result: Vec<PrometheusResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusResult {
    pub metric: HashMap<String, String>,
    pub value: (f64, String), // [timestamp, value]
}

/// Converts internal query results to Prometheus-compatible HTTP response format
/// Based on Python utils/http.py format_results_as_http_response()
pub fn format_results_as_http_response(
    results: &[PrecomputedOutput],
    timestamp: f64,
) -> PrometheusResponse {
    let prometheus_results: Vec<PrometheusResult> = results
        .iter()
        .map(|result| PrometheusResult {
            metric: result.key.label_values.clone(),
            value: (timestamp, result.value.to_string()),
        })
        .collect();

    PrometheusResponse {
        status: "success".to_string(),
        data: PrometheusData {
            result_type: "vector".to_string(),
            result: prometheus_results,
        },
    }
}

/// Format error response for HTTP API
pub fn format_error_response(error_msg: &str) -> PrometheusResponse {
    PrometheusResponse {
        status: "error".to_string(),
        data: PrometheusData {
            result_type: "error".to_string(),
            result: vec![],
        },
    }
}
```

**src/utils/promql.rs** - Based on Python `utils/promql.py`
```rust
use std::collections::HashMap;

/// Normalizes spatial filter for query processing
/// Based on Python utils/promql.py normalize_spatial_filter()
pub fn normalize_spatial_filter(
    spatial_filter: &HashMap<String, String>
) -> HashMap<String, String> {
    let mut normalized = HashMap::new();

    for (key, value) in spatial_filter {
        // Remove any quotes and normalize the value
        let normalized_value = value.trim_matches('"').to_string();
        normalized.insert(key.clone(), normalized_value);
    }

    normalized
}

/// Parse PromQL query for spatial components
pub fn extract_spatial_components(query: &str) -> Option<HashMap<String, String>> {
    // TODO: Implement PromQL parsing for spatial filters
    // This should extract label filters from PromQL queries
    None
}
```

**src/utils/file_io.rs** - Based on Python `utils/file_io.py`
```rust
use crate::data_model::config::{QueryConfig, MetricConfig, AggregationConfig};
use serde_yaml;
use std::fs;
use std::io::Result;

/// Read configuration from YAML file
/// Based on Python utils/file_io.py read_config_from_yaml()
pub fn read_config_from_yaml<T>(file_path: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let contents = fs::read_to_string(file_path)?;
    let config: T = serde_yaml::from_str(&contents)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(config)
}

/// Read query configuration from file
pub fn read_query_config(file_path: &str) -> Result<QueryConfig> {
    read_config_from_yaml(file_path)
}

/// Read metric configuration from file
pub fn read_metric_config(file_path: &str) -> Result<MetricConfig> {
    read_config_from_yaml(file_path)
}

/// Read aggregation configuration from file
pub fn read_aggregation_config(file_path: &str) -> Result<AggregationConfig> {
    read_config_from_yaml(file_path)
}
```

### 1.2 Fix Engine Core Functionality

**Priority**: CRITICAL
**Estimated Time**: 4-5 days

#### Update src/engines/simple_engine.rs

**Add accumulator merging functionality:**
```rust
// Replace existing TODO comments with actual implementation

impl SimpleEngine {
    // Add proper accumulator merging method
    fn merge_accumulators<T>(&self, mut accumulators: Vec<T>) -> Option<T>
    where
        T: crate::precompute_operators::traits::AccumulatorTrait,
    {
        if accumulators.is_empty() {
            return None;
        }

        if accumulators.len() == 1 {
            return Some(accumulators.into_iter().next().unwrap());
        }

        let mut primary = accumulators.remove(0);
        primary.merge_accumulators(accumulators);
        Some(primary)
    }

    // Fix handle_simple_temporal_aggregation to match Python logic
    fn handle_simple_temporal_aggregation(
        &self,
        pattern_type: &PatternType,
        statistic: &Statistic,
        spatial_filter: &HashMap<String, String>,
        start_time: i64,
        end_time: i64,
        step: i64,
    ) -> Vec<PrecomputedOutput> {
        use crate::utils::promql::normalize_spatial_filter;

        let normalized_filter = normalize_spatial_filter(spatial_filter);

        match pattern_type {
            PatternType::OneTemporal => {
                self.handle_one_temporal(statistic, &normalized_filter, start_time, end_time, step)
            },
            PatternType::OneTemporalOneSpatial => {
                self.handle_one_temporal_one_spatial(statistic, &normalized_filter, start_time, end_time, step)
            },
            PatternType::OneSpatial => {
                self.handle_one_spatial(statistic, &normalized_filter, start_time, end_time)
            },
        }
    }

    // Implement proper multiple key handling
    fn handle_multiple_keys<T>(&self, precompute_results: Vec<T>) -> Vec<PrecomputedOutput>
    where
        T: crate::precompute_operators::traits::MultipleKeyTrait,
    {
        let mut results = Vec::new();

        for precompute in precompute_results {
            // Get all keys from the precompute result
            for key in precompute.get_keys() {
                if let Some(output) = precompute.get_output_for_key(&key) {
                    results.push(output);
                }
            }
        }

        results
    }

    // Add proper spatial aggregation handling
    fn handle_spatial_aggregation(
        &self,
        results: Vec<PrecomputedOutput>,
        aggregation_type: &str,
    ) -> Vec<PrecomputedOutput> {
        match aggregation_type {
            "sum" => self.spatial_sum_aggregation(results),
            "avg" => self.spatial_avg_aggregation(results),
            "min" => self.spatial_min_aggregation(results),
            "max" => self.spatial_max_aggregation(results),
            _ => results, // No aggregation
        }
    }
}
```

### 1.3 Update HTTP Server Integration

**Priority**: HIGH
**Estimated Time**: 2 days

#### Update src/drivers/http_server.rs

```rust
use crate::utils::http::{format_results_as_http_response, format_error_response};

// Add to handle_query method
fn handle_query(&self, query_params: QueryParams) -> String {
    match self.engine.query(&query_params.into()) {
        Ok(results) => {
            let timestamp = chrono::Utc::now().timestamp() as f64;
            let response = format_results_as_http_response(&results, timestamp);
            serde_json::to_string(&response).unwrap_or_else(|_| {
                serde_json::to_string(&format_error_response("Serialization error")).unwrap()
            })
        },
        Err(e) => {
            let error_response = format_error_response(&e.to_string());
            serde_json::to_string(&error_response).unwrap()
        }
    }
}
```

## Phase 2: Data Model Completion (Days 11-17)

### 2.1 Add Missing Data Structures

**Priority**: HIGH
**Estimated Time**: 3 days

#### Create src/data_model/precomputed_output.rs
```rust
use crate::data_model::keys::KeyByLabelValues;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecomputedOutput {
    pub key: KeyByLabelValues,
    pub value: f64,
    pub timestamp: i64,
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

impl PrecomputedOutput {
    pub fn new(key: KeyByLabelValues, value: f64, timestamp: i64) -> Self {
        Self {
            key,
            value,
            timestamp,
            metadata: None,
        }
    }

    pub fn with_metadata(
        key: KeyByLabelValues,
        value: f64,
        timestamp: i64,
        metadata: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            key,
            value,
            timestamp,
            metadata: Some(metadata),
        }
    }
}
```

#### Create src/data_model/measurement.rs
```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Measurement {
    pub metric_name: String,
    pub labels: HashMap<String, String>,
    pub value: f64,
    pub timestamp: i64,
}

impl Measurement {
    pub fn new(
        metric_name: String,
        labels: HashMap<String, String>,
        value: f64,
        timestamp: i64,
    ) -> Self {
        Self {
            metric_name,
            labels,
            value,
            timestamp,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}
```

### 2.2 Enhance Existing Data Models

**Priority**: MEDIUM
**Estimated Time**: 2 days

#### Update src/data_model/keys.rs
```rust
// Add missing functionality to KeyByLabelValues
impl KeyByLabelValues {
    // Add JSON serialization support
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    // Add hash computation for efficient lookups
    pub fn compute_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        for (key, value) in &self.label_values {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        hasher.finish()
    }
}
```

## Phase 3: Precompute Operators Enhancement (Days 18-25)

### 3.1 Add AccumulatorTrait Definition

**Priority**: HIGH
**Estimated Time**: 1 day

#### Create src/precompute_operators/traits.rs
```rust
pub trait AccumulatorTrait {
    fn merge_accumulators(&mut self, others: Vec<Self>);
    fn get_value(&self) -> f64;
    fn reset(&mut self);
}

pub trait MultipleKeyTrait {
    fn get_keys(&self) -> Vec<crate::data_model::keys::KeyByLabelValues>;
    fn get_output_for_key(&self, key: &crate::data_model::keys::KeyByLabelValues)
        -> Option<crate::data_model::precomputed_output::PrecomputedOutput>;
}
```

### 3.2 Complete MultipleSumAccumulator

**Priority**: HIGH
**Estimated Time**: 2 days

#### Update src/precompute_operators/multiple_sum_accumulator.rs
```rust
use super::traits::{AccumulatorTrait, MultipleKeyTrait};
use crate::data_model::{keys::KeyByLabelValues, precomputed_output::PrecomputedOutput};
use std::collections::HashMap;

impl AccumulatorTrait for MultipleSumAccumulator {
    fn merge_accumulators(&mut self, others: Vec<Self>) {
        for other in others {
            for (key, value) in other.sums {
                *self.sums.entry(key).or_insert(0.0) += value;
            }
        }
    }

    fn get_value(&self) -> f64 {
        self.sums.values().sum()
    }

    fn reset(&mut self) {
        self.sums.clear();
    }
}

impl MultipleKeyTrait for MultipleSumAccumulator {
    fn get_keys(&self) -> Vec<KeyByLabelValues> {
        self.sums.keys().cloned().collect()
    }

    fn get_output_for_key(&self, key: &KeyByLabelValues) -> Option<PrecomputedOutput> {
        self.sums.get(key).map(|&value| {
            PrecomputedOutput::new(key.clone(), value, chrono::Utc::now().timestamp())
        })
    }
}
```

### 3.3 Complete CountMinSketchAccumulator

**Priority**: MEDIUM
**Estimated Time**: 3 days

#### Update src/precompute_operators/count_min_sketch_accumulator.rs
```rust
use super::traits::AccumulatorTrait;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

impl CountMinSketchAccumulator {
    // Add complete Count-Min Sketch implementation
    pub fn add(&mut self, item: &str, count: f64) {
        for i in 0..self.depth {
            let hash = self.hash_function(item, i);
            let index = (hash % self.width as u64) as usize;
            self.table[i][index] += count;
        }
    }

    pub fn estimate(&self, item: &str) -> f64 {
        let mut min_estimate = f64::INFINITY;

        for i in 0..self.depth {
            let hash = self.hash_function(item, i);
            let index = (hash % self.width as u64) as usize;
            let estimate = self.table[i][index];

            if estimate < min_estimate {
                min_estimate = estimate;
            }
        }

        min_estimate
    }

    fn hash_function(&self, item: &str, seed: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish()
    }
}

impl AccumulatorTrait for CountMinSketchAccumulator {
    fn merge_accumulators(&mut self, others: Vec<Self>) {
        for other in others {
            if other.width == self.width && other.depth == self.depth {
                for i in 0..self.depth {
                    for j in 0..self.width {
                        self.table[i][j] += other.table[i][j];
                    }
                }
            }
        }
    }

    fn get_value(&self) -> f64 {
        // Return total count estimate
        self.table[0].iter().sum()
    }

    fn reset(&mut self) {
        for row in &mut self.table {
            for cell in row {
                *cell = 0.0;
            }
        }
    }
}
```

## Phase 4: Integration and Testing (Days 26-30)

### 4.1 PromQL Utilities Integration

**Priority**: HIGH
**Estimated Time**: 2 days

#### Update src/engines/simple_engine.rs to use PromQL utilities
```rust
// Add imports for PromQL utilities
use crate::promql::ast_matching::PromQLPatternBuilder;
use crate::promql::query_results::{TimeSeries, QueryResult};

impl SimpleEngine {
    pub fn query(&self, query_config: &QueryConfig) -> Result<Vec<PrecomputedOutput>, Box<dyn std::error::Error>> {
        // Use PromQL pattern builder for sophisticated query parsing
        let pattern_builder = PromQLPatternBuilder::new(&query_config.promql_query)?;
        let patterns = pattern_builder.build_patterns()?;

        let mut all_results = Vec::new();

        for pattern in patterns {
            let results = self.handle_simple_temporal_aggregation(
                &pattern.pattern_type,
                &pattern.statistic,
                &pattern.spatial_filter,
                query_config.start_time,
                query_config.end_time,
                query_config.step,
            );
            all_results.extend(results);
        }

        Ok(all_results)
    }
}
```

### 4.2 Add Comprehensive Error Handling

**Priority**: MEDIUM
**Estimated Time**: 2 days

#### Create src/errors.rs
```rust
use std::fmt;

#[derive(Debug)]
pub enum QueryEngineError {
    InvalidQuery(String),
    AccumulatorError(String),
    StorageError(String),
    ConfigurationError(String),
    SerializationError(String),
}

impl fmt::Display for QueryEngineError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryEngineError::InvalidQuery(msg) => write!(f, "Invalid query: {}", msg),
            QueryEngineError::AccumulatorError(msg) => write!(f, "Accumulator error: {}", msg),
            QueryEngineError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            QueryEngineError::ConfigurationError(msg) => write!(f, "Configuration error: {}", msg),
            QueryEngineError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
        }
    }
}

impl std::error::Error for QueryEngineError {}
```

## Implementation Checklist

### Phase 1 Checklist (Days 1-10)
- [ ] Create utils module structure
- [ ] Implement HTTP response formatting
- [ ] Implement spatial filter normalization
- [ ] Implement file I/O utilities
- [ ] Fix accumulator merging in engine
- [ ] Add multiple key handling support
- [ ] Implement spatial aggregation logic
- [ ] Update HTTP server integration

### Phase 2 Checklist (Days 11-17)
- [ ] Create PrecomputedOutput structure
- [ ] Create Measurement structure
- [ ] Enhance KeyByLabelValues with JSON and hashing
- [ ] Update configuration structures
- [ ] Add serialization support throughout

### Phase 3 Checklist (Days 18-25)
- [ ] Define AccumulatorTrait and MultipleKeyTrait
- [ ] Complete MultipleSumAccumulator implementation
- [ ] Complete CountMinSketchAccumulator with full CMS logic
- [ ] Enhance IncreaseAccumulator with proper calculation
- [ ] Complete MinMaxAccumulator implementation
- [ ] Add proper merging logic to all accumulators

### Phase 4 Checklist (Days 26-30)
- [ ] Integrate PromQL utilities into engine
- [ ] Add comprehensive error handling
- [ ] Create end-to-end tests
- [ ] Validate Prometheus compatibility
- [ ] Performance testing and optimization

## Testing Strategy

### Unit Tests Required
1. **Utils Module Tests**
   - HTTP response formatting validation
   - Spatial filter normalization
   - Configuration file reading

2. **Engine Tests**
   - Accumulator merging correctness
   - Multiple key handling
   - Spatial aggregation accuracy

3. **Precompute Operator Tests**
   - Individual accumulator functionality
   - Merging operations
   - Count-Min Sketch accuracy

### Integration Tests Required
1. **End-to-End Query Processing**
   - HTTP API compatibility with Prometheus
   - Complex query handling
   - Error response formatting

2. **Performance Tests**
   - Large query processing
   - Memory usage validation
   - Concurrent request handling

## Success Criteria

1. **Functional Parity**: All Python functionality replicated in Rust
2. **API Compatibility**: 100% Prometheus API compatibility
3. **Performance**: Query processing time within 10% of Python implementation
4. **Reliability**: Zero critical bugs in core functionality
5. **Test Coverage**: >90% code coverage for critical paths

This roadmap provides a systematic approach to achieving complete feature parity between the Python and Rust implementations, with clear priorities, timelines, and success criteria.
