use crate::data_model::{
    InferenceConfig, KeyByLabelValues, QueryConfig, QueryLanguage, SchemaConfig, StreamingConfig,
};
use crate::engines::query_result::{InstantVectorElement, QueryResult};
use crate::stores::Store;
use core::panic;
use promql_utilities::get_is_collapsable;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

use crate::AggregateCore;

use promql_utilities::ast_matching::{PromQLMatchResult, PromQLPattern, PromQLPatternBuilder};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{QueryPatternType, Statistic};
use promql_utilities::query_logics::parsing::{
    get_metric_and_spatial_filter, get_spatial_aggregation_output_labels, get_statistics_to_compute,
};

use sql_utilities::ast_matching::QueryType;
use sql_utilities::ast_matching::{SQLPatternMatcher, SQLPatternParser, SQLQuery};
use sql_utilities::sqlhelper::AggregationInfo;
use sqlparser::dialect::*;
use sqlparser::parser::Parser as parser;

// SQL issue: refactor simpleengine to create matchresult similar to SQLquerydata

// Type aliases for complex types to satisfy clippy
type PrecomputeOutputsMap = HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>>;
type MergedOutputsMap = HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>;

/// Aggregation IDs and types for key and value
#[derive(Debug, Clone)]
pub struct AggregationIdInfo {
    pub aggregation_id_for_key: u64,
    pub aggregation_id_for_value: u64,
    pub aggregation_type_for_key: String,
    pub aggregation_type_for_value: String,
}

/// Metadata extracted from a query, independent of query language
#[derive(Debug, Clone)]
pub struct QueryMetadata {
    /// Labels that will appear in the query output
    pub query_output_labels: KeyByLabelNames,
    /// The primary statistic to compute (sum, max, quantile, etc.)
    pub statistic_to_compute: Statistic,
    /// Additional parameters (e.g., "quantile" -> "0.95", "k" -> "10")
    pub query_kwargs: HashMap<String, String>,
}

/// Parameters for a single store query
#[derive(Debug, Clone)]
pub struct StoreQueryParams {
    pub metric: String,
    pub aggregation_id: u64,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    /// true for sliding windows (exact match), false for tumbling (range)
    pub is_exact_query: bool,
}

/// Complete plan for querying store (values + optional separate keys)
#[derive(Debug, Clone)]
pub struct StoreQueryPlan {
    pub values_query: StoreQueryParams,
    /// Some when key and value use different aggregations (DeltaSet/SetAggregator)
    pub keys_query: Option<StoreQueryParams>,
}

/// Timestamps for query execution
#[derive(Debug, Clone)]
pub struct QueryTimestamps {
    pub start_timestamp: u64,
    pub end_timestamp: u64,
}

/// Complete execution context for a query
#[derive(Debug, Clone)]
pub struct QueryExecutionContext {
    pub metric: String,
    pub metadata: QueryMetadata,
    pub store_plan: StoreQueryPlan,
    pub agg_info: AggregationIdInfo,
    /// Whether to merge multiple precomputes (true for temporal queries)
    pub do_merge: bool,
    #[allow(dead_code)]
    pub spatial_filter: String,
    pub query_time: u64,
}

/// Simple query engine for processing PromQL-like queries against precomputed data
pub struct SimpleEngine {
    store: Arc<dyn Store>,
    inference_config: InferenceConfig,
    streaming_config: Arc<StreamingConfig>,
    prometheus_scrape_interval: u64,
    controller_patterns: HashMap<QueryPatternType, Vec<PromQLPattern>>,
    query_language: QueryLanguage,
}

impl SimpleEngine {
    pub fn new(
        store: Arc<dyn Store>,
        inference_config: InferenceConfig,
        streaming_config: Arc<StreamingConfig>,
        prometheus_scrape_interval: u64,
        query_language: QueryLanguage,
    ) -> Self {
        // Create temporal pattern blocks
        let mut temporal_pattern_blocks = HashMap::new();
        temporal_pattern_blocks.insert(
            "quantile".to_string(),
            PromQLPatternBuilder::function(
                vec!["quantile_over_time"],
                vec![
                    PromQLPatternBuilder::number(None, Some("quantile_param")),
                    PromQLPatternBuilder::matrix_selector(
                        PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                        None,
                        Some("range_vector"),
                    ),
                ],
                Some("function"),
                Some("function_args"),
            ),
        );

        temporal_pattern_blocks.insert(
            "generic".to_string(),
            PromQLPatternBuilder::function(
                vec![
                    "sum_over_time",
                    "count_over_time",
                    "avg_over_time",
                    "min_over_time",
                    "max_over_time",
                    "increase",
                    "rate",
                ],
                vec![PromQLPatternBuilder::matrix_selector(
                    PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                    None,
                    Some("range_vector"),
                )],
                Some("function"),
                Some("function_args"),
            ),
        );

        // Create spatial pattern blocks
        let mut spatial_pattern_blocks = HashMap::new();
        spatial_pattern_blocks.insert(
            "generic".to_string(),
            PromQLPatternBuilder::aggregation(
                vec!["sum", "count", "avg", "quantile", "min", "max", "topk"],
                PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                None,
                None,
                None,
                Some("aggregation"),
            ),
        );

        // Helper functions (these would be closures or separate methods)
        fn temporal_pattern(
            pattern_type: &str,
            blocks: &HashMap<String, Option<HashMap<String, Value>>>,
        ) -> PromQLPattern {
            PromQLPattern::new(blocks[pattern_type].clone())
        }

        fn spatial_pattern(
            pattern_type: &str,
            blocks: &HashMap<String, Option<HashMap<String, Value>>>,
        ) -> PromQLPattern {
            PromQLPattern::new(blocks[pattern_type].clone())
        }

        fn spatial_of_temporal_pattern(
            temporal_block: &Option<HashMap<String, Value>>,
        ) -> PromQLPattern {
            let pattern = PromQLPatternBuilder::aggregation(
                vec!["sum", "count", "avg", "quantile", "min", "max"],
                temporal_block.clone(),
                None,
                None,
                None,
                Some("aggregation"),
            );
            PromQLPattern::new(pattern)
        }

        // Create controller patterns
        let mut controller_patterns = HashMap::new();
        controller_patterns.insert(
            QueryPatternType::OnlyTemporal,
            vec![
                temporal_pattern("quantile", &temporal_pattern_blocks),
                temporal_pattern("generic", &temporal_pattern_blocks),
            ],
        );
        controller_patterns.insert(
            QueryPatternType::OnlySpatial,
            vec![spatial_pattern("generic", &spatial_pattern_blocks)],
        );
        controller_patterns.insert(
            QueryPatternType::OneTemporalOneSpatial,
            vec![
                spatial_of_temporal_pattern(&temporal_pattern_blocks["quantile"]),
                spatial_of_temporal_pattern(&temporal_pattern_blocks["generic"]),
            ],
        );

        Self {
            store,
            inference_config,
            streaming_config,
            prometheus_scrape_interval,
            controller_patterns,
            query_language,
        }
    }

    /// Convert query timestamp (seconds) to data timestamp (milliseconds)
    pub fn convert_query_time_to_data_time(query_time: f64) -> u64 {
        (query_time * 1000.0) as u64
    }

    /// Finds the query configuration for a given query string
    fn find_query_config(&self, query: &str) -> Option<&QueryConfig> {
        self.inference_config
            .query_configs
            .iter()
            .find(|config| config.query == query)
    }

    /// Validates and potentially aligns end timestamp based on query pattern
    fn validate_and_align_end_timestamp(
        &self,
        mut end_timestamp: u64,
        query_pattern_type: QueryPatternType,
    ) -> u64 {
        let interval_ms = self.prometheus_scrape_interval * 1000;

        if !end_timestamp.is_multiple_of(interval_ms) {
            warn!(
                "Query end timestamp {} is not aligned with Prometheus scrape interval of {} seconds. \
                 This may lead to inaccurate results.",
                end_timestamp, self.prometheus_scrape_interval
            );
        }

        // For OnlySpatial, align end_timestamp to nearest scrape interval
        if query_pattern_type == QueryPatternType::OnlySpatial
            && !end_timestamp.is_multiple_of(interval_ms)
        {
            let aligned_end_timestamp = (end_timestamp / interval_ms) * interval_ms;
            debug!(
                "OnlySpatial query: Aligning end_timestamp from {} to {} using scrape interval of {} seconds",
                end_timestamp, aligned_end_timestamp, self.prometheus_scrape_interval
            );
            end_timestamp = aligned_end_timestamp;
        }

        end_timestamp
    }

    /// Calculates start timestamp for PromQL queries
    fn calculate_start_timestamp_promql(
        &self,
        end_timestamp: u64,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> u64 {
        match query_pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                let range_seconds = match_result.get_range_duration().unwrap().num_seconds() as u64;
                end_timestamp - (range_seconds * 1000)
            }
            QueryPatternType::OnlySpatial => {
                end_timestamp - (self.prometheus_scrape_interval * 1000)
            }
        }
    }

    /// Calculates start timestamp for SQL queries
    fn calculate_start_timestamp_sql(
        &self,
        end_timestamp: u64,
        query_pattern_type: QueryPatternType,
        match_result: &SQLQuery,
    ) -> u64 {
        match query_pattern_type {
            QueryPatternType::OnlyTemporal => {
                let scrape_intervals =
                    match_result.query_data[0].time_info.clone().get_duration() as u64;
                end_timestamp - (scrape_intervals * self.prometheus_scrape_interval * 1000)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                let scrape_intervals =
                    match_result.query_data[1].time_info.clone().get_duration() as u64;
                end_timestamp - (scrape_intervals * self.prometheus_scrape_interval * 1000)
            }
            QueryPatternType::OnlySpatial => {
                end_timestamp - (self.prometheus_scrape_interval * 1000)
            }
        }
    }

    /// Calculates and validates query timestamps for PromQL
    fn calculate_query_timestamps_promql(
        &self,
        query_time: u64,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> QueryTimestamps {
        let mut end_timestamp = if let Some(at_modifier) = match_result
            .tokens
            .get("metric")
            .and_then(|t| t.metric.as_ref())
            .and_then(|m| m.at_modifier)
        {
            at_modifier * 1000
        } else {
            query_time
        };

        end_timestamp = self.validate_and_align_end_timestamp(end_timestamp, query_pattern_type);
        let start_timestamp =
            self.calculate_start_timestamp_promql(end_timestamp, query_pattern_type, match_result);

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }

    /// Calculates and validates query timestamps for SQL
    fn calculate_query_timestamps_sql(
        &self,
        query_time: u64,
        query_pattern_type: QueryPatternType,
        match_result: &SQLQuery,
    ) -> QueryTimestamps {
        let mut end_timestamp = query_time;
        end_timestamp = self.validate_and_align_end_timestamp(end_timestamp, query_pattern_type);
        let start_timestamp =
            self.calculate_start_timestamp_sql(end_timestamp, query_pattern_type, match_result);

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }

    /// Extracts quantile parameter from PromQL match result
    fn extract_quantile_param_promql(
        &self,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Option<String> {
        let quantile_value = match query_pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                match_result
                    .tokens
                    .get("function_args")
                    .and_then(|token| token.function.as_ref())
                    .and_then(|func| func.args.first())
            }
            QueryPatternType::OnlySpatial => match_result
                .tokens
                .get("aggregation")
                .and_then(|token| token.aggregation.as_ref())
                .and_then(|agg| agg.param.as_ref()),
        };

        quantile_value.map(|s| s.to_string())
    }

    /// Extracts quantile parameter from SQL match result
    fn extract_quantile_param_sql(&self, match_result: &SQLQuery) -> Option<String> {
        match_result
            .query_data
            .first()
            .map(|data| data.aggregation_info.get_args()[0].to_string())
    }

    /// Extracts topk k parameter from PromQL match result
    fn extract_topk_param(
        &self,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Result<String, String> {
        match query_pattern_type {
            QueryPatternType::OnlySpatial => match_result
                .tokens
                .get("aggregation")
                .and_then(|token| token.aggregation.as_ref())
                .and_then(|agg| agg.param.as_ref())
                .map(|s| s.to_string())
                .ok_or_else(|| "Missing k parameter for top-k query".to_string()),
            _ => Err(format!(
                "Top-k statistic is only supported for OnlySpatial pattern, found {:?}",
                query_pattern_type
            )),
        }
    }

    /// Builds query kwargs (quantile, k, etc.) for PromQL queries
    fn build_query_kwargs_promql(
        &self,
        statistic: &Statistic,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Result<HashMap<String, String>, String> {
        let mut query_kwargs = HashMap::new();

        match statistic {
            Statistic::Quantile => {
                let quantile = self
                    .extract_quantile_param_promql(query_pattern_type, match_result)
                    .ok_or_else(|| "Missing quantile parameter for quantile query".to_string())?;
                debug!("Extracted quantile value: {:?}", quantile);
                query_kwargs.insert("quantile".to_string(), quantile);
            }
            Statistic::Topk => {
                let k = self.extract_topk_param(query_pattern_type, match_result)?;
                debug!("Extracted k value: {:?}", k);
                query_kwargs.insert("k".to_string(), k);
            }
            _ => {}
        }

        Ok(query_kwargs)
    }

    /// Builds query kwargs for SQL queries
    fn build_query_kwargs_sql(
        &self,
        statistic: &Statistic,
        match_result: &SQLQuery,
    ) -> Result<HashMap<String, String>, String> {
        let mut query_kwargs = HashMap::new();

        if *statistic == Statistic::Quantile {
            let quantile = self
                .extract_quantile_param_sql(match_result)
                .ok_or_else(|| "Missing quantile parameter for quantile query".to_string())?;
            query_kwargs.insert("quantile".to_string(), quantile);
        }
        // Note: SQL doesn't support topk limiting yet

        Ok(query_kwargs)
    }

    /// Creates query parameters for separate keys query
    fn create_keys_query_params(
        &self,
        metric: &str,
        end_timestamp: u64,
        agg_info: &AggregationIdInfo,
    ) -> Result<StoreQueryParams, String> {
        let (start_timestamp, end_timestamp) = match agg_info.aggregation_type_for_key.as_str() {
            "DeltaSetAggregator" => {
                // All keys from beginning of time
                (0, end_timestamp)
            }
            "SetAggregator" => {
                // Latest window only
                let tumbling_window_size = self
                    .streaming_config
                    .get_aggregation_config(agg_info.aggregation_id_for_key)
                    .map(|config| config.tumbling_window_size * 1000)
                    .ok_or_else(|| {
                        format!(
                            "Failed to get tumbling window size for aggregation {}",
                            agg_info.aggregation_id_for_key
                        )
                    })?;
                (end_timestamp - tumbling_window_size, end_timestamp)
            }
            other => {
                return Err(format!("Unsupported key aggregation type: {}", other));
            }
        };

        Ok(StoreQueryParams {
            metric: metric.to_string(),
            aggregation_id: agg_info.aggregation_id_for_key,
            start_timestamp,
            end_timestamp,
            is_exact_query: false, // Keys always use range queries
        })
    }

    /// Creates a plan for querying the store based on aggregation configuration
    fn create_store_query_plan(
        &self,
        metric: &str,
        timestamps: &QueryTimestamps,
        agg_info: &AggregationIdInfo,
    ) -> Result<StoreQueryPlan, String> {
        // Get aggregation config for value to determine window type
        let aggregation_config_for_value = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .ok_or_else(|| {
                format!(
                    "Aggregation config not found for aggregation_id: {}",
                    agg_info.aggregation_id_for_value
                )
            })?;

        let window_type = &aggregation_config_for_value.window_type;
        let is_exact_query = window_type == "sliding";

        // Determine start/end for values query based on window type
        let (values_start, values_end) = if is_exact_query {
            // Sliding window: exact window match
            let exact_start =
                timestamps.end_timestamp - (aggregation_config_for_value.window_size * 1000);
            (exact_start, timestamps.end_timestamp)
        } else {
            // Tumbling window: range query
            (timestamps.start_timestamp, timestamps.end_timestamp)
        };

        let values_query = StoreQueryParams {
            metric: metric.to_string(),
            aggregation_id: agg_info.aggregation_id_for_value,
            start_timestamp: values_start,
            end_timestamp: values_end,
            is_exact_query,
        };

        // Determine if we need a separate keys query
        let keys_query = if agg_info.aggregation_id_for_key != agg_info.aggregation_id_for_value {
            Some(self.create_keys_query_params(metric, timestamps.end_timestamp, agg_info)?)
        } else {
            None
        };

        Ok(StoreQueryPlan {
            values_query,
            keys_query,
        })
    }

    /// Executes a single store query based on parameters
    fn execute_store_query(
        &self,
        params: &StoreQueryParams,
    ) -> Result<PrecomputeOutputsMap, String> {
        debug!(
            "Querying store: metric={}, agg_id={}, range=[{}, {}], exact={}",
            params.metric,
            params.aggregation_id,
            params.start_timestamp,
            params.end_timestamp,
            params.is_exact_query
        );

        let store_query_start_time = Instant::now();

        let result = if params.is_exact_query {
            debug!(
                "Sliding window query: Looking for exact window [{}, {}]",
                params.start_timestamp, params.end_timestamp
            );
            let res = self.store.query_precomputed_output_exact(
                &params.metric,
                params.aggregation_id,
                params.start_timestamp,
                params.end_timestamp,
            );
            if let Ok(ref outputs) = res {
                let store_query_duration = store_query_start_time.elapsed();
                debug!(
                    "Sliding window exact query took: {:.2}ms, found {} unique keys",
                    store_query_duration.as_secs_f64() * 1000.0,
                    outputs.len()
                );
            }
            res
        } else {
            debug!(
                "Tumbling window query: range [{}, {}]",
                params.start_timestamp, params.end_timestamp
            );
            let res = self.store.query_precomputed_output(
                &params.metric,
                params.aggregation_id,
                params.start_timestamp,
                params.end_timestamp,
            );
            if res.is_ok() {
                let store_query_duration = store_query_start_time.elapsed();
                debug!(
                    "Tumbling window range query took: {:.2}ms",
                    store_query_duration.as_secs_f64() * 1000.0
                );
            }
            res
        };

        result.map_err(|e| {
            format!(
                "Error querying store for metric {}, agg {}, range [{}, {}]: {}",
                params.metric,
                params.aggregation_id,
                params.start_timestamp,
                params.end_timestamp,
                e
            )
        })
    }

    /// Executes the full store query plan and returns merged results
    fn execute_and_merge_store_queries(
        &self,
        plan: &StoreQueryPlan,
        do_merge: bool,
        agg_info: &AggregationIdInfo,
    ) -> Result<(MergedOutputsMap, Option<MergedOutputsMap>), String> {
        // Query and merge values
        let values_map = self.execute_store_query(&plan.values_query).map_err(|e| {
            warn!("Error querying store for values: {}", e);
            e
        })?;

        if values_map.is_empty() {
            return Err(format!(
                "No precomputed outputs found for metric: {}, aggregation_id: {}",
                plan.values_query.metric, plan.values_query.aggregation_id
            ));
        }

        debug!("Store query returned {} unique keys", values_map.len());

        let merge_start_time = Instant::now();
        let window_type = if plan.values_query.is_exact_query {
            "sliding"
        } else {
            "tumbling"
        };

        let merged_values = if plan.values_query.is_exact_query {
            // Sliding window: no merge needed
            debug!("Sliding window mode: Skipping merge (expecting 1 precompute per key)");
            values_map
                .into_iter()
                .map(|(key, precomputes)| {
                    if precomputes.len() != 1 {
                        warn!(
                            "Sliding window expected 1 precompute per key, found {}. Using first.",
                            precomputes.len()
                        );
                    }
                    (key, precomputes.into_iter().next().unwrap())
                })
                .collect()
        } else {
            // Tumbling window: merge needed
            debug!("Tumbling window mode: Merging {} outputs", values_map.len());
            self.merge_precomputed_outputs(
                &values_map,
                do_merge,
                agg_info.aggregation_type_for_value.clone(),
            )
        };

        let merge_duration = merge_start_time.elapsed();
        debug!(
            "[LATENCY] Precomputed output processing ({}): {:.2}ms, resulted in {} merged outputs",
            if window_type == "sliding" {
                "no merge"
            } else {
                "merge"
            },
            merge_duration.as_secs_f64() * 1000.0,
            merged_values.len()
        );

        // Query and merge keys if needed
        let merged_keys = if let Some(keys_params) = &plan.keys_query {
            let keys_store_query_start_time = Instant::now();
            let keys_map = self.execute_store_query(keys_params).map_err(|e| {
                warn!("Error querying store for keys: {}", e);
                e
            })?;
            debug!(
                "[LATENCY] Keys store query (metric: {}, agg: {}): {}ms",
                &keys_params.metric,
                keys_params.aggregation_id,
                keys_store_query_start_time.elapsed().as_millis()
            );
            debug!("Keys query returned {} unique keys", keys_map.len());

            let keys_merge_start_time = Instant::now();
            let merged = self.merge_precomputed_outputs(
                &keys_map,
                do_merge,
                agg_info.aggregation_type_for_key.clone(),
            );
            debug!(
                "[LATENCY] Keys merge operation: {:.2}ms, resulted in {} merged outputs",
                keys_merge_start_time.elapsed().as_secs_f64() * 1000.0,
                merged.len()
            );
            Some(merged)
        } else {
            None
        };

        Ok((merged_values, merged_keys))
    }

    /// Collects all results based on whether keys are separate or not
    fn collect_all_results(
        &self,
        merged_values: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        merged_keys: Option<&HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
        enable_topk_limiting: bool,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        if let Some(keys_map) = merged_keys {
            // Separate keys and values
            self.collect_results_separate_keys(merged_values, keys_map, statistic, query_kwargs)
        } else {
            // Same aggregation for keys and values
            self.collect_results_same_aggregation(
                merged_values,
                statistic,
                query_kwargs,
                enable_topk_limiting,
            )
        }
    }

    /// Executes the complete query pipeline: plan, execute, collect, and format
    fn execute_query_pipeline(
        &self,
        context: &QueryExecutionContext,
        enable_topk: bool,
    ) -> Result<Vec<InstantVectorElement>, String> {
        // Step 1: Execute the query plan (already created in context.store_plan)
        let (merged_values, merged_keys) = self.execute_and_merge_store_queries(
            &context.store_plan,
            context.do_merge,
            &context.agg_info,
        )?;

        // Step 2: Collect results
        let unformatted_results_start_time = Instant::now();
        let unformatted_results = self.collect_all_results(
            &merged_values,
            merged_keys.as_ref(),
            &context.metadata.statistic_to_compute,
            &context.metadata.query_kwargs,
            enable_topk, // SQL=false, PromQL=true
        )?;
        debug!(
            "[LATENCY] Unformatted results collection: {:.2}ms",
            unformatted_results_start_time.elapsed().as_secs_f64() * 1000.0
        );

        // Step 3: Format results
        let results_start_time = Instant::now();
        let results = self.format_final_results(
            unformatted_results,
            &context.metadata.statistic_to_compute,
            &context.metric,
            enable_topk, // SQL=false, PromQL=true
        );
        debug!(
            "[LATENCY] Results collection: {}ms",
            results_start_time.elapsed().as_millis()
        );

        Ok(results)
    }

    /// Formats unformatted results into final InstantVectorElement format
    /// For topk queries (when enabled), sorts by value and prepends metric name to keys
    fn format_final_results(
        &self,
        unformatted_results: HashMap<Option<KeyByLabelValues>, f64>,
        statistic: &Statistic,
        metric: &str,
        enable_topk_formatting: bool,
    ) -> Vec<InstantVectorElement> {
        let sorted_results: Vec<(Option<KeyByLabelValues>, f64)> =
            if *statistic == Statistic::Topk && enable_topk_formatting {
                // Sort by value descending for topk
                let mut sorted: Vec<_> = unformatted_results.into_iter().collect();
                sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

                // Prepend metric name to each key's label values
                sorted
                    .into_iter()
                    .map(|(key_opt, value)| {
                        let updated_key = key_opt.map(|mut key| {
                            let mut new_labels = vec![metric.to_string()];
                            new_labels.extend(key.labels);
                            key.labels = new_labels;
                            key
                        });
                        (updated_key, value)
                    })
                    .collect()
            } else {
                unformatted_results.into_iter().collect()
            };

        sorted_results
            .into_iter()
            .filter_map(|(key, value)| key.map(|k| InstantVectorElement::new(k, value)))
            .collect()
    }

    fn sql_get_is_collapsable(
        &self,
        temporal_aggregation: &AggregationInfo,
        spatial_aggregation: &AggregationInfo,
    ) -> bool {
        match spatial_aggregation.get_name() {
            "SUM" => matches!(
                temporal_aggregation.get_name(),
                "SUM" | "COUNT" // Note: "increase" and "rate" are commented out in Python
            ),
            "MIN" => temporal_aggregation.get_name() == "MIN",
            "MAX" => temporal_aggregation.get_name() == "MAX",
            _ => false,
        }
    }

    fn get_aggregation_id_info(&self, query_config: &QueryConfig) -> AggregationIdInfo {
        let query_config_aggregations = &query_config.aggregations;
        let mut aggregation_id_for_key: Option<u64> = None;
        let mut aggregation_id_for_value: Option<u64> = None;
        let mut aggregation_type_for_key: Option<String> = None;
        let mut aggregation_type_for_value: Option<String> = None;

        if query_config_aggregations.is_empty() {
            panic!("Query config for query has no aggregations defined",);
        } else if query_config_aggregations.len() > 2 {
            panic!("Query config with > 2 aggregations is not supported");
        } else if query_config_aggregations.len() == 2 {
            for aggregation in query_config_aggregations {
                let aggregation_type = self
                    .streaming_config
                    .get_aggregation_config(aggregation.aggregation_id)
                    .map(|config| config.aggregation_type.clone());

                if aggregation_type.as_ref().unwrap() == "DeltaSetAggregator"
                    || aggregation_type.as_ref().unwrap() == "SetAggregator"
                {
                    if aggregation_id_for_key.is_some() {
                        panic!("Aggregation ID for key must be None");
                    }
                    if aggregation_type_for_key.is_some() {
                        panic!("Aggregation type for key must be None");
                    }
                    aggregation_id_for_key = Some(aggregation.aggregation_id);
                    aggregation_type_for_key = aggregation_type;
                } else {
                    if aggregation_id_for_value.is_some() {
                        panic!("Aggregation ID for value must be None");
                    }
                    aggregation_id_for_value = Some(aggregation.aggregation_id);
                    aggregation_type_for_value = aggregation_type;
                }
            }
        } else {
            aggregation_id_for_key = Some(query_config_aggregations[0].aggregation_id);
            aggregation_id_for_value = aggregation_id_for_key;
            // aggregation_type_for_key = Some(query_config_aggregations[0].aggregation_type.clone());
            aggregation_type_for_key = self
                .streaming_config
                .get_aggregation_config(aggregation_id_for_key.unwrap())
                .map(|config| config.aggregation_type.clone());
            aggregation_type_for_value = self
                .streaming_config
                .get_aggregation_config(aggregation_id_for_value.unwrap())
                .map(|config| config.aggregation_type.clone());
        }

        // check for None
        if aggregation_id_for_key.is_none() || aggregation_id_for_value.is_none() {
            panic!("Aggregation IDs must not be None");
        }

        AggregationIdInfo {
            aggregation_id_for_key: aggregation_id_for_key.unwrap(),
            aggregation_id_for_value: aggregation_id_for_value.unwrap(),
            aggregation_type_for_key: aggregation_type_for_key.unwrap(),
            aggregation_type_for_value: aggregation_type_for_value.unwrap(),
        }
    }

    pub fn handle_query_sql(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let context = self.build_query_execution_context_sql(query, time)?;
        // Execute complete query pipeline
        let results = self
            .execute_query_pipeline(&context, false) // SQL: topk disabled
            .map_err(|e| {
                warn!("Query execution failed: {}", e);
                e
            })
            .ok()?;

        Some((
            context.metadata.query_output_labels,
            QueryResult::vector(results, context.query_time),
        ))
    }

    pub fn build_query_execution_context_sql(
        &self,
        query: String,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        // Get SQL schema from inference config
        let schema = match &self.inference_config.schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::PromQL(_) => {
                warn!("SQL query requested but config has PromQL schema");
                return None;
            }
        };

        let statements = parser::parse_sql(&GenericDialect {}, query.as_str()).unwrap();
        let query_data = SQLPatternParser::new(&schema, time).parse_query(&statements);

        let query_data = match query_data {
            Some(data) => data,
            None => {
                debug!("Could not parse query");
                return None;
            }
        };

        let matcher = SQLPatternMatcher::new(schema, self.prometheus_scrape_interval as f64);
        let match_result = matcher.query_info_to_pattern(&query_data);

        debug!("Match result: {:?}", match_result);
        debug!("Validity: {}", match_result.is_valid());

        if !match_result.is_valid() {
            return None;
        }

        let query_pattern_type = match &match_result.query_type[..] {
            [x] => match x {
                QueryType::Spatial => QueryPatternType::OnlySpatial,
                QueryType::TemporalGeneric => QueryPatternType::OnlyTemporal,
                QueryType::TemporalQuantile => QueryPatternType::OnlyTemporal,
            },
            [x, y] => match (x, y) {
                (QueryType::Spatial, QueryType::TemporalGeneric) => {
                    QueryPatternType::OneTemporalOneSpatial
                }
                (QueryType::Spatial, QueryType::TemporalQuantile) => {
                    QueryPatternType::OneTemporalOneSpatial
                }
                _ => panic!("Unsupported query type found"),
            },
            _ => panic!("Unsupported query type found"),
        };

        let query_config = self.find_query_config(&query)?;

        // For nested queries (spatial of temporal), the outer query has no time clause,
        // so we need to use the inner (temporal) query's time_info to compute query_time
        let query_time = match query_pattern_type {
            QueryPatternType::OneTemporalOneSpatial => {
                let inner_time_info = &match_result.query_data[1].time_info;
                Self::convert_query_time_to_data_time(
                    inner_time_info.get_start() + inner_time_info.get_duration(),
                )
            }
            _ => Self::convert_query_time_to_data_time(
                query_data.time_info.get_start() + query_data.time_info.get_duration(),
            ),
        };

        //     self.handle_sql_temporal_aggregation(
        //         query_config,
        //         &match_result,
        //         query_time,
        //         query_pattern_type,
        //     )
        // }

        // fn handle_sql_temporal_aggregation(
        //     &self,
        //     query_config: &QueryConfig,
        //     match_result: &SQLQuery,
        //     query_time: u64,
        //     query_pattern_type: QueryPatternType,
        // ) -> Option<(KeyByLabelNames, QueryResult)> {
        // Labels

        let query_output_labels = match &match_result.query_type.len() {
            // Potentially change SQLQueryType
            1 => {
                // For non-nested queries, output associated labels
                let labels = &match_result.query_data[0].labels;

                KeyByLabelNames::new(labels.clone().into_iter().collect())
            }
            2 => {
                // Extract spatial aggregation output labels using AST-based approach
                let temporal_labels = &match_result.query_data[1].labels;
                let spatial_labels = &match_result.query_data[0].labels;

                let temporal_aggregation = &match_result.query_data[1].aggregation_info;
                let spatial_aggregation = &match_result.query_data[0].aggregation_info;

                match self.sql_get_is_collapsable(temporal_aggregation, spatial_aggregation) {
                    // If false: get all labels, which are all temporal labels. If true, get only spatial labels
                    false => KeyByLabelNames::new(temporal_labels.clone().into_iter().collect()),
                    true => KeyByLabelNames::new(spatial_labels.clone().into_iter().collect()),
                }
            }
            _ => {
                warn!("Invalid query type: {}", query_pattern_type);
                KeyByLabelNames::new(Vec::new())
            }
        };

        // Statistic - determine based on query pattern type
        let statistic_name = match query_pattern_type {
            QueryPatternType::OnlyTemporal => {
                // Use the temporal aggregation (first subquery)
                match_result.query_data[0]
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
            QueryPatternType::OneTemporalOneSpatial => {
                // Use the temporal aggregation (second subquery contains temporal)
                match_result.query_data[1]
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
            QueryPatternType::OnlySpatial => {
                // Use the spatial aggregation (first subquery)
                match_result.query_data[0]
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
        };

        let statistics_to_compute: Vec<Statistic> = if statistic_name == "avg" {
            vec![Statistic::Sum, Statistic::Count]
        } else if let Ok(stat) = statistic_name.parse::<Statistic>() {
            vec![stat]
        } else {
            panic!("Unsupported statistic: {}", statistic_name);
        };

        if statistics_to_compute.len() != 1 {
            panic!(
                "Expected exactly one statistic to compute, found {}",
                statistics_to_compute.len()
            );
        }
        let statistic_to_compute = statistics_to_compute.first().unwrap();

        let query_kwargs = self
            .build_query_kwargs_sql(statistic_to_compute, &match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        // Create query metadata
        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute: *statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };

        // Time
        let timestamps =
            self.calculate_query_timestamps_sql(query_time, query_pattern_type, &match_result);

        // Precomputed output

        let agg_info = self.get_aggregation_id_info(query_config);

        let metric = &match_result.query_data[0].metric;

        let spatial_filter = if query_pattern_type == QueryPatternType::OneTemporalOneSpatial {
            match_result.query_data[0]
                .labels
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        } else {
            String::new()
        };

        // Create query plan and execute values query
        let query_plan = self
            .create_store_query_plan(metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        // Create execution context
        // do_merge is true for temporal queries (OnlyTemporal or OneTemporalOneSpatial)
        let do_merge = query_pattern_type == QueryPatternType::OnlyTemporal
            || query_pattern_type == QueryPatternType::OneTemporalOneSpatial;

        Some(QueryExecutionContext {
            metric: metric.to_string(),
            metadata,
            store_plan: query_plan.clone(),
            agg_info: agg_info.clone(),
            do_merge,
            spatial_filter,
            query_time,
        })

        // TODO: Handle spatial aggregation for OneTemporalOneSpatial when not collapsable
    }

    /// Handle a query following Python's unified architecture
    // pub async fn handle_query(
    pub fn handle_query(&self, query: String, time: f64) -> Option<(KeyByLabelNames, QueryResult)> {
        match self.query_language {
            QueryLanguage::promql => self.handle_query_promql(query, time),
            QueryLanguage::sql => self.handle_query_sql(query, time),
        }
    }

    pub fn handle_query_promql(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let query_start_time = Instant::now();
        debug!("Handling query: {} at time {}", query, time);

        let context = self.build_query_execution_context_promql(query, time)?;

        debug!(
            "Querying store for metric: {}, aggregation_id: {}, range: [{}, {}]",
            context.metric,
            context.agg_info.aggregation_id_for_value,
            context.store_plan.values_query.start_timestamp,
            context.store_plan.values_query.end_timestamp
        );

        // Execute complete query pipeline
        let results = self
            .execute_query_pipeline(&context, true) // PromQL: topk enabled
            .map_err(|e| {
                warn!("Query execution failed: {}", e);
                e
            })
            .ok()?;

        let result = Some((
            context.metadata.query_output_labels,
            QueryResult::vector(results, context.query_time),
        ));

        let total_query_duration = query_start_time.elapsed();
        debug!(
            "Total query handling took: {:.2}ms",
            total_query_duration.as_secs_f64() * 1000.0
        );
        result
    }

    pub fn build_query_execution_context_promql(
        &self,
        query: String,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        // Track query configuration processing latency
        let config_start_time = Instant::now();

        let query_config = self.find_query_config(&query)?;

        let config_duration = config_start_time.elapsed();
        debug!(
            "[LATENCY] Query configuration processing: {:.2}ms",
            config_duration.as_secs_f64() * 1000.0
        );

        let query_time = Self::convert_query_time_to_data_time(time);

        // Parse PromQL AST using promql-parser crate
        let parse_start_time = Instant::now();
        let ast = match promql_parser::parser::parse(&query) {
            Ok(ast) => {
                let parse_duration = parse_start_time.elapsed();
                debug!(
                    "PromQL parsing took: {:.2}ms",
                    parse_duration.as_secs_f64() * 1000.0
                );
                ast
            }
            Err(e) => {
                warn!("Failed to parse PromQL query '{}': {}", query, e);
                return None;
            }
        };

        let pattern_match_start_time = Instant::now();

        let mut found_match = None;
        for (pattern_type, patterns) in &self.controller_patterns {
            for pattern in patterns {
                debug!(
                    "Trying pattern type: {:?} for query: {}",
                    pattern_type, query
                );
                let match_result = pattern.matches(&ast);
                debug!("Match result: {:?}", match_result);
                if match_result.matches {
                    found_match = Some((*pattern_type, match_result));
                    break;
                }
            }
            if found_match.is_some() {
                break;
            }
        }

        let (query_pattern_type, match_result) = match found_match {
            Some((pt, result)) => {
                let pattern_match_duration = pattern_match_start_time.elapsed();
                debug!(
                    "Pattern matching took: {:.2}ms",
                    pattern_match_duration.as_secs_f64() * 1000.0
                );
                (pt, result)
            }
            None => {
                warn!("No matching pattern found for query: {}", query);
                return None;
            }
        };

        debug!("Found matching query config for: {}", query);

        // Track query metadata setup latency
        let query_metadata_start_time = Instant::now();

        // Extract metric and spatial filter using AST-based approach
        // SQL issue: table name and filter label names, return empty filter for now but compute later
        let (metric, spatial_filter) = get_metric_and_spatial_filter(&match_result);

        // Get all labels from inference config for this metric
        let promql_schema = match &self.inference_config.schema {
            SchemaConfig::PromQL(schema) => schema,
            SchemaConfig::SQL(_) => {
                warn!("PromQL query requested but config has SQL schema");
                return None;
            }
        };
        let all_labels = promql_schema
            .get_labels(&metric)
            .cloned()
            .unwrap_or_else(|| {
                warn!(
                    "No metric configuration found for '{}', using empty labels",
                    metric
                );
                panic!("No metric configuration found");
            });

        // Determine query output labels based on pattern type
        // TODO: should we be returning this and using it to convert to final HTTP response?
        let mut query_output_labels = match query_pattern_type {
            QueryPatternType::OnlyTemporal => {
                // For temporal-only queries, output all labels
                all_labels.clone()
            }
            QueryPatternType::OnlySpatial => {
                // Extract spatial aggregation output labels using AST-based approach
                get_spatial_aggregation_output_labels(&match_result, &all_labels)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                // Extract spatial aggregation output labels for combined queries
                let temporal_aggregation = match_result.get_function_name().unwrap();
                let spatial_aggregation = match_result.get_aggregation_op().unwrap();
                // iff temporal outer labels issubset of spatial inner labels, collapse
                // SQL issue: take into account labels from the query, not needed at present because only uses promql translations
                match get_is_collapsable(&temporal_aggregation, &spatial_aggregation) {
                    false => all_labels.clone(),
                    true => get_spatial_aggregation_output_labels(&match_result, &all_labels),
                }
            }
        };

        let timestamps =
            self.calculate_query_timestamps_promql(query_time, query_pattern_type, &match_result);

        // Extract statistics to compute using AST-based approach
        let statistics_to_compute = get_statistics_to_compute(query_pattern_type, &match_result);
        if statistics_to_compute.len() != 1 {
            panic!(
                "Expected exactly one statistic to compute, found {}",
                statistics_to_compute.len()
            );
        }
        let statistic_to_compute = statistics_to_compute.first().unwrap();

        // For topk queries, prepend "__name__" to query_output_labels
        if *statistic_to_compute == Statistic::Topk {
            let mut new_labels = vec!["__name__".to_string()];
            new_labels.extend(query_output_labels.labels);
            query_output_labels = KeyByLabelNames::new(new_labels);
        }

        let query_kwargs = self
            .build_query_kwargs_promql(statistic_to_compute, query_pattern_type, &match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let query_metadata_duration = query_metadata_start_time.elapsed();
        debug!(
            "[LATENCY] Query metadata calculation: {:.2}ms",
            query_metadata_duration.as_secs_f64() * 1000.0
        );

        // Create query metadata
        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute: *statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };

        // Track aggregation configuration processing latency
        let agg_config_start_time = Instant::now();

        let agg_info = self.get_aggregation_id_info(query_config);

        let agg_config_duration = agg_config_start_time.elapsed();
        debug!(
            "[LATENCY] Aggregation configuration processing: {:.2}ms",
            agg_config_duration.as_secs_f64() * 1000.0
        );

        // Create query plan (determines window type and calculates timestamps)
        let query_plan = self
            .create_store_query_plan(&metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        // let window_type = if query_plan.values_query.is_exact_query {
        //     "sliding"
        // } else {
        //     "tumbling"
        // };

        // Create execution context
        // do_merge is true for temporal queries (OnlyTemporal or OneTemporalOneSpatial)
        let do_merge = query_pattern_type == QueryPatternType::OnlyTemporal
            || query_pattern_type == QueryPatternType::OneTemporalOneSpatial;

        Some(QueryExecutionContext {
            metric: metric.clone(),
            metadata,
            store_plan: query_plan.clone(),
            agg_info: agg_info.clone(),
            do_merge,
            spatial_filter,
            query_time,
        })

        // TODO: Handle spatial aggregation for OneTemporalOneSpatial when not collapsable
    }

    /// Merge precomputed outputs
    fn merge_precomputed_outputs(
        &self,
        precomputed_outputs_map: &HashMap<
            Option<KeyByLabelValues>,
            Vec<Box<dyn crate::data_model::AggregateCore>>,
        >,
        do_merge: bool,
        aggregation_type: String,
    ) -> HashMap<Option<KeyByLabelValues>, Box<dyn crate::data_model::AggregateCore>> {
        #[cfg(feature = "extra_debugging")]
        let start_time = Instant::now();
        #[cfg(feature = "extra_debugging")]
        debug!("Starting merge for {} keys", precomputed_outputs_map.len());
        #[cfg(feature = "extra_debugging")]
        debug!(
            "do_merge: {}, aggregation_type: {}",
            do_merge, aggregation_type
        );

        // Merge if: temporal query OR DeltaSetAggregator (which accumulates keys over time)
        let should_merge = do_merge || aggregation_type == "DeltaSetAggregator";

        let mut merged = HashMap::with_capacity(precomputed_outputs_map.len());

        for (idx, (key, precomputes)) in precomputed_outputs_map.iter().enumerate() {
            #[cfg(feature = "extra_debugging")]
            debug!(
                "Processing key {} of {}: {:?}",
                idx + 1,
                precomputed_outputs_map.len(),
                key
            );
            #[cfg(feature = "extra_debugging")]
            debug!(
                "  Number of precomputes for this key: {}",
                precomputes.len()
            );

            if !precomputes.is_empty() {
                if should_merge {
                    #[cfg(feature = "extra_debugging")]
                    debug!("  Merging accumulators (should_merge=true)");
                    #[cfg(feature = "extra_debugging")]
                    let merge_start = Instant::now();
                    let merged_accumulator = self.merge_accumulators(precomputes);
                    #[cfg(feature = "extra_debugging")]
                    let merge_duration = merge_start.elapsed();
                    #[cfg(feature = "extra_debugging")]
                    debug!(
                        "  Merge completed in {:.2}ms, result type: {}",
                        merge_duration.as_secs_f64() * 1000.0,
                        merged_accumulator.get_accumulator_type()
                    );
                    merged.insert(key.clone(), merged_accumulator);
                } else {
                    assert_eq!(
                        precomputes.len(),
                        1,
                        "Spatial queries should have exactly 1 precompute per key"
                    );
                    merged.insert(key.clone(), precomputes[0].clone_boxed_core());
                }
            }
        }

        #[cfg(feature = "extra_debugging")]
        let total_duration = start_time.elapsed();
        #[cfg(feature = "extra_debugging")]
        debug!(
            "[LATENCY] Complete merge operation: {:.2}ms, merged {} keys",
            total_duration.as_secs_f64() * 1000.0,
            merged.len()
        );

        merged
    }

    /// Merge multiple accumulators using the merge_with method from AggregateCore trait
    /// This follows the Python merge_accumulators approach
    fn merge_accumulators(
        &self,
        accumulators: &[Box<dyn crate::data_model::AggregateCore>],
    ) -> Box<dyn crate::data_model::AggregateCore> {
        if accumulators.is_empty() {
            panic!("No accumulators to merge");
        }

        if accumulators.len() == 1 {
            return accumulators[0].clone_boxed_core();
        }

        // Try to use optimized batch merge for KLL accumulators
        if !accumulators.is_empty()
            && accumulators[0].get_accumulator_type() == "DatasketchesKLLAccumulator"
        {
            use crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;

            match DatasketchesKLLAccumulator::merge_multiple(accumulators) {
                Ok(merged) => return Box::new(merged),
                Err(e) => {
                    warn!(
                        "Batch merge failed: {}. Falling back to sequential merge.",
                        e
                    );
                    // Fall through to sequential merge below
                }
            }
        }

        // Try to use optimized batch merge for CountMinSketch accumulators
        if !accumulators.is_empty()
            && accumulators[0].get_accumulator_type() == "CountMinSketchAccumulator"
        {
            use crate::precompute_operators::count_min_sketch_accumulator::CountMinSketchAccumulator;

            match CountMinSketchAccumulator::merge_multiple(accumulators) {
                Ok(merged) => return Box::new(merged),
                Err(e) => {
                    warn!(
                        "Batch merge failed: {}. Falling back to sequential merge.",
                        e
                    );
                    // Fall through to sequential merge below
                }
            }
        }

        // Fallback: sequential merge for other accumulator types
        // (Still benefits from Phase 1 optimization of merge_with)
        let mut result = accumulators[0].clone_boxed_core();

        for accumulator in &accumulators[1..] {
            match result.merge_with(accumulator.as_ref()) {
                Ok(merged) => {
                    result = merged;
                }
                Err(e) => {
                    warn!("Failed to merge accumulator: {}. Using existing result.", e);
                    // Continue with the current result if merge fails
                }
            }
        }

        result
    }

    /// Collects results when key and value use different aggregations
    fn collect_results_separate_keys(
        &self,
        merged_values: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        merged_keys: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        let mut unformatted_results = HashMap::new();

        for (key, precompute) in merged_keys {
            let keys_for_this_precompute = precompute
                .get_keys()
                .ok_or_else(|| "Keys required for separate aggregation".to_string())?;

            for key_for_this_precompute in keys_for_this_precompute {
                let value_precompute = merged_values
                    .get(key)
                    .ok_or_else(|| format!("No value for key: {:?}", key))?;

                let value = self
                    .query_precompute_for_statistic(
                        value_precompute.as_ref(),
                        statistic,
                        &Some(key_for_this_precompute.clone()),
                        query_kwargs,
                    )
                    .map_err(|e| format!("Query failed: {}", e))?;

                unformatted_results.insert(Some(key_for_this_precompute.clone()), value);
            }
        }

        Ok(unformatted_results)
    }

    /// Collects results when key and value use same aggregation
    fn collect_results_same_aggregation(
        &self,
        merged_outputs: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
        enable_topk_limiting: bool,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        let mut unformatted_results = HashMap::new();

        for (key, precompute) in merged_outputs {
            if let Some(unwrapped_keys) = precompute.get_keys() {
                let keys_to_process = if enable_topk_limiting {
                    self.limit_keys_for_topk(unwrapped_keys, statistic, query_kwargs)?
                } else {
                    unwrapped_keys
                };

                for key_for_this_precompute in keys_to_process {
                    let value = self
                        .query_precompute_for_statistic(
                            precompute.as_ref(),
                            statistic,
                            &Some(key_for_this_precompute.clone()),
                            query_kwargs,
                        )
                        .map_err(|e| format!("Query failed: {}", e))?;

                    unformatted_results.insert(Some(key_for_this_precompute.clone()), value);
                }
            } else {
                let value = self
                    .query_precompute_for_statistic(
                        precompute.as_ref(),
                        statistic,
                        &None,
                        query_kwargs,
                    )
                    .map_err(|e| format!("Query failed: {}", e))?;

                unformatted_results.insert(key.clone(), value);
            }
        }

        Ok(unformatted_results)
    }

    /// Limits keys for topk queries
    fn limit_keys_for_topk(
        &self,
        keys: Vec<KeyByLabelValues>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<Vec<KeyByLabelValues>, String> {
        if *statistic != Statistic::Topk {
            return Ok(keys);
        }

        let k_str = query_kwargs
            .get("k")
            .ok_or_else(|| "Missing k parameter for topk".to_string())?;

        let k = k_str
            .parse::<usize>()
            .map_err(|_| format!("Failed to parse k: '{}'", k_str))?;

        Ok(keys.into_iter().take(k).collect())
    }

    /// Query a precompute for a specific statistic
    /// This follows the Python approach where precompute.query(statistic, key) is called
    fn query_precompute_for_statistic(
        &self,
        precompute: &dyn AggregateCore,
        statistic: &Statistic,
        key: &Option<KeyByLabelValues>,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // Handle different accumulator types and statistics using the trait methods
        // TODO: change this logic to just check Single vs MultipleSubpopulationAggregate
        match precompute.get_accumulator_type() {
            "SumAccumulator" => {
                if let Some(sum_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::sum_accumulator::SumAccumulator>() {
                    use crate::data_model::SingleSubpopulationAggregate;
                    sum_acc.query(*statistic, None)
                } else {
                    Err("Failed to downcast to SumAccumulator".into())
                }
            }
            "MinMaxAccumulator" => {
                if let Some(minmax_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::min_max_accumulator::MinMaxAccumulator>() {
                    use crate::data_model::SingleSubpopulationAggregate;
                    minmax_acc.query(*statistic, None)
                } else {
                    Err("Failed to downcast to MinMaxAccumulator".into())
                }
            }
            "IncreaseAccumulator" => {
                if let Some(inc_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::increase_accumulator::IncreaseAccumulator>() {
                    use crate::data_model::SingleSubpopulationAggregate;
                    inc_acc.query(*statistic, None)
                } else {
                    Err("Failed to downcast to IncreaseAccumulator".into())
                }
            }
            "MultipleSumAccumulator" => {
                if let Some(multi_sum_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::multiple_sum_accumulator::MultipleSumAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        multi_sum_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for MultipleSumAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to MultipleSumAccumulator".into())
                }
            }
            "MultipleMinMaxAccumulator" => {
                if let Some(multi_minmax_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::multiple_min_max_accumulator::MultipleMinMaxAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        multi_minmax_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for MultipleMinMaxAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to MultipleMinMaxAccumulator".into())
                }
            }
            "MultipleIncreaseAccumulator" => {
                if let Some(multi_inc_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::multiple_increase_accumulator::MultipleIncreaseAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        multi_inc_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for MultipleIncreaseAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to MultipleIncreaseAccumulator".into())
                }
            }
            "CountMinSketchAccumulator" => {
                if let Some(cms_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::count_min_sketch_accumulator::CountMinSketchAccumulator>() {
                    use crate::data_model::MultipleSubpopulationAggregate;
                    if let Some(key_val) = key {
                        cms_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for CountMinSketchAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to CountMinSketchAccumulator".into())
                }
            }
            "CountMinSketchWithHeapAccumulator" => {
                if let Some(cms_heap_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::count_min_sketch_with_heap_accumulator::CountMinSketchWithHeapAccumulator>() {
                    use crate::data_model::MultipleSubpopulationAggregate;
                    if let Some(key_val) = key {
                        cms_heap_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for CountMinSketchWithHeapAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to CountMinSketchWithHeapAccumulator".into())
                }
            }
            "DatasketchesKLLAccumulator" => {
                if let Some(kll_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator>() {
                    use crate::data_model::SingleSubpopulationAggregate;
                    kll_acc.query(*statistic, Some(query_kwargs))
                } else {
                    Err("Failed to downcast to DatasketchesKLLAccumulator".into())
                }
            }
            "HydraKllSketchAccumulator" => {
                if let Some(hydra_kll_acc) = precompute.as_any()
                    .downcast_ref::<crate::precompute_operators::hydra_kll_accumulator::HydraKllSketchAccumulator>()
                {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        hydra_kll_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for HydraKllSketchAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to HydraKllSketchAccumulator".into())
                }
            }
            "DeltaSetAggregatorAccumulator" => {
                if let Some(delta_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::delta_set_aggregator_accumulator::DeltaSetAggregatorAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        delta_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        // For DeltaSetAggregatorAccumulator without a key, return the union size
                        Ok((delta_acc.added.union(&delta_acc.removed).count()) as f64)
                    }
                } else {
                    Err("Failed to downcast to DeltaSetAggregatorAccumulator".into())
                }
            }
            "SetAggregatorAccumulator" => {
                if let Some(set_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::set_aggregator_accumulator::SetAggregatorAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        set_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        // For SetAggregatorAccumulator without a key, return the set size
                        Ok(set_acc.added.len() as f64)
                    }
                } else {
                    Err("Failed to downcast to SetAggregatorAccumulator".into())
                }
            }
            _ => {
                Err(format!("Unknown accumulator type: {}", precompute.get_accumulator_type()).into())
            }
        }
    }
}
