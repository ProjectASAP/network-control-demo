use crate::data_model::{InferenceConfig, KeyByLabelValues, QueryConfig, StreamingConfig};
use crate::engines::query_result::{InstantVectorElement, QueryResult};
use crate::stores::Store;
use core::panic;
use promql_utilities::get_is_collapsable;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

use crate::AggregateCore;

use promql_utilities::ast_matching::{PromQLMatchResult, PromQLPattern, PromQLPatternBuilder};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{QueryPatternType, Statistic};
use promql_utilities::query_logics::parsing::{
    get_metric_and_spatial_filter, get_spatial_aggregation_output_labels, get_statistics_to_compute,
};

/// Simple query engine for processing PromQL-like queries against precomputed data
pub struct SimpleEngine {
    store: Arc<dyn Store>,
    inference_config: InferenceConfig,
    streaming_config: Arc<StreamingConfig>,
    prometheus_scrape_interval: u64,
    controller_patterns: HashMap<QueryPatternType, Vec<PromQLPattern>>,
}

impl SimpleEngine {
    pub fn new(
        store: Arc<dyn Store>,
        inference_config: InferenceConfig,
        streaming_config: Arc<StreamingConfig>,
        prometheus_scrape_interval: u64,
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
                vec!["sum", "count", "avg", "quantile", "min", "max"],
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
            PromQLPattern::new(
                blocks[pattern_type].clone(),
                vec![
                    "metric".to_string(),
                    "function".to_string(),
                    "range_vector".to_string(),
                ],
            )
        }

        fn spatial_pattern(
            pattern_type: &str,
            blocks: &HashMap<String, Option<HashMap<String, Value>>>,
        ) -> PromQLPattern {
            PromQLPattern::new(
                blocks[pattern_type].clone(),
                vec!["metric".to_string(), "aggregation".to_string()],
            )
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
            PromQLPattern::new(
                pattern,
                vec![
                    "metric".to_string(),
                    "function".to_string(),
                    "range_vector".to_string(),
                    "aggregation".to_string(),
                ],
            )
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
        }
    }

    /// Convert query timestamp (seconds) to data timestamp (milliseconds)
    pub fn convert_query_time_to_data_time(query_time: f64) -> u64 {
        (query_time * 1000.0) as u64
    }

    /// Handle a query following Python's unified architecture
    // pub async fn handle_query(
    pub fn handle_query(
        &self,
        // query_dict: &HashMap<String, Vec<String>>,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let query_start_time = Instant::now();
        debug!("Handling query: {} at time {}", query, time);
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

        let mut query_config = None;

        for config in &self.inference_config.query_configs {
            if config.query == query {
                query_config = Some(config);
                break;
            }
        }

        if query_config.is_none() {
            warn!("No matching query config found for query: {}", query);
            return None;
        }

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

        let (pattern_type, match_result) = match found_match {
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

        // // Use enhanced pattern matcher with parsed AST to find matching pattern
        // let pattern_match_start_time = Instant::now();
        // let enhanced_matcher = EnhancedPromQLPatternMatcher::new();

        // let (pattern_type, enhanced_match_result) = match enhanced_matcher.match_ast(&ast) {
        //     Some((pt, result)) => {
        //         let pattern_match_duration = pattern_match_start_time.elapsed();
        //         debug!(
        //             "Pattern matching took: {:.2}ms",
        //             pattern_match_duration.as_secs_f64() * 1000.0
        //         );
        //         (pt, result)
        //     }
        //     None => {
        //         warn!("No matching pattern found for query: {}", query);
        //         return None;
        //     }
        // };

        // Find matching query configuration
        // let query_config = self
        //     .inference_config
        //     .query_configs
        //     .iter()
        //     .find(|config| config.query == *query)?;

        debug!("Found matching query config for: {}", query);

        let result = self
            .handle_simple_temporal_aggregation(
                &query,
                query_config.unwrap(),
                &match_result,
                query_time,
                pattern_type,
            )
            // .await;
            ;

        let total_query_duration = query_start_time.elapsed();
        debug!(
            "Total query handling took: {:.2}ms",
            total_query_duration.as_secs_f64() * 1000.0
        );
        result
    }

    /// Unified handler for all query pattern types following Python architecture
    // async fn handle_simple_temporal_aggregation(
    fn handle_simple_temporal_aggregation(
        &self,
        query: &str,
        query_config: &QueryConfig,
        match_result: &PromQLMatchResult,
        query_time: u64,
        query_pattern_type: QueryPatternType,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        debug!("Handling simple temporal aggregation for query: {}", query);

        // Track query configuration processing latency
        let config_start_time = Instant::now();

        // Extract metric and spatial filter using AST-based approach
        let (metric, _spatial_filter) = get_metric_and_spatial_filter(match_result);

        // Get all labels from inference config for this metric
        let all_labels = self
            .inference_config
            .metric_config
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
        let query_output_labels = match query_pattern_type {
            QueryPatternType::OnlyTemporal => {
                // For temporal-only queries, output all labels
                all_labels.clone()
            }
            QueryPatternType::OnlySpatial => {
                // Extract spatial aggregation output labels using AST-based approach
                get_spatial_aggregation_output_labels(match_result, &all_labels)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                // Extract spatial aggregation output labels for combined queries
                let temporal_aggregation = match_result.get_function_name().unwrap();
                let spatial_aggregation = match_result.get_aggregation_op().unwrap();
                match get_is_collapsable(&temporal_aggregation, &spatial_aggregation) {
                    false => all_labels.clone(),
                    true => get_spatial_aggregation_output_labels(match_result, &all_labels),
                }
            }
        };

        let config_duration = config_start_time.elapsed();
        debug!(
            "[LATENCY] Query configuration processing: {:.2}ms",
            config_duration.as_secs_f64() * 1000.0
        );

        // Track timestamp calculations latency
        let timestamp_start_time = Instant::now();

        let mut end_timestamp: u64;

        if let Some(at_modifier) = match_result
            .tokens
            .get("metric")?
            .metric
            .as_ref()
            .and_then(|m| m.at_modifier)
        {
            end_timestamp = at_modifier * 1000;
        } else {
            end_timestamp = query_time;
        }

        if !end_timestamp.is_multiple_of(self.prometheus_scrape_interval * 1000) {
            warn!("Query end timestamp {} is not aligned with Prometheus scrape interval of {} seconds. This may lead to inaccurate results.", end_timestamp, self.prometheus_scrape_interval);
        }

        // For OnlySpatial, align end_timestamp to nearest scrape interval
        if query_pattern_type == QueryPatternType::OnlySpatial
            && !end_timestamp.is_multiple_of(self.prometheus_scrape_interval * 1000)
        {
            let interval_ms = self.prometheus_scrape_interval * 1000;
            let aligned_end_timestamp = (end_timestamp / interval_ms) * interval_ms;
            debug!(
                "OnlySpatial query: Aligning end_timestamp from {} to {} using scrape interval of {} seconds",
                end_timestamp, aligned_end_timestamp, self.prometheus_scrape_interval
            );
            end_timestamp = aligned_end_timestamp;
        }

        // Determine time range based on pattern type
        let start_timestamp = match query_pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                // Extract range from AST-based match result
                let range_seconds = match_result.get_range_duration().unwrap().num_seconds() as u64;
                end_timestamp - (range_seconds * 1000)
            }
            QueryPatternType::OnlySpatial => end_timestamp - self.prometheus_scrape_interval * 1000,
        };

        let timestamp_duration = timestamp_start_time.elapsed();
        debug!(
            "[LATENCY] Timestamp calculations: {:.2}ms",
            timestamp_duration.as_secs_f64() * 1000.0
        );

        // Track statistics setup latency
        let stats_start_time = Instant::now();

        // Extract statistics to compute using AST-based approach
        let statistics_to_compute = get_statistics_to_compute(query_pattern_type, match_result);
        if statistics_to_compute.len() != 1 {
            panic!(
                "Expected exactly one statistic to compute, found {}",
                statistics_to_compute.len()
            );
        }
        let statistic_to_compute = statistics_to_compute.first().unwrap();

        let mut query_kwargs: HashMap<String, String> = HashMap::new();
        if *statistic_to_compute == Statistic::Quantile {
            let quantile_value = match query_pattern_type {
                QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                    // Extract from function args - first argument should be the quantile parameter
                    match_result
                        .tokens
                        .get("function_args")
                        .and_then(|token| token.function.as_ref())
                        .and_then(|func| func.args.first())
                }
                QueryPatternType::OnlySpatial => {
                    // Extract from aggregation param
                    match_result
                        .tokens
                        .get("aggregation")
                        .and_then(|token| token.aggregation.as_ref())
                        .and_then(|agg| agg.param.as_ref())
                }
            };

            debug!("Extracted quantile value: {:?}", quantile_value);

            let quantile_str = match quantile_value {
                Some(value) => value,
                None => {
                    warn!("Missing quantile parameter for quantile query: {}", query);
                    return None;
                }
            };

            // // Parse the quantile value from the string format (e.g., "NumberLiteral(0.5)" -> "0.5")
            // let quantile_str =
            //     if quantile_value.starts_with("NumberLiteral(") && quantile_value.ends_with(")") {
            //         &quantile_value[14..quantile_value.len() - 1] // Extract number from NumberLiteral(0.5)
            //     } else {
            //         quantile_value
            //     };

            query_kwargs.insert("quantile".to_string(), quantile_str.to_string());
        }

        let stats_duration = stats_start_time.elapsed();
        debug!(
            "[LATENCY] Statistics and query arguments setup: {:.2}ms",
            stats_duration.as_secs_f64() * 1000.0
        );

        // Track aggregation configuration processing latency
        let agg_config_start_time = Instant::now();

        let query_config_aggregations = &query_config.aggregations;
        let mut aggregation_id_for_key: Option<u64> = None;
        let mut aggregation_id_for_value: Option<u64> = None;
        let mut aggregation_type_for_key: Option<String> = None;

        if query_config_aggregations.is_empty() {
            panic!(
                "Query config for query '{}' has no aggregations defined",
                query
            );
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
        }

        // check for None
        if aggregation_id_for_key.is_none() || aggregation_id_for_value.is_none() {
            panic!("Aggregation IDs must not be None");
        }

        let agg_config_duration = agg_config_start_time.elapsed();
        debug!(
            "[LATENCY] Aggregation configuration processing: {:.2}ms",
            agg_config_duration.as_secs_f64() * 1000.0
        );

        debug!(
            "Querying store for metric: {}, aggregation_id: {}, time range: {} - {}",
            metric, query_config.aggregations[0].aggregation_id, start_timestamp, end_timestamp
        );

        let store_query_start_time = Instant::now();

        // Query the store for precomputed outputs
        let precomputed_outputs_map = match self
            .store
            .query_precomputed_output(
                &metric,
                aggregation_id_for_value?,
                start_timestamp,
                end_timestamp,
            )
            // .await
        {
            Ok(outputs) => {
                let store_query_duration = store_query_start_time.elapsed();
                debug!(
                    "Store query took: {:.2}ms",
                    store_query_duration.as_secs_f64() * 1000.0
                );
                outputs
            }
            Err(e) => {
                warn!("Error querying store: {}", e);
                return None;
            }
        };

        if precomputed_outputs_map.is_empty() {
            info!("No precomputed outputs found for metric: {}", metric);
            return None;
        }

        // Merge precomputed outputs based on pattern type
        let merge_start_time = Instant::now();
        debug!(
            "Starting merge with {} outputs for query pattern: {:?}",
            precomputed_outputs_map.len(),
            query_pattern_type
        );

        // let merged_precompute_outputs_map = match query_pattern_type {
        //     QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
        //         // For temporal queries, merge all precomputes for each key
        //         self.merge_temporal_precomputes(&precomputed_outputs_map)
        //     }
        //     QueryPatternType::OnlySpatial => {
        //         // For spatial queries, use single precompute per key
        //         self.use_single_precomputes(&precomputed_outputs_map)
        //     }
        // };
        let merged_precompute_outputs_map =
            self.merge_precomputed_outputs(&precomputed_outputs_map, query_pattern_type);
        let merge_duration = merge_start_time.elapsed();
        debug!(
            "[LATENCY] Precomputed output merging: {:.2}ms",
            merge_duration.as_secs_f64() * 1000.0
        );

        let unformatted_results_start_time = Instant::now();
        let mut unformatted_results: HashMap<Option<KeyByLabelValues>, f64> = HashMap::new();

        if aggregation_id_for_key == aggregation_id_for_value {
            for (key, precompute) in &merged_precompute_outputs_map {
                let keys_for_this_precompute = precompute.get_keys();
                if let Some(unwrapped_keys_for_this_precompute) = keys_for_this_precompute {
                    for key_for_this_precompute in unwrapped_keys_for_this_precompute {
                        unformatted_results.insert(
                            Some(key_for_this_precompute.clone()),
                            self.query_precompute_for_statistic(
                                precompute.as_ref(),
                                statistic_to_compute,
                                &Some(key_for_this_precompute),
                                &query_kwargs,
                            )
                            .unwrap_or_else(|e| {
                                panic!("Failed to query precompute for statistic: {}", e)
                            }),
                        );
                    }
                } else {
                    unformatted_results.insert(
                        key.clone(),
                        self.query_precompute_for_statistic(
                            precompute.as_ref(),
                            statistic_to_compute,
                            &None,
                            &query_kwargs,
                        )
                        .unwrap_or_else(|e| {
                            panic!("Failed to query precompute for statistic: {}", e)
                        }),
                    );
                }
            }
        } else {
            // TODO: make this more efficient
            // ideally, we should just cache the set of keys from ther previous query, basically like a subtractable PrecomputeOutput

            // TODO: for DeltaSetAggregator, we need to get all precomputes from the beginning of time to end_timestamp
            // for SetAggregator, we can just get the latest precompute

            let precomputed_outputs_map_for_keys;
            let keys_store_query_start_time = Instant::now();

            if aggregation_type_for_key.is_none() {
                panic!("Aggregation type for key must not be None");
            }

            if aggregation_type_for_key.as_ref().unwrap() == "DeltaSetAggregator" {
                precomputed_outputs_map_for_keys = self.store.query_precomputed_output(
                    &metric,
                    aggregation_id_for_key?,
                    // 0 because we want to get all keys from the beginning of time
                    0,
                    end_timestamp,
                )
                // .await
                // .ok();
                ;
            } else if aggregation_type_for_key.as_ref().unwrap() == "SetAggregator" {
                precomputed_outputs_map_for_keys = self
                    .store
                    .query_precomputed_output(
                        &metric,
                        aggregation_id_for_key?,
                        // NOTE: this is a hack so that we get only the latest aggregation for each KeyByLabelValues. This might not work if SimpleMapStore implementation changes
                        end_timestamp
                            - self
                                .streaming_config
                                .get_aggregation_config(aggregation_id_for_key?)
                                .map(|config| config.tumbling_window_size * 1000)
                                .unwrap_or_else(|| panic!("Failed to get tumbling window size")),
                        end_timestamp,
                    )
                    // .await
                    // .ok();
                    ;
            } else {
                panic!(
                    "Unsupported aggregation type: {}",
                    aggregation_type_for_key.as_ref().unwrap()
                );
            }

            debug!(
                "[LATENCY] Keys store query (metric: {}, agg: {}): {}ms",
                &metric,
                aggregation_id_for_key?,
                (Instant::now() - keys_store_query_start_time).as_millis()
            );

            if precomputed_outputs_map_for_keys.is_err() {
                warn!("No precomputed outputs found for keys for metric: {}, aggregation_id: {}, time range: {} - {}", metric, aggregation_id_for_key?, 0, end_timestamp);
                return None;
            }

            let merged_precompute_outputs_map_for_keys = self.merge_precomputed_outputs(
                &precomputed_outputs_map_for_keys.unwrap(),
                query_pattern_type,
            );

            // merged_precompute_outputs_map_for_keys = self.merge_precomputed_outputs(
            //     precomputed_outputs_map_for_keys, query_pattern_type
            // )

            for (key, precompute) in &merged_precompute_outputs_map_for_keys {
                let keys_for_this_precompute = precompute.get_keys();
                assert!(
                    keys_for_this_precompute.is_some(),
                    "Keys for precompute must not be None when aggregation_id_for_key is different from aggregation_id_for_value"
                );
                for key_for_this_precompute in keys_for_this_precompute.unwrap() {
                    // unformatted_results[key_for_this_precompute] = merged_precompute_outputs_map
                    //     [key]
                    //     .query(statistic_to_compute, key_for_this_precompute, query_kwargs)

                    unformatted_results.insert(
                        Some(key_for_this_precompute.clone()),
                        self.query_precompute_for_statistic(
                            merged_precompute_outputs_map[key].as_ref(),
                            statistic_to_compute,
                            &Some(key_for_this_precompute),
                            &query_kwargs,
                        )
                        .unwrap_or_else(|e| {
                            panic!("Failed to query precompute for statistic: {}", e);
                        }),
                    );
                }
            }
        }

        debug!(
            "[LATENCY] Unformatted results collection: {:.2}ms",
            unformatted_results_start_time.elapsed().as_secs_f64() * 1000.0
        );

        let results_start_time = Instant::now();
        let mut results: Vec<InstantVectorElement> = Vec::new();

        for (key, value) in unformatted_results {
            // if key.is_none() {
            //     panic!("Need to add support for None key")
            // } else {
            //     results.push(InstantVectorElement::new(key.unwrap(), value));
            // }

            if let Some(k) = key {
                results.push(InstantVectorElement::new(k, value));
            } else {
                panic!("Need to add support for None key")
            }
        }

        debug!(
            "[LATENCY] Results collection: {}ms",
            results_start_time.elapsed().as_millis()
        );

        Some((
            query_output_labels,
            QueryResult::vector(results, query_time),
        ))

        // TODO: Handle spatial aggregation for OneTemporalOneSpatial when not collapsable
    }

    /// Merge precomputed outputs
    fn merge_precomputed_outputs(
        &self,
        precomputed_outputs_map: &HashMap<
            Option<KeyByLabelValues>,
            Vec<Box<dyn crate::data_model::AggregateCore>>,
        >,
        query_pattern_type: QueryPatternType,
    ) -> HashMap<Option<KeyByLabelValues>, Box<dyn crate::data_model::AggregateCore>> {
        let start_time = Instant::now();
        debug!("Starting merge for {} keys", precomputed_outputs_map.len());

        let mut merged = HashMap::new();

        for (key, precomputes) in precomputed_outputs_map.iter() {
            if !precomputes.is_empty() {
                if query_pattern_type == QueryPatternType::OnlyTemporal
                    || query_pattern_type == QueryPatternType::OneTemporalOneSpatial
                {
                    let merged_accumulator = self.merge_accumulators(precomputes);
                    merged.insert(key.clone(), merged_accumulator);
                } else if query_pattern_type == QueryPatternType::OnlySpatial {
                    assert_eq!(
                        precomputes.len(),
                        1,
                        "Spatial queries should have exactly 1 precompute per key"
                    );
                    merged.insert(key.clone(), precomputes[0].clone());
                }
            }
        }

        let total_duration = start_time.elapsed();
        debug!(
            "[LATENCY] Complete merge operation: {:.2}ms",
            total_duration.as_secs_f64() * 1000.0
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
            return accumulators[0].clone();
        }

        // Start with the first accumulator and merge all others into it
        let mut result = accumulators[0].clone();

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

    /// Query a precompute for a specific statistic
    /// This follows the Python approach where precompute.query(statistic, key) is called
    fn query_precompute_for_statistic(
        &self,
        precompute: &dyn AggregateCore,
        statistic: &Statistic,
        key: &Option<KeyByLabelValues>,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // TODO: use query_kwargs (now implemented for DatasketchesKLLAccumulator)
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
                        multi_sum_acc.query(*statistic, key_val)
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
                        multi_minmax_acc.query(*statistic, key_val)
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
                        multi_inc_acc.query(*statistic, key_val)
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
                        cms_acc.query(*statistic, key_val)
                    } else {
                        Err("Key required for CountMinSketchAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to CountMinSketchAccumulator".into())
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
            "DeltaSetAggregatorAccumulator" => {
                if let Some(delta_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::delta_set_aggregator_accumulator::DeltaSetAggregatorAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        delta_acc.query(*statistic, key_val)
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
                        set_acc.query(*statistic, key_val)
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::stores::SimpleMapStore;

//     fn create_test_engine() -> SimpleEngine {
//         let store = Arc::new(SimpleMapStore::new());
//         let inference_config = InferenceConfig::default();
//         SimpleEngine::new(store, inference_config, 15000)
//     }

//     #[test]
//     fn test_time_conversion() {
//         let query_time = 1609459200.0; // 2021-01-01 00:00:00 UTC
//         let data_time = SimpleEngine::convert_query_time_to_data_time(query_time);
//         assert_eq!(data_time, 1609459200000);
//     }

//     // #[test]
//     // fn test_duration_parsing() {
//     //     // Test the AST query extractor's duration parsing
//     //     assert_eq!(ASTQueryExtractor::parse_duration("5m"), Some(300));
//     //     assert_eq!(ASTQueryExtractor::parse_duration("1h"), Some(3600));
//     //     assert_eq!(ASTQueryExtractor::parse_duration("30s"), Some(30));
//     //     assert_eq!(ASTQueryExtractor::parse_duration("2d"), Some(172800));
//     //     assert_eq!(ASTQueryExtractor::parse_duration("invalid"), None);
//     //     assert_eq!(ASTQueryExtractor::parse_duration(""), None);
//     // }

//     // #[test]
//     // fn test_ast_metric_extraction() {
//     //     let matcher = EnhancedPromQLPatternMatcher::new();

//     //     // Test AST-based metric extraction
//     //     if let Some((_, match_result)) = matcher.match_query("sum_over_time(cpu_usage[5m])") {
//     //         assert_eq!(
//     //             match_result.get_metric_name(),
//     //             Some("cpu_usage".to_string())
//     //         );
//     //     }

//     //     if let Some((_, match_result)) = matcher.match_query("rate(http_requests_total[1h])") {
//     //         assert_eq!(
//     //             match_result.get_metric_name(),
//     //             Some("http_requests_total".to_string())
//     //         );
//     //     }

//     //     if let Some((_, match_result)) = matcher.match_query("sum(memory_usage)") {
//     //         assert_eq!(
//     //             match_result.get_metric_name(),
//     //             Some("memory_usage".to_string())
//     //         );
//     //     }

//     //     // Invalid query should return None
//     //     assert!(matcher.match_query("invalid_query").is_none());
//     // }

//     // #[test]
//     // fn test_ast_pattern_type_detection() {
//     //     let matcher = EnhancedPromQLPatternMatcher::new();

//     //     // Temporal only
//     //     if let Some((pattern_type, _)) = matcher.match_query("sum_over_time(cpu_usage[5m])") {
//     //         assert_eq!(pattern_type, QueryPatternType::OnlyTemporal);
//     //     }

//     //     // Spatial only
//     //     if let Some((pattern_type, _)) = matcher.match_query("sum(memory_usage)") {
//     //         assert_eq!(pattern_type, QueryPatternType::OnlySpatial);
//     //     }

//     //     // Combined
//     //     if let Some((pattern_type, _)) = matcher.match_query("sum(rate(http_requests[5m]))") {
//     //         assert_eq!(pattern_type, QueryPatternType::OneTemporalOneSpatial);
//     //     }
//     // }

//     // #[test]
//     // fn test_ast_statistic_extraction() {
//     //     let matcher = EnhancedPromQLPatternMatcher::new();

//     //     // Test temporal function statistics
//     //     if let Some((pattern_type, match_result)) =
//     //         matcher.match_query("sum_over_time(cpu_usage[5m])")
//     //     {
//     //         let stats = ASTQueryExtractor::get_statistics_to_compute(pattern_type, &match_result);
//     //         assert!(!stats.is_empty());
//     //         assert_eq!(stats[0], Statistic::Sum);
//     //     }

//     //     if let Some((pattern_type, match_result)) = matcher.match_query("rate(http_requests[5m])") {
//     //         let stats = ASTQueryExtractor::get_statistics_to_compute(pattern_type, &match_result);
//     //         assert!(!stats.is_empty());
//     //         assert_eq!(stats[0], Statistic::Rate);
//     //     }

//     //     // Test spatial aggregation statistics
//     //     if let Some((pattern_type, match_result)) = matcher.match_query("max(memory_usage)") {
//     //         let stats = ASTQueryExtractor::get_statistics_to_compute(pattern_type, &match_result);
//     //         assert!(!stats.is_empty());
//     //         assert_eq!(stats[0], Statistic::Max);
//     //     }
//     // }

//     #[tokio::test]
//     async fn test_query_handling_no_data() {
//         let engine = create_test_engine();
//         let mut query_dict = HashMap::new();
//         query_dict.insert(
//             "query".to_string(),
//             vec!["sum_over_time(cpu_usage[5m])".to_string()],
//         );
//         query_dict.insert("time".to_string(), vec!["1609459200".to_string()]);

//         // Should return None since there's no matching query config or data
//         let result = engine.handle_query(&query_dict).await;
//         assert!(result.is_none());
//     }
// }
