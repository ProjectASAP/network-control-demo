//! Integration tests for the AST-based QueryEngineRust architecture
//!
//! This module provides comprehensive end-to-end testing of the pattern matching system,
//! validating that AST-based parsing correctly replaces heuristic methods and produces
//! accurate query processing results.

// TODO: need to go through this code and un-comment/update tests

// #[cfg(test)]
// use crate::data_model::{AggregateCore, KeyByLabelValues};
// use crate::precompute_operators::SumAccumulator;

// use crate::stores::Store;
// use async_trait::async_trait;
// use std::collections::HashMap;

// /// Mock store implementation for testing
// struct MockStore {
//     data: HashMap<String, HashMap<String, f64>>,
// }

// impl MockStore {
//     fn new() -> Self {
//         Self {
//             data: HashMap::new(),
//         }
//     }

//     fn add_data(&mut self, metric: &str, key: &str, value: f64) {
//         self.data
//             .entry(metric.to_string())
//             .or_default()
//             .insert(key.to_string(), value);
//     }
// }

// #[async_trait]
// impl Store for MockStore {
//     async fn query_precomputed_output(
//         &self,
//         metric: &str,
//         _aggregation_id: u64,
//         _start_timestamp: u64,
//         _end_timestamp: u64,
//     ) -> Result<
//         HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>>,
//         Box<dyn std::error::Error + Send + Sync>,
//     > {
//         let mut result = HashMap::new();

//         if let Some(metric_data) = self.data.get(metric) {
//             for value in metric_data.values() {
//                 let accumulator = SumAccumulator::with_sum(*value);
//                 let aggregate: Box<dyn AggregateCore> = Box::new(accumulator);

//                 // For this test, we'll use None as the key (simplified)
//                 result.insert(None, vec![aggregate]);
//             }
//         }

//         Ok(result)
//     }

//     fn insert_precomputed_output(
//         &self,
//         _output: crate::data_model::PrecomputedOutput,
//         _precompute: Box<dyn AggregateCore>,
//     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//         // Mock implementation - do nothing
//         Ok(())
//     }

//     fn insert_precomputed_output_batch(
//         &self,
//         _outputs: Vec<(
//             crate::data_model::PrecomputedOutput,
//             Box<dyn crate::data_model::AggregateCore>,
//         )>,
//     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//         // Mock implementation - do nothing
//         Ok(())
//     }

//     fn get_earliest_timestamp_per_aggregation_id(
//         &self,
//     ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
//         // Mock implementation - return empty map
//         Ok(HashMap::new())
//     }

//     fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//         // Mock implementation - do nothing
//         Ok(())
//     }
// }

// /// Create a test SimpleEngine with mock data
// fn create_test_simple_engine() -> SimpleEngine {
//     let mut mock_store = MockStore::new();
//     mock_store.add_data("http_requests_total", "test_key", 1000.0);
//     mock_store.add_data("cpu_usage", "test_key", 75.5);
//     mock_store.add_data("memory_usage", "test_key", 512.0);

//     let inference_config = create_test_inference_config();

//     SimpleEngine::new(
//         Arc::new(mock_store),
//         inference_config,
//         15000, // 15s scrape interval
//     )
// }

// /// Create test inference configuration
// fn create_test_inference_config() -> InferenceConfig {
//     let query_configs = vec![
//         // OnlyTemporal query config
//         QueryConfig {
//             query: "rate(http_requests_total[5m])".to_string(),
//             aggregations: vec![AggregationConfig::new(
//                 1,
//                 "http_requests_total".to_string(),
//                 KeyByLabelNames::new(vec![]), // No grouping for temporal
//                 KeyByLabelNames::new(vec![]),
//                 KeyByLabelNames::new(vec![]),
//                 "".to_string(), // No spatial filter
//                 "rate".to_string(),
//                 300, // 5m tumbling window
//             )],
//         },
//         // OnlySpatial query config
//         QueryConfig {
//             query: "sum(memory_usage) by (instance)".to_string(),
//             aggregations: vec![AggregationConfig::new(
//                 2,
//                 "memory_usage".to_string(),
//                 KeyByLabelNames::new(vec!["instance".to_string()]),
//                 KeyByLabelNames::new(vec![]),
//                 KeyByLabelNames::new(vec![]),
//                 "".to_string(),
//                 "sum".to_string(),
//                 60, // 1m tumbling window
//             )],
//         },
//         // OneTemporalOneSpatial query config
//         QueryConfig {
//             query: "sum(rate(cpu_usage[5m])) by (job)".to_string(),
//             aggregations: vec![AggregationConfig::new(
//                 3,
//                 "cpu_usage".to_string(),
//                 KeyByLabelNames::new(vec!["job".to_string()]),
//                 KeyByLabelNames::new(vec![]),
//                 KeyByLabelNames::new(vec![]),
//                 "".to_string(),
//                 "rate".to_string(),
//                 300, // 5m tumbling window
//             )],
//         },
//     ];

//     let metric_config = MetricConfig {
//         config: HashMap::new(), // Simplified for tests
//     };

//     InferenceConfig {
//         query_configs,
//         metric_config,
//     }
// }

// #[tokio::test]
// async fn test_ast_pattern_matching_integration() {
//     let matcher = PromQLPatternMatcher::new();

//     // Test OnlyTemporal pattern matching
//     let (pattern_type, match_result) = matcher
//         .match_query("rate(http_requests_total[5m])")
//         .expect("Should match OnlyTemporal pattern");

//     assert_eq!(pattern_type, QueryPatternType::OnlyTemporal);
//     assert!(match_result.matches);
//     assert_eq!(
//         match_result.get_metric_name(),
//         Some("http_requests_total".to_string())
//     );
//     assert_eq!(match_result.get_function_name(), Some("rate".to_string()));
//     assert_eq!(match_result.get_range_duration(), Some("5m".to_string()));

//     // Test OnlySpatial pattern matching
//     let (pattern_type, match_result) = matcher
//         .match_query("sum(memory_usage) by (instance)")
//         .expect("Should match OnlySpatial pattern");

//     assert_eq!(pattern_type, QueryPatternType::OnlySpatial);
//     assert!(match_result.matches);
//     assert_eq!(
//         match_result.get_metric_name(),
//         Some("memory_usage".to_string())
//     );
//     assert_eq!(match_result.get_aggregation_op(), Some("sum".to_string()));

//     // Test OneTemporalOneSpatial pattern matching
//     let (pattern_type, match_result) = matcher
//         .match_query("sum(rate(cpu_usage[5m])) by (job)")
//         .expect("Should match OneTemporalOneSpatial pattern");

//     assert_eq!(pattern_type, QueryPatternType::OneTemporalOneSpatial);
//     assert!(match_result.matches);
//     assert_eq!(
//         match_result.get_metric_name(),
//         Some("cpu_usage".to_string())
//     );
//     assert_eq!(match_result.get_function_name(), Some("rate".to_string()));
//     assert_eq!(match_result.get_aggregation_op(), Some("sum".to_string()));
//     assert_eq!(match_result.get_range_duration(), Some("5m".to_string()));
// }

// #[tokio::test]
// async fn test_ast_query_extraction() {
//     let matcher = PromQLPatternMatcher::new();

//     // Test metric and spatial filter extraction
//     let (pattern_type, match_result) = matcher
//         .match_query(
//             "sum(http_requests_total{job=\"prometheus\", instance=\"localhost:9090\"}) by (method)",
//         )
//         .expect("Should match pattern");

//     let (metric_name, _spatial_filter) = get_metric_and_spatial_filter(&match_result);
//     assert_eq!(metric_name, "http_requests_total");
//     // Note: spatial filter format depends on implementation

//     // Test statistics extraction for spatial functions
//     let statistics = get_statistics_to_compute(
//         pattern_type, // Use the actual pattern type from the match
//         &match_result,
//     );
//     assert!(!statistics.is_empty());

//     // Test range duration extraction
//     let (_, temporal_match) = matcher
//         .match_query("rate(cpu_usage[10m])")
//         .expect("Should match temporal pattern");

//     let range_seconds = ASTQueryExtractor::get_range_duration(&temporal_match);
//     assert_eq!(range_seconds, Some(600)); // 10 minutes = 600 seconds
// }

// #[tokio::test]
// async fn test_spatial_aggregation_labels() {
//     let matcher = PromQLPatternMatcher::new();

//     // Test "by" modifier extraction
//     let (_, match_result) = matcher
//         .match_query("sum(http_requests_total) by (job, instance)")
//         .expect("Should match spatial pattern");

//     let all_labels = KeyByLabelNames::new(vec![
//         "job".to_string(),
//         "instance".to_string(),
//         "method".to_string(),
//         "status".to_string(),
//     ]);

//     let output_labels =
//         ASTQueryExtractor::get_spatial_aggregation_output_labels(&match_result, &all_labels);

//     // Should contain the "by" labels
//     assert!(output_labels.labels.contains(&"job".to_string()));
//     assert!(output_labels.labels.contains(&"instance".to_string()));

//     // Should be sorted
//     let mut expected_labels = vec!["instance".to_string(), "job".to_string()];
//     expected_labels.sort();
//     assert_eq!(output_labels.labels, expected_labels);
// }

// #[tokio::test]
// async fn test_simple_engine_integration() {
//     let engine = create_test_simple_engine();

//     // Test query handling with AST-based pattern matching
//     let result = engine
//         .handle_query(
//             "rate(http_requests_total[5m])".to_string(),
//             1609459200 as f64,
//         )
//         .await;

//     // Note: This test may return None due to mock store limitations
//     // In a real implementation, the store would return appropriate precomputed data
//     // For now, we just verify the query is processed without errors
//     println!("Query result: {result:?}");
// }

// #[tokio::test]
// async fn test_pattern_type_accuracy() {
//     let matcher = PromQLPatternMatcher::new();

//     // Test various query patterns to ensure accurate classification
//     let test_cases = vec![
//         ("rate(cpu_usage[5m])", QueryPatternType::OnlyTemporal),
//         (
//             "increase(http_requests_total[1h])",
//             QueryPatternType::OnlyTemporal,
//         ),
//         (
//             "sum_over_time(memory_usage[10m])",
//             QueryPatternType::OnlyTemporal,
//         ),
//         ("sum(http_requests_total)", QueryPatternType::OnlySpatial),
//         (
//             "avg(cpu_usage) by (instance)",
//             QueryPatternType::OnlySpatial,
//         ),
//         (
//             "max(memory_usage) without (job)",
//             QueryPatternType::OnlySpatial,
//         ),
//         (
//             "sum(rate(http_requests_total[5m]))",
//             QueryPatternType::OneTemporalOneSpatial,
//         ),
//         (
//             "avg(increase(cpu_usage[1m])) by (job)",
//             QueryPatternType::OneTemporalOneSpatial,
//         ),
//         (
//             "count(sum_over_time(memory_usage[5m])) without (instance)",
//             QueryPatternType::OneTemporalOneSpatial,
//         ),
//     ];

//     for (query, expected_pattern) in test_cases {
//         let result = matcher.match_query(query);
//         match result {
//             Some((pattern_type, _)) => {
//                 assert_eq!(
//                     pattern_type, expected_pattern,
//                     "Query '{query}' should match {expected_pattern:?}, got {pattern_type:?}"
//                 );
//             }
//             None => {
//                 panic!(
//                     "Query '{query}' should match pattern {expected_pattern:?} but no match found"
//                 );
//             }
//         }
//     }
// }

// #[test]
// fn test_token_data_structures() {
//     // Test MetricToken creation and usage
//     let metric_token = MetricToken {
//         name: "http_requests_total".to_string(),
//         labels: {
//             let mut labels = HashMap::new();
//             labels.insert("job".to_string(), "prometheus".to_string());
//             labels.insert("instance".to_string(), "localhost:9090".to_string());
//             labels
//         },
//         at_modifier: None,
//     };

//     assert_eq!(metric_token.name, "http_requests_total");
//     assert_eq!(metric_token.labels.len(), 2);

//     // Test FunctionToken creation and usage
//     let function_token = FunctionToken {
//         name: "rate".to_string(),
//         args: vec!["arg1".to_string()],
//     };

//     assert_eq!(function_token.name, "rate");
//     assert_eq!(function_token.args.len(), 1);

//     // Test AggregationToken creation and usage
//     let aggregation_token = AggregationToken {
//         op: "sum".to_string(),
//         modifier: Some(AggregationModifier {
//             modifier_type: "by".to_string(),
//             labels: vec!["job".to_string(), "instance".to_string()],
//         }),
//         param: None,
//     };

//     assert_eq!(aggregation_token.op, "sum");
//     assert!(aggregation_token.modifier.is_some());

//     if let Some(modifier) = &aggregation_token.modifier {
//         assert_eq!(modifier.modifier_type, "by");
//         assert_eq!(modifier.labels.len(), 2);
//     }
// }

// #[test]
// fn test_enhanced_match_result() {
//     // Test EnhancedPromQLMatchResult creation and access methods
//     let mut tokens = HashMap::new();

//     let metric_token = MetricToken {
//         name: "test_metric".to_string(),
//         labels: HashMap::new(),
//         at_modifier: None,
//     };

//     let function_token = FunctionToken {
//         name: "rate".to_string(),
//         args: vec![],
//     };

//     let aggregation_token = AggregationToken {
//         op: "sum".to_string(),
//         modifier: None,
//         param: None,
//     };

//     // Add tokens
//     tokens.insert(
//         "metric".to_string(),
//         TokenData {
//             metric: Some(metric_token),
//             function: None,
//             aggregation: None,
//             range_vector: None,
//             binary_op: None,
//             number: None,
//         },
//     );

//     tokens.insert(
//         "function".to_string(),
//         TokenData {
//             metric: None,
//             function: Some(function_token),
//             aggregation: None,
//             range_vector: None,
//             binary_op: None,
//             number: None,
//         },
//     );

//     tokens.insert(
//         "aggregation".to_string(),
//         TokenData {
//             metric: None,
//             function: None,
//             aggregation: Some(aggregation_token),
//             range_vector: None,
//             binary_op: None,
//             number: None,
//         },
//     );

//     let match_result = PromQLMatchResult::with_tokens(tokens);

//     // Test access methods
//     assert!(match_result.matches);
//     assert_eq!(
//         match_result.get_metric_name(),
//         Some("test_metric".to_string())
//     );
//     assert_eq!(match_result.get_function_name(), Some("rate".to_string()));
//     assert_eq!(match_result.get_aggregation_op(), Some("sum".to_string()));
// }

// #[test]
// fn test_pattern_factory() {
//     // Test that pattern factory creates all expected patterns
//     let patterns = PromQLPatternFactory::get_all_patterns();
//     assert_eq!(patterns.len(), 3);

//     let pattern_types: Vec<QueryPatternType> =
//         patterns.iter().map(|p| p.expected_pattern_type).collect();

//     assert!(pattern_types.contains(&QueryPatternType::OnlyTemporal));
//     assert!(pattern_types.contains(&QueryPatternType::OnlySpatial));
//     assert!(pattern_types.contains(&QueryPatternType::OneTemporalOneSpatial));

//     // Test individual pattern creation
//     let temporal_pattern = PromQLPatternFactory::only_temporal_pattern();
//     assert_eq!(
//         temporal_pattern.expected_pattern_type,
//         QueryPatternType::OnlyTemporal
//     );

//     let spatial_pattern = PromQLPatternFactory::only_spatial_pattern();
//     assert_eq!(
//         spatial_pattern.expected_pattern_type,
//         QueryPatternType::OnlySpatial
//     );

//     let combined_pattern = PromQLPatternFactory::one_temporal_one_spatial_pattern();
//     assert_eq!(
//         combined_pattern.expected_pattern_type,
//         QueryPatternType::OneTemporalOneSpatial
//     );
// }

// #[tokio::test]
// async fn test_performance_comparison() {
//     // Performance test comparing AST vs heuristic approaches
//     // Note: This is a conceptual test - actual heuristic methods have been replaced

//     let matcher = PromQLPatternMatcher::new();
//     let test_queries = vec![
//         "rate(http_requests_total[5m])",
//         "sum(memory_usage) by (instance)",
//         "avg(rate(cpu_usage[1m])) by (job, instance)",
//         "count(increase(disk_io_total[10m])) without (device)",
//     ];

//     let start_time = std::time::Instant::now();

//     for query in &test_queries {
//         let result = matcher.match_query(query);
//         assert!(result.is_some(), "Query '{query}' should match a pattern");
//     }

//     let ast_duration = start_time.elapsed();

//     // Log performance results
//     println!(
//         "AST-based pattern matching took: {:?} for {} queries",
//         ast_duration,
//         test_queries.len()
//     );
//     println!(
//         "Average time per query: {:?}",
//         ast_duration / test_queries.len() as u32
//     );

//     // Assert reasonable performance (adjust threshold as needed)
//     assert!(
//         ast_duration.as_millis() < 100,
//         "AST pattern matching should be reasonably fast"
//     );
// }

// #[tokio::test]
// async fn test_error_handling() {
//     let matcher = PromQLPatternMatcher::new();

//     // Test invalid PromQL queries
//     let invalid_queries = vec![
//         "invalid_syntax(((",
//         "rate(metric",                     // Missing closing bracket
//         "sum(rate(metric[5m]) by (label)", // Missing closing parenthesis
//         "",                                // Empty query
//     ];

//     for query in invalid_queries {
//         let result = matcher.match_query(query);
//         // Invalid queries should return None rather than panic
//         assert!(
//             result.is_none(),
//             "Invalid query '{query}' should return None"
//         );
//     }
// }

// #[tokio::test]
// async fn test_backward_compatibility() {
//     // Test that existing interfaces still work after AST integration
//     let engine = create_test_simple_engine();

//     // This should not panic and should handle the query gracefully
//     let _result = engine
//         .handle_query("sum(cpu_usage)".to_string(), 1609459200 as f64)
//         .await;

//     // Test with various query formats that should be supported
//     let test_queries = vec![
//         "rate(http_requests_total[5m])",
//         "sum(memory_usage)",
//         "avg(cpu_usage) by (instance)",
//         "sum(rate(disk_io[1m])) by (device)",
//     ];

//     for query in test_queries {
//         // Should not panic
//         let _result = engine
//             .handle_query(query.to_string(), 1609459200 as f64)
//             .await;
//     }
// }
