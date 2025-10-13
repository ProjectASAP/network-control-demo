// #[cfg(test)]
// use crate::data_model::{AggregationConfig, KeyByLabelValues, PrecomputedOutput, StreamingConfig};
// use crate::precompute_operators::SumAccumulator;
// use crate::stores::{SimpleMapStore, Store};
// use std::collections::HashMap;
// use std::sync::Arc;

// use promql_utilities::data_model::KeyByLabelNames;

// #[tokio::test]
// #[test]
// fn test_end_to_end_precompute_data_flow() {
//     // Create test data matching what would come from Kafka
//     let labels = KeyByLabelNames::from_names(vec!["instance".to_string()]);
//     let empty_labels = KeyByLabelNames::new(vec![]);
//     let config = AggregationConfig::new(
//         1,
//         "sum".to_string(),
//         "".to_string(),
//         HashMap::new(),
//         labels,
//         empty_labels.clone(),
//         empty_labels,
//         "".to_string(),
//         10,
//         "".to_string(),
//         "cpu_usage".to_string(),
//         Some(10),
//     );
//     let streaming_config = Arc::new(StreamingConfig::new(
//         vec![(1, config.clone())].into_iter().collect(),
//     ));

//     let key = Some(KeyByLabelValues::new_with_labels(vec![
//         "instance".to_string()
//     ]));

//     let aggregation_id = 1;

//     let precomputed_output = PrecomputedOutput::new(
//         1000, // start_timestamp
//         2000, // end_timestamp
//         key.clone(),
//         aggregation_id,
//     );

//     // Create real accumulator with actual data (not placeholder)
//     let real_accumulator = SumAccumulator::with_sum(42.5);

//     // Test JSON round-trip (simulating Kafka message)
//     let mut json_with_precompute =
//         precomputed_output.serialize_to_json_with_precompute(&real_accumulator);

//     // add aggregation ID to JSON
//     json_with_precompute["aggregation_id"] = 1.into();

//     // add missing label fields required by deserializer
//     json_with_precompute["config"]["groupingLabels"] = serde_json::json!(["instance"]);
//     json_with_precompute["config"]["aggregatedLabels"] = serde_json::json!([]);
//     json_with_precompute["config"]["rollupLabels"] = serde_json::json!([]);

//     // Deserialize using factory method (simulating Kafka consumer)
//     let (deserialized_output, deserialized_accumulator) =
//         PrecomputedOutput::deserialize_from_json_with_precompute(&json_with_precompute).unwrap();

//     // Verify factory extracted real data, not placeholder
//     assert_eq!(deserialized_accumulator.serialize_to_json()["sum"], 42.5);

//     // Test store integration with real data
//     let store = Arc::new(SimpleMapStore::new(streaming_config.clone()));

//     // Insert using batch method with real accumulator data
//     let batch = vec![(deserialized_output.clone(), deserialized_accumulator)];
//     store.insert_precomputed_output_batch(batch).unwrap();

//     // Query and verify we get real accumulated data back
//     let results = store
//         .query_precomputed_output("cpu_usage", 1, 1000, 2000)
//         // .await
//         .unwrap();

//     assert_eq!(results.len(), 1);
//     let stored_accumulators = &results[&key];
//     assert_eq!(stored_accumulators.len(), 1);

//     // Verify the stored accumulator contains real data, not placeholder
//     let stored_json = stored_accumulators[0].serialize_to_json();
//     assert_eq!(stored_json["sum"], 42.5);

//     println!(
//         "✅ End-to-end test passed: Real precompute data flows from JSON → Factory → Store → Query"
//     );
// }
