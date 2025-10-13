use promql_utilities::KeyByLabelNames;
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::engines::QueryResult;

// /// Prometheus-compatible response structure
// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// pub struct PrometheusResponse {
//     pub status: String,
//     pub data: PrometheusData,
// }

// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// pub struct PrometheusData {
//     #[serde(rename = "resultType")]
//     pub result_type: String,
//     pub result: Vec<PrometheusResult>,
// }

// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// pub struct PrometheusResult {
//     pub metric: HashMap<String, String>,
//     pub value: (f64, String), // [timestamp, value]
// }

// /// Format results as Prometheus-compatible HTTP response
// pub fn format_results_as_http_response(
//     results: &[PrecomputedOutput],
//     timestamp: f64,
// ) -> Result<PrometheusResponse> {
//     let mut prometheus_results = Vec::new();

//     for result in results {
//         if let Some(ref key) = result.key {
//             let prometheus_result = PrometheusResult {
//                 metric: key.labels.clone(),
//                 value: (timestamp, "0.0".to_string()), // TODO: Extract actual value from accumulator
//             };
//             prometheus_results.push(prometheus_result);
//         }
//     }

//     let response = PrometheusResponse {
//         status: "success".to_string(),
//         data: PrometheusData {
//             result_type: "vector".to_string(),
//             result: prometheus_results,
//         },
//     };

//     Ok(response)
// }

// /// Format error response in Prometheus format
// pub fn format_error_response(error_msg: &str) -> PrometheusResponse {
//     tracing::error!("Error: {}", error_msg);
//     PrometheusResponse {
//         status: "error".to_string(),
//         data: PrometheusData {
//             result_type: "vector".to_string(),
//             result: vec![],
//         },
//     }
// }

// /// Parse query parameters from HTTP request
// pub fn parse_query_params(query_string: &str) -> HashMap<String, Vec<String>> {
//     let mut params = HashMap::new();

//     for pair in query_string.split('&') {
//         if let Some((key, value)) = pair.split_once('=') {
//             let decoded_key = urlencoding::decode(key).unwrap_or_default().into_owned();
//             let decoded_value = urlencoding::decode(value).unwrap_or_default().into_owned();

//             params
//                 .entry(decoded_key)
//                 .or_insert_with(Vec::new)
//                 .push(decoded_value);
//         }
//     }

//     params
// }

// /// Format results as Prometheus-compatible HTTP response
// pub fn format_results_as_http_response(
//     result_type: QueryResultType,
//     results: &HashMap<String, f64>, // Simplified - key as string, value as f64
//     grouping_labels: &KeyByLabelNames,
//     time: u64,
// ) -> Value {
//     match result_type {
//         QueryResultType::InstantVector => {
//             let mut result = Vec::new();
//             for (k, v) in results.iter() {
//                 // Parse the key string back to values - this is a simplification
//                 // In the Python version, k is a Key object with values attribute
//                 let key_values: Vec<&str> = k.split(',').collect();

//                 let metric: HashMap<String, String> = grouping_labels
//                     .keys
//                     .iter()
//                     .zip(key_values.iter())
//                     .map(|(label, value)| (label.clone(), value.to_string()))
//                     .collect();

//                 result.push(json!({
//                     "metric": metric,
//                     "value": [time as f64 / 1000.0, v.to_string()]
//                 }));
//             }

//             json!({
//                 "status": "success",
//                 "data": {
//                     "resultType": "vector",
//                     "result": result
//                 }
//             })
//         }
//     }
// }

/// Convert QueryResult to Prometheus-compatible format
pub fn convert_query_result_to_prometheus(
    result: &QueryResult,
    query_output_labels: &KeyByLabelNames,
) -> Value {
    match result {
        QueryResult::Vector(instant_vector) => {
            let mut prometheus_results = Vec::new();
            let timestamp = instant_vector.timestamp as f64 / 1000.0;

            for element in &instant_vector.values {
                // zip over query_output_labels.keys and element.labels.labels and collect into metric_map
                let mut metric_map = HashMap::new();
                for (key, label) in query_output_labels
                    .labels
                    .iter()
                    .zip(element.labels.labels.iter())
                {
                    metric_map.insert(key, label);
                }

                let prometheus_result = json!({
                    "metric": metric_map,
                    "value": [timestamp, element.value.to_string()]
                });
                prometheus_results.push(prometheus_result);
            }
            json!({
                "resultType": "vector",
                "result": prometheus_results
            })
        }
    }
}
