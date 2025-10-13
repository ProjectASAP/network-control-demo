use crate::data_model::KeyByLabelValues;
use serde::{Deserialize, Serialize};

use promql_utilities::query_logics::enums::QueryResultType;

/// Represents the result of a PromQL query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResult {
    Vector(InstantVector),
}

impl QueryResult {
    pub fn result_type(&self) -> QueryResultType {
        match self {
            QueryResult::Vector(_) => QueryResultType::InstantVector,
        }
    }

    pub fn vector(values: Vec<InstantVectorElement>, timestamp: u64) -> Self {
        QueryResult::Vector(InstantVector { values, timestamp })
    }
}

/// Instant vector - a set of time series containing a single sample for each time series, all sharing the same timestamp
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstantVector {
    pub values: Vec<InstantVectorElement>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstantVectorElement {
    pub labels: KeyByLabelValues,
    pub value: f64,
}

impl InstantVectorElement {
    pub fn new(labels: KeyByLabelValues, value: f64) -> Self {
        Self { labels, value }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    fn create_test_labels() -> KeyByLabelValues {
        KeyByLabelValues::new_with_labels(vec![
            "localhost:9090".to_string(),
            "prometheus".to_string(),
        ])
    }

    #[test]
    fn test_instant_vector_creation() {
        let labels = create_test_labels();
        let element = InstantVectorElement::new(labels.clone(), 42.0);
        let vector = QueryResult::vector(vec![element], 1000);

        assert_eq!(vector.result_type(), QueryResultType::InstantVector);

        let QueryResult::Vector(iv) = vector;
        assert_eq!(iv.values.len(), 1);
        assert_eq!(iv.values[0].value, 42.0);
        // assert_eq!(iv.values[0].timestamp, 1000);
        assert_eq!(iv.values[0].labels, labels);
    }

    #[test]
    fn test_serialization() {
        let labels = create_test_labels();
        let element = InstantVectorElement::new(labels, 42.0);
        let vector = QueryResult::vector(vec![element], 1000);

        let json = serde_json::to_string(&vector).unwrap();
        let deserialized: QueryResult = serde_json::from_str(&json).unwrap();

        assert_eq!(vector.result_type(), deserialized.result_type());
    }
}
