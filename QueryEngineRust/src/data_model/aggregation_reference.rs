use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationReference {
    pub aggregation_id: u64,
    pub num_aggregates_to_retain: Option<u64>,
}

impl AggregationReference {
    pub fn new(aggregation_id: u64, num_aggregates_to_retain: Option<u64>) -> Self {
        Self {
            aggregation_id,
            num_aggregates_to_retain,
        }
    }
}
