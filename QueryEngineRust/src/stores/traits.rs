use crate::data_model::{AggregateCore, KeyByLabelValues, PrecomputedOutput};
use std::collections::HashMap;

/// Trait defining the interface for precomputed data storage backends
// #[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Insert a single precomputed output
    fn insert_precomputed_output(
        &self,
        output: PrecomputedOutput,
        precompute: Box<dyn AggregateCore>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Insert multiple precomputed outputs in a batch (for Kafka consumer)
    fn insert_precomputed_output_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Query precomputed outputs for a given metric and time range
    // async fn query_precomputed_output(
    #[allow(clippy::type_complexity)]
    fn query_precomputed_output(
        &self,
        metric: &str,
        aggregation_id: u64,
        start: u64,
        end: u64,
    ) -> Result<
        HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>>,
        Box<dyn std::error::Error + Send + Sync>,
    >;

    /// Get earliest timestamp for each aggregation ID (for monitoring)
    fn get_earliest_timestamp_per_aggregation_id(
        &self,
    ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>>;

    /// Close the store and clean up resources
    fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Result type for store operations
pub type StoreResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
