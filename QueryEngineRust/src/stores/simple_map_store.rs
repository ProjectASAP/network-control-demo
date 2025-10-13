use crate::data_model::{AggregateCore, KeyByLabelValues, PrecomputedOutput, StreamingConfig};
use crate::stores::{Store, StoreResult};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tracing::{debug, error, info};

type TimestampRange = (u64, u64); // (start_timestamp, end_timestamp)
type StoreKey = (String, u64); // (metric, aggregation_id)
type StoreValue = Vec<(Option<KeyByLabelValues>, Box<dyn AggregateCore>)>;

/// In-memory storage implementation using single mutex (like Python version)
pub struct SimpleMapStore {
    // Single global mutex protecting all data structures
    lock: Mutex<StoreData>,

    // Store the streaming configuration
    streaming_config: Arc<StreamingConfig>,
}

struct StoreData {
    // Main storage: (metric, aggregation_id) -> (start_time, end_time) -> [(key, precompute)]
    store: HashMap<StoreKey, HashMap<TimestampRange, StoreValue>>,

    // Track metrics that have been created
    metrics: std::collections::HashSet<String>,

    // Count items inserted per metric for logging
    items_inserted: HashMap<String, u64>,

    // Track earliest timestamp per aggregation ID
    earliest_timestamp_per_aggregation_id: HashMap<u64, u64>,
}

impl SimpleMapStore {
    pub fn new(streaming_config: Arc<StreamingConfig>) -> Self {
        Self {
            lock: Mutex::new(StoreData {
                store: HashMap::new(),
                metrics: std::collections::HashSet::new(),
                items_inserted: HashMap::new(),
                earliest_timestamp_per_aggregation_id: HashMap::new(),
            }),
            streaming_config,
        }
    }

    fn create_table(&self, data: &mut StoreData, metric: &str) {
        // In the in-memory implementation, "creating a table" just means
        // marking the metric as known
        data.metrics.insert(metric.to_string());
    }

    fn cleanup_old_aggregates(
        &self,
        data: &mut StoreData,
        metric: &str,
        aggregation_id: u64,
        num_aggregates_to_retain: Option<u64>,
    ) {
        // Return early if no retention limit configured
        let configured_limit = match num_aggregates_to_retain {
            Some(limit) => limit as usize,
            None => return,
        };

        let retention_limit = configured_limit * 4;
        let store_key = (metric.to_string(), aggregation_id);

        // Get the time map for this store key
        if let Some(time_map) = data.store.get_mut(&store_key) {
            if time_map.len() <= retention_limit {
                return; // Nothing to clean up
            }

            // Collect all timestamp ranges and sort by start timestamp (oldest first)
            let mut timestamp_windows: Vec<TimestampRange> = time_map.keys().copied().collect();
            timestamp_windows.sort_by_key(|&(start, _end)| start);

            // Calculate which ones to remove (oldest first)
            let num_to_remove = timestamp_windows.len() - retention_limit;
            let windows_to_remove: Vec<TimestampRange> =
                timestamp_windows.into_iter().take(num_to_remove).collect();

            // Remove old windows
            for window in windows_to_remove {
                if time_map.remove(&window).is_some() {
                    tracing::debug!(
                        "Removed old aggregate for {} aggregation_id {} window {}-{} (retention limit: {}, configured: {})",
                        metric,
                        aggregation_id,
                        window.0,
                        window.1,
                        retention_limit,
                        configured_limit
                    );
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Store for SimpleMapStore {
    fn insert_precomputed_output(
        &self,
        output: PrecomputedOutput,
        precompute: Box<dyn AggregateCore>,
    ) -> StoreResult<()> {
        self.insert_precomputed_output_batch(vec![(output, precompute)])
    }

    fn insert_precomputed_output_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> StoreResult<()> {
        let batch_insert_start_time = Instant::now();
        let batch_size = outputs.len();

        // Single lock for entire batch (like Python version)
        let mut data = self.lock.lock().unwrap();

        for (output, precompute) in outputs {
            let aggregation_config = self
                .streaming_config
                .get_aggregation_config(output.aggregation_id);

            if aggregation_config.is_none() {
                error!(
                    "Aggregation config not found for aggregation_id {}. Skipping insert.",
                    output.aggregation_id
                );
                continue;
            }
            let aggregation_config = aggregation_config.unwrap();

            let metric = aggregation_config.metric.clone();
            let aggregation_id = output.aggregation_id;

            // Create table if it doesn't exist
            if !data.metrics.contains(&metric) {
                self.create_table(&mut data, &metric);
            }

            // Update earliest timestamp tracking
            if let Some(current_earliest) = data
                .earliest_timestamp_per_aggregation_id
                .get_mut(&aggregation_id)
            {
                if output.start_timestamp < *current_earliest {
                    *current_earliest = output.start_timestamp;
                }
            } else {
                data.earliest_timestamp_per_aggregation_id
                    .insert(aggregation_id, output.start_timestamp);
            }

            let store_key = (metric.clone(), aggregation_id);
            let timestamp_range = (output.start_timestamp, output.end_timestamp);

            // Get or create the time-based map for this metric/aggregation
            let time_map = data.store.entry(store_key).or_default();

            // Get or create the value vector for this timestamp range
            let store_value = time_map.entry(timestamp_range).or_default();

            // Add the new entry with the real precompute data
            store_value.push((output.key, precompute));

            // Apply retention policy if configured (but exclude DeltaSetAggregator)
            if aggregation_config.aggregation_type != "DeltaSetAggregator" {
                self.cleanup_old_aggregates(
                    &mut data,
                    &metric,
                    aggregation_id,
                    aggregation_config.num_aggregates_to_retain,
                );
            }

            // Update insertion count
            let current_count = data.items_inserted.entry(metric.clone()).or_insert(0);
            *current_count += 1;

            if (*current_count).is_multiple_of(1000) {
                debug!("Inserted {} items into {}", current_count, metric);
            }
        }

        let batch_insert_duration = batch_insert_start_time.elapsed();
        debug!(
            "Batch insert of {} items took: {:.2}ms",
            batch_size,
            batch_insert_duration.as_secs_f64() * 1000.0
        );
        Ok(())
    }

    fn query_precomputed_output(
        &self,
        metric: &str,
        aggregation_id: u64,
        start: u64,
        end: u64,
    ) -> Result<
        HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let query_start_time = Instant::now();
        let store_key = (metric.to_string(), aggregation_id);

        // Single lock for entire query (like Python version)
        let data = self.lock.lock().unwrap();

        let time_map = match data.store.get(&store_key) {
            Some(map) => map,
            None => {
                info!("Metric {} not found in store", metric);
                return Ok(HashMap::new());
            }
        };

        let mut results: HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>> =
            HashMap::new();
        let mut total_entries = 0;

        // Find all timestamp ranges that overlap with our query range
        let range_scan_start_time = Instant::now();
        for (timestamp_range, store_values) in time_map.iter() {
            let (range_start, range_end) = *timestamp_range;

            // Check if range is fully contained in [start, end]
            if start <= range_start && end >= range_end {
                for (key_opt, precompute) in store_values.iter() {
                    results
                        .entry(key_opt.clone())
                        .or_default()
                        .push(precompute.clone_boxed_core());

                    total_entries += 1;
                }
            }
        }

        let range_scan_duration = range_scan_start_time.elapsed();
        debug!(
            "Range scanning took: {:.2}ms",
            range_scan_duration.as_secs_f64() * 1000.0
        );

        let query_duration = query_start_time.elapsed();
        debug!(
            "Total query took: {:.2}ms",
            query_duration.as_secs_f64() * 1000.0
        );

        debug!(
            "Found {} entries for query on {} (aggregation_id: {}, start: {}, end: {})",
            total_entries, metric, aggregation_id, start, end
        );
        debug!("Found {} unique keys", results.len());

        Ok(results)
    }

    fn get_earliest_timestamp_per_aggregation_id(
        &self,
    ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
        let data = self.lock.lock().unwrap();
        Ok(data.earliest_timestamp_per_aggregation_id.clone())
    }

    fn close(&self) -> StoreResult<()> {
        // For in-memory store, no cleanup needed
        info!("SimpleMapStore closed");
        Ok(())
    }
}
