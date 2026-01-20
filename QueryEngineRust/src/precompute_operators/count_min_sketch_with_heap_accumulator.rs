use crate::data_model::{
    AggregateCore, KeyByLabelValues, MergeableAccumulator, MultipleSubpopulationAggregate,
    SerializableToSink,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use xxhash_rust::xxh32::xxh32;

use promql_utilities::query_logics::enums::Statistic;

/// TODO: Modify this file to match the countminsketchwithheap UDF jinja template in ArroyoSketch.
/// Count-Min Sketch with Heap for top-k tracking
/// Combines probabilistic frequency counting with efficient top-k maintenance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountMinSketchWithHeapAccumulator {
    pub sketch: Vec<Vec<f64>>,
    pub row_num: usize,
    pub col_num: usize,
    pub topk_heap: Vec<HeapItem>,
    pub heap_size: usize,
}

/// Item in the heap representing a key-value pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeapItem {
    pub key: String,
    pub value: f64,
}

/// Helper struct matching Arroyo's nested serialization format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CountMinSketch {
    sketch: Vec<Vec<f64>>,
    row_num: usize,
    col_num: usize,
}

/// Helper struct matching Arroyo's serialization format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CountMinSketchWithHeapSerialized {
    sketch: CountMinSketch,
    topk_heap: Vec<HeapItem>,
    heap_size: usize,
}

impl CountMinSketchWithHeapAccumulator {
    pub fn new(row_num: usize, col_num: usize, heap_size: usize) -> Self {
        let sketch = vec![vec![0.0; col_num]; row_num];
        Self {
            sketch,
            row_num,
            col_num,
            topk_heap: Vec::new(),
            heap_size,
        }
    }

    pub fn query_key(&self, key: &KeyByLabelValues) -> f64 {
        // Match Python logic: ";".join(key.serialize_to_json())
        let key_string = key.labels.join(";");
        let key_bytes = key_string.as_bytes();

        let mut min_value = f64::MAX;

        // Query each row and take the minimum
        for i in 0..self.row_num {
            let hash_value = xxh32(key_bytes, i as u32);
            let col_index = (hash_value as usize) % self.col_num;
            min_value = min_value.min(self.sketch[i][col_index]);
        }

        min_value
    }

    /// This function seems will never be used anymore. Keep it for possible future use.
    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let row_num = data["row_num"]
            .as_f64()
            .ok_or("Missing or invalid 'row_num' field")? as usize;
        let col_num = data["col_num"]
            .as_f64()
            .ok_or("Missing or invalid 'col_num' field")? as usize;
        let heap_size = data["heap_size"]
            .as_f64()
            .ok_or("Missing or invalid 'heap_size' field")? as usize;

        let sketch_data = data["sketch"]
            .as_array()
            .ok_or("Missing or invalid 'sketch' field")?;

        let mut sketch = Vec::new();
        for row in sketch_data {
            let row_array = row.as_array().ok_or("Invalid row in sketch data")?;
            let mut sketch_row = Vec::new();
            for cell in row_array {
                let value = cell.as_f64().ok_or("Invalid cell value in sketch data")?;
                sketch_row.push(value);
            }
            sketch.push(sketch_row);
        }

        let topk_heap_data = data["topk_heap"]
            .as_array()
            .ok_or("Missing or invalid 'topk_heap' field")?;

        let mut topk_heap = Vec::new();
        for item in topk_heap_data {
            let key = item["key"]
                .as_str()
                .ok_or("Missing or invalid 'key' in heap item")?
                .to_string();
            let value = item["value"]
                .as_f64()
                .ok_or("Missing or invalid 'value' in heap item")?;
            topk_heap.push(HeapItem { key, value });
        }

        Ok(Self {
            sketch,
            row_num,
            col_num,
            topk_heap,
            heap_size,
        })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Deserialize the nested Arroyo format
        let serialized: CountMinSketchWithHeapSerialized =
            rmp_serde::from_slice(buffer).map_err(|e| {
                format!("Failed to deserialize CountMinSketchWithHeap from MessagePack: {e}")
            })?;

        // Sort the topk_heap by value from largest to smallest
        let mut sorted_topk_heap = serialized.topk_heap;
        // We must sort here since the vectorized heap does not guarantee order.
        sorted_topk_heap.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());

        // Convert to flat structure
        Ok(Self {
            sketch: serialized.sketch.sketch,
            row_num: serialized.sketch.row_num,
            col_num: serialized.sketch.col_num,
            topk_heap: sorted_topk_heap,
            heap_size: serialized.heap_size,
        })
    }

    pub fn deserialize_from_bytes(_buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        // For Flink: we need to parse the binary format
        // This is a placeholder - implement based on Flink's serialization format
        // raise unimplemented error here
        Err("deserialize_from_bytes for CountMinSketchWithHeapAccumulator not implemented".into())
        // rmp_serde::from_slice(buffer)
        //     .map_err(|e| format!("Failed to deserialize CountMinSketchWithHeap: {e}").into())
    }

    /// Get all keys from the top-k heap
    pub fn get_topk_keys(&self) -> Vec<KeyByLabelValues> {
        self.topk_heap
            .iter()
            .map(|item| {
                // Parse the semicolon-separated key string back to KeyByLabelValues
                let labels: Vec<String> = item.key.split(';').map(|s| s.to_string()).collect();
                KeyByLabelValues { labels }
            })
            .collect()
    }
}

impl SerializableToSink for CountMinSketchWithHeapAccumulator {
    fn serialize_to_json(&self) -> Value {
        let heap_items: Vec<Value> = self
            .topk_heap
            .iter()
            .map(|item| {
                serde_json::json!({
                    "key": item.key,
                    "value": item.value
                })
            })
            .collect();

        serde_json::json!({
            "row_num": self.row_num,
            "col_num": self.col_num,
            "heap_size": self.heap_size,
            "sketch": self.sketch,
            "topk_heap": heap_items
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        // Match Arroyo UDF: serialize with nested MessagePack format
        let serialized = CountMinSketchWithHeapSerialized {
            sketch: CountMinSketch {
                sketch: self.sketch.clone(),
                row_num: self.row_num,
                col_num: self.col_num,
            },
            topk_heap: self.topk_heap.clone(),
            heap_size: self.heap_size,
        };

        let mut buf = Vec::new();
        serialized
            .serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .unwrap();
        buf
    }
}

impl AggregateCore for CountMinSketchWithHeapAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "CountMinSketchWithHeapAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        // Check if other is also a CountMinSketchWithHeapAccumulator
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge CountMinSketchWithHeapAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        // Downcast to CountMinSketchWithHeapAccumulator
        let other_cms = other
            .as_any()
            .downcast_ref::<CountMinSketchWithHeapAccumulator>()
            .ok_or("Failed to downcast to CountMinSketchWithHeapAccumulator")?;

        // Use the existing merge_accumulators method
        let merged = Self::merge_accumulators(vec![self.clone(), other_cms.clone()])?;

        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> &'static str {
        "CountMinSketchWithHeapAccumulator"
    }

    /// Should the sketch contain the k value of topk or take them as a parameter?
    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        // Return the top-k keys from the heap
        Some(self.get_topk_keys())
    }
}

impl MultipleSubpopulationAggregate for CountMinSketchWithHeapAccumulator {
    fn query(
        &self,
        _statistic: Statistic,
        key: &KeyByLabelValues,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.query_key(key))
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<CountMinSketchWithHeapAccumulator> for CountMinSketchWithHeapAccumulator {
    fn merge_accumulators(
        accumulators: Vec<CountMinSketchWithHeapAccumulator>,
    ) -> Result<CountMinSketchWithHeapAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        if accumulators.len() == 1 {
            return Ok(accumulators.into_iter().next().unwrap());
        }

        // Check that all accumulators have the same dimensions
        let row_num = accumulators[0].row_num;
        let col_num = accumulators[0].col_num;

        for acc in &accumulators {
            if acc.row_num != row_num || acc.col_num != col_num {
                return Err(
                    "Cannot merge CountMinSketchWithHeap accumulators with different dimensions"
                        .into(),
                );
            }
        }

        // Merge the Count-Min Sketch tables element-wise
        let mut merged_sketch = vec![vec![0.0; col_num]; row_num];
        for acc in &accumulators {
            for (i, row) in merged_sketch.iter_mut().enumerate() {
                for (j, cell) in row.iter_mut().enumerate() {
                    *cell += acc.sketch[i][j];
                }
            }
        }

        // Find the minimum heap size across all accumulators
        let min_heap_size = accumulators
            .iter()
            .map(|acc| acc.heap_size)
            .min()
            .unwrap_or(0);

        // Enumerate all unique keys from all heaps
        let mut all_keys: HashSet<String> = HashSet::new();
        for acc in &accumulators {
            for item in &acc.topk_heap {
                all_keys.insert(item.key.clone());
            }
        }

        // Create a temporary merged accumulator to query frequencies
        let temp_merged = CountMinSketchWithHeapAccumulator {
            sketch: merged_sketch.clone(),
            row_num,
            col_num,
            topk_heap: Vec::new(),
            heap_size: min_heap_size,
        };

        // Query the merged CMS for each key and build heap items
        let mut heap_items: Vec<HeapItem> = all_keys
            .into_iter()
            .map(|key_str| {
                // Parse the key string to KeyByLabelValues for querying
                let labels: Vec<String> = key_str.split(';').map(|s| s.to_string()).collect();
                let key = KeyByLabelValues { labels };

                // Query the merged sketch for this key's frequency
                let frequency = temp_merged.query_key(&key);

                HeapItem {
                    key: key_str,
                    value: frequency,
                }
            })
            .collect();

        // Sort by frequency (descending) and take top min_heap_size items
        // Sorting may not be a problem since it takes O(nlogn) time,
        // which can be amortized to each element
        heap_items.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());
        heap_items.truncate(min_heap_size);

        // Return the final merged accumulator
        Ok(CountMinSketchWithHeapAccumulator {
            sketch: merged_sketch,
            row_num,
            col_num,
            topk_heap: heap_items,
            heap_size: min_heap_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_min_sketch_with_heap_creation() {
        let cms = CountMinSketchWithHeapAccumulator::new(4, 1000, 20);
        assert_eq!(cms.row_num, 4);
        assert_eq!(cms.col_num, 1000);
        assert_eq!(cms.heap_size, 20);
        assert_eq!(cms.sketch.len(), 4);
        assert_eq!(cms.sketch[0].len(), 1000);
        assert_eq!(cms.topk_heap.len(), 0);

        // Check all values are initialized to 0
        for row in &cms.sketch {
            for &value in row {
                assert_eq!(value, 0.0);
            }
        }
    }

    #[test]
    fn test_count_min_sketch_with_heap_query() {
        let cms = CountMinSketchWithHeapAccumulator::new(2, 10, 5);
        let key = KeyByLabelValues::new();

        // Test key-based query implementation
        assert_eq!(cms.query_key(&key), 0.0);

        // Test through MultipleSubpopulationAggregate trait
        let multi_trait: &dyn MultipleSubpopulationAggregate = &cms;
        assert_eq!(multi_trait.query(Statistic::Sum, &key, None).unwrap(), 0.0);
    }

    #[test]
    fn test_count_min_sketch_with_heap_merge() {
        // Test merging two CountMinSketchWithHeap accumulators
        let mut cms1 = CountMinSketchWithHeapAccumulator::new(2, 10, 5);
        let mut cms2 = CountMinSketchWithHeapAccumulator::new(2, 10, 3);

        // Set some sketch values
        cms1.sketch[0][0] = 10.0;
        cms1.sketch[1][1] = 20.0;
        cms2.sketch[0][0] = 5.0;
        cms2.sketch[1][1] = 15.0;

        // Add some heap items
        cms1.topk_heap.push(HeapItem {
            key: "key1".to_string(),
            value: 100.0,
        });
        cms1.topk_heap.push(HeapItem {
            key: "key2".to_string(),
            value: 50.0,
        });
        cms2.topk_heap.push(HeapItem {
            key: "key3".to_string(),
            value: 75.0,
        });
        cms2.topk_heap.push(HeapItem {
            key: "key1".to_string(), // Duplicate key
            value: 80.0,
        });

        let result = CountMinSketchWithHeapAccumulator::merge_accumulators(vec![cms1, cms2]);
        assert!(result.is_ok());

        let merged = result.unwrap();

        // Check sketch was merged (element-wise addition)
        assert_eq!(merged.sketch[0][0], 15.0); // 10 + 5
        assert_eq!(merged.sketch[1][1], 35.0); // 20 + 15

        // Check heap size is the minimum
        assert_eq!(merged.heap_size, 3); // min(5, 3)

        // Check that the heap contains at most 3 items
        assert!(merged.topk_heap.len() <= 3);

        // Check that all unique keys are considered
        let keys: Vec<String> = merged
            .topk_heap
            .iter()
            .map(|item| item.key.clone())
            .collect();
        // We had 3 unique keys: key1, key2, key3
        // The heap should contain the top 3 by frequency from the merged sketch
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_count_min_sketch_with_heap_merge_single() {
        // Merging a single accumulator should return itself
        let cms = CountMinSketchWithHeapAccumulator::new(2, 3, 5);

        let result = CountMinSketchWithHeapAccumulator::merge_accumulators(vec![cms.clone()]);
        assert!(result.is_ok());
        let merged = result.unwrap();
        assert_eq!(merged.row_num, cms.row_num);
        assert_eq!(merged.col_num, cms.col_num);
        assert_eq!(merged.heap_size, cms.heap_size);
    }

    #[test]
    fn test_count_min_sketch_with_heap_merge_dimension_mismatch() {
        // Test that merging accumulators with different dimensions fails
        let cms1 = CountMinSketchWithHeapAccumulator::new(2, 10, 5);
        let cms2 = CountMinSketchWithHeapAccumulator::new(3, 10, 5); // Different row_num

        let result = CountMinSketchWithHeapAccumulator::merge_accumulators(vec![cms1, cms2]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("different dimensions"));
    }

    #[test]
    fn test_count_min_sketch_with_heap_serialization() {
        let mut cms = CountMinSketchWithHeapAccumulator::new(2, 3, 5);
        cms.sketch[0][1] = 42.0;
        cms.sketch[1][2] = 100.0;
        cms.topk_heap.push(HeapItem {
            key: "test_key".to_string(),
            value: 99.0,
        });

        // Test binary serialization
        let bytes = cms.serialize_to_bytes();
        let deserialized_bytes =
            CountMinSketchWithHeapAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();

        assert_eq!(deserialized_bytes.row_num, 2);
        assert_eq!(deserialized_bytes.col_num, 3);
        assert_eq!(deserialized_bytes.heap_size, 5);
        assert_eq!(deserialized_bytes.sketch[0][1], 42.0);
        assert_eq!(deserialized_bytes.sketch[1][2], 100.0);
        assert_eq!(deserialized_bytes.topk_heap.len(), 1);
        assert_eq!(deserialized_bytes.topk_heap[0].key, "test_key");
        assert_eq!(deserialized_bytes.topk_heap[0].value, 99.0);
    }

    #[test]
    fn test_count_min_sketch_with_heap_as_aggregate_core() {
        let cms = CountMinSketchWithHeapAccumulator::new(2, 3, 5);
        assert_eq!(cms.type_name(), "CountMinSketchWithHeapAccumulator");
    }

    #[test]
    fn test_get_topk_keys() {
        let mut cms = CountMinSketchWithHeapAccumulator::new(2, 3, 5);
        cms.topk_heap.push(HeapItem {
            key: "label1;label2".to_string(),
            value: 100.0,
        });
        cms.topk_heap.push(HeapItem {
            key: "label3;label4".to_string(),
            value: 50.0,
        });

        let keys = cms.get_topk_keys();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].labels, vec!["label1", "label2"]);
        assert_eq!(keys[1].labels, vec!["label3", "label4"]);
    }

    #[test]
    fn test_multiple_subpopulation_aggregate() {
        let cms = CountMinSketchWithHeapAccumulator::new(3, 50, 10);
        let key = KeyByLabelValues::new();

        let multi_trait: &dyn MultipleSubpopulationAggregate = &cms;
        let result = multi_trait.query(Statistic::Sum, &key, None).unwrap();
        assert_eq!(result, 0.0);

        // get_keys should return the top-k keys
        let keys = multi_trait.get_keys();
        assert!(keys.is_some());
        assert_eq!(keys.unwrap().len(), 0); // Empty initially
    }
}
