use crate::data_model::{
    AggregateCore, KeyByLabelValues, MergeableAccumulator, MultipleSubpopulationAggregate,
    SerializableToSink,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use xxhash_rust::xxh32::xxh32;

use promql_utilities::query_logics::enums::Statistic;

/// Count-Min Sketch probabilistic data structure for frequency counting
/// Provides approximate frequency counts with error bounds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountMinSketchAccumulator {
    pub sketch: Vec<Vec<f64>>,
    pub row_num: usize,
    pub col_num: usize,
}

impl CountMinSketchAccumulator {
    pub fn new(row_num: usize, col_num: usize) -> Self {
        let sketch = vec![vec![0.0; col_num]; row_num];
        Self {
            sketch,
            row_num,
            col_num,
        }
    }

    // Marked as _update, and removed pub, since this is only called internally
    fn _update(&mut self, key: &KeyByLabelValues, value: f64) {
        // Match Python logic: ";".join(key.serialize_to_json())
        let key_json = key.serialize_to_json();
        let key_values: Vec<String> = if let Some(obj) = key_json.as_object() {
            obj.values()
                .map(|v| v.as_str().unwrap_or("").to_string())
                .collect()
        } else {
            vec!["".to_string()]
        };
        let key_str = key_values.join(";");
        let key_bytes = key_str.as_bytes();

        // Update each row using different hash functions
        for i in 0..self.row_num {
            let hash_value = xxh32(key_bytes, i as u32);
            let col_index = (hash_value as usize) % self.col_num;
            self.sketch[i][col_index] += value;
        }
    }

    pub fn query_key(&self, key: &KeyByLabelValues) -> f64 {
        // Match Python logic: ";".join(key.serialize_to_json())
        let key_string = key.labels.join(";");
        // let key_json = key.serialize_to_json();
        // let key_values: Vec<String> = if let Some(obj) = key_json.as_object() {
        //     obj.values()
        //         .map(|v| v.as_str().unwrap_or("").to_string())
        //         .collect()
        // } else {
        //     // TODO: why is this there?
        //     vec!["".to_string()]
        // };
        // let key_str = key_values.join(";");
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

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let row_num = data["row_num"]
            .as_f64()
            .ok_or("Missing or invalid 'row_num' field")? as usize;
        let col_num = data["col_num"]
            .as_f64()
            .ok_or("Missing or invalid 'col_num' field")? as usize;

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

        Ok(Self {
            sketch,
            row_num,
            col_num,
        })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let deserialized_struct = rmp_serde::from_slice(buffer)
            .map_err(|e| format!("Failed to deserialize CountMinSketch from MessagePack: {e}"))?;

        Ok(deserialized_struct)
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        if buffer.len() < 8 {
            return Err("Buffer too short for row_num and col_num".into());
        }

        // TODO: this logic will need to be checked for i32 -> f64
        // Github Issue #11

        let row_num = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        let col_num = u32::from_le_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]) as usize;

        let expected_size = 8 + (row_num * col_num * 4);
        if buffer.len() < expected_size {
            return Err("Buffer too short for sketch data".into());
        }

        let mut sketch = Vec::new();
        let mut offset = 8;

        for _ in 0..row_num {
            let mut row = Vec::new();
            for _ in 0..col_num {
                let value = f64::from_le_bytes([
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                    buffer[offset + 4],
                    buffer[offset + 5],
                    buffer[offset + 6],
                    buffer[offset + 7],
                ]);
                row.push(value);
                offset += 8;
            }
            sketch.push(row);
        }

        Ok(Self {
            row_num,
            col_num,
            sketch,
        })
    }

    /// Merge multiple accumulators efficiently without cloning all of them
    /// This is a batch merge operation that creates one sketch and adds all others element-wise
    ///
    /// # Arguments
    /// * `accumulators` - Slice of boxed AggregateCore trait objects to merge
    ///
    /// # Returns
    /// * `Result<Self, Box<dyn std::error::Error + Send + Sync>>` - Merged accumulator or error
    ///
    /// # Performance
    /// This method performs 1 clone (of the first accumulator), compared to the
    /// sequential merge approach which would perform 3N clones for N accumulators.
    pub fn merge_multiple(
        accumulators: &[Box<dyn crate::data_model::AggregateCore>],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        // Downcast and validate all accumulators first
        let mut cms_accumulators = Vec::with_capacity(accumulators.len());
        for acc in accumulators {
            if acc.get_accumulator_type() != "CountMinSketchAccumulator" {
                return Err(format!(
                    "Cannot merge CountMinSketchAccumulator with {}",
                    acc.get_accumulator_type()
                )
                .into());
            }

            let cms_acc = acc
                .as_any()
                .downcast_ref::<CountMinSketchAccumulator>()
                .ok_or("Failed to downcast to CountMinSketchAccumulator")?;
            cms_accumulators.push(cms_acc);
        }

        // Check dimensions are consistent
        let row_num = cms_accumulators[0].row_num;
        let col_num = cms_accumulators[0].col_num;
        for acc in &cms_accumulators {
            if acc.row_num != row_num || acc.col_num != col_num {
                return Err(
                    "Cannot merge CountMinSketch accumulators with different dimensions".into(),
                );
            }
        }

        // Clone first accumulator, then add all others element-wise WITHOUT cloning
        // Use iterator-based element-wise addition instead of indexing for clarity
        let mut merged = cms_accumulators[0].clone();
        for acc in &cms_accumulators[1..] {
            for (merged_row, acc_row) in merged.sketch.iter_mut().zip(&acc.sketch) {
                for (m_cell, a_cell) in merged_row.iter_mut().zip(acc_row.iter()) {
                    *m_cell += *a_cell;
                }
            }
        }

        Ok(merged)
    }
}

impl SerializableToSink for CountMinSketchAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "row_num": self.row_num,
            "col_num": self.col_num,
            "sketch": self.sketch
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        // Match Arroyo UDF: countminsketch.serialize(&mut Serializer::new(&mut buf))
        let mut buf = Vec::new();
        self.serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .unwrap();
        buf
    }
}

impl AggregateCore for CountMinSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "CountMinSketchAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        // Check if other is also a CountMinSketchAccumulator
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge CountMinSketchAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        // Downcast to CountMinSketchAccumulator
        let other_cms = other
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .ok_or("Failed to downcast to CountMinSketchAccumulator")?;

        // Check dimensions match
        if self.row_num != other_cms.row_num || self.col_num != other_cms.col_num {
            return Err(
                "Cannot merge CountMinSketch accumulators with different dimensions".into(),
            );
        }

        // Clone self ONCE, then add other's sketch element-wise directly
        // This reduces 3 clones to 1 clone
        let mut merged = self.clone();
        for i in 0..self.row_num {
            for j in 0..self.col_num {
                merged.sketch[i][j] += other_cms.sketch[i][j];
            }
        }

        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> &'static str {
        "CountMinSketchAccumulator"
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
    }
}

// CountMinSketchAccumulator only supports MultipleSubpopulationAggregate since it's key-based

impl MultipleSubpopulationAggregate for CountMinSketchAccumulator {
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

impl MergeableAccumulator<CountMinSketchAccumulator> for CountMinSketchAccumulator {
    fn merge_accumulators(
        accumulators: Vec<CountMinSketchAccumulator>,
    ) -> Result<CountMinSketchAccumulator, Box<dyn std::error::Error + Send + Sync>> {
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
                    "Cannot merge CountMinSketch accumulators with different dimensions".into(),
                );
            }
        }

        let mut merged = accumulators[0].clone();

        // Add all sketches element-wise
        for acc in &accumulators[1..] {
            for i in 0..row_num {
                for j in 0..col_num {
                    merged.sketch[i][j] += acc.sketch[i][j];
                }
            }
        }

        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_min_sketch_creation() {
        let cms = CountMinSketchAccumulator::new(4, 1000);
        assert_eq!(cms.row_num, 4);
        assert_eq!(cms.col_num, 1000);
        assert_eq!(cms.sketch.len(), 4);
        assert_eq!(cms.sketch[0].len(), 1000);

        // Check all values are initialized to 0
        for row in &cms.sketch {
            for &value in row {
                assert_eq!(value, 0.0);
            }
        }
    }

    #[test]
    fn test_count_min_sketch_update() {
        let mut cms = CountMinSketchAccumulator::new(2, 10);
        let key = KeyByLabelValues::new();

        // Update should work with hash functions
        cms._update(&key, 1.0);

        // Query should return the updated value
        let result = cms.query_key(&key);
        assert!(result >= 1.0); // Should be at least 1.0 due to the update
    }

    #[test]
    fn test_count_min_sketch_query() {
        let cms = CountMinSketchAccumulator::new(2, 10);
        let key = KeyByLabelValues::new();

        // Test key-based query implementation
        assert_eq!(cms.query_key(&key), 0.0);

        // Test through MultipleSubpopulationAggregate trait
        let multi_trait: &dyn MultipleSubpopulationAggregate = &cms;
        assert_eq!(multi_trait.query(Statistic::Sum, &key, None).unwrap(), 0.0);
    }

    #[test]
    fn test_count_min_sketch_merge() {
        let mut cms1 = CountMinSketchAccumulator::new(2, 3);
        let mut cms2 = CountMinSketchAccumulator::new(2, 3);

        // Set some values
        cms1.sketch[0][0] = 5.0;
        cms1.sketch[1][2] = 10.0;

        cms2.sketch[0][0] = 3.0;
        cms2.sketch[0][1] = 7.0;

        let merged = CountMinSketchAccumulator::merge_accumulators(vec![cms1, cms2]).unwrap();

        assert_eq!(merged.sketch[0][0], 8.0); // 5 + 3
        assert_eq!(merged.sketch[0][1], 7.0); // 0 + 7
        assert_eq!(merged.sketch[1][2], 10.0); // 10 + 0
    }

    #[test]
    fn test_count_min_sketch_merge_dimension_mismatch() {
        let cms1 = CountMinSketchAccumulator::new(2, 3);
        let cms2 = CountMinSketchAccumulator::new(3, 3); // Different row count

        let result = CountMinSketchAccumulator::merge_accumulators(vec![cms1, cms2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_count_min_sketch_serialization() {
        let mut cms = CountMinSketchAccumulator::new(2, 3);
        cms.sketch[0][1] = 42.0;
        cms.sketch[1][2] = 100.0;

        // // Test JSON serialization
        // let json_value = cms.serialize_to_json();
        // let deserialized = CountMinSketchAccumulator::deserialize_from_json(&json_value).unwrap();

        // assert_eq!(deserialized.row_num, 2);
        // assert_eq!(deserialized.col_num, 3);
        // assert_eq!(deserialized.sketch[0][1], 42.0);
        // assert_eq!(deserialized.sketch[1][2], 100.0);

        // Test binary serialization
        let bytes = cms.serialize_to_bytes();
        let deserialized_bytes =
            CountMinSketchAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();

        assert_eq!(deserialized_bytes.row_num, 2);
        assert_eq!(deserialized_bytes.col_num, 3);
        assert_eq!(deserialized_bytes.sketch[0][1], 42.0);
        assert_eq!(deserialized_bytes.sketch[1][2], 100.0);
    }

    #[test]
    fn test_count_min_sketch_as_aggregate_core() {
        let cms = CountMinSketchAccumulator::new(2, 3);
        assert_eq!(cms.type_name(), "CountMinSketchAccumulator");
    }

    #[test]
    fn test_trait_object() {
        let cms = CountMinSketchAccumulator::new(2, 3);
        let trait_obj: Box<dyn AggregateCore> = Box::new(cms);

        assert_eq!(trait_obj.type_name(), "CountMinSketchAccumulator");
    }

    #[test]
    fn test_count_min_sketch_key_query() {
        let mut cms = CountMinSketchAccumulator::new(4, 100);
        let key = KeyByLabelValues::new();

        // Initially should return 0
        assert_eq!(cms.query_key(&key), 0.0);

        // Update with some value
        cms._update(&key, 5.0);

        // Query should return the value (might be higher due to collisions)
        let result = cms.query_key(&key);
        assert!(result >= 5.0);
    }

    #[test]
    fn test_multiple_subpopulation_aggregate() {
        let mut cms = CountMinSketchAccumulator::new(3, 50);
        let key = KeyByLabelValues::new();

        // Update and query through the trait
        cms._update(&key, 10.0);

        let multi_trait: &dyn MultipleSubpopulationAggregate = &cms;
        let result = multi_trait.query(Statistic::Sum, &key, None).unwrap();
        assert!(result >= 10.0);

        // get_keys should return empty vector for CountMinSketch
        let keys = multi_trait.get_keys();
        assert!(keys.is_none());
    }

    #[test]
    fn test_count_min_sketch_merge_multiple() {
        // Create 3 CMS accumulators
        let mut cms1 = CountMinSketchAccumulator::new(2, 3);
        let mut cms2 = CountMinSketchAccumulator::new(2, 3);
        let mut cms3 = CountMinSketchAccumulator::new(2, 3);

        // Set different values in each
        cms1.sketch[0][0] = 5.0;
        cms1.sketch[1][2] = 10.0;

        cms2.sketch[0][0] = 3.0;
        cms2.sketch[0][1] = 7.0;

        cms3.sketch[0][0] = 2.0;
        cms3.sketch[1][2] = 5.0;

        // Box them as AggregateCore trait objects
        let boxed_accs: Vec<Box<dyn AggregateCore>> =
            vec![Box::new(cms1), Box::new(cms2), Box::new(cms3)];

        // Use merge_multiple
        let merged = CountMinSketchAccumulator::merge_multiple(&boxed_accs).unwrap();

        // Verify the merged result
        assert_eq!(merged.sketch[0][0], 10.0); // 5 + 3 + 2
        assert_eq!(merged.sketch[0][1], 7.0); // 0 + 7 + 0
        assert_eq!(merged.sketch[1][2], 15.0); // 10 + 0 + 5
    }

    #[test]
    fn test_count_min_sketch_merge_multiple_error_cases() {
        // Test empty slice
        let empty: Vec<Box<dyn AggregateCore>> = vec![];
        assert!(CountMinSketchAccumulator::merge_multiple(&empty).is_err());

        // Test mismatched dimensions
        let cms1 = CountMinSketchAccumulator::new(2, 3);
        let cms2 = CountMinSketchAccumulator::new(3, 3); // Different row count

        let boxed_accs: Vec<Box<dyn AggregateCore>> = vec![Box::new(cms1), Box::new(cms2)];
        assert!(CountMinSketchAccumulator::merge_multiple(&boxed_accs).is_err());

        // Test wrong accumulator type
        use crate::precompute_operators::sum_accumulator::SumAccumulator;
        let cms = CountMinSketchAccumulator::new(2, 3);
        let sum = SumAccumulator::new();

        let mixed_accs: Vec<Box<dyn AggregateCore>> = vec![Box::new(cms), Box::new(sum)];
        assert!(CountMinSketchAccumulator::merge_multiple(&mixed_accs).is_err());
    }
}
