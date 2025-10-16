use crate::{KeyByLabelValues, data_model::{
    AggregateCore, MergeableAccumulator, SerializableToSink, SingleSubpopulationAggregate,
}, precompute_operators::DatasketchesKLLAccumulator};
use base64::{engine::general_purpose, Engine as _};
use core::panic;
use dsrs::KllDoubleSketch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::debug;
use xxhash_rust::xxh32::xxh32;
use std::cmp::Ordering;

use promql_utilities::query_logics::enums::Statistic;

// duplicate from datasketches_kll_accumulator
#[derive(Deserialize, Serialize)]
struct KllSketchData {
    k: u16,
    sketch_bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct HydraKllSketchData {
    row_num: usize,
    col_num: usize,
    sketches: Vec<Vec<KllSketchData>>,
}
struct HydraKllSketchAccumulator {
    sketch: Vec<Vec<DatasketchesKLLAccumulator>>,
    row_num: usize,
    col_num: usize,
}

impl HydraKllSketchAccumulator {
    pub fn new(row_num: usize, col_num: usize, k: u16) -> Self {
        let sketch = vec![vec![DatasketchesKLLAccumulator::new(k);col_num]; row_num];
        Self {
            sketch,
            row_num,
            col_num,
        }
    }

    pub fn deserialize_from_bytes_arroyo(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let deserialized_sketch_data: HydraKllSketchData = rmp_serde::from_slice(buffer)
        .map_err(|e| format!("Failed to deserialize HydraKLL from MessagePack: {e}"))?;
        
        if deserialized_sketch_data.sketches.len() != deserialized_sketch_data.row_num {
            return Err(format!(
                "HydraKLL row count mismatch: expected {}, got {}",
                deserialized_sketch_data.row_num,
                deserialized_sketch_data.sketches.len()
            )
            .into());
        }

        let mut sketch: Vec<Vec<DatasketchesKLLAccumulator>> =
            Vec::with_capacity(deserialized_sketch_data.row_num);

        for (row_idx, row) in deserialized_sketch_data.sketches.into_iter().enumerate() {
            if row.len() != deserialized_sketch_data.col_num {
                return Err(format!(
                    "HydraKLL column count mismatch in row {}: expected {}, got {}",
                    row_idx,
                    deserialized_sketch_data.col_num,
                    row.len()
                )
                .into());
            }

            let mut accum_row: Vec<DatasketchesKLLAccumulator> =
                Vec::with_capacity(deserialized_sketch_data.col_num);
            for cell in row {
                let cell_bytes = rmp_serde::to_vec(&cell)
                    .map_err(|e| format!("Failed to serialize nested KLL sketch: {e}"))?;
                let accumulator = DatasketchesKLLAccumulator::deserialize_from_bytes_arroyo(&cell_bytes)?;
                accum_row.push(accumulator);
            }

            sketch.push(accum_row);
        }

        Ok(Self {
            sketch,
            row_num: deserialized_sketch_data.row_num,
            col_num: deserialized_sketch_data.col_num,
        })
    }

    pub fn query_key(&self, key: &KeyByLabelValues, quantile: f64) -> f64 {
        let mut quantiles = Vec::with_capacity(self.row_num);
        let key_string = key.labels.join(";");

        let key_bytes = key_string.as_bytes();

        // Query each row and take the median
        for i in 0..self.row_num {
            let hash_value = xxh32(key_bytes, i as u32);
            let col_index = (hash_value as usize) % self.col_num;
            quantiles.push(self.sketch[i][col_index].get_quantile(quantile));
        }

        if quantiles.is_empty() {
            return 0.0;
        }

        quantiles.sort_by(|a, b| match a.partial_cmp(b) {
            Some(ordering) => ordering,
            None => Ordering::Equal,
        });

        let mid = quantiles.len() / 2;
        if quantiles.len() % 2 == 0 {
            (quantiles[mid - 1] + quantiles[mid]) / 2.0
        } else {
            quantiles[mid]
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f64 = 1e-6;
    fn serialized_hydra(keys: &[&str], values: &[f64]) -> Vec<u8> {
        let mut hydra = HydraKllSketch::new();
        for key in keys {
            for &value in values {
                hydra.update(key, value);
            }
        }
        hydra.serialize_bytes()
    }

    #[test]
    fn hydra_deserialize_and_query_single_label() {
        let buffer =
            serialized_hydra(&["key1;key2;key3", "key1;key3;key4"], &[10.0, 20.0, 30.0, 40.0, 50.0]);

        let accumulator =
            HydraKllSketchAccumulator::deserialize_from_bytes_arroyo(&buffer).unwrap();

        let key = KeyByLabelValues::new_with_labels(vec![
            "key1".to_string(),
            "key2".to_string(),
            "key3".to_string(),
        ]);

        let result = accumulator.query_key(&key, 0.5);
        // assert_eq!(result, 20.0, "result is {}", result);
        assert!((result - 30.0).abs() < EPSILON);
    }

    #[test]
    fn hydra_median_testing() {
        let mut hydra = HydraKllSketch::new();
        hydra.update("key1;key2;key3", 10.0);
        hydra.update("key1;key2;key3", 20.0);
        hydra.update("key1;key2;key3", 30.0);
        hydra.update("key4;key5;key6", 40.0);
        hydra.update("key4;key5;key6", 50.0);
        hydra.update("key4;key5;key6", 60.0);
        hydra.update("key7;key8;key9", 70.0);
        hydra.update("key7;key8;key9", 80.0);
        hydra.update("key7;key8;key9", 90.0);

        let accumulator =
            HydraKllSketchAccumulator::deserialize_from_bytes_arroyo(&hydra.serialize_bytes()).unwrap();

        let key = KeyByLabelValues::new_with_labels(vec![
            "key1".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert_eq!(result, 20.0, "result is {}", result);
        assert!((result - 20.0).abs() < EPSILON);
        
        let key = KeyByLabelValues::new_with_labels(vec![
            "key2".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert!((result - 20.0).abs() < EPSILON);
        
        // // mysterious failure
        let key = KeyByLabelValues::new_with_labels(vec![
            "key3".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.3);
        assert_eq!(result, 20.0, "result is {}", result);
        assert!((result - 20.0).abs() < EPSILON);

        let key = KeyByLabelValues::new_with_labels(vec![
            "key1".to_string(),
            "key2".to_string(),
            "key3".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert_eq!(result, 20.0, "result is {}", result);
        assert!((result - 20.0).abs() < EPSILON);

        let key = KeyByLabelValues::new_with_labels(vec![
            "key4".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert!((result - 50.0).abs() < EPSILON);
        
        let key = KeyByLabelValues::new_with_labels(vec![
            "key5".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert!((result - 50.0).abs() < EPSILON);
            
        let key = KeyByLabelValues::new_with_labels(vec![
            "key6".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert_eq!(result, 50.0, "result is {}", result);

        let key = KeyByLabelValues::new_with_labels(vec![
            "key4".to_string(),
            "key5".to_string(),
            "key6".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert!((result -50.0).abs() < EPSILON);

        let key = KeyByLabelValues::new_with_labels(vec![
            "key7".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert!((result - 80.0).abs() < EPSILON);
        
        let key = KeyByLabelValues::new_with_labels(vec![
            "key8".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert!((result - 80.0).abs() < EPSILON);
            
        let key = KeyByLabelValues::new_with_labels(vec![
            "key9".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert_eq!(result, 80.0, "result is {}", result);
        
        let key = KeyByLabelValues::new_with_labels(vec![
            "key7".to_string(),
            "key8".to_string(),
            "key9".to_string(),
        ]);
        let result = accumulator.query_key(&key, 0.5);
        assert!((result - 80.0).abs() < EPSILON);
    }

    #[test]
    fn hydra_min_max_testing() {
        let mut hydra = HydraKllSketch::new();
        for value in 1..=10 {
            hydra.update("key1;key2;key3", value as f64);
        }

        let accumulator =
            HydraKllSketchAccumulator::deserialize_from_bytes_arroyo(&hydra.serialize_bytes()).unwrap();

        let key = KeyByLabelValues::new_with_labels(vec![
            "key1".to_string(),
            "key2".to_string(),
            "key3".to_string(),
        ]);

        let min_val = accumulator.query_key(&key, 0.0);
        let max_val = accumulator.query_key(&key, 1.0);

        assert!(min_val >= 1.0 - EPSILON);
        assert!(min_val <= 1.0 + EPSILON);
        assert!(max_val <= 10.0 + EPSILON);
        assert!(max_val >= 10.0 - EPSILON);
        
        let key = KeyByLabelValues::new_with_labels(vec![
            "key1".to_string(),
        ]);

        let min_val = accumulator.query_key(&key, 0.0);
        let max_val = accumulator.query_key(&key, 1.0);

        assert!(min_val >= 1.0 - EPSILON);
        assert!(min_val <= 1.0 + EPSILON);
        assert!(max_val <= 10.0 + EPSILON);
        assert!(max_val >= 10.0 - EPSILON);

        let key = KeyByLabelValues::new_with_labels(vec![
            "key2".to_string(),
        ]);

        let min_val = accumulator.query_key(&key, 0.0);
        let max_val = accumulator.query_key(&key, 1.0);

        assert!(min_val >= 1.0 - EPSILON);
        assert!(min_val <= 1.0 + EPSILON);
        assert!(max_val <= 10.0 + EPSILON);
        assert!(max_val >= 10.0 - EPSILON);

        let key = KeyByLabelValues::new_with_labels(vec![
            "key3".to_string(),
        ]);

        let min_val = accumulator.query_key(&key, 0.0);
        let max_val = accumulator.query_key(&key, 1.0);

        assert!(min_val >= 1.0 - EPSILON);
        assert!(min_val <= 1.0 + EPSILON);
        assert!(max_val <= 10.0 + EPSILON);
        assert!(max_val >= 10.0 - EPSILON);
    } 

}

// Following part copied from UDF to make the test of deserialization possible 
// KLL parameters
const DEFAULT_K: u16 = 20;

struct KllSketchWrapper {
    k: u16,
    sketch: KllDoubleSketch,
}

impl KllSketchWrapper {
    fn new(k: u16) -> Self {
        KllSketchWrapper {
            k,
            sketch: KllDoubleSketch::with_k(k),
        }
    }

    fn update(&mut self, value: f64) {
        self.sketch.update(value);
    }

    fn to_data(&self) -> KllSketchData {
        let sketch_data = self.sketch.serialize();
        KllSketchData {
            k: self.k,
            sketch_bytes: sketch_data.as_ref().to_vec(),
        }
    }

    fn serialize_bytes(&self) -> Vec<u8> {
        let serialized = self.to_data();
        let mut buf = Vec::new();
        rmp_serde::encode::write(&mut buf, &serialized).unwrap();
        buf
    }
}

impl Clone for KllSketchWrapper {
    fn clone(&self) -> Self {
        let bytes = self.sketch.serialize();
        let sketch =
            KllDoubleSketch::deserialize(bytes.as_ref()).expect("failed to clone KLL sketch");
        Self { k: self.k, sketch }
    }
}

// Count-Min Sketch parameters
const DEPTH: usize = 3; // Number of hash functions
const WIDTH: usize = 32; // Number of buckets per hash function

struct HydraKllSketch {
    sketch: Vec<Vec<KllSketchWrapper>>,
    row_num: usize,
    col_num: usize,
}

impl HydraKllSketch {
    fn new() -> Self {
        HydraKllSketch {
            sketch: vec![vec![KllSketchWrapper::new(DEFAULT_K); WIDTH]; DEPTH],
            row_num: DEPTH,
            col_num: WIDTH,
        }
    }

    // Update the sketch with a key-value pair
    fn update(&mut self, key: &str, value: f64) {
        let parts: Vec<&str> = key.split(';').filter(|s| !s.is_empty()).collect();
        let n = parts.len();
        let mut result = Vec::new();
        for i in 1..(1 << n) {
            let mut current_combination: Vec<&str> = Vec::new();
            for j in 0..n {
                if (i >> j) & 1 == 1 {
                    current_combination.push(parts[j]);
                }
            }
            result.push(current_combination.join(";"));
        }
        // println!("result: {:?}", result);
        for i in 0..self.row_num {
            for subkey in &result {
                let hash = xxh32(subkey.as_bytes(), i as u32);
                let bucket = (hash as usize) % self.col_num;
                // println!("bucket: {}", bucket);
                self.sketch[i][bucket].update(value);
            }
        }
    }

    fn serialize_bytes(&self) -> Vec<u8> {
        let mut sketches = Vec::with_capacity(self.row_num);
        for row in &self.sketch {
            let mut row_data = Vec::with_capacity(self.col_num);
            for cell in row {
                row_data.push(cell.to_data());
            }
            sketches.push(row_data);
        }

        let serialized = HydraKllSketchData {
            row_num: self.row_num,
            col_num: self.col_num,
            sketches,
        };

        let mut buf = Vec::new();
        rmp_serde::encode::write(&mut buf, &serialized).unwrap();
        buf
    }
}
