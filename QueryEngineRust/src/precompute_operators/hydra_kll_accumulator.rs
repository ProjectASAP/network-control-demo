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

    // fn _update(&mut self, key: &KeyByLabelValues, value: f64) {
    //     // Match Python logic: ";".join(key.serialize_to_json())
    //     let key_json = key.serialize_to_json();
    //     let key_values: Vec<String> = if let Some(obj) = key_json.as_object() {
    //         obj.values()
    //             .map(|v| v.as_str().unwrap_or("").to_string())
    //             .collect()
    //     } else {
    //         vec!["".to_string()]
    //     };
    //     let key_str = key_values.join(";");
    //     let key_bytes = key_str.as_bytes();

    //     // Update each row using different hash functions
    //     for i in 0..self.row_num {
    //         let hash_value = xxh32(key_bytes, i as u32);
    //         let col_index = (hash_value as usize) % self.col_num;
    //         self.sketch[i][col_index]._update(value);
    //     }
    // }

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
        if self.row_num == 0 || self.col_num == 0 {
            return 0.0;
        }

        let label_count = if key.labels.is_empty() { 1 } else { key.labels.len() };
        let mut quantiles = Vec::with_capacity(self.row_num * label_count);

        for (i, row) in self.sketch.iter().enumerate() {
            if key.labels.is_empty() {
                let hash_value = xxh32(&[], i as u32);
                let col_index = (hash_value as usize) % self.col_num;
                quantiles.push(row[col_index].get_quantile(quantile));
            } else {
                for label in &key.labels {
                    let hash_value = xxh32(label.as_bytes(), i as u32);
                    let col_index = (hash_value as usize) % self.col_num;
                    quantiles.push(row[col_index].get_quantile(quantile));
                }
            }
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
