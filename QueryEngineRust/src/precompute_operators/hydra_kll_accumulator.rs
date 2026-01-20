use crate::{
    data_model::{
        AggregateCore, MergeableAccumulator, MultipleSubpopulationAggregate, SerializableToSink,
    },
    precompute_operators::DatasketchesKLLAccumulator,
    KeyByLabelValues,
};
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use xxhash_rust::xxh32::xxh32;

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
#[derive(Debug, Clone)]
pub struct HydraKllSketchAccumulator {
    sketch: Vec<Vec<DatasketchesKLLAccumulator>>,
    row_num: usize,
    col_num: usize,
}

impl HydraKllSketchAccumulator {
    pub fn new(row_num: usize, col_num: usize, k: u16) -> Self {
        let sketch = vec![vec![DatasketchesKLLAccumulator::new(k); col_num]; row_num];
        Self {
            sketch,
            row_num,
            col_num,
        }
    }

    // Update the sketch with a key-value pair
    pub fn update(&mut self, key: &KeyByLabelValues, value: f64) {
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
            self.sketch[i][col_index]._update(value);
        }
    }

    pub fn deserialize_from_bytes(_buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        // HydraKLLSketch is only used with Arroyo, not Flink
        Err("deserialize_from_bytes for HydraKllSketchAccumulator not implemented for Flink".into())
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
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
                let accumulator =
                    DatasketchesKLLAccumulator::deserialize_from_bytes_arroyo(&cell_bytes)?;
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

impl SerializableToSink for HydraKllSketchAccumulator {
    fn serialize_to_json(&self) -> Value {
        // Mirror Python implementation: {"sketch": base64_encoded_string}
        let sketch_bytes = self.serialize_to_bytes();
        let sketch_b64 = general_purpose::STANDARD.encode(&sketch_bytes);

        serde_json::json!({
            "sketch": sketch_b64
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        let mut sketches = Vec::with_capacity(self.row_num);
        for row in &self.sketch {
            let mut row_data = Vec::with_capacity(self.col_num);
            for cell in row {
                // Serialize each DatasketchesKLLAccumulator to KllSketchData
                let cell_bytes = cell.serialize_to_bytes();
                let kll_data: KllSketchData = rmp_serde::from_slice(&cell_bytes)
                    .expect("Failed to deserialize KllSketchData from cell");
                row_data.push(kll_data);
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

impl MergeableAccumulator<HydraKllSketchAccumulator> for HydraKllSketchAccumulator {
    fn merge_accumulators(
        accumulators: Vec<HydraKllSketchAccumulator>,
    ) -> Result<HydraKllSketchAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        // Check dimensions match
        let row_num = accumulators[0].row_num;
        let col_num = accumulators[0].col_num;
        for acc in &accumulators {
            if acc.row_num != row_num || acc.col_num != col_num {
                return Err(
                    "Cannot merge HydraKllSketchAccumulator with different dimensions".into(),
                );
            }
        }

        // Merge each cell independently
        let mut merged_sketch = Vec::with_capacity(row_num);
        for i in 0..row_num {
            let mut merged_row = Vec::with_capacity(col_num);
            for j in 0..col_num {
                // Collect all cells at position [i][j] from all accumulators
                let cells_to_merge: Vec<DatasketchesKLLAccumulator> = accumulators
                    .iter()
                    .map(|acc| acc.sketch[i][j].clone())
                    .collect();

                // Merge the cells
                let merged_cell = DatasketchesKLLAccumulator::merge_accumulators(cells_to_merge)?;
                merged_row.push(merged_cell);
            }
            merged_sketch.push(merged_row);
        }

        Ok(HydraKllSketchAccumulator {
            sketch: merged_sketch,
            row_num,
            col_num,
        })
    }
}

impl AggregateCore for HydraKllSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "HydraKllSketchAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        // Check if other is also a HydraKllSketchAccumulator
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge HydraKllSketchAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        // Downcast to HydraKllSketchAccumulator
        let hk = other
            .as_any()
            .downcast_ref::<HydraKllSketchAccumulator>()
            .ok_or("Failed to downcast to HydraKllSketchAccumulator")?;

        // Use the existing merge_accumulators method
        let merged = Self::merge_accumulators(vec![self.clone(), hk.clone()])?;

        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> &'static str {
        "HydraKllSketchAccumulator"
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
    }
}

impl MultipleSubpopulationAggregate for HydraKllSketchAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        key: &KeyByLabelValues,
        query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        match statistic {
            Statistic::Quantile => {
                // Extract quantile from query_kwargs (like DatasketchesKLL does)
                let quantile = query_kwargs
                    .and_then(|kwargs| kwargs.get("quantile"))
                    .ok_or("Missing quantile parameter for quantile query")?
                    .parse::<f64>()
                    .map_err(|_| "Invalid quantile parameter format")?;

                if !(0.0..=1.0).contains(&quantile) {
                    return Err("Quantile must be between 0.0 and 1.0".into());
                }

                // Use the provided quantile instead of hardcoded 0.5
                Ok(self.query_key(key, quantile))
            }
            _ => Err(
                format!("Unsupported statistic in HydraKllSketchAccumulator: {statistic:?}").into(),
            ),
        }
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}
