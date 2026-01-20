use crate::data_model::{
    AggregateCore, MergeableAccumulator, SerializableToSink, SingleSubpopulationAggregate,
};
use base64::{engine::general_purpose, Engine as _};
use core::panic;
use dsrs::KllDoubleSketch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Instant;
use tracing::debug;

use promql_utilities::query_logics::enums::Statistic;

#[derive(Deserialize, Serialize)]
struct KllSketchData {
    k: u16,
    sketch_bytes: Vec<u8>,
}

pub struct DatasketchesKLLAccumulator {
    k: u16,
    sketch: KllDoubleSketch,
}

impl DatasketchesKLLAccumulator {
    pub fn new(k: u16) -> Self {
        Self {
            k,
            sketch: KllDoubleSketch::with_k(k),
        }
    }

    pub fn _update(&mut self, value: f64) {
        self.sketch.update(value);
    }

    pub fn get_quantile(&self, quantile: f64) -> f64 {
        if self.sketch.get_n() == 0 {
            return 0.0;
        }
        self.sketch.get_quantile(quantile)
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        // Mirror Python implementation: expects {"sketch": base64_encoded_string}
        let sketch_b64 = data["sketch"]
            .as_str()
            .ok_or("Missing or invalid 'sketch' field")?;

        let sketch_bytes = general_purpose::STANDARD
            .decode(sketch_b64)
            .map_err(|e| format!("Failed to decode base64 sketch data: {e}"))?;

        let sketch = KllDoubleSketch::deserialize(&sketch_bytes)
            .map_err(|e| format!("Failed to deserialize KLL sketch: {e}"))?;

        // TODO: remove this hardcoding once FlinkSketch serializes k in its output
        Ok(Self { k: 200, sketch })
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        // Mirror Python implementation: deserialize sketch directly from bytes
        let sketch = KllDoubleSketch::deserialize(buffer)
            .map_err(|e| format!("Failed to deserialize KLL sketch: {e}"))?;

        // TODO: remove this hardcoding once FlinkSketch serializes k in its output
        Ok(Self { k: 200, sketch })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        debug!(
            "Deserializing DatasketchesKLLAccumulator from Arroyo MessagePack buffer of size {}",
            buffer.len()
        );

        let deserialized_sketch_data: KllSketchData = rmp_serde::from_slice(buffer)
            .map_err(|e| format!("Failed to deserialize KllSketchData from MessagePack: {e}"))?;

        debug!(
            "Deserialized KllSketchData with k={} and sketch_bytes length={}",
            deserialized_sketch_data.k,
            deserialized_sketch_data.sketch_bytes.len()
        );

        let sketch: KllDoubleSketch =
            KllDoubleSketch::deserialize(&deserialized_sketch_data.sketch_bytes)
                .map_err(|e| format!("Failed to deserialize KLL sketch: {e}"))?;

        Ok(Self {
            k: deserialized_sketch_data.k,
            sketch,
        })
    }

    /// Merge multiple accumulators efficiently without cloning all of them
    /// This is a batch merge operation that creates one empty sketch and merges all others into it
    ///
    /// # Arguments
    /// * `accumulators` - Slice of boxed AggregateCore trait objects to merge
    ///
    /// # Returns
    /// * `Result<Self, Box<dyn std::error::Error + Send + Sync>>` - Merged accumulator or error
    ///
    /// # Performance
    /// This method performs 0 clones (just creates 1 new empty sketch), compared to the
    /// sequential merge approach which would perform N clones for N accumulators.
    pub fn merge_multiple(
        accumulators: &[Box<dyn crate::data_model::AggregateCore>],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        // Downcast and validate all accumulators first
        let mut kll_accumulators = Vec::with_capacity(accumulators.len());
        for acc in accumulators {
            if acc.get_accumulator_type() != "DatasketchesKLLAccumulator" {
                return Err(format!(
                    "Cannot merge DatasketchesKLLAccumulator with {}",
                    acc.get_accumulator_type()
                )
                .into());
            }

            let kll_acc = acc
                .as_any()
                .downcast_ref::<DatasketchesKLLAccumulator>()
                .ok_or("Failed to downcast to DatasketchesKLLAccumulator")?;
            kll_accumulators.push(kll_acc);
        }

        // Check k values are consistent
        let k = kll_accumulators[0].k;
        for acc in &kll_accumulators {
            if acc.k != k {
                return Err(
                    "Cannot merge DatasketchesKLLAccumulator with different k values".into(),
                );
            }
        }

        // Create new sketch and merge all others into it WITHOUT cloning
        let mut merged = DatasketchesKLLAccumulator::new(k);
        for acc in kll_accumulators {
            merged.sketch.merge(&acc.sketch);
        }

        Ok(merged)
    }
}

// Manual trait implementations since the C++ library doesn't provide them
impl Clone for DatasketchesKLLAccumulator {
    fn clone(&self) -> Self {
        let bytes = self.sketch.serialize();
        let new_sketch = KllDoubleSketch::deserialize(bytes.as_ref()).unwrap();
        Self {
            k: self.k,
            sketch: new_sketch,
        }
    }
}

impl std::fmt::Debug for DatasketchesKLLAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasketchesKLLAccumulator")
            .field("k", &self.k)
            .field("sketch_n", &self.sketch.get_n())
            .finish()
    }
}

// TODO: verify this
// Thread safety: The C++ library is not thread-safe by default, but since we're using it
// in a single-threaded context per accumulator instance and only sharing read-only operations,
// this should be safe. The actual sketch data is immutable once created.
unsafe impl Send for DatasketchesKLLAccumulator {}
unsafe impl Sync for DatasketchesKLLAccumulator {}

impl SerializableToSink for DatasketchesKLLAccumulator {
    fn serialize_to_json(&self) -> Value {
        // Mirror Python implementation: {"sketch": base64_encoded_string}
        let sketch_bytes = self.sketch.serialize();
        let sketch_b64 = general_purpose::STANDARD.encode(&sketch_bytes);

        serde_json::json!({
            "sketch": sketch_b64
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        // Create KllSketchData compatible with deserialize_from_bytes_arroyo()
        // This matches exactly what the Arroyo UDF does
        let sketch_data = self.sketch.serialize();
        let serialized = KllSketchData {
            k: self.k,
            sketch_bytes: sketch_data.as_ref().to_vec(),
        };

        // Use the same serialization method as Arroyo UDF
        let mut buf = Vec::new();
        match rmp_serde::encode::write(&mut buf, &serialized) {
            Ok(_) => buf,
            Err(_) => {
                panic!("Failed to serialize KllSketchData to MessagePack");
            }
        }
    }
}

impl AggregateCore for DatasketchesKLLAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "DatasketchesKLLAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        #[cfg(feature = "extra_debugging")]
        let merge_with_start = Instant::now();
        #[cfg(feature = "extra_debugging")]
        debug!(
            "[PERF] DatasketchesKLLAccumulator::merge_with() started - self.k={}, self.n={}",
            self.k,
            self.sketch.get_n()
        );

        // Check if other is also a DatasketchesKLLAccumulator
        #[cfg(feature = "extra_debugging")]
        let type_check_start = Instant::now();
        if other.get_accumulator_type() != self.get_accumulator_type() {
            #[cfg(feature = "extra_debugging")]
            debug!(
                "[PERF] DatasketchesKLLAccumulator::merge_with() type check failed after {:?} - other type: {}",
                type_check_start.elapsed(),
                other.get_accumulator_type()
            );
            return Err(format!(
                "Cannot merge DatasketchesKLLAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }
        #[cfg(feature = "extra_debugging")]
        debug!(
            "[PERF] DatasketchesKLLAccumulator::merge_with() type check passed in {:?}",
            type_check_start.elapsed()
        );

        // Downcast to DatasketchesKLLAccumulator
        #[cfg(feature = "extra_debugging")]
        let downcast_start = Instant::now();
        let other_kll = other
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .ok_or("Failed to downcast to DatasketchesKLLAccumulator")?;
        #[cfg(feature = "extra_debugging")]
        debug!(
            "[PERF] DatasketchesKLLAccumulator::merge_with() downcast completed in {:?} - other.k={}, other.n={}",
            downcast_start.elapsed(),
            other_kll.k,
            other_kll.sketch.get_n()
        );

        // Clone self ONCE, then merge other directly without cloning
        // This reduces 2 serialize/deserialize operations to just 1
        #[cfg(feature = "extra_debugging")]
        let merge_accumulators_start = Instant::now();
        let mut merged = self.clone();
        merged.sketch.merge(&other_kll.sketch);
        #[cfg(feature = "extra_debugging")]
        debug!(
            "[PERF] DatasketchesKLLAccumulator merge_accumulators completed in {:?} - merged.k={}, merged.n={}",
            merge_accumulators_start.elapsed(),
            merged.k,
            merged.sketch.get_n()
        );

        // Box the result
        #[cfg(feature = "extra_debugging")]
        let boxing_start = Instant::now();
        let result = Box::new(merged);
        #[cfg(feature = "extra_debugging")]
        debug!(
            "[PERF] DatasketchesKLLAccumulator::merge_with() boxing completed in {:?}",
            boxing_start.elapsed()
        );

        #[cfg(feature = "extra_debugging")]
        debug!(
            "[PERF] DatasketchesKLLAccumulator::merge_with() TOTAL TIME: {:?}",
            merge_with_start.elapsed()
        );

        Ok(result)
    }

    fn get_accumulator_type(&self) -> &'static str {
        "DatasketchesKLLAccumulator"
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
    }
}

impl SingleSubpopulationAggregate for DatasketchesKLLAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        match statistic {
            Statistic::Quantile => {
                debug!(
                    "Querying DatasketchesKLLAccumulator for quantile with kwargs: {:?}",
                    query_kwargs
                );
                let quantile = query_kwargs
                    .and_then(|kwargs| kwargs.get("quantile"))
                    .ok_or("Missing quantile parameter for quantile query")?
                    .parse::<f64>()
                    .map_err(|_| "Invalid quantile parameter format")?;

                if !(0.0..=1.0).contains(&quantile) {
                    return Err("Quantile must be between 0.0 and 1.0".into());
                }

                Ok(self.get_quantile(quantile))
            }
            _ => Err(
                format!("Unsupported statistic in DatasketchesKLLAccumulator: {statistic:?}")
                    .into(),
            ),
        }
    }

    fn clone_boxed(&self) -> Box<dyn SingleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<DatasketchesKLLAccumulator> for DatasketchesKLLAccumulator {
    fn merge_accumulators(
        accumulators: Vec<DatasketchesKLLAccumulator>,
    ) -> Result<DatasketchesKLLAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        // check K values for all and merge
        let k = accumulators[0].k;
        for acc in &accumulators {
            if acc.k != k {
                return Err(
                    "Cannot merge DatasketchesKLLAccumulator with different k values".into(),
                );
            }
        }

        let mut merged = DatasketchesKLLAccumulator::new(k);

        // Merge all sketches
        for accumulator in accumulators {
            merged.sketch.merge(&accumulator.sketch);
        }

        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datasketches_kll_creation() {
        let kll = DatasketchesKLLAccumulator::new(200);
        assert!(kll.sketch.get_n() == 0);
        assert_eq!(kll.k, 200);
    }

    #[test]
    fn test_datasketches_kll_update() {
        let mut kll = DatasketchesKLLAccumulator::new(200);

        kll._update(10.0);
        kll._update(20.0);
        kll._update(15.0);

        assert_eq!(kll.sketch.get_n(), 3);
        // assert!(kll.sketch.get_values().contains(&10.0));
        // assert!(kll.sketch.get_values().contains(&20.0));
        // assert!(kll.sketch.get_values().contains(&15.0));
    }

    #[test]
    fn test_datasketches_kll_quantile() {
        let mut kll = DatasketchesKLLAccumulator::new(200);

        // Add values 1.0 to 10.0
        for i in 1..=10 {
            kll._update(i as f64);
        }

        // Test different quantiles
        assert_eq!(kll.get_quantile(0.0), 1.0); // Min
        assert_eq!(kll.get_quantile(1.0), 10.0); // Max
        assert_eq!(kll.get_quantile(0.5), 6.0); // Median - updated to match datasketches behavior
    }

    #[test]
    fn test_datasketches_kll_query() {
        let mut kll = DatasketchesKLLAccumulator::new(200);

        for i in 1..=10 {
            kll._update(i as f64);
        }

        // Test quantile query with default median (0.5)
        let mut query_kwargs = HashMap::new();
        query_kwargs.insert("quantile".to_string(), "0.5".to_string());
        let result = kll.query(Statistic::Quantile, Some(&query_kwargs)).unwrap();
        assert_eq!(result, 6.0); // Updated to match actual datasketches behavior

        // Test unsupported statistic
        assert!(kll.query(Statistic::Sum, Some(&query_kwargs)).is_err());
    }

    #[test]
    fn test_datasketches_kll_merge() {
        let mut kll1 = DatasketchesKLLAccumulator::new(200);
        let mut kll2 = DatasketchesKLLAccumulator::new(200);

        // Add different values to each
        for i in 1..=5 {
            kll1._update(i as f64);
        }

        for i in 6..=10 {
            kll2._update(i as f64);
        }

        let merged = DatasketchesKLLAccumulator::merge_accumulators(vec![kll1, kll2]).unwrap();

        assert_eq!(merged.sketch.get_n(), 10);
        assert_eq!(merged.get_quantile(0.0), 1.0);
        assert_eq!(merged.get_quantile(1.0), 10.0);
    }

    #[test]
    fn test_datasketches_kll_serialization() {
        let mut kll = DatasketchesKLLAccumulator::new(200);

        for i in 1..=5 {
            kll._update(i as f64);
        }

        // // Test JSON serialization
        // let json_value = kll.serialize_to_json();
        // let deserialized = DatasketchesKLLAccumulator::deserialize_from_json(&json_value).unwrap();

        // assert_eq!(deserialized.k, 200);
        // assert_eq!(deserialized.sketch.get_n(), 5);
        // assert_eq!(deserialized.get_quantile(0.0), 1.0);
        // assert_eq!(deserialized.get_quantile(1.0), 5.0);

        // Test binary serialization
        let bytes = kll.serialize_to_bytes();
        let deserialized_bytes =
            DatasketchesKLLAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();

        assert_eq!(deserialized_bytes.k, 200);
        assert_eq!(deserialized_bytes.sketch.get_n(), 5);
        assert_eq!(deserialized_bytes.get_quantile(0.0), 1.0);
        assert_eq!(deserialized_bytes.get_quantile(1.0), 5.0);
    }

    #[test]
    fn test_datasketches_kll_get_keys() {
        let kll = DatasketchesKLLAccumulator::new(200);
        // DatasketchesKLLAccumulator doesn't have a get_keys method like MultipleSubpopulationAggregate
        // so we just test type name
        assert_eq!(kll.type_name(), "DatasketchesKLLAccumulator");
    }

    #[test]
    fn test_trait_object() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        kll._update(5.0);

        let trait_obj: Box<dyn AggregateCore> = Box::new(kll);

        assert_eq!(trait_obj.type_name(), "DatasketchesKLLAccumulator");
    }

    // #[test]
    // fn test_datasketches_kll_arroyo_deserialization() {
    //     // Create a test KLL with some data
    //     let mut original_kll = DatasketchesKLLAccumulator::new(200);
    //     for i in 1..=5 {
    //         original_kll._update(i as f64);
    //     }

    //     // Serialize to bytes (simulating normal serialization)
    //     let sketch_data = original_kll.serialize_to_bytes();

    //     // Create MessagePack format that Arroyo would send: sketch_data_bytes (direct bytes)
    //     let arroyo_buffer = rmp_serde::to_vec(&sketch_data).unwrap();

    //     // Test Arroyo deserialization
    //     let deserialized_kll =
    //         DatasketchesKLLAccumulator::deserialize_from_bytes_arroyo(&arroyo_buffer).unwrap();

    //     // Verify the deserialized KLL has the same data
    //     assert_eq!(deserialized_kll.sketch.get_n(), 5);
    //     assert_eq!(deserialized_kll.k, 200);
    //     assert_eq!(deserialized_kll.get_quantile(0.0), 1.0);
    //     assert_eq!(deserialized_kll.get_quantile(1.0), 5.0);
    // }

    #[test]
    fn test_datasketches_kll_query_with_kwargs() {
        let mut kll = DatasketchesKLLAccumulator::new(200);

        for i in 1..=10 {
            kll._update(i as f64);
        }

        // Test with query_kwargs
        let mut query_kwargs = HashMap::new();
        query_kwargs.insert("quantile".to_string(), "0.5".to_string());

        let result = kll.query(Statistic::Quantile, Some(&query_kwargs)).unwrap();
        assert_eq!(result, 6.0); // Updated to match actual datasketches behavior

        // Test with different quantile
        query_kwargs.insert("quantile".to_string(), "0.9".to_string());
        let result = kll.query(Statistic::Quantile, Some(&query_kwargs)).unwrap();
        assert_eq!(result, 10.0); // Updated to match actual datasketches behavior

        // Test minimum quantile
        query_kwargs.insert("quantile".to_string(), "0.0".to_string());
        let result = kll.query(Statistic::Quantile, Some(&query_kwargs)).unwrap();
        assert_eq!(result, 1.0);

        // Test maximum quantile
        query_kwargs.insert("quantile".to_string(), "1.0".to_string());
        let result = kll.query(Statistic::Quantile, Some(&query_kwargs)).unwrap();
        assert_eq!(result, 10.0);

        // Test error cases
        assert!(kll.query(Statistic::Quantile, None).is_err());

        query_kwargs.insert("quantile".to_string(), "invalid".to_string());
        assert!(kll.query(Statistic::Quantile, Some(&query_kwargs)).is_err());

        query_kwargs.insert("quantile".to_string(), "1.5".to_string());
        assert!(kll.query(Statistic::Quantile, Some(&query_kwargs)).is_err());

        query_kwargs.insert("quantile".to_string(), "-0.1".to_string());
        assert!(kll.query(Statistic::Quantile, Some(&query_kwargs)).is_err());

        // Test unsupported statistic
        query_kwargs.insert("quantile".to_string(), "0.5".to_string());
        assert!(kll.query(Statistic::Sum, Some(&query_kwargs)).is_err());
    }

    #[test]
    fn test_datasketches_kll_merge_multiple() {
        // Create 3 KLL accumulators with different data
        let mut kll1 = DatasketchesKLLAccumulator::new(200);
        let mut kll2 = DatasketchesKLLAccumulator::new(200);
        let mut kll3 = DatasketchesKLLAccumulator::new(200);

        // Add different values to each
        for i in 1..=5 {
            kll1._update(i as f64);
        }
        for i in 6..=10 {
            kll2._update(i as f64);
        }
        for i in 11..=15 {
            kll3._update(i as f64);
        }

        // Box them as AggregateCore trait objects
        let boxed_accs: Vec<Box<dyn AggregateCore>> =
            vec![Box::new(kll1), Box::new(kll2), Box::new(kll3)];

        // Use merge_multiple
        let merged = DatasketchesKLLAccumulator::merge_multiple(&boxed_accs).unwrap();

        // Verify the merged result
        assert_eq!(merged.sketch.get_n(), 15); // Total number of values
        assert_eq!(merged.get_quantile(0.0), 1.0); // Min value
        assert_eq!(merged.get_quantile(1.0), 15.0); // Max value
        assert_eq!(merged.get_quantile(0.5), 8.0); // Median
    }

    #[test]
    fn test_datasketches_kll_merge_multiple_error_cases() {
        // Test empty slice
        let empty: Vec<Box<dyn AggregateCore>> = vec![];
        assert!(DatasketchesKLLAccumulator::merge_multiple(&empty).is_err());

        // Test mismatched k values
        let kll1 = DatasketchesKLLAccumulator::new(200);
        let kll2 = DatasketchesKLLAccumulator::new(100); // Different k

        let boxed_accs: Vec<Box<dyn AggregateCore>> = vec![Box::new(kll1), Box::new(kll2)];
        assert!(DatasketchesKLLAccumulator::merge_multiple(&boxed_accs).is_err());

        // Test wrong accumulator type
        use crate::precompute_operators::sum_accumulator::SumAccumulator;
        let kll = DatasketchesKLLAccumulator::new(200);
        let sum = SumAccumulator::new();

        let mixed_accs: Vec<Box<dyn AggregateCore>> = vec![Box::new(kll), Box::new(sum)];
        assert!(DatasketchesKLLAccumulator::merge_multiple(&mixed_accs).is_err());
    }
}
