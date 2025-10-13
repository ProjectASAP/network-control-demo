use crate::data_model::{
    AggregateCore, MergeableAccumulator, SerializableToSink, SingleSubpopulationAggregate,
};
use base64::{engine::general_purpose, Engine as _};
use core::panic;
use dsrs::KllDoubleSketch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
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

    fn _update(&mut self, value: f64) {
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
        debug!("Buffer bytes: {:?}", buffer);

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

        debug!(
            "Successfully deserialized KLL sketch with n={}",
            sketch.get_n()
        );

        Ok(Self {
            k: deserialized_sketch_data.k,
            sketch,
        })
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
        // Check if other is also a DatasketchesKLLAccumulator
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge DatasketchesKLLAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        // Downcast to DatasketchesKLLAccumulator
        let other_kll = other
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .ok_or("Failed to downcast to DatasketchesKLLAccumulator")?;

        // Use the existing merge_accumulators method
        let merged = Self::merge_accumulators(vec![self.clone(), other_kll.clone()])?;

        Ok(Box::new(merged))
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
}
