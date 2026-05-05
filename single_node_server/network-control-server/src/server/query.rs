use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::types::{
    AggregationEngine, AggregationKind, AggregationRegistration, AppState, LocalAggregationPlan,
    QueryContext, metric_field_for_name,
};

pub struct SketchAggregationEngine;

impl AggregationEngine for SketchAggregationEngine {
    fn evaluate(
        &self,
        state: &AppState,
        store: &dyn crate::metrics::MetricStore,
        context: &QueryContext,
        plan: &LocalAggregationPlan,
    ) -> Result<Option<Value>, String> {
        match &plan.kind {
            AggregationKind::Percentiles(pct) => {
                if pct.percents.is_empty() {
                    return Ok(None);
                }
                let index_name = context
                    .index_name
                    .as_deref()
                    .ok_or_else(|| "query index is required".to_string())?;
                let field = metric_field_for_name(&state.runtime_config, index_name, &pct.field)
                    .ok_or_else(|| format!("unsupported percentile field: {}", pct.field))?;
                let key = context
                    .key
                    .clone()
                    .ok_or_else(|| "percentiles key is required".to_string())?;
                let query_results = store.query_percentiles(&key, &field, &pct.percents)?;

                let mut values = BTreeMap::new();
                for (percent, value) in pct.percents.iter().zip(query_results.iter()) {
                    if let Some(value) = value {
                        values.insert(percent.to_string(), *value);
                    }
                }
                Ok(Some(json!({ "values": values })))
            }
            AggregationKind::Sum(sum) => {
                let index_name = context
                    .index_name
                    .as_deref()
                    .ok_or_else(|| "query index is required".to_string())?;
                let field = metric_field_for_name(&state.runtime_config, index_name, &sum.field)
                    .ok_or_else(|| format!("unsupported sum field: {}", sum.field))?;
                let key = context
                    .key
                    .clone()
                    .ok_or_else(|| "sum key is required".to_string())?;
                let value = store.cumulative_value(&key, &field)?;
                Ok(Some(json!({ "key": key, "value": value })))
            }
        }
    }

    fn registration(&self, name: &str) -> Option<AggregationRegistration> {
        match name.trim().to_ascii_lowercase().as_str() {
            "percentiles" => Some(AggregationRegistration {
                name: "percentiles",
                supports_search: true,
                supports_batch: true,
            }),
            "sum" => Some(AggregationRegistration {
                name: "sum",
                supports_search: true,
                supports_batch: true,
            }),
            _ => None,
        }
    }

    fn supported_features(&self) -> Vec<String> {
        vec![
            "aggregations.percentiles".to_string(),
            "aggregations.sum".to_string(),
            "query.bool.filter.term".to_string(),
            "size=0".to_string(),
            "batch.percentiles".to_string(),
            "batch.sum".to_string(),
        ]
    }
}

pub(crate) fn parse_quantile_spec(spec: &str) -> Option<f64> {
    let trimmed = spec.trim();
    let candidate = trimmed
        .strip_prefix('p')
        .or_else(|| trimmed.strip_prefix('P'))
        .unwrap_or(trimmed)
        .trim();
    if candidate.is_empty() {
        return None;
    }
    candidate.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::parse_quantile_spec;

    #[test]
    fn parse_quantile_accepts_prefixed_and_raw_numeric_values() {
        assert_eq!(parse_quantile_spec("p50"), Some(50.0));
        assert_eq!(parse_quantile_spec("P95"), Some(95.0));
        assert_eq!(parse_quantile_spec("  12.5  "), Some(12.5));
        assert_eq!(parse_quantile_spec(" p 75 "), Some(75.0));
    }

    #[test]
    fn parse_quantile_rejects_empty_or_invalid_values() {
        assert_eq!(parse_quantile_spec(""), None);
        assert_eq!(parse_quantile_spec("p"), None);
        assert_eq!(parse_quantile_spec("not-a-number"), None);
    }
}
