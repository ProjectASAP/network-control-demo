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
        context: &QueryContext,
        plan: &LocalAggregationPlan,
    ) -> Result<Option<Value>, String> {
        match &plan.kind {
            AggregationKind::Percentiles(pct) => {
                if pct.percents.is_empty() {
                    return Ok(None);
                }
                let field = metric_field_for_name(&state.runtime_config, &pct.field)
                    .ok_or_else(|| format!("unsupported percentile field: {}", pct.field))?;
                let explicit_key = pct
                    .key
                    .as_ref()
                    .map(|key| key.trim())
                    .filter(|key| !key.is_empty())
                    .map(|key| key.to_string());
                let key = explicit_key
                    .or_else(|| context.key.clone())
                    .ok_or_else(|| "percentiles key is required".to_string())?;
                let query_results = state.store.query_percentiles(&key, field, &pct.percents)?;

                let mut values = BTreeMap::new();
                for (percent, value) in pct.percents.iter().zip(query_results.iter()) {
                    if let Some(value) = value {
                        values.insert(percent.to_string(), *value);
                    }
                }
                Ok(Some(json!({ "values": values })))
            }
            AggregationKind::Cumulative(cum) => {
                let field = metric_field_for_name(&state.runtime_config, &cum.field)
                    .ok_or_else(|| format!("unsupported cumulative field: {}", cum.field))?;
                let explicit_key = cum
                    .key
                    .as_ref()
                    .map(|key| key.trim())
                    .filter(|key| !key.is_empty())
                    .map(|key| key.to_string());
                let key = explicit_key
                    .or_else(|| context.key.clone())
                    .ok_or_else(|| "cumulative key is required".to_string())?;
                let value = state.store.cumulative_value(&key, field)?;
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
            "cumulative" => Some(AggregationRegistration {
                name: "cumulative",
                supports_search: true,
                supports_batch: true,
            }),
            _ => None,
        }
    }

    fn supported_features(&self) -> Vec<String> {
        vec![
            "aggregations.percentiles".to_string(),
            "aggregations.cumulative".to_string(),
            "query.bool.filter.term".to_string(),
            "size=0".to_string(),
            "batch.percentiles".to_string(),
            "batch.cumulative".to_string(),
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
