use std::collections::BTreeMap;

use crate::metrics::MetricField;

use super::types::AppState;
use super::types::{CumulativeAggregation, PercentileAggregation};

pub(crate) fn handle_percentiles(
    state: &AppState,
    pct: &PercentileAggregation,
    query_key: Option<&str>,
) -> Result<Option<BTreeMap<String, f64>>, String> {
    if pct.percents.is_empty() {
        return Ok(None);
    }
    if !state
        .agg_config
        .percentile_fields
        .contains(&pct.field.trim().to_ascii_lowercase())
    {
        return Ok(None);
    }
    let field = MetricField::from_spec(&pct.field)
        .ok_or_else(|| format!("unsupported percentile field: {}", pct.field))?;

    let explicit_key = pct
        .key
        .as_ref()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty());
    if pct.key.is_some() && explicit_key.is_none() {
        return Err("percentiles key is required when provided".to_string());
    }
    let key = explicit_key.or(query_key);
    let node_id = key.ok_or_else(|| "percentiles key is required".to_string())?;
    let query_results = state
        .node_store
        .query_percentiles(node_id, field, &pct.percents)?;

    let mut values = BTreeMap::new();
    for (percent, value) in pct.percents.iter().zip(query_results.iter()) {
        if let Some(value) = value {
            values.insert(percent.to_string(), *value);
        }
    }

    Ok(Some(values))
}

pub(crate) fn handle_cumulative(
    state: &AppState,
    cum: &CumulativeAggregation,
) -> Result<f64, String> {
    if !state
        .agg_config
        .cumulative_metrics
        .contains(&cum.field.trim().to_ascii_lowercase())
    {
        return Err(format!("unsupported cumulative field: {}", cum.field));
    }
    let field = MetricField::from_spec(&cum.field)
        .ok_or_else(|| format!("unsupported cumulative field: {}", cum.field))?;
    let node_id = cum.key.trim();
    if node_id.is_empty() {
        return Err("cumulative key is required".to_string());
    }
    let value = state.node_store.cumulative_value(node_id, field)?;
    Ok(value)
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
