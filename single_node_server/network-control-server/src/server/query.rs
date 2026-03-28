use std::collections::BTreeMap;

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
    let normalized_field = normalize_field_name(&pct.field);
    if !state
        .agg_config
        .supports_percentile_field(&normalized_field, pct.key.is_some() || query_key.is_some())
    {
        return Ok(None);
    }

    let explicit_key = pct
        .key
        .as_ref()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty());
    if pct.key.is_some() && explicit_key.is_none() {
        return Err("percentiles key is required when provided".to_string());
    }
    let key = explicit_key.or(query_key);
    let query_results = state
        .metric_store
        .query_percentiles(key, &normalized_field, &pct.percents)?;

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
    let normalized_field = normalize_field_name(&cum.field);
    if !state
        .agg_config
        .supports_cumulative_field(&normalized_field)
    {
        return Err(format!("unsupported cumulative field: {}", cum.field));
    }
    let key = cum.key.trim();
    if key.is_empty() {
        return Err("cumulative key is required".to_string());
    }
    let value = state
        .metric_store
        .cumulative_value(Some(key), &normalized_field)?;
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

fn normalize_field_name(spec: &str) -> String {
    spec.trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace(' ', "_")
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
