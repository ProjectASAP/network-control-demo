use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use crate::metrics::{EntityEstimate, MetricField};

use super::types::AppState;
use super::types::{
    CumulativeAggregation, FrequencyAggregation, PercentileAggregation, QueryKeyStatus,
    TopEntitiesAggregation, TopEntitiesResult,
};

pub(crate) fn build_percentile_response(percents: &[f64], values: &[f64]) -> BTreeMap<String, f64> {
    let mut response = BTreeMap::new();
    for (percent, value) in percents.iter().zip(values.iter()) {
        response.insert(percent.to_string(), *value);
    }
    response
}

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

    let mut values = BTreeMap::new();
    let explicit_key = pct
        .key
        .as_ref()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty());
    if pct.key.is_some() && explicit_key.is_none() {
        return Err("percentiles key is required when provided".to_string());
    }
    let key = explicit_key.or(query_key);
    if let Some(cached) = state.cache.get_percentiles(field, key, &pct.percents) {
        return Ok(Some(build_percentile_response(&pct.percents, &cached)));
    }

    let mut cache_values = Vec::with_capacity(pct.percents.len());
    let mut all_present = true;
    for percent in &pct.percents {
        let value = if let Some(key) = key {
            state.store.query_percentile_by_key(field, key, *percent)
        } else {
            state.store.query_percentile(field, *percent)
        };

        if let Some(value) = value {
            values.insert(percent.to_string(), value);
            cache_values.push(value);
        } else {
            all_present = false;
        }
    }

    if all_present && !cache_values.is_empty() {
        state
            .cache
            .set_percentiles(field, key, &pct.percents, cache_values);
    }

    Ok(Some(values))
}

fn handle_multi_top_entities(
    state: &AppState,
    fields: &[String],
) -> Result<HashMap<String, EntityEstimate>, String> {
    let mut results = HashMap::new();

    for field_name in fields {
        let trimmed = field_name.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !state
            .agg_config
            .top_entities_metrics
            .contains(&trimmed.to_ascii_lowercase())
        {
            return Err(format!("unsupported top_entities field: {}", field_name));
        }
        let field = MetricField::from_spec(trimmed)
            .ok_or_else(|| format!("unsupported top_entities field: {}", field_name))?;

        if let Some(entity) = state.store.top_entity(field) {
            results.insert(field_name.clone(), entity);
        }
    }

    if results.is_empty() {
        return Err("no top entity available".to_string());
    }

    Ok(results)
}

pub(crate) fn handle_top_entities(
    state: &AppState,
    top: &TopEntitiesAggregation,
) -> Result<TopEntitiesResult, String> {
    if let Some(fields) = top.fields.as_ref().filter(|fields| !fields.is_empty()) {
        let results = handle_multi_top_entities(state, fields)?;
        return Ok(TopEntitiesResult::Multi(results));
    }

    let field_name = top
        .field
        .as_ref()
        .map(|field| field.trim())
        .filter(|field| !field.is_empty())
        .ok_or_else(|| "top_entities field is required".to_string())?;
    if !state
        .agg_config
        .top_entities_metrics
        .contains(&field_name.to_ascii_lowercase())
    {
        return Err(format!("unsupported top_entities field: {}", field_name));
    }
    let field = MetricField::from_spec(field_name)
        .ok_or_else(|| format!("unsupported top_entities field: {}", field_name))?;
    let entity = state
        .store
        .top_entity(field)
        .ok_or_else(|| "no top entity available".to_string())?;
    Ok(TopEntitiesResult::Single(entity))
}

pub(crate) fn handle_cumulative(
    state: &AppState,
    cum: &CumulativeAggregation,
) -> Result<i32, String> {
    if !state
        .agg_config
        .cumulative_metrics
        .contains(&cum.field.trim().to_ascii_lowercase())
    {
        return Err(format!("unsupported cumulative field: {}", cum.field));
    }
    let field = MetricField::from_spec(&cum.field)
        .ok_or_else(|| format!("unsupported cumulative field: {}", cum.field))?;
    if cum.key.trim().is_empty() {
        return Err("cumulative key is required".to_string());
    }
    Ok(state.store.cumulative_value(field, cum.key.trim()))
}

pub(crate) fn handle_frequency(
    state: &AppState,
    freq: &FrequencyAggregation,
) -> Result<i32, String> {
    let field = MetricField::from_spec(&freq.field)
        .ok_or_else(|| format!("unsupported frequency field: {}", freq.field))?;
    let key = freq.key.trim();
    if key.is_empty() {
        return Err("frequency key is required".to_string());
    }

    state
        .store
        .frequency_estimate(field, key, freq.value)
        .ok_or_else(|| format!("invalid frequency value: {}", freq.value))
}

pub(crate) fn extract_query_key(query_value: Option<&Value>) -> QueryKeyStatus {
    let Some(query_value) = query_value else {
        return QueryKeyStatus::None;
    };
    if query_value.is_null() {
        return QueryKeyStatus::None;
    }

    let query_obj = match query_value.as_object() {
        Some(obj) => obj,
        None => return QueryKeyStatus::Unsupported,
    };

    let mut cluster = None;
    let mut task = None;

    if let Some(term_value) = query_obj.get("term") {
        if parse_term_object(term_value, &mut cluster, &mut task).is_err() {
            return QueryKeyStatus::Unsupported;
        }
    } else if let Some(bool_value) = query_obj.get("bool") {
        let bool_obj = match bool_value.as_object() {
            Some(obj) => obj,
            None => return QueryKeyStatus::Unsupported,
        };
        if bool_obj.len() != 1 || !bool_obj.contains_key("must") {
            return QueryKeyStatus::Unsupported;
        }
        let must_value = match bool_obj.get("must") {
            Some(value) => value,
            None => return QueryKeyStatus::Unsupported,
        };
        let must_items = match must_value.as_array() {
            Some(items) => items,
            None => return QueryKeyStatus::Unsupported,
        };
        for item in must_items {
            let term_value = match item.get("term") {
                Some(value) => value,
                None => return QueryKeyStatus::Unsupported,
            };
            if parse_term_object(term_value, &mut cluster, &mut task).is_err() {
                return QueryKeyStatus::Unsupported;
            }
        }
    } else {
        return QueryKeyStatus::Unsupported;
    }

    match (cluster, task) {
        (None, None) => QueryKeyStatus::None,
        (Some(cluster), Some(task)) => QueryKeyStatus::Key(format!("{cluster};{task}")),
        (Some(cluster), None) => QueryKeyStatus::Key(cluster),
        (None, Some(task)) => QueryKeyStatus::Key(task),
    }
}

fn parse_term_object(
    term_value: &Value,
    cluster: &mut Option<String>,
    task: &mut Option<String>,
) -> Result<(), ()> {
    let term_obj = term_value.as_object().ok_or(())?;
    for (field, value) in term_obj {
        let normalized = field.trim().to_ascii_lowercase();
        let term_value = extract_term_value(value).ok_or(())?;
        let term_value = term_value.trim();
        if term_value.is_empty() {
            return Err(());
        }
        match normalized.as_str() {
            "cluster" | "cluster.keyword" => {
                *cluster = Some(term_value.to_string());
            }
            "task" | "task.keyword" => {
                *task = Some(term_value.to_string());
            }
            _ => return Err(()),
        }
    }
    Ok(())
}

fn extract_term_value(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        return Some(value.to_string());
    }
    if let Some(value) = value.as_i64() {
        return Some(value.to_string());
    }
    if let Some(value) = value.as_u64() {
        return Some(value.to_string());
    }
    if let Some(value) = value.as_f64() {
        return Some(value.to_string());
    }
    if let Some(obj) = value.as_object() {
        if let Some(inner) = obj.get("value") {
            return extract_term_value(inner);
        }
    }
    None
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
