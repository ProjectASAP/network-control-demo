use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use crate::metrics::{EntityEstimate, MetricField};

use super::QueryCache;
use super::types::AppState;
use super::types::{
    CumulativeAggregation, PercentileAggregation, QueryKeyStatus, TopEntitiesAggregation,
    TopEntitiesResult,
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

    let explicit_key = pct
        .key
        .as_ref()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty());
    if pct.key.is_some() && explicit_key.is_none() {
        return Err("percentiles key is required when provided".to_string());
    }
    let key = explicit_key.or(query_key);
    let time_window = extract_time_window(pct.current_time_ms, pct.time_range_ms)?;
    let cache_key = if state.cache.is_enabled() && time_window.is_none() {
        Some(QueryCache::build_percentiles_cache_key(
            field,
            key,
            &pct.percents,
        ))
    } else {
        None
    };
    if let Some(cache_key) = cache_key.as_ref() {
        if let Some(cached) = state.cache.get_percentiles_with_key(cache_key) {
            return Ok(Some(build_percentile_response(&pct.percents, &cached)));
        }
    }

    let query_results = match (key, time_window) {
        (Some(key), Some((current_time_ms, time_range_ms))) => state
            .store
            .query_percentiles_by_key_time(field, key, &pct.percents, current_time_ms, time_range_ms),
        (Some(key), None) => state
            .store
            .query_percentiles_by_key(field, key, &pct.percents),
        (None, Some((current_time_ms, time_range_ms))) => state
            .store
            .query_percentiles_time(field, &pct.percents, current_time_ms, time_range_ms),
        (None, None) => state.store.query_percentiles(field, &pct.percents),
    };
    let query_results = query_results.unwrap_or_else(|| vec![None; pct.percents.len()]);

    let mut values = BTreeMap::new();
    let mut cache_values = Vec::with_capacity(pct.percents.len());
    let mut all_present = true;
    for (percent, value) in pct.percents.iter().zip(query_results.iter()) {
        if let Some(value) = value {
            values.insert(percent.to_string(), *value);
            cache_values.push(*value);
        } else {
            all_present = false;
        }
    }

    if all_present && !cache_values.is_empty() {
        if let Some(cache_key) = cache_key {
            state
                .cache
                .set_percentiles_with_key(cache_key, cache_values);
        }
    }

    Ok(Some(values))
}

fn handle_multi_top_entities(
    state: &AppState,
    fields: &[String],
    time_window: Option<(u64, u64)>,
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

        let entity = match time_window {
            Some((current_time_ms, time_range_ms)) => {
                state.store.top_entity_time(field, current_time_ms, time_range_ms)
            }
            None => state.store.top_entity(field),
        };
        if let Some(entity) = entity {
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
    let time_window = extract_time_window(top.current_time_ms, top.time_range_ms)?;
    if let Some(fields) = top.fields.as_ref().filter(|fields| !fields.is_empty()) {
        let results = handle_multi_top_entities(state, fields, time_window)?;
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
    let entity = match time_window {
        Some((current_time_ms, time_range_ms)) => state
            .store
            .top_entity_time(field, current_time_ms, time_range_ms),
        None => state.store.top_entity(field),
    };
    match entity {
        Some(value) => Ok(TopEntitiesResult::Single(value)),
        None => Ok(TopEntitiesResult::Multi(std::collections::HashMap::new())),
    }
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
    let value = match (cum.current_time_ms, cum.time_range_ms) {
        (Some(current_time_ms), Some(time_range_ms)) => state
            .store
            .cumulative_value_time(field, cum.key.trim(), current_time_ms, time_range_ms)
            .unwrap_or(0),
        (Some(current_time_ms), None) => state
            .store
            .cumulative_value_at_time(field, cum.key.trim(), current_time_ms)
            .unwrap_or(0),
        (None, None) => state.store.cumulative_value(field, cum.key.trim()),
        _ => {
            return Err(
                "current_time_ms and time_range_ms must be provided together".to_string(),
            )
        }
    };
    Ok(value)
}

fn extract_time_window(
    current_time_ms: Option<u64>,
    time_range_ms: Option<u64>,
) -> Result<Option<(u64, u64)>, String> {
    match (current_time_ms, time_range_ms) {
        (None, None) => Ok(None),
        (Some(current), Some(range)) => Ok(Some((current, range))),
        _ => Err("current_time_ms and time_range_ms must be provided together".to_string()),
    }
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
