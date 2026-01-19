use std::collections::BTreeMap;
use std::time::Instant;

use axum::http::HeaderMap;
use serde_json::{Value, json};

use super::types::AppState;

/// Tracks timing for each step of query processing
pub struct QueryTiming {
    start: Instant,
    last_step: Instant,
    steps: Vec<(String, f64)>, // (step_name, duration_ms)
}

impl QueryTiming {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            start: now,
            last_step: now,
            steps: Vec::new(),
        }
    }

    /// Record a step with elapsed time since last step (in ms)
    pub fn step(&mut self, name: &str) {
        let now = Instant::now();
        let duration_ms = now.duration_since(self.last_step).as_secs_f64() * 1000.0;
        self.steps.push((name.to_string(), duration_ms));
        self.last_step = now;
    }

    /// Get total elapsed time in ms
    pub fn total_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }

    /// Log timing to stderr
    pub fn log(&self) {
        let steps_str: Vec<String> = self
            .steps
            .iter()
            .map(|(name, ms)| format!("{}={:.3}ms", name, ms))
            .collect();
        eprintln!(
            "[TIMING] {} total={:.3}ms",
            steps_str.join(" "),
            self.total_ms()
        );
    }

    /// Convert to JSON value for response
    pub fn to_json(&self) -> Value {
        let mut steps_obj = serde_json::Map::new();
        for (name, ms) in &self.steps {
            steps_obj.insert(format!("{}_ms", name), json!(ms));
        }
        json!({
            "total_ms": self.total_ms(),
            "steps": steps_obj
        })
    }

    /// Format as header value
    pub fn to_header(&self) -> String {
        format!("{:.3}", self.total_ms())
    }
}

pub(crate) fn write_timing_log(
    state: &AppState,
    headers: &HeaderMap,
    request_type: &str,
    _method: &str,
    _path: &str,
    status: axum::http::StatusCode,
    timing: &QueryTiming,
) {
    let Some(sender) = state.timing_sender.as_ref() else {
        return;
    };
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");
    let request_type = if request_type.is_empty() {
        "unknown"
    } else {
        request_type
    };
    let mut steps: BTreeMap<&str, f64> = BTreeMap::new();
    for (name, ms) in &timing.steps {
        steps.insert(name.as_str(), *ms);
    }
    let total_ms: f64 = steps.values().copied().sum();
    let step_names = [
        "parse_json",
        "deserialize",
        "aggregations",
        "prepare_upstream",
        "upstream",
        "merge",
        "serialize",
        "parse_field",
        "validate",
        "query_percentiles",
        "build_response",
    ];
    let format_value =
        |value: Option<&f64>| -> String { value.map(|ms| format!("{ms:.3}")).unwrap_or_default() };
    let mut row = Vec::with_capacity(6 + step_names.len());
    row.push(request_id.to_string());
    row.push(request_type.to_string());
    row.push(status.to_string());
    row.push(format!("{total_ms:.3}"));
    for name in step_names {
        row.push(format_value(steps.get(name)));
    }
    let _ = sender.send(row.join(","));
}
