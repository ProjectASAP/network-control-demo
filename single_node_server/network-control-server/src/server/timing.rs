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

    /// Record elapsed time for a specific operation (independent of step timing)
    pub fn record(&mut self, name: &str, duration_ms: f64) {
        self.steps.push((name.to_string(), duration_ms));
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
        // Accumulate times for repeated step names (e.g., multiple sketch_estimate calls)
        *steps.entry(name.as_str()).or_insert(0.0) += *ms;
    }
    let total_ms: f64 = steps.values().copied().sum();

    // Simplified output: total, estimate (sketch query), json processing
    let estimate_ms = steps.get("sketch_estimate").copied().unwrap_or(0.0);
    let json_ms = steps.get("parse_json").copied().unwrap_or(0.0)
        + steps.get("deserialize").copied().unwrap_or(0.0);

    let row = format!(
        "{},{},{},{:.3},{:.3},{:.3}",
        request_id, request_type, status, total_ms, estimate_ms, json_ms
    );
    let _ = sender.send(row);
}
