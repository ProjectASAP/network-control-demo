use axum::{
    body::{Body, Bytes, to_bytes},
    extract::State,
    http::HeaderMap,
    middleware::Next,
    response::Response,
};
use serde_json::Value;
use tokio::sync::mpsc;

use super::types::AppState;

#[derive(Debug)]
pub struct LogEntry {
    pub method: axum::http::Method,
    pub uri: axum::http::Uri,
    pub headers: HeaderMap,
    pub body: Bytes,
}

pub type LogSender = mpsc::Sender<LogEntry>;

pub(crate) async fn log_request_middleware(
    State(state): State<AppState>,
    req: axum::http::Request<Body>,
    next: Next,
) -> Response {
    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let uri = parts.uri.clone();
    let headers = parts.headers.clone();

    let body_bytes = match to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("failed to read request body: {err}");
            Bytes::new()
        }
    };

    let log_body = body_bytes.clone();
    let log_method = method.clone();
    let log_uri = uri.clone();
    let log_headers = headers.clone();
    if let Some(log_tx) = &state.log_tx {
        let log_entry = LogEntry {
            method: log_method,
            uri: log_uri,
            headers: log_headers,
            body: log_body,
        };
        let _ = log_tx.try_send(log_entry);
    }

    let req = axum::http::Request::from_parts(parts, Body::from(body_bytes));
    let response = next.run(req).await;
    let status = response.status();
    eprintln!("response status: {}", status);
    response
}

fn log_request_details(
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) {
    const MAX_LOG_BODY_BYTES: usize = 1024 * 1024;

    eprintln!("incoming request: {method} {uri}");

    let mut header_pairs: Vec<(String, String)> = headers
        .iter()
        .map(|(name, value)| {
            let value_str = value
                .to_str()
                .map(|val| val.to_string())
                .unwrap_or_else(|_| format!("<non-utf8:{} bytes>", value.as_bytes().len()));
            (name.to_string(), value_str)
        })
        .collect();
    header_pairs.sort_by(|a, b| a.0.cmp(&b.0));

    eprintln!("headers:");
    if header_pairs.is_empty() {
        eprintln!("  <none>");
    } else {
        for (name, value) in header_pairs {
            eprintln!("  {name}: {value}");
        }
    }

    if body.is_empty() {
        eprintln!("body (0 bytes): <empty>");
        eprintln!("end request");
        return;
    }

    let total_len = body.len();
    if total_len > MAX_LOG_BODY_BYTES {
        eprintln!(
            "body ({} bytes, showing first {}):",
            total_len, MAX_LOG_BODY_BYTES
        );
        let preview = &body[..MAX_LOG_BODY_BYTES];
        eprintln!("{}", String::from_utf8_lossy(preview));
        eprintln!("body truncated");
        eprintln!("end request");
        return;
    }

    eprintln!("body ({} bytes):", total_len);
    match serde_json::from_slice::<Value>(&body) {
        Ok(value) => match serde_json::to_string_pretty(&value) {
            Ok(pretty) => eprintln!("{pretty}"),
            Err(_) => eprintln!("{value}"),
        },
        Err(_) => eprintln!("{}", String::from_utf8_lossy(&body)),
    }
    eprintln!("end request");
}

pub fn start_request_logger(buffer: usize) -> LogSender {
    let (log_tx, mut log_rx) = mpsc::channel::<LogEntry>(buffer);
    tokio::spawn(async move {
        while let Some(entry) = log_rx.recv().await {
            log_request_details(entry.method, entry.uri, entry.headers, entry.body);
        }
    });
    log_tx
}
