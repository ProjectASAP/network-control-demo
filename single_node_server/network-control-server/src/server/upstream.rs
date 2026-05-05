use std::collections::{BTreeMap, HashSet};

use async_trait::async_trait;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use serde_json::{Value, json};

use super::types::{AppState, UpstreamClient};

pub struct EsFallbackUpstreamClient;

#[async_trait]
impl UpstreamClient for EsFallbackUpstreamClient {
    async fn forward(
        &self,
        state: &AppState,
        index_name: &str,
        headers: &HeaderMap,
        body: &Value,
    ) -> Result<Value, axum::response::Response> {
        let Some(url) = state.runtime_config.upstream_search_url_for(index_name) else {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "upstream fallback requested but upstream search URL is not configured"
                    .to_string(),
            )
                .into_response());
        };
        let allowed_headers: HashSet<String> = state
            .runtime_config
            .upstream
            .forward_headers
            .iter()
            .map(|value| value.trim().to_ascii_lowercase())
            .collect();
        let mut upstream_req = state.http_client.post(&url).json(body);

        if let Some(api_key) = &state.runtime_config.upstream.es_api_key {
            upstream_req = upstream_req.header(
                axum::http::header::AUTHORIZATION,
                format!("ApiKey {api_key}"),
            );
        }

        for (name, value) in headers.iter() {
            if name == axum::http::header::HOST
                || name == axum::http::header::CONTENT_TYPE
                || name == axum::http::header::CONTENT_LENGTH
            {
                continue;
            }
            if !allowed_headers.is_empty()
                && !allowed_headers.contains(&name.as_str().to_ascii_lowercase())
            {
                continue;
            }
            upstream_req = upstream_req.header(name, value);
        }

        let upstream_resp = match upstream_req.send().await {
            Ok(resp) => resp,
            Err(err) => {
                return Err((
                    axum::http::StatusCode::BAD_GATEWAY,
                    format!("failed to contact upstream elasticsearch: {err}"),
                )
                    .into_response());
            }
        };

        let body_val: Value = upstream_resp.json().await.unwrap_or_else(|_| Value::Null);
        Ok(body_val)
    }
}

pub(crate) fn merge_aggregations(response: &mut Value, handled: BTreeMap<String, Value>) {
    let obj = match response.as_object_mut() {
        Some(obj) => obj,
        None => {
            *response = json!({ "aggregations": handled });
            return;
        }
    };

    let aggs = obj.entry("aggregations").or_insert_with(|| json!({}));
    if let Some(aggs_obj) = aggs.as_object_mut() {
        for (name, value) in handled {
            aggs_obj.insert(name, value);
        }
    } else {
        *aggs = json!(handled);
    }
}
