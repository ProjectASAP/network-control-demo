use std::collections::BTreeMap;

use axum::http::HeaderMap;
use axum::response::IntoResponse;
use serde_json::{Value, json};

use super::types::AppState;

pub(crate) async fn forward_to_upstream(
    state: &AppState,
    headers: &HeaderMap,
    body: &Value,
) -> Result<Value, axum::response::Response> {
    let mut upstream_req = state.http_client.post(&state.upstream_url).json(body);

    for (name, value) in headers.iter() {
        if name == axum::http::header::HOST
            || name == axum::http::header::CONTENT_TYPE
            || name == axum::http::header::CONTENT_LENGTH
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
