mod config;
mod ingest;
mod metrics;
mod server;

use std::{env, sync::Arc};

use config::AggregationConfig;
use ingest::load_metric_store;
use server::{AppState, run_http_server};
use reqwest::Client;

#[tokio::main]
async fn main() {
    let agg_config = match AggregationConfig::load() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("failed to load aggregation config: {err}");
            return;
        }
    };

    eprintln!("loading metrics from CSV...");
    let store = match tokio::task::spawn_blocking(load_metric_store).await {
        Ok(Ok(store)) => store,
        Ok(Err(err)) => {
            eprintln!("failed to load metric store: {err}");
            return;
        }
        Err(join_err) => {
            eprintln!("loader task panicked: {join_err}");
            return;
        }
    };

    let state = AppState {
        store: Arc::new(store),
        agg_config: Arc::new(agg_config),
        http_client: Client::new(),
        upstream_url: env::var("UPSTREAM_URL")
            .unwrap_or_else(|_| "http://localhost:9200/cluster-metrics/_search".to_string()),
    };

    eprintln!("metrics ready, starting server on 0.0.0.0:10101");
    if let Err(err) = run_http_server(state).await {
        eprintln!("server error: {err}");
    }
}
