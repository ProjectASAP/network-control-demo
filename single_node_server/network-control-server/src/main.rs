mod config;
mod ingest;
mod metrics;
mod server;

use std::{env, sync::Arc, time::Instant};

use config::AggregationConfig;
use ingest::load_metric_store;
use reqwest::Client;
use server::{AppState, run_http_server};

#[tokio::main]
async fn main() {
    // Parse CLI flags
    let args: Vec<String> = env::args().collect();
    let timing_enabled = args.iter().any(|arg| arg == "--timing");
    if timing_enabled {
        eprintln!("timing enabled via --timing flag");
    }

    let startup_start = Instant::now();
    let config_start = Instant::now();
    let agg_config = match AggregationConfig::load() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("failed to load aggregation config: {err}");
            return;
        }
    };
    eprintln!(
        "aggregation config loaded in {:.2?}",
        config_start.elapsed()
    );

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
        timing_enabled,
    };

    eprintln!("startup complete in {:.2?}", startup_start.elapsed());
    eprintln!("metrics ready, starting server on 0.0.0.0:10101");
    if let Err(err) = run_http_server(state).await {
        eprintln!("server error: {err}");
    }
}
