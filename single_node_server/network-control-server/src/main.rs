mod config;
mod ingest;
mod metrics;
mod server;

use std::{
    env,
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
    sync::{Arc, mpsc},
    thread,
    time::Instant,
};

use config::AggregationConfig;
use ingest::load_metric_store;
use reqwest::Client;
use server::{AppState, QueryCache, TimingSender, run_http_server, start_request_logger};

#[tokio::main]
async fn main() {
    // Parse CLI flags
    let args: Vec<String> = env::args().collect();
    let timing_enabled = args.iter().any(|arg| arg == "--timing");
    let no_ingest = args.iter().any(|arg| arg == "--no-ingest");
    if timing_enabled {
        eprintln!("timing enabled via --timing flag");
    }
    if no_ingest {
        eprintln!("ingest disabled via --no-ingest flag");
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
    let store = match tokio::task::spawn_blocking(move || load_metric_store(timing_enabled)).await {
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
        timing_sender: if timing_enabled {
            init_timing_sender()
        } else {
            None
        },
        no_ingest,
        cache: Arc::new(QueryCache::new(
            env::var("QUERY_CACHE_TTL_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(500),
        )),
        log_tx: Some(start_request_logger(1000)),
    };

    eprintln!("startup complete in {:.2?}", startup_start.elapsed());
    eprintln!("metrics ready, starting server on 0.0.0.0:10101");
    if let Err(err) = run_http_server(state).await {
        eprintln!("server error: {err}");
    }
}

fn init_timing_sender() -> Option<TimingSender> {
    let path =
        env::var("SERVER_TIMING_CSV").unwrap_or_else(|_| "server_request_timing.csv".to_string());
    let is_empty = Path::new(&path)
        .metadata()
        .map(|meta| meta.len() == 0)
        .unwrap_or(true);

    let file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("failed to open timing CSV {path}: {err}");
            return None;
        }
    };

    let mut writer = BufWriter::new(file);
    if is_empty {
        if let Err(err) = writeln!(
            writer,
            "request_id,request_type,status,total_ms,parse_json_ms,deserialize_ms,aggregations_ms,prepare_upstream_ms,upstream_ms,merge_ms,serialize_ms,parse_field_ms,validate_ms,query_percentiles_ms,build_response_ms"
        ) {
            eprintln!("failed to write timing CSV header: {err}");
            return None;
        }
        let _ = writer.flush();
    }

    let (sender, receiver) = mpsc::channel::<String>();
    thread::spawn(move || {
        for line in receiver {
            if writeln!(writer, "{line}").is_err() {
                break;
            }
            let _ = writer.flush();
        }
    });

    Some(sender)
}
