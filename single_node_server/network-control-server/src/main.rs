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
// use ingest::load_metric_store;
use metrics::MetricStore;
use reqwest::Client;
use server::{AppState, TimingSender, run_http_server, start_request_logger};

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

    let metric_store = MetricStore::new();

    // Startup ingestion is disabled; start with an empty store.
    // eprintln!("loading metrics from CSV...");
    // let store = match tokio::task::spawn_blocking(move || load_metric_store(timing_enabled)).await {
    //     Ok(Ok(store)) => store,
    //     Ok(Err(err)) => {
    //         eprintln!("failed to load metric store: {err}");
    //         return;
    //     }
    //     Err(join_err) => {
    //         eprintln!("loader task panicked: {join_err}");
    //         return;
    //     }
    // };

    let state = AppState {
        metric_store: Arc::new(metric_store),
        current_epoch: Arc::new(std::sync::Mutex::new(None)),
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
            "request_id,request_type,status,total_ms,estimate_ms,json_ms"
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
