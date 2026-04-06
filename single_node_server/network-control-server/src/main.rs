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

use config::ServerRuntimeConfig;
use metrics::{InMemoryNodeStore, RangeKeyCatalog};
use reqwest::Client;
use server::{
    AppState, DefaultRequestPlanner, EsFallbackUpstreamClient, SketchAggregationEngine,
    TimingSender, run_http_server, start_request_logger,
};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let startup_start = Instant::now();
    let mut runtime_config = match ServerRuntimeConfig::load_from_env_and_args(&args) {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("failed to load server runtime config: {err}");
            return;
        }
    };
    if args.iter().any(|arg| arg == "--timing") {
        runtime_config.server.enable_timing = true;
    }

    let catalog = match RangeKeyCatalog::from_config(&runtime_config.storage.node_catalog) {
        Ok(catalog) => catalog,
        Err(err) => {
            eprintln!("failed to build key catalog: {err}");
            return;
        }
    };
    let store = InMemoryNodeStore::from_catalog(&catalog);

    eprintln!(
        "resolved config: bind={} index={} upstream_mode={} timing={}",
        runtime_config.bind_addr(),
        runtime_config.api.index_name,
        runtime_config.upstream.mode,
        runtime_config.server.enable_timing
    );

    let timing_sender = if runtime_config.server.enable_timing {
        init_timing_sender(&runtime_config.server.timing_csv_path)
    } else {
        None
    };

    let state = AppState {
        store: Arc::new(store),
        current_epoch: Arc::new(std::sync::Mutex::new(None)),
        runtime_config: Arc::new(runtime_config.clone()),
        aggregation_engine: Arc::new(SketchAggregationEngine),
        request_planner: Arc::new(DefaultRequestPlanner),
        upstream_client: Arc::new(EsFallbackUpstreamClient),
        http_client: Client::new(),
        timing_enabled: runtime_config.server.enable_timing,
        timing_sender,
        log_tx: Some(start_request_logger(
            runtime_config.server.request_log_buffer,
        )),
    };

    eprintln!("startup complete in {:.2?}", startup_start.elapsed());
    eprintln!("server listening on {}", runtime_config.bind_addr());
    if let Err(err) = run_http_server(state).await {
        eprintln!("server error: {err}");
    }
}

fn init_timing_sender(path: &str) -> Option<TimingSender> {
    let is_empty = Path::new(path)
        .metadata()
        .map(|meta| meta.len() == 0)
        .unwrap_or(true);

    let file = match OpenOptions::new().create(true).append(true).open(path) {
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
