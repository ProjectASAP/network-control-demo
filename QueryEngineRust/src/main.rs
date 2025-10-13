use clap::Parser;
use std::fs;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info};

use query_engine_rust::data_model::enums::{InputFormat, StreamingEngine};
use query_engine_rust::utils::file_io::{read_inference_config, read_streaming_config};
use query_engine_rust::{
    HttpServer, HttpServerConfig, KafkaConsumer, KafkaConsumerConfig, Result, SimpleEngine,
    SimpleMapStore,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Kafka topic to consume from
    #[arg(long)]
    kafka_topic: String,

    /// Input format for Kafka messages
    #[arg(long, value_enum)]
    input_format: InputFormat,

    /// Configuration file path
    #[arg(long)]
    config: String,

    /// File path for streaming_config
    #[arg(long)]
    streaming_config: String,

    /// Streaming engine to use
    #[arg(long, value_enum)]
    streaming_engine: StreamingEngine,

    /// Prometheus scrape interval in seconds
    #[arg(long)]
    prometheus_scrape_interval: u64,

    /// HTTP server port
    #[arg(long, default_value = "8088")]
    http_port: u16,

    /// Prometheus server URL
    #[arg(long, default_value = "http://localhost:9090")]
    prometheus_server: String,

    /// Forward unsupported queries to Prometheus
    #[arg(long)]
    forward_unsupported_queries: bool,

    /// Kafka broker address
    #[arg(long, default_value = "localhost:9092")]
    kafka_broker: String,

    /// Database path (currently unused, kept for compatibility)
    #[arg(long, default_value = "sketchdb.db")]
    db_path: String,

    /// Delete existing database (currently unused, kept for compatibility)
    #[arg(long)]
    delete_existing_db: bool,

    /// Output directory for logs
    #[arg(long)]
    output_dir: String,

    /// Log level
    #[arg(long, default_value = "INFO")]
    log_level: String,

    /// Enable profiling (currently unused, kept for compatibility)
    #[arg(long)]
    do_profiling: bool,

    /// Decompress JSON messages
    #[arg(long)]
    decompress_json: bool,

    /// Enable dumping received precomputes to files for debugging
    #[arg(long)]
    dump_precomputes: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Create output directory
    fs::create_dir_all(&args.output_dir)?;

    // Initialize logging similar to Python's create_loggers function
    setup_logging(&args.output_dir, &args.log_level)?;

    info!("Starting Query Engine Rust");
    info!("Config file: {}", args.config);
    info!("Output directory: {}", args.output_dir);

    // Read config (equivalent to utils.file_io.read_inference_config)
    let inference_config = read_inference_config(&args.config)?;
    info!(
        "Loaded inference config with {} query configs",
        inference_config.query_configs.len()
    );
    info!("Inference config: {:?}", inference_config);

    let streaming_config = Arc::new(read_streaming_config(
        &args.streaming_config,
        &inference_config,
    )?);
    info!(
        "Loaded streaming config with {} entries",
        streaming_config.get_all_aggregation_configs().len()
    );
    info!("Streaming config: {:?}", streaming_config);

    // Setup store (equivalent to Python's SimpleMapStore())
    let store = Arc::new(SimpleMapStore::new(streaming_config.clone()));

    // Setup query engine
    let engine = Arc::new(SimpleEngine::new(
        store.clone(),
        inference_config,
        streaming_config.clone(),
        args.prometheus_scrape_interval,
    ));

    // Setup Kafka consumer (equivalent to Python's kafka_thread)
    let kafka_config = KafkaConsumerConfig {
        broker: args.kafka_broker.clone(),
        topic: args.kafka_topic.clone(),
        group_id: "query-engine-rust".to_string(),
        auto_offset_reset: "beginning".to_string(),
        input_format: args.input_format,
        decompress_json: args.decompress_json,
        batch_size: 1000,
        poll_timeout_ms: 1000,
        streaming_engine: args.streaming_engine.clone(),
        dump_precomputes: args.dump_precomputes,
        dump_output_dir: if args.dump_precomputes {
            Some(args.output_dir.clone())
        } else {
            None
        },
    };

    let store_for_kafka = store.clone();
    let kafka_consumer_result =
        KafkaConsumer::new(kafka_config, store_for_kafka, streaming_config.clone());
    let kafka_handle = match kafka_consumer_result {
        Ok(mut consumer) => {
            info!("Starting Kafka consumer for topic: {}", args.kafka_topic);
            Some(tokio::spawn(async move {
                if let Err(e) = consumer.run().await {
                    error!("Kafka consumer error: {}", e);
                }
            }))
        }
        Err(e) => {
            error!("Failed to create Kafka consumer: {}", e);
            info!("Continuing without Kafka consumer");
            None
        }
    };

    // Setup HTTP server (equivalent to Python's server_thread)
    let http_config = HttpServerConfig {
        port: args.http_port,
        handle_http_requests: true,
        prometheus_server_url: args.prometheus_server.clone(),
        forward_unsupported_queries: args.forward_unsupported_queries,
    };

    let server = HttpServer::new(http_config, engine, store);
    info!("Starting HTTP server on port {}", args.http_port);

    // Wait for shutdown signal
    tokio::select! {
        result = server.run() => {
            if let Err(e) = result {
                error!("HTTP server error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            info!("Shutdown signal received");
        }
    }

    // Cleanup - gracefully shutdown Kafka consumer if it exists
    if let Some(handle) = kafka_handle {
        info!("Shutting down Kafka consumer...");
        handle.abort();
        let _ = handle.await;
    }

    info!("Shutdown complete");
    Ok(())
}

fn setup_logging(_output_dir: &str, log_level: &str) -> Result<()> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    // Create env filter that respects RUST_LOG, with fallback to command line arg
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_file(true)
                .with_line_number(true)
                .with_target(true),
        )
        .init();

    info!("Logging initialized (respects RUST_LOG environment variable)");
    Ok(())
}
