#!/bin/bash

controller_path="/users/yuanyc/network-control-demo/Controller/main_controller.py"
query_engine_rust="/users/yuanyc/asap-internal/QueryEngineRust/target/release/query_engine_rust"

output_dir="/users/yuanyc/asap-internal/QueryEngineRust/test_output"

python "${controller_path}" \
    --input_config "test_output/generated_workload_prometheus_20251219_092439_1.yaml" \
    --prometheus_scrape_interval 10 \
    --output_dir "${output_dir}" \
    --streaming_engine "arroyo"


"${query_engine_rust}" \
    --kafka-topic "flink_output" \
    --input-format "json" \
    --config "${output_dir}/inference_config.yaml" \
    --streaming-config "${output_dir}/streaming_config.yaml" \
    --streaming-engine "arroyo" \
    --prometheus-scrape-interval 10 \
    --output-dir "test_output" \
    --query-language "PROMQL" \
    --lock-strategy "per-key"