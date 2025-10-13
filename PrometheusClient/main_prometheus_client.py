import os
import yaml
import time
import requests
import argparse
import datetime
import numpy as np
import logging

# import urllib3
from loguru import logger
from typing import Dict
import threading
import subprocess
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import similarity_scores
from prometheus_api_client import PrometheusConnect
from classes.config import Config
from classes.QueryLatencyExporter import QueryLatencyExporter
from promql_utilities.query_results.classes import QueryResult, QueryResultAcrossTime
from promql_utilities.query_results.serializers import SerializerFactory


class PrometheusDebugRetry(Retry):
    def __init__(self, server_name, *args, **kwargs):
        self.server_name = server_name
        super().__init__(*args, **kwargs)

    def increment(
        self,
        method=None,
        url=None,
        response=None,
        error=None,
        _pool=None,
        _stacktrace=None,
    ):
        # Calculate current attempt number
        current_retries = self.total - (
            self.total if hasattr(self, "history") and self.history else 0
        )
        attempt_num = (3 - current_retries) + 1  # Assuming max 3 retries

        if response:
            logger.bind(module="http_debug").debug(
                f"RETRY ATTEMPT {attempt_num} for {self.server_name}: "
                f"{method} {url} -> HTTP {response.status} "
                f"(will retry: {response.status in self.status_forcelist})"
            )
        elif error:
            logger.bind(module="http_debug").debug(
                f"RETRY ATTEMPT {attempt_num} for {self.server_name}: "
                f"{method} {url} -> ERROR: {type(error).__name__}: {error}"
            )

        return super().increment(method, url, response, error, _pool, _stacktrace)


class PrometheusDebugHTTPAdapter(HTTPAdapter):
    def __init__(self, server_name, *args, **kwargs):
        self.server_name = server_name
        super().__init__(*args, **kwargs)

    def send(self, request, *args, **kwargs):
        logger.bind(module="http_debug").debug(
            f"HTTP REQUEST START for {self.server_name}: "
            f"{request.method} {request.url}"
        )
        start_time = time.time()

        try:
            response = super().send(request, *args, **kwargs)
            elapsed = time.time() - start_time

            logger.bind(module="http_debug").debug(
                f"HTTP REQUEST END for {self.server_name}: "
                f"{request.method} {request.url} -> HTTP {response.status_code} "
                f"({elapsed:.3f}s, {len(response.content)} bytes)"
            )
            return response
        except Exception as e:
            elapsed = time.time() - start_time
            logger.bind(module="http_debug").error(
                f"HTTP REQUEST FAILED for {self.server_name}: "
                f"{request.method} {request.url} -> {type(e).__name__}: {e} "
                f"(after {elapsed:.3f}s)"
            )
            raise


def create_loggers(logging_dir, log_level):
    logger.remove(None)  # remove default loggers

    logger.add("{}/prometheus_client.log".format(logging_dir), filter="__main__")

    logger.add(  # add latency exporter logger
        "{}/query_latency_exporter.log".format(logging_dir),
        level=log_level,
        filter=lambda record: record["extra"].get("module") == "query_latency_exporter",
    )

    # NEW: HTTP request debugging logger
    logger.add(
        "{}/http_requests.log".format(logging_dir),
        level="DEBUG",
        filter=lambda record: record["extra"].get("module") == "http_debug",
    )

    # Enable urllib3 debug logging for connection-level details
    urllib3_logger = logging.getLogger("urllib3.connectionpool")
    urllib3_logger.setLevel(logging.DEBUG)
    urllib3_handler = logging.FileHandler("{}/urllib3_debug.log".format(logging_dir))
    urllib3_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    urllib3_logger.addHandler(urllib3_handler)


def compare_results(results_across_servers: Dict[str, QueryResult], fout):
    # each result is a list of dictionaries
    # each dictionary has the following keys: metric, value
    # metric is a dictionary representing metadata about the metric
    # value is a list, where the first element is the timestamp and the second element is the value
    # for each unique metadata, we need to compare the values

    server_names = list(results_across_servers.keys())
    assert sorted(server_names) == ["prometheus", "sketchdb"]

    # prom_dict = {
    #     frozenset(prom["metric"].items()): prom["value"][1] for prom in results_prom
    # }
    # sketch_dict = {
    #     frozenset(sketch["metric"].items()): sketch["value"][1]
    #     for sketch in results_sketchdb
    # }
    prom_dict = results_across_servers["prometheus"].result
    sketch_dict = results_across_servers["sketchdb"].result
    assert prom_dict is not None
    assert sketch_dict is not None

    for metric in prom_dict:
        if metric not in sketch_dict:
            print(
                f"Metric {dict(metric)} found in Prometheus but not in SketchDB",
                file=fout,
            )
            continue
        prom_value = float(prom_dict[metric])
        sketch_value = float(sketch_dict[metric])
        if prom_value != sketch_value:
            if prom_value == 0:
                error = np.inf
            else:
                error = (prom_value - sketch_value) / prom_value * 100
            print(
                f"Error found for metric {dict(metric)}: Error = {error}% Prometheus value = {prom_value}, SketchDB value = {sketch_value}",
                file=fout,
            )
        else:
            pass
            # print(f"Values match for metric {dict(metric)}: {prom_value}", file=fout)

    for metric in sketch_dict:
        if metric not in prom_dict:
            print(
                f"Metric {dict(metric)} found in SketchDB but not in Prometheus",
                file=fout,
            )


def get_timeseries_similarity_scores(
    results_across_servers, query_group, similarity_functions
):
    similarity_scores = {
        f.__name__: {q: 0 for q in query_group.queries} for f in similarity_functions
    }

    for f in similarity_functions:
        for query_idx, query in enumerate(query_group.queries):
            prom_results = results_across_servers["prometheus"][
                query_idx
            ].get_all_timeseries()
            sketchdb_results = results_across_servers["sketchdb"][
                query_idx
            ].get_all_timeseries()

            scores_per_key = {}

            for timeseries_key in prom_results:
                if timeseries_key not in sketchdb_results:
                    print(
                        f"Skipping timeseries {timeseries_key} because it is not present in SketchDB"
                    )
                    continue

                prom_timeseries = prom_results[timeseries_key].values
                sketchdb_timeseries = sketchdb_results[timeseries_key].values

                score = f(prom_timeseries, sketchdb_timeseries)
                scores_per_key[timeseries_key] = score
                similarity_scores[f.__name__][query] += score / len(prom_results)

    return similarity_scores


def get_query_unix_time(query, query_unix_time, query_start_times, repetition_delay):
    if query_start_times is None or query not in query_start_times:
        return query_unix_time

    query_alignment_time = query_start_times[query]
    # we want the latest timestamp that is query_aligment_time + N * repetition_delay
    query_unix_time = (
        query_unix_time - (query_unix_time - query_alignment_time) % repetition_delay
    )
    return query_unix_time


def execute_single_query(
    server_name,
    server_object,
    query,
    query_idx,
    repetition_idx,
    query_unix_time,
    dry_run,
) -> QueryResult:
    """Execute a single query and return the result with latency information."""
    logger.debug(
        f"Running query {query} on server {server_name} at time {query_unix_time}"
    )

    # Enhanced HTTP debug logging for query start
    logger.bind(module="http_debug").info(
        f"QUERY START - Server: {server_name}, Query: {query[:100]}{'...' if len(query) > 100 else ''}, "
        f"QueryIdx: {query_idx}, Rep: {repetition_idx}, Time: {query_unix_time}"
    )

    empty_query_result = QueryResult(
        server_name,
        query,
        query_idx,
        repetition_idx,
        result=None,
        latency=None,
        cumulative_latency=None,
    )

    if dry_run:
        logger.bind(module="http_debug").debug(
            f"DRY RUN - Skipping actual HTTP request for {server_name}"
        )
        return empty_query_result

    try:
        query_start_time = time.time()
        # The actual HTTP request happens here - will be logged by our adapter
        if query_unix_time:
            query_result = server_object.custom_query(
                query=query, params={"time": query_unix_time}
            )
        else:
            query_result = server_object.custom_query(query=query)
        query_end_time = time.time()

        latency = query_end_time - query_start_time
        logger.debug("Latency: {}", latency)

        # Enhanced HTTP debug logging for query success
        logger.debug("Query result: {}", query_result)
        result_count = len(query_result) if query_result else 0
        logger.bind(module="http_debug").info(
            f"QUERY SUCCESS - Server: {server_name}, Total latency: {latency:.3f}s, "
            f"Results: {result_count} data points"
        )

    except Exception as e:
        logger.error(f"Error running query: {str(e)}")

        # Enhanced HTTP debug logging for query error
        logger.bind(module="http_debug").error(
            f"QUERY ERROR - Server: {server_name}, Error: {type(e).__name__}: {e}"
        )
        return empty_query_result

    return QueryResult(
        server_name,
        query,
        query_idx,
        repetition_idx,
        result=query_result,
        latency=latency,
        cumulative_latency=None,
    )


def handle_query_group(
    servers,
    query_group,
    query_start_times,
    dry_run,
    parallel=False,
    latency_exporter=None,
    streaming_serializer=None,
):
    logger.debug(f"Starting query group {query_group.id}")
    if query_group.starting_delay:
        logger.debug(
            f"Waiting for {query_group.starting_delay} seconds before starting"
        )
        time.sleep(query_group.starting_delay)

    logger.debug("Query start times: {}", query_start_times)

    current_time = None
    query_unix_time = None

    result = {
        server_name: {
            query_idx: QueryResultAcrossTime(
                server_name, query, query_idx, query_group.repetitions
            )
            for query_idx, query in enumerate(query_group.queries)
        }
        for server_name in servers
    }

    for repetition_idx in range(query_group.repetitions):
        current_time = datetime.datetime.now()
        logger.debug("Current unix time: {}", int(current_time.timestamp()))

        if hasattr(query_group, "query_time_offset"):
            current_time = current_time - datetime.timedelta(
                seconds=query_group.query_time_offset
            )
            logger.debug(
                "Offsetting query time by {} seconds", query_group.query_time_offset
            )

        query_unix_time = int(current_time.timestamp())
        logger.debug("Unix time after query_time_offset: {}", query_unix_time)

        if parallel:
            # Execute queries in parallel
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for query_idx, query in enumerate(query_group.queries):
                    current_query_unix_time = get_query_unix_time(
                        query,
                        query_unix_time,
                        query_start_times,
                        query_group.repetition_delay,
                    )

                    for server_name, server_object in servers.items():
                        futures.append(
                            executor.submit(
                                execute_single_query,
                                server_name,
                                server_object,
                                query,
                                query_idx,
                                repetition_idx,
                                current_query_unix_time,
                                dry_run,
                            )
                        )

                # Collect results
                for future in concurrent.futures.as_completed(futures):
                    query_result = future.result()
                    server_name = query_result.server_name
                    query_idx = query_result.query_idx

                    query_result.cumulative_latency = query_result.latency

                    result[server_name][query_idx].add_result(query_result)

                    # Stream result immediately if streaming serializer is provided
                    if streaming_serializer is not None and not dry_run:
                        streaming_serializer.streaming_write_result(query_result)
        else:
            # Reset cumulative latency for each repetition
            cumulative_latency = {server_name: 0.0 for server_name in servers}

            # Serial execution - use the same execute_single_query function
            for query_idx, query in enumerate(query_group.queries):
                current_query_unix_time = get_query_unix_time(
                    query,
                    query_unix_time,
                    query_start_times,
                    query_group.repetition_delay,
                )

                logger.debug("Unix time for query: {}", current_query_unix_time)

                for server_name, server_object in servers.items():
                    query_result = execute_single_query(
                        server_name,
                        server_object,
                        query,
                        query_idx,
                        repetition_idx,
                        current_query_unix_time,
                        dry_run,
                    )

                    # Update cumulative latency for this repetition
                    if query_result.latency is not None:
                        cumulative_latency[server_name] += query_result.latency

                    query_result.cumulative_latency = cumulative_latency[server_name]

                    result[server_name][query_idx].add_result(query_result)

                    # Stream result immediately if streaming serializer is provided
                    if streaming_serializer is not None and not dry_run:
                        streaming_serializer.streaming_write_result(query_result)

        if latency_exporter is not None:
            latency_exporter.export_repetition(repetition_idx, result)

        if repetition_idx < query_group.repetitions - 1:
            time.sleep(query_group.repetition_delay)

    if latency_exporter is not None:
        latency_exporter.shutdown()

    return result


def get_query_start_times(server_url, query_engine_config):
    aggregation_id_start_time_map = {}
    query_aggregation_id_map = {}
    query_start_time_map = {}

    required_aggregation_ids = set()
    for query_yaml in query_engine_config["queries"]:
        # add all aggregation IDs from the query YAML to the required_aggregation_ids set
        required_aggregation_ids.update(
            int(aggregation["aggregation_id"])
            for aggregation in query_yaml["aggregations"]
        )
        # assert len(query_yaml["aggregations"]) == 1
        # required_aggregation_ids.add(
        #    int(query_yaml["aggregations"][0]["aggregation_id"])
        # )
    logger.debug("Required aggregation IDs: {}", required_aggregation_ids)

    # wait for all required aggregation IDs to be present
    while True:
        server_response = requests.get(
            server_url + "/api/v1/status/runtimeinfo",
            headers={"Content-Type": "application/json"},
        )
        server_response.raise_for_status()
        server_response_json = server_response.json()
        logger.debug("Server response: {}", server_response_json)
        aggregation_id_start_time_map = server_response_json["data"][
            "earliest_timestamp_per_aggregation_id"
        ]

        # change all keys from string to int
        aggregation_id_start_time_map = {
            int(k): v for k, v in aggregation_id_start_time_map.items()
        }

        if not set(aggregation_id_start_time_map.keys()).issuperset(
            required_aggregation_ids
        ):
            logger.debug(
                "Waiting for aggregation IDs {} to be present",
                required_aggregation_ids - set(aggregation_id_start_time_map.keys()),
            )
            time.sleep(10)
        else:
            break

    # TODO: make this more robust. What happens if there are multiple aggregations with
    # different tumbling windows? How long do we wait here? What happens with multiple query groups?

    # get query to aggregate ID mapping from query_engine_config
    for query_yaml in query_engine_config["queries"]:
        # TODO: this assert will fail if there are multiple aggregations in a query YAML, including for DeltaSet, so commenting it out
        # assert len(query_yaml["aggregations"]) == 1
        # for now, just take the first aggregation ID
        # TODO: make this more robust, eg for cases where aggregations for the same query have different tumbling windows or start times
        query_aggregation_id_map[query_yaml["query"]] = int(
            query_yaml["aggregations"][0]["aggregation_id"]
        )

    for query, aggregation_id in query_aggregation_id_map.items():
        # aggregation_id_start_time_map is in milliseconds, convert to seconds
        query_start_time_map[query] = (
            # aggregation_id_start_time_map[str(aggregation_id)] / 1000
            aggregation_id_start_time_map[aggregation_id]
            / 1000
        )

    return query_start_time_map


def check_args(args):
    if args.align_query_time and args.query_engine_config_file is None:
        raise ValueError(
            "If align_query_time is set, query_engine_config_file must be provided"
        )


def start_query_engine_profiler(pid, output_dir, starting_delay, duration):
    """
    Create and start a subprocess to run py-spy on the specified process.

    Args:
        pid: Process ID of the query engine
        output_dir: Directory to save the profile output
        duration: Duration in seconds to run the profiler

    Returns:
        subprocess.Popen: The created subprocess
    """
    output_file = os.path.join(output_dir, "query_engine_profile.svg")
    logger.debug(f"Waiting for {starting_delay} seconds before starting profiler")
    time.sleep(starting_delay)
    logger.debug(f"Starting py-spy profiling of PID {pid} for {duration} seconds")

    try:
        cmd = "bash --login -c 'sudo env \"PATH=$PATH\" py-spy record --pid {} -o {} --duration {} --idle'".format(
            str(pid), output_file, str(duration)
        )
        logger.info(f"Running command: {cmd}")

        subprocess.run(cmd, shell=True)
    except Exception as e:
        logger.error(f"Error starting profiler: {str(e)}")
        raise e


def start_prometheus_profiler(output_dir, starting_delay, duration):
    output_file = os.path.join(output_dir, "prometheus_profile.pprof")
    logger.debug(f"Waiting for {starting_delay} seconds before starting profiler")
    time.sleep(starting_delay)
    logger.debug(f"Starting pprof profiling of Prometheus for {duration} seconds")

    try:
        # cmd = "go tool pprof -seconds {} -output {} http://localhost:9090/debug/pprof/profile".format(
        cmd = "curl -o {} http://localhost:9090/debug/pprof/profile?seconds={}".format(
            output_file,
            str(duration),
        )
        logger.info(f"Running command: {cmd}")

        subprocess.run(cmd, shell=True)
    except Exception as e:
        logger.error(f"Error starting profiler: {str(e)}")


def main(args):
    check_args(args)
    os.makedirs(args.output_dir, exist_ok=True)

    create_loggers(args.output_dir, "DEBUG")

    if args.dry_run:
        logger.info("Running in dry-run mode")

    if args.parallel:
        logger.info("Running queries in parallel mode")

    with open(args.config_file, "r") as file:
        config_data = yaml.safe_load(file)

    query_engine_config = None
    if args.query_engine_config_file:
        with open(args.query_engine_config_file, "r") as file:
            query_engine_config = yaml.safe_load(file)

    config = Config.from_dict(config_data)

    logger.debug("Read config")

    # can spawn one thread per query group, if we need to support multiple query groups
    if len(config.query_groups) != 1:
        raise ValueError("Only one query group is supported for now")

    server_url_for_alignment = None

    servers = {}
    for server in config.servers:
        # Create custom retry adapter with debug logging
        debug_retry = PrometheusDebugRetry(
            server_name=server.name,
            total=3,
            backoff_factor=1,
            status_forcelist=[408, 429, 500, 502, 503, 504],
        )

        prom_connect = PrometheusConnect(
            url=server.url,
            disable_ssl=True,
            retry=debug_retry,
        )

        # Still need to replace adapters for the send() method logging
        debug_adapter = PrometheusDebugHTTPAdapter(server.name)
        prom_connect._session.mount("http://", debug_adapter)
        prom_connect._session.mount("https://", debug_adapter)

        servers[server.name] = prom_connect
        logger.debug(
            "Connected to server {} with HTTP debug logging enabled", server.name
        )

        if args.align_query_time and server.name == args.server_for_alignment:
            server_url_for_alignment = server.url

    query_start_times = None
    if args.align_query_time:
        query_start_times = get_query_start_times(
            server_url_for_alignment, query_engine_config
        )
        logger.debug("Got query start times")
    query_group = config.query_groups[0]

    query_engine_profiler_thread = None
    if args.profile_query_engine_pid:
        query_engine_profiler_thread = threading.Thread(
            target=start_query_engine_profiler,
            args=(
                args.profile_query_engine_pid,
                args.output_dir,
                query_group.starting_delay,
                query_group.repetition_delay * query_group.repetitions,
            ),
        )
        if query_engine_profiler_thread:
            logger.debug("Starting query engine profiler thread...")
            query_engine_profiler_thread.daemon = True
            query_engine_profiler_thread.start()

    prometheus_profiler_thread = None
    if args.profile_prometheus_time is not None:
        prometheus_profiler_thread = threading.Thread(
            target=start_prometheus_profiler,
            args=(
                args.output_dir,
                query_group.starting_delay,
                args.profile_prometheus_time,
            ),
        )
        if prometheus_profiler_thread:
            prometheus_profiler_thread.daemon = True
            prometheus_profiler_thread.start()

    if args.export_latencies_for_prometheus is not None:
        exporter_socket_addr = args.export_latencies_for_prometheus.split(sep=":")
        exporter_ip = exporter_socket_addr[0]
        exporter_port = int(exporter_socket_addr[1])
        latency_exporter = QueryLatencyExporter(addr=exporter_ip, port=exporter_port)
        logger.debug(
            f"Running with query latency exporter at {args.export_latencies_for_prometheus}"
        )
        latency_exporter.launch()
    else:
        latency_exporter = None

    # Initialize streaming serializer if not in dry run mode
    streaming_serializer = None
    if not args.dry_run:
        streaming_serializer = SerializerFactory.create(
            args.serialization_format, args.output_dir
        )

        # Prepare metadata for streaming
        metadata = {
            "queries": query_group.queries,
            "servers": list(servers.keys()),
            "repetitions": query_group.repetitions,
            "total_queries": len(query_group.queries),
        }
        streaming_serializer.streaming_write_start(metadata)

    results_across_servers = handle_query_group(
        servers,
        query_group,
        query_start_times,
        args.dry_run,
        args.parallel,
        latency_exporter,
        streaming_serializer,
    )

    if not args.dry_run:
        # Finalize streaming write
        streaming_serializer.streaming_write_end()

        # deprecated: save results in a pickle file
        # with open(os.path.join(args.output_dir, args.result_output_file), "wb") as fout:
        #    pickle.dump(results_across_servers, fout)

    if not args.dry_run and args.compare_results:
        timeseries_similarity_scores = get_timeseries_similarity_scores(
            results_across_servers,
            query_group,
            [
                similarity_scores.correlation,
                similarity_scores.l1_norm,
                similarity_scores.l2_norm,
            ],
        )

        with open(os.path.join(args.output_dir, args.output_file), "w") as fout:
            for f in timeseries_similarity_scores:
                for query in timeseries_similarity_scores[f]:
                    print(
                        f"{f}: {query} = {timeseries_similarity_scores[f][query]}",
                        file=fout,
                    )

            assert query_group.repetitions is not None
            for query_idx in range(len(query_group.queries)):
                for repetition_idx in range(query_group.repetitions):
                    print(f"Query {query_idx}, Repetition {repetition_idx}", file=fout)
                    compare_results(
                        {
                            "prometheus": results_across_servers["prometheus"][
                                query_idx
                            ].query_results[repetition_idx],
                            "sketchdb": results_across_servers["sketchdb"][
                                query_idx
                            ].query_results[repetition_idx],
                        },
                        fout,
                    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--config_file", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    parser.add_argument("--output_file", type=str, required=True)
    # deprecated:
    # parser.add_argument("--result_output_file", type=str, default="results.pkl")

    parser.add_argument("--query_engine_config_file", type=str, required=False)
    parser.add_argument("--align_query_time", action="store_true", required=False)
    parser.add_argument("--server_for_alignment", type=str, default="sketchdb")

    parser.add_argument("--dry_run", action="store_true", required=False)
    parser.add_argument("--compare_results", action="store_true", required=False)
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Execute queries in parallel",
        required=False,
    )

    parser.add_argument("--profile_query_engine_pid", type=int, required=False)
    parser.add_argument("--profile_prometheus_time", type=int, required=False)

    parser.add_argument(
        "--export_latencies_for_prometheus",
        type=str,
        help="Run prometheus query latency exporter at <IP:PORT>",
        required=False,
    )

    parser.add_argument(
        "--serialization_format",
        type=str,
        choices=["jsonl", "parquet"],
        default="jsonl",
        help="Format for serializing query results (jsonl or parquet)",
        required=False,
    )

    args = parser.parse_args()
    main(args)
