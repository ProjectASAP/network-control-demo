import os
import json
import yaml
import argparse
from jinja2 import Template
from typing import Tuple, List

from utils import arroyo_utils, http_utils, jinja_utils
from classes.MetricConfig import MetricConfig
from classes.StreamingAggregationConfig import StreamingAggregationConfig


def check_args(args):
    if args.output_file_path:
        raise NotImplementedError("Output file path is not implemented yet")

    # Validate source type specific parameters
    if args.source_type == "kafka":
        if args.input_kafka_topic is None:
            raise ValueError("Input Kafka topic is required when using Kafka source")
        if args.kafka_input_format != "json":
            raise NotImplementedError(
                "Kafka input format {} is not implemented yet".format(
                    args.kafka_input_format
                )
            )
    elif args.source_type == "prometheus_remote_write":
        if args.prometheus_base_port is None:
            raise ValueError(
                "Prometheus base port is required when using prometheus_remote_write source"
            )
        if args.prometheus_path is None:
            raise ValueError(
                "Prometheus path is required when using prometheus_remote_write source"
            )
        if args.prometheus_bind_ip is None:
            raise ValueError(
                "Prometheus bind IP is required when using prometheus_remote_write source"
            )
        if args.parallelism is None:
            raise ValueError(
                "Parallelism is required when using prometheus_remote_write source"
            )
    elif args.source_type == "file":
        if args.input_file_path is None:
            raise ValueError("Input file path is required when using file source")
        raise NotImplementedError("File source is not implemented yet")

    if args.output_kafka_topic is None:
        raise ValueError("Output Kafka topic is required")

    if args.output_format != "json":
        raise NotImplementedError(
            "Output format {} is not implemented yet".format(args.output_format)
        )


def create_connection_profile(args, template_dir) -> str:
    """Create a connection profile JSON based on template"""
    template = jinja_utils.load_template(template_dir, "connection_profile.j2")

    rendered = template.render(
        profile_name=args.profile_name, bootstrap_servers=args.bootstrap_servers
    )

    # Save to file
    output_path = os.path.join(args.output_dir, "connection_profile.json")
    with open(output_path, "w") as f:
        f.write(rendered)

    print(f"Created connection profile at: {output_path}")

    if args.dry_run:
        # Generate a dummy profile ID for dry run
        profile_id = "dry_run_profile_id"
        print(f"[DRY RUN] Would create connection profile with ID: {profile_id}")
        return profile_id

    # If API URL provided, create connection profile via API
    response = http_utils.create_arroyo_resource(
        arroyo_url=args.arroyo_url,
        endpoint="connection_profiles",
        data=rendered,
        resource_type="connection profile",
    )
    profile_id = json.loads(response).get("id")

    return profile_id


def delete_connection_profile(args):
    if args.dry_run:
        print(
            f"[DRY RUN] Would delete connection profiles with name: {args.profile_name}"
        )
        return

    # list all connection profiles
    response = http_utils.make_api_request(
        url=f"{args.arroyo_url}/connection_profiles",
        method="get",
    )
    response = json.loads(response)

    # get the ID of the connection profile with the name args.profile_name
    profiles = [
        profile for profile in response["data"] if profile["name"] == args.profile_name
    ]
    if len(profiles) == 0:
        print(f"No connection profile found with name {args.profile_name}")
        return

    # delete the connection profile with the ID
    for profile in profiles:
        http_utils.make_api_request(
            url=f"{args.arroyo_url}/connection_profiles/{profile['id']}",
            method="delete",
        )


def create_source_connection_table(
    args,
    topic_name,
    table_name,
    profile_id,
    metric_labels: List[str],
    template_dir,
):
    """Create a connection table JSON (source) based on template"""

    # Select template based on source type
    if args.source_type == "kafka":
        template_name = "connection_table_kafka.j2"
    elif args.source_type == "prometheus_remote_write":
        template_name = "connection_table_prometheus_remote_write.j2"
    elif args.source_type == "file":
        template_name = "connection_table_file.j2"
    else:
        raise ValueError(f"Unsupported source type: {args.source_type}")

    template = jinja_utils.load_template(template_dir, template_name)

    # Create JSON schema definition for label fields
    label_properties = {}
    label_fields_json = []

    for field in metric_labels:
        # Add field to JSON schema properties
        label_properties[field] = {"type": "string", "description": f"{field} label"}

        # Add field to fields array for schema
        label_fields_json.append(
            {
                "fieldName": field,
                "fieldType": {"type": {"primitive": "String"}, "sqlName": "TEXT"},
                "nullable": False,
                "metadataKey": None,
            }
        )

    # Generate the complete JSON schema definition
    json_schema = {
        "type": "object",
        "required": ["labels", "value", "name", "timestamp"],
        "properties": {
            "labels": {
                "type": "object",
                "required": metric_labels,
                "properties": label_properties,
                "additionalProperties": False,
            },
            "value": {"type": "number", "description": "Metric value"},
            "name": {"type": "string", "description": "Metric name"},
            "timestamp": {
                "type": "string",
                "format": "date-time",
                "description": "Time when the metric was recorded, in RFC 3339 format",
            },
        },
        "additionalProperties": False,
    }

    if args.source_type == "kafka":
        json_schema["properties"]["timestamp"] = {
            "type": "string",
            "format": "date-time",
            "description": "Time when the metric was recorded, in RFC 3339 format",
        }
    elif args.source_type == "prometheus_remote_write":
        json_schema["properties"]["timestamp"] = {
            "type": "integer",
            "description": "Unix timestamp in milliseconds when the metric was recorded",
        }

    template_vars = {
        "table_name": table_name,
        "label_fields": label_fields_json,
        "json_schema": json.dumps(json_schema, indent=2)
        .replace("\n", "\\n")
        .replace('"', '\\"'),
    }

    if args.source_type == "kafka":
        template_vars["topic_name"] = topic_name
        template_vars["profile_id"] = profile_id
    elif args.source_type == "prometheus_remote_write":
        template_vars["base_port"] = args.prometheus_base_port
        template_vars["parallelism"] = args.parallelism
        template_vars["path"] = args.prometheus_path
        template_vars["bind_ip"] = args.prometheus_bind_ip
    elif args.source_type == "file":
        template_vars["file_path"] = args.input_file_path

    rendered = template.render(**template_vars)

    # Save to file
    filename = "connection_table_source.json"
    output_path = os.path.join(args.output_dir, filename)
    with open(output_path, "w") as f:
        f.write(rendered)

    print(f"Created source table at: {output_path}")

    if args.dry_run:
        print(f"[DRY RUN] Would create source connection table: {table_name}")
        return

    # If API URL provided, create connection table via API
    http_utils.create_arroyo_resource(
        arroyo_url=args.arroyo_url,
        endpoint="connection_tables",
        data=rendered,
        resource_type="source table",
    )


def create_sink_connection_table(
    args,
    topic_name,
    table_name,
    profile_id,
    template_dir,
):
    """Create a connection table JSON (sink) based on template"""

    template = jinja_utils.load_template(template_dir, "connection_table_sink.j2")

    rendered = template.render(
        table_name=table_name, topic_name=topic_name, profile_id=profile_id
    )

    # Save to file
    filename = "connection_table_sink.json"
    output_path = os.path.join(args.output_dir, filename)
    with open(output_path, "w") as f:
        f.write(rendered)

    print(f"Created sink table at: {output_path}")

    if args.dry_run:
        print(f"[DRY RUN] Would create sink connection table: {table_name}")
        return

    # If API URL provided, create connection table via API
    http_utils.create_arroyo_resource(
        arroyo_url=args.arroyo_url,
        endpoint="connection_tables",
        data=rendered,
        resource_type="sink table",
    )


def delete_connection_table(args, table_name):
    if args.dry_run:
        print(f"[DRY RUN] Would delete connection table: {table_name}")
        return

    # list all connection tables
    response = http_utils.make_api_request(
        url=f"{args.arroyo_url}/connection_tables",
        method="get",
    )
    response = json.loads(response)

    # get the ID of the connection table with table_name
    tables = [table for table in response["data"] if table["name"] == table_name]
    if len(tables) == 0:
        print(f"No connection table found with name {table_name}")
        return

    # delete the connection table with the ID
    for table in tables:
        http_utils.make_api_request(
            url=f"{args.arroyo_url}/connection_tables/{table['id']}",
            method="delete",
        )


def create_pipeline(
    args: argparse.Namespace,
    sql_queries: List[str],
    agg_functions_with_params: List[Tuple[str, dict]],
    streaming_aggregation_configs: List,
    json_template_dir: str,
    udf_dir: str,
):
    """Create a pipeline JSON based on template"""

    # Escape newlines in SQL query for JSON compatibility
    sql_queries = [sql_query.replace("\n", "\\n") for sql_query in sql_queries]
    sql_query = "\\n\\n".join(sql_queries)

    # UDFs handling
    udfs = []
    # NOTE: if we're using Arroyo built from source (v0.15.0-dev), we can directly support &str arguments in UDAFs, and thus don't need string_to_hash
    # udf_names = list(set(agg_functions)) + ["string_to_hash"]
    unique_agg_functions = list(
        set([agg_func for agg_func, _ in agg_functions_with_params])
    )
    udf_names = unique_agg_functions + ["gzip_compress"]
    # udf_names = list(set(agg_functions))

    # Create a mapping of agg_function to parameters for UDF rendering
    agg_function_params = {}
    for agg_func, params in agg_functions_with_params:
        if agg_func not in agg_function_params:
            agg_function_params[agg_func] = params

    # Special handling for deltasetaggregator - need separate UDF instances per aggregation_id
    deltasetaggregator_instances = []
    for config in streaming_aggregation_configs:
        if config.aggregationType.lower() == "deltasetaggregator":
            deltasetaggregator_instances.append(config.aggregationId)

    for udf_name in udf_names:
        # Special case for deltasetaggregator - generate separate UDF for each aggregation_id
        if udf_name == "deltasetaggregator_":
            for aggregation_id in deltasetaggregator_instances:
                template_path = os.path.join(udf_dir, f"{udf_name}.rs.j2")

                if os.path.exists(template_path):
                    # Render the Jinja template with aggregation_id
                    udf_template = jinja_utils.load_template(
                        udf_dir, f"{udf_name}.rs.j2"
                    )
                    udf_body = udf_template.render(aggregation_id=aggregation_id)
                    udfs.append({"definition": udf_body, "language": "rust"})
                else:
                    raise FileNotFoundError(
                        f"Template {template_path} not found for deltasetaggregator"
                    )
        else:
            # Regular UDF processing for non-deltasetaggregator UDFs
            template_path = os.path.join(udf_dir, f"{udf_name}.rs.j2")
            regular_path = os.path.join(udf_dir, f"{udf_name}.rs")

            # Get parameters for this UDF
            params = agg_function_params.get(udf_name, {})

            if len(params) > 0 and not os.path.exists(template_path):
                raise ValueError(
                    f"UDF {udf_name} requires parameters {params} but no template found at {template_path}"
                )

            if os.path.exists(template_path):
                # Read template source and get required parameters
                with open(template_path, "r") as file:
                    template_source = file.read()

                # Render the Jinja template with parameters
                udf_template = jinja_utils.load_template(udf_dir, f"{udf_name}.rs.j2")

                # Get all required template variables
                required_params = jinja_utils.get_template_variables(
                    template_source, udf_template.environment
                )

                # Handle config key mapping (K -> k for KLL)
                if "K" in params and "k" in required_params:
                    params["k"] = params["K"]

                # Check that all required parameters are provided
                missing_params = required_params - set(params.keys())
                if missing_params:
                    raise ValueError(
                        f"UDF {udf_name} requires parameters {missing_params} but they were not provided in the configuration"
                    )

                udf_body = udf_template.render(**params)
            elif os.path.exists(regular_path):
                # Use regular file if no template exists
                with open(regular_path, "r") as f:
                    udf_body = f.read()
            else:
                raise FileNotFoundError(
                    f"Neither {template_path} nor {regular_path} exists"
                )

            udfs.append({"definition": udf_body, "language": "rust"})

    # Load pipeline template
    pipeline_template = jinja_utils.load_template(json_template_dir, "pipeline.j2")

    rendered = pipeline_template.render(
        pipeline_name=args.pipeline_name,
        sql_query=sql_query,
        udfs=udfs,
        parallelism=args.parallelism,
    )

    # Save to file
    output_path = os.path.join(args.output_dir, "pipeline.json")
    with open(output_path, "w") as f:
        f.write(rendered)

    print(f"Created pipeline at: {output_path}")

    if args.dry_run:
        pipeline_id = "dry_run_pipeline_id"
        print(f"[DRY RUN] Would create pipeline with ID: {pipeline_id}")
        return

    # If API URL provided, create pipeline via API
    response = http_utils.create_arroyo_resource(
        arroyo_url=args.arroyo_url,
        endpoint="pipelines",
        data=rendered,
        resource_type="pipeline",
    )

    response = json.loads(response)
    pipeline_id = response["id"]
    print(f"Pipeline created with ID: {pipeline_id}")


def delete_pipelines(args):
    if args.dry_run:
        print("[DRY RUN] Would delete all existing pipelines")
        return

    # # list all pipelines
    # response = http_utils.make_api_request(
    #     url=f"{args.arroyo_url}/pipelines",
    #     method="get",
    # )
    # response = json.loads(response)
    # if response["data"] is None:
    #     print("No pipelines found")
    #     return

    # pipeline_ids = [pipeline["id"] for pipeline in response["data"]]
    pipeline_ids = arroyo_utils.get_all_pipelines(arroyo_url=args.arroyo_url)

    arroyo_utils.stop_and_delete_pipelines(
        arroyo_url=args.arroyo_url, pipeline_ids=pipeline_ids
    )

    # # stop and delete all pipelines
    # for pipeline_id in pipeline_ids:
    #     response = http_utils.make_api_request(
    #         url=f"{args.arroyo_url}/pipelines/{pipeline_id}",
    #         method="patch",
    #         data=json.dumps({"stop": "immediate"}),
    #     )

    # time.sleep(5)
    # for pipeline_id in pipeline_ids:
    #     success = False
    #     for _ in range(num_retries):
    #         try:
    #             response = http_utils.make_api_request(
    #                 url=f"{args.arroyo_url}/pipelines/{pipeline_id}",
    #                 method="delete",
    #             )
    #             success = True
    #         except Exception as e:
    #             print(f"Failed to delete pipeline {pipeline_id}: {e}")
    #             time.sleep(5)

    #         if not success:
    #             raise Exception(
    #                 f"Failed to delete pipeline {pipeline_id} after {num_retries} retries"
    #             )


def get_sql_query(
    streaming_aggregation_config: StreamingAggregationConfig,
    sql_template: Template,
    source_table: str,
    sink_table: str,
    source_type: str,
) -> Tuple[str, str, dict]:

    window_interval = "{} seconds".format(
        streaming_aggregation_config.tumblingWindowSize
    )

    agg_function = "{}_{}".format(
        streaming_aggregation_config.aggregationType,
        streaming_aggregation_config.aggregationSubType,
    )

    fully_qualified_group_by_columns = [
        "{}.{}".format("labels", label)
        for label in streaming_aggregation_config.labels["grouping"].keys
    ]
    fully_qualified_agg_columns = [
        "{}.{}".format("labels", label)
        for label in streaming_aggregation_config.labels["aggregated"].keys
    ]

    # Determine if timestamps should be included as argument
    include_timestamps_as_argument = (
        streaming_aggregation_config.aggregationType == "multipleincrease"
    )

    sql_query = sql_template.render(
        aggregation_id=streaming_aggregation_config.aggregationId,
        sink_table=sink_table,
        agg_function=agg_function,
        agg_columns=", ".join(fully_qualified_agg_columns),
        source_table=source_table,
        group_by_columns=", ".join(fully_qualified_group_by_columns),
        window_interval=window_interval,
        include_timestamps_as_argument=include_timestamps_as_argument,
        source_type=source_type,
    )

    return sql_query, agg_function, streaming_aggregation_config.parameters


def get_source_table_name(args, metric_name):
    """Get the source table name based on the metric name and source type"""
    if args.source_type == "kafka":
        return "{}_{}".format(args.input_kafka_topic, metric_name.replace(" ", "_"))
    elif args.source_type == "prometheus_remote_write":
        return "prometheus_{}_{}".format(
            args.prometheus_base_port, metric_name.replace(" ", "_")
        )
    elif args.source_type == "file":
        # Use filename without extension for table name
        filename = os.path.basename(args.input_file_path)
        filename_no_ext = os.path.splitext(filename)[0]
        return "{}_{}".format(filename_no_ext, metric_name.replace(" ", "_"))
    else:
        raise ValueError(f"Unsupported source type: {args.source_type}")


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    # source_table = args.input_kafka_topic + "_table"
    sink_table = args.output_kafka_topic + "_table"

    with open(args.config_file_path, "r") as fin:
        config = yaml.safe_load(fin)

    metric_config = MetricConfig(config["metrics"])
    streaming_aggregation_configs = [
        StreamingAggregationConfig.from_dict(aggregation_config)
        for aggregation_config in config["aggregations"]
    ]

    for streaming_aggregation_config in streaming_aggregation_configs:
        streaming_aggregation_config.aggregationType = (
            streaming_aggregation_config.aggregationType.lower()
        )
        streaming_aggregation_config.aggregationSubType = (
            streaming_aggregation_config.aggregationSubType.lower()
        )
        streaming_aggregation_config.validate(metric_config)

    json_template_dir = os.path.join(args.template_dir, "json")
    sql_template_dir = os.path.join(args.template_dir, "sql")
    udf_dir = os.path.join(args.template_dir, "udfs")

    # Create connection profile for Kafka, since we definitely need it for sink
    delete_connection_profile(args)
    profile_id = create_connection_profile(args, json_template_dir)

    for metric_name, metric_labels in metric_config.config.items():
        source_table = get_source_table_name(args, metric_name)
        delete_connection_table(args, source_table)

        # Set topic_name based on source type (only needed for Kafka)
        topic_name = args.input_kafka_topic if args.source_type == "kafka" else None

        create_source_connection_table(
            args,
            topic_name,
            source_table,
            profile_id,
            metric_labels.keys,
            json_template_dir,
        )

    delete_connection_table(args, sink_table)
    create_sink_connection_table(
        args, args.output_kafka_topic, sink_table, profile_id, json_template_dir
    )

    aggregation_sql_template = jinja_utils.load_template(
        sql_template_dir, "single_windowed_aggregation.j2"
    )
    labels_sql_template = jinja_utils.load_template(
        sql_template_dir, "distinct_windowed_labels.j2"
    )
    deltasetaggregator_sql_template = jinja_utils.load_template(
        sql_template_dir, "distinct_windowed_labels_deltasetaggregator.j2"
    )

    sql_queries = []
    agg_functions_with_params = []

    for streaming_aggregation_config in streaming_aggregation_configs:
        source_table = get_source_table_name(args, streaming_aggregation_config.metric)

        is_labels_accumulator: bool = (
            streaming_aggregation_config.aggregationType == "setaggregator"
            or streaming_aggregation_config.aggregationType == "deltasetaggregator"
        )

        # Choose appropriate SQL template
        if streaming_aggregation_config.aggregationType == "deltasetaggregator":
            sql_template = deltasetaggregator_sql_template
        elif is_labels_accumulator:
            sql_template = labels_sql_template
        else:
            sql_template = aggregation_sql_template

        sql_query, agg_function, parameters = get_sql_query(
            streaming_aggregation_config,
            sql_template,
            source_table,
            sink_table,
            args.source_type,
        )

        sql_queries.append(sql_query)
        # if not is_labels_accumulator:
        agg_functions_with_params.append((agg_function, parameters))

        print(
            "Generated SQL query for aggregation ID {}: \n{}".format(
                streaming_aggregation_config.aggregationId, sql_query
            )
        )
    delete_pipelines(args)
    create_pipeline(
        args,
        sql_queries,
        agg_functions_with_params,
        streaming_aggregation_configs,
        json_template_dir,
        udf_dir,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Dry run option
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Test the logic without making API calls",
    )

    # StreamingConfig
    parser.add_argument(
        "--config_file_path",
        type=str,
        required=True,
        help="Path to the configuration file",
    )

    # Connection profile parameters
    parser.add_argument(
        "--profile_name",
        default="default-kafka-profile",
        help="Name for the connection profile",
    )
    parser.add_argument(
        "--bootstrap_servers", default="localhost:9092", help="Kafka bootstrap servers"
    )

    # Source type selection
    parser.add_argument(
        "--source_type",
        type=str,
        choices=["kafka", "prometheus_remote_write", "file"],
        required=True,
        help="Type of source to use",
    )

    # Connection table parameters
    parser.add_argument(
        "--input_kafka_topic", type=str, required=False, help="Input Kafka topic"
    )
    parser.add_argument(
        "--input_file_path", type=str, required=False, help="Path to the input file"
    )

    # Prometheus remote write source parameters
    parser.add_argument(
        "--prometheus_base_port",
        type=int,
        required=False,
        help="Base port for Prometheus remote write endpoint",
    )
    parser.add_argument(
        "--prometheus_path",
        type=str,
        required=False,
        help="Path for Prometheus remote write endpoint",
    )
    parser.add_argument(
        "--prometheus_bind_ip",
        type=str,
        required=False,
        help="IP address to bind Prometheus remote write endpoint to",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        required=False,
        help="Pipeline parallelism (number of parallel tasks)",
    )

    parser.add_argument(
        "--output_kafka_topic", type=str, required=False, help="Output Kafka topic"
    )
    parser.add_argument(
        "--output_file_path", type=str, required=False, help="Path to the output file"
    )

    parser.add_argument(
        "--kafka_input_format",
        required=False,
        choices=["json", "avro-json", "avro-binary"],
    )
    parser.add_argument("--output_format", required=True, choices=["json", "byte"])

    parser.add_argument("--pipeline_name", required=True, help="Pipeline name")

    parser.add_argument(
        "--template_dir",
        default="./templates",
        help="Directory containing template files",
    )

    parser.add_argument(
        "--output_dir",
        default="./outputs",
        help="Directory to save the generated files",
    )

    parser.add_argument(
        "--arroyo_url",
        default="http://localhost:5115/api/v1",
        help="URL of the Arroyo API server",
    )

    args = parser.parse_args()
    check_args(args)
    main(args)
