import os
import yaml
import promql_parser

import argparse
from loguru import logger
from typing import Dict, Set

from classes.SingleQueryConfig import SingleQueryConfig
from classes.MetricConfig import MetricConfig


def read_config(config_path) -> dict:
    config_yaml = None
    with open(config_path, "r") as f:
        config_yaml = yaml.safe_load(f)
        # yaml = YAML(typ="safe")
        # config_yaml = yaml.load(f)
    return config_yaml


def validate_config(config_yaml):
    # NOTE: only allow unique query strings for now
    query_strings = set()
    for query_group_yaml in config_yaml["query_groups"]:
        for query_string in query_group_yaml["queries"]:
            if query_string in query_strings:
                raise ValueError(f"Duplicate query string: {query_string}")
            query_strings.add(query_string)


def ast_walk_to_get_labels(ast) -> Dict[str, Set[str]]:
    if isinstance(ast, promql_parser.VectorSelector):
        metric_name = ast.name
        if metric_name is None:
            return {}
        matchers = ast.matchers.matchers + ast.matchers.or_matchers
        labels = set([matcher.name for matcher in matchers])
        return {metric_name: labels}
    elif isinstance(ast, promql_parser.MatrixSelector):
        return ast_walk_to_get_labels(ast.vector_selector)
    elif isinstance(ast, promql_parser.BinaryExpr):
        left_labels = ast_walk_to_get_labels(ast.lhs)
        right_labels = ast_walk_to_get_labels(ast.rhs)
        return {**left_labels, **right_labels}
    elif isinstance(ast, promql_parser.ParenExpr):
        return ast_walk_to_get_labels(ast.expr)
    elif isinstance(ast, promql_parser.Call):
        labels = {}
        for arg in ast.args:
            labels.update(ast_walk_to_get_labels(arg))
        return labels
    elif isinstance(ast, promql_parser.AggregateExpr):
        return ast_walk_to_get_labels(ast.expr)
    elif isinstance(ast, promql_parser.SubqueryExpr):
        return ast_walk_to_get_labels(ast.expr)
    else:
        raise ValueError(f"Unsupported AST node type: {type(ast).__name__}")


def get_labels_for_each_metric(promql_queries) -> Dict[str, Set[str]]:
    metric_labels_map = {}
    for query_string in promql_queries:
        ast = promql_parser.parse(query_string)
        # get set of labels used in any filter in promql_parser.VectorSelector
        labels = ast_walk_to_get_labels(ast)
        for metric_name, labels_set in labels.items():
            if metric_name not in metric_labels_map:
                metric_labels_map[metric_name] = set()
            metric_labels_map[metric_name].update(labels_set)
    return metric_labels_map


def main(args):
    promql_queries = open(args.input_rules_file, "r").readlines()
    # input_config_yaml = read_config(args.input_config)

    # validate_config(input_config_yaml)

    metric_labels_map = get_labels_for_each_metric(promql_queries)
    metric_config_yaml = [
        {"metric": metric_name, "labels": list(labels_set)}
        for metric_name, labels_set in metric_labels_map.items()
    ]
    metric_config = MetricConfig(metric_config_yaml)

    streaming_aggregation_configs_map = {}
    query_aggregation_config_keys_map = {}

    for query_string in promql_queries:
        single_query_config_yaml = {
            "query": query_string,
            "t_repeat": args.prometheus_scrape_interval,
            "options": {},
        }

        logger.debug("Processing query {}", query_string)

        single_query_config = SingleQueryConfig(
            single_query_config_yaml, metric_config, args.prometheus_scrape_interval
        )

        if single_query_config.is_supported():
            query_aggregation_config_keys_map[single_query_config.query] = []
            current_configs, num_aggregates_to_retain = (
                single_query_config.get_streaming_aggregation_configs()
            )

            for current_config in current_configs:
                key = current_config.get_identifying_key()
                query_aggregation_config_keys_map[single_query_config.query].append(
                    (key, num_aggregates_to_retain)
                )
                if key not in streaming_aggregation_configs_map:
                    streaming_aggregation_configs_map[key] = current_config
        else:
            logger.warning("Unsupported query")

    for idx, k in enumerate(streaming_aggregation_configs_map.keys()):
        streaming_aggregation_configs_map[k].aggregationId = idx + 1

    streaming_config = {
        "aggregations": [
            config.to_dict(metric_config)
            for config in streaming_aggregation_configs_map.values()
        ],
        "metrics": metric_config.config,
    }
    inference_config = {
        "queries": [],
        "metrics": metric_config.config,
    }
    for query, streaming_config_keys in query_aggregation_config_keys_map.items():
        inference_config["queries"].append({"query": query, "aggregations": []})
        for streaming_config_key in streaming_config_keys:
            inference_config["queries"][-1]["aggregations"].append(
                {
                    "aggregation_id": streaming_aggregation_configs_map[
                        streaming_config_key[0]
                    ].aggregationId,
                    "num_aggregates_to_retain": streaming_config_key[1],
                }
            )

    # yaml_writer = YAML()
    # yaml_writer.indent(mapping=2, sequence=4, offset=2)
    # yaml_writer.preserve_quotes = True

    os.makedirs(args.output_dir, exist_ok=True)
    with open(f"{args.output_dir}/streaming_config.yaml", "w") as f:
        f.write(yaml.dump(streaming_config))
        # yaml_writer.dump(streaming_config, f)
    with open(f"{args.output_dir}/inference_config.yaml", "w") as f:
        f.write(yaml.dump(inference_config))
        # yaml_writer.dump(inference_config, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_rules_file", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    parser.add_argument("--prometheus_scrape_interval", type=int, required=True)
    args = parser.parse_args()
    main(args)
