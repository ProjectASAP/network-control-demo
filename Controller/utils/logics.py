import copy

from promql_utilities.data_model.KeyByLabelNames import KeyByLabelNames
from promql_utilities.query_logics.enums import QueryPatternType
from promql_utilities.query_logics.logics import (
    does_precompute_operator_support_subpopulations,
)


def get_precompute_operator_parameters(
    aggregation_type: str, aggregation_sub_type: str
) -> dict:
    if aggregation_type in [
        "Increase",
        "MinMax",
        "Sum",
        "MultipleIncrease",
        "MultipleMinMax",
        "MultipleSum",
        "DeltaSetAggregator",
        "SetAggregator",
    ]:
        return {}
    elif aggregation_type == "CountMinSketch":
        return {"depth": 3, "width": 65536}
    elif aggregation_type == "DatasketchesKLL":
        return {"K": 200}
    # elif aggregation_type == "UnivMon":
    #     return {"depth": 3, "width": 2048, "levels": 16}
    else:
        raise NotImplementedError(f"Aggregation type {aggregation_type} not supported")


def get_num_aggregates_to_retain(
    query_pattern_type, query_pattern_match, query_t_repeat
):
    if query_pattern_type == QueryPatternType.ONLY_SPATIAL:
        return 1
    elif (
        query_pattern_type == QueryPatternType.ONLY_TEMPORAL
        or query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL
    ):
        return (
            int(query_pattern_match.tokens["range_vector"]["range"].total_seconds())
            // query_t_repeat
        )
    else:
        raise ValueError(f"Query pattern type {query_pattern_type} not supported")


def set_tumbling_window_size(
    query_pattern_type, t_repeat, prometheus_scrape_interval, template_config
):
    if (
        query_pattern_type == QueryPatternType.ONLY_TEMPORAL
        or query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL
    ):
        template_config.tumblingWindowSize = t_repeat
    elif query_pattern_type == QueryPatternType.ONLY_SPATIAL:
        template_config.tumblingWindowSize = prometheus_scrape_interval
    else:
        raise ValueError("Invalid query pattern type")


def set_subpopulation_labels(
    statistic_to_compute,
    aggregation_type,
    subpopulation_labels: KeyByLabelNames,
    template_config,
):
    if does_precompute_operator_support_subpopulations(
        statistic_to_compute, aggregation_type
    ):
        template_config.labels["grouping"] = KeyByLabelNames([])
        template_config.labels["aggregated"] = copy.deepcopy(subpopulation_labels)
    else:
        template_config.labels["grouping"] = copy.deepcopy(subpopulation_labels)
        template_config.labels["aggregated"] = KeyByLabelNames([])
