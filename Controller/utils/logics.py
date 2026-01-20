import copy
from loguru import logger

from promql_utilities.data_model.KeyByLabelNames import KeyByLabelNames
from promql_utilities.query_logics.enums import QueryPatternType
from promql_utilities.ast_matching.PromQLPattern import MatchResult
from promql_utilities.query_logics.logics import (
    does_precompute_operator_support_subpopulations,
)

CMS_WITH_HEAP_MULT = 4

# Default sketch parameters for backward compatibility
DEFAULT_SKETCH_PARAMETERS = {
    "CountMinSketch": {"depth": 3, "width": 1024},
    "CountMinSketchWithHeap": {"depth": 3, "width": 1024, "heap_multiplier": 4},
    "DatasketchesKLL": {"K": 20},
    "HydraKLL": {"row_num": 3, "col_num": 1024, "k": 20},
}


# TODO:
# We only show the logic of `get_precompute_operator_parameters` here.
# Semantics for topk query will be added in later PRs.
def get_precompute_operator_parameters(
    aggregation_type: str,
    aggregation_sub_type: str,
    query_pattern_match: MatchResult,
    sketch_parameters: dict,
) -> dict:
    # Allow partial overrides: use provided parameters, fall back to defaults per sketch type
    if sketch_parameters is None:
        sketch_parameters = {}

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
        params = sketch_parameters.get(
            "CountMinSketch", DEFAULT_SKETCH_PARAMETERS["CountMinSketch"]
        )
        return {"depth": params["depth"], "width": params["width"]}
    elif aggregation_type == "CountMinSketchWithHeap":
        if aggregation_sub_type == "topk":
            if "aggregation" not in query_pattern_match.tokens:
                raise ValueError(
                    f"{aggregation_sub_type} query missing aggregator in the match tokens"
                )
            if "param" not in query_pattern_match.tokens["aggregation"]:
                raise ValueError(
                    f"{aggregation_sub_type} query missing required 'k' parameter"
                )
            k = int(query_pattern_match.tokens["aggregation"]["param"].val)
            params = sketch_parameters.get(
                "CountMinSketchWithHeap",
                DEFAULT_SKETCH_PARAMETERS["CountMinSketchWithHeap"],
            )
            heap_mult = params.get("heap_multiplier", CMS_WITH_HEAP_MULT)
            return {
                "depth": params["depth"],
                "width": params["width"],
                "heapsize": k * heap_mult,
            }
        else:
            raise ValueError(
                f"Aggregation sub-type {aggregation_sub_type} for CountMinSketchWithHeap not supported"
            )
    elif aggregation_type == "DatasketchesKLL":
        params = sketch_parameters.get(
            "DatasketchesKLL", DEFAULT_SKETCH_PARAMETERS["DatasketchesKLL"]
        )
        return {"K": params["K"]}
    elif aggregation_type == "HydraKLL":
        params = sketch_parameters.get(
            "HydraKLL", DEFAULT_SKETCH_PARAMETERS["HydraKLL"]
        )
        return {
            "row_num": params["row_num"],
            "col_num": params["col_num"],
            "k": params["k"],
        }
    # elif aggregation_type == "UnivMon":
    #     return {"depth": 3, "width": 2048, "levels": 16}
    else:
        raise NotImplementedError(f"Aggregation type {aggregation_type} not supported")


def get_num_aggregates_to_retain(
    query_pattern_type, query_pattern_match, query_t_repeat, window_type="tumbling"
):
    """
    Calculate number of aggregates to retain based on query pattern and window type.

    For sliding windows: Only need 1 aggregate (no merging required)
    For tumbling windows: Need enough aggregates to cover the range window
    """
    # For sliding windows, only need 1 aggregate (no merging)
    if window_type == "sliding":
        logger.debug(
            "Sliding window mode: num_aggregates_to_retain = 1 (no merging needed)"
        )
        return 1

    # TUMBLING WINDOW logic (original)
    if query_pattern_type == QueryPatternType.ONLY_SPATIAL:
        return 1
    elif (
        query_pattern_type == QueryPatternType.ONLY_TEMPORAL
        or query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL
    ):
        num_aggregates = (
            int(query_pattern_match.tokens["range_vector"]["range"].total_seconds())
            // query_t_repeat
        )
        logger.debug(
            f"Tumbling window mode: num_aggregates_to_retain = {num_aggregates} "
            f"(range={query_pattern_match.tokens['range_vector']['range'].total_seconds()}s, "
            f"t_repeat={query_t_repeat}s)"
        )
        return num_aggregates
    else:
        raise ValueError(f"Query pattern type {query_pattern_type} not supported")


def should_use_sliding_window(query_pattern_type, aggregation_type):
    """
    Decide if sliding windows should be used based on query type and aggregation type.

    For Issue #236: Use sliding windows for ALL ONLY_TEMPORAL queries except DeltaSetAggregator.
    This eliminates merging overhead in QueryEngine at the cost of more computation in Arroyo.

    Args:
        query_pattern_type: ONLY_TEMPORAL, ONLY_SPATIAL, or ONE_TEMPORAL_ONE_SPATIAL
        aggregation_type: Type of aggregation (e.g., 'DatasketchesKLL', 'Sum', etc.)

    Returns:
        bool: True if sliding windows should be used
    """
    # NOTE: returning False since sliding window pipelines are causing arroyo to crash
    return False
    # Only use sliding for ONLY_TEMPORAL queries (not ONE_TEMPORAL_ONE_SPATIAL or ONLY_SPATIAL)
    if query_pattern_type != QueryPatternType.ONLY_TEMPORAL:
        logger.debug(
            f"Query pattern {query_pattern_type} not eligible for sliding windows "
            f"(only ONLY_TEMPORAL supported)"
        )
        return False

    # Explicitly exclude DeltaSetAggregator (paired with CMS but needs tumbling)
    if aggregation_type == "DeltaSetAggregator":
        logger.debug("DeltaSetAggregator excluded from sliding windows")
        return False

    # All other ONLY_TEMPORAL aggregations use sliding windows
    logger.info(
        f"Aggregation type '{aggregation_type}' with {query_pattern_type} -> SLIDING windows"
    )
    return True


def set_window_parameters(
    query_pattern_type,
    query_pattern_match,
    t_repeat,
    prometheus_scrape_interval,
    aggregation_type,
    template_config,
):
    """
    Set window parameters for streaming aggregation config.
    Auto-decides between sliding and tumbling windows based on query type and aggregation cost.

    For ONLY_TEMPORAL queries with expensive aggregations (KLL, CMS):
    - Uses SLIDING windows: windowSize = range duration, slideInterval = t_repeat
    - This reduces QueryEngine latency by avoiding merges (Arroyo does more work upfront)

    For other queries:
    - Uses TUMBLING windows: windowSize = slideInterval = tumbling size
    - This is the original behavior

    Args:
        query_pattern_type: Pattern type (ONLY_TEMPORAL, ONLY_SPATIAL, ONE_TEMPORAL_ONE_SPATIAL)
        query_pattern_match: Matched PromQL pattern containing query metadata
        t_repeat: Query repeat interval in seconds
        prometheus_scrape_interval: Scrape interval in seconds
        aggregation_type: Type of aggregation operator
        template_config: StreamingAggregationConfig to update
    """
    # Decide if we should use sliding windows
    use_sliding_window = should_use_sliding_window(query_pattern_type, aggregation_type)

    if use_sliding_window:
        # SLIDING WINDOW for ONLY_TEMPORAL queries with expensive aggregations
        logger.info(
            f"Configuring SLIDING WINDOW for {query_pattern_type} "
            f"with {aggregation_type}"
        )

        if query_pattern_type == QueryPatternType.ONLY_TEMPORAL:
            # Window size = range duration (e.g., 15m = 900s)
            range_seconds = int(
                query_pattern_match.tokens["range_vector"]["range"].total_seconds()
            )

            # Check if this is actually a tumbling window (windowSize == slideInterval)
            if range_seconds == t_repeat:
                logger.info(
                    f"Detected windowSize == slideInterval ({range_seconds}s). "
                    f"Using tumbling window instead of sliding for efficiency."
                )
                template_config.windowSize = t_repeat
                template_config.slideInterval = t_repeat
                template_config.windowType = "tumbling"
                template_config.tumblingWindowSize = t_repeat
            else:
                # True sliding window
                template_config.windowSize = range_seconds
                template_config.slideInterval = t_repeat  # e.g., 30s
                template_config.windowType = "sliding"

                logger.info(
                    f"Sliding window params: windowSize={range_seconds}s, "
                    f"slideInterval={t_repeat}s "
                    f"(each window has {range_seconds} seconds of data, slides every {t_repeat}s)"
                )

                # Set deprecated field for backward compatibility
                template_config.tumblingWindowSize = t_repeat
        else:
            # This should never be reached due to should_use_sliding_window() check
            assert False, (
                f"should_use_sliding_window returned True for {query_pattern_type}, "
                f"but sliding windows only supported for ONLY_TEMPORAL"
            )
    else:
        # TUMBLING WINDOW (existing logic)
        logger.info(
            f"Configuring TUMBLING WINDOW for {query_pattern_type} "
            f"with {aggregation_type}"
        )
        _set_tumbling_window_parameters(
            query_pattern_type, t_repeat, prometheus_scrape_interval, template_config
        )


def _set_tumbling_window_parameters(
    query_pattern_type, t_repeat, prometheus_scrape_interval, template_config
):
    """
    Original tumbling window logic - kept for compatibility and non-temporal queries.
    """
    if (
        query_pattern_type == QueryPatternType.ONLY_TEMPORAL
        or query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL
    ):
        template_config.windowSize = t_repeat
        template_config.slideInterval = t_repeat
        template_config.windowType = "tumbling"
        template_config.tumblingWindowSize = t_repeat

        logger.debug(
            f"Tumbling window params: windowSize={t_repeat}s, slideInterval={t_repeat}s"
        )
    elif query_pattern_type == QueryPatternType.ONLY_SPATIAL:
        template_config.windowSize = prometheus_scrape_interval
        template_config.slideInterval = prometheus_scrape_interval
        template_config.windowType = "tumbling"
        template_config.tumblingWindowSize = prometheus_scrape_interval

        logger.debug(
            f"Tumbling window params: windowSize={prometheus_scrape_interval}s, "
            f"slideInterval={prometheus_scrape_interval}s"
        )
    else:
        raise ValueError("Invalid query pattern type")


# COMMENTED OUT - Original function kept for rollback
# Issue #236: Replaced with set_window_parameters() to support sliding windows
#
# def set_tumbling_window_size(
#     query_pattern_type, t_repeat, prometheus_scrape_interval, template_config
# ):
#     if (
#         query_pattern_type == QueryPatternType.ONLY_TEMPORAL
#         or query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL
#     ):
#         template_config.tumblingWindowSize = t_repeat
#     elif query_pattern_type == QueryPatternType.ONLY_SPATIAL:
#         template_config.tumblingWindowSize = prometheus_scrape_interval
#     else:
#         raise ValueError("Invalid query pattern type")


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
