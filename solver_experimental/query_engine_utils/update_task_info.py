import sys
import os
from enum import Enum
from statistics import median, mean, mode
from loguru import logger

folder_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(folder_path)

from scheduler.entities import Task, RunningTask
from query_engine_utils.config import QueryResult, UpdateMethod


def aggregate_by_task(
    group_results: list[QueryResult],
    running_tasks: dict[str, RunningTask],
    metric_type: Task.MetricType,
    aggregation_type="median",
):
    """
    Given vector results from one or more PromQL queries (metrics), computes the estimated requirement for a given resource by
    computing the median of all the metrics (for each task). Assumes that "task_id" is a label for each metric.

    Args:
        group_results: List of QueryResult objects containing Prometheus response data.
        running_tasks: Dictionary of task ids (str) and their corresponding assignments.
    Returns:
        Dictionary mapping task ids to the median value for the queried metrics.
    """
    if metric_type == Task.MetricType.PEER_BANDWIDTHS:
        logger.warning(f"Aggregation currently does not support peer bandwidth update.")

    agg_func_dict = {
        "median": median,
        "mean": mean,
        "mode": mode,
        "min": min,
        "max": max,
    }

    # Get values from each query in the group, separated by task id.
    metric_values_by_task = {task_id: [] for task_id in running_tasks.keys()} | {
        None: []
    }
    for query_result in group_results:
        for bucket in query_result.buckets:
            logger.trace(f"Query Bucket: {bucket}")
            task_id = bucket.task_id
            metric_values = metric_values_by_task[task_id]
            if task_id in metric_values_by_task:
                metric_values.append(bucket.value)

    agg_func = agg_func_dict[aggregation_type]
    for task_id, metric_vals in metric_values_by_task.items():
        if task_id is None:
            continue
        if metric_vals:
            task = running_tasks[task_id].task
            if metric_type == Task.MetricType.CPU:
                task.initial_cpu = agg_func(metric_vals)
            elif metric_type == Task.MetricType.MEMORY:
                task.initial_memory = agg_func(metric_vals)


def no_op(*args, **kwargs):
    pass


UPDATE_METHOD_NAME_MAPPING = {
    UpdateMethod.AGGREGATE_BY_TASK: aggregate_by_task,
    UpdateMethod.NO_OP: no_op,
}
