import sys
import os
from enum import Enum
from statistics import median

folder_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(folder_path)

from scheduler.entities import Task, RunningTask


class UpdateMethod(Enum):
    MEDIAN_CPU_BY_TASK = 'median_cpu_by_task'
    IDENTITY = 'identity'


def median_cpu_by_task(group_results: dict, running_tasks: dict[str, RunningTask]):
    """
    Given vector results from one or more PromQL queries (metrics), computes the estimated requirement for a given resource by
    computing the median of all the metrics (separated by tasks). Assumes that "task_id" is a label for each metric.

    Args:
        group_results: Dictionary mapping PromQL query strings to Prometheus response data.
        running_tasks: Dictionary of task ids (str) and their corresponding assignments.
    Returns:
        Dictionary mapping task ids to the median value for the queried metrics.
    """
    metric_values_by_task = {task_id: [] for task_id in running_tasks.keys()}
    for query_string, query_results in group_results:
        data = query_results['data']
        result = data['result']
        for metric in result:
            task_id = metric['metric']['task_id']
            metric_values = metric_values_by_task[task_id]
            metric_value = float(metric['value'][1])
            metric_values.append(metric_value)
    for task_id, metric_vals in metric_values_by_task.items():
        if metric_vals:
            task = running_tasks[task_id].task
            task.initial_cpu = median(metric_vals)


def no_op(*args, **kwargs):
    pass


UPDATE_METHOD_NAME_MAPPING = {
    UpdateMethod.MEDIAN_CPU_BY_TASK: median_cpu_by_task,
    UpdateMethod.IDENTITY: no_op
}