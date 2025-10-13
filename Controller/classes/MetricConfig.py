from promql_utilities.data_model.KeyByLabelNames import KeyByLabelNames


class MetricConfig:
    def __init__(self, yaml_str):
        self.config = {}
        for metric_data in yaml_str:
            self.config[metric_data["metric"]] = KeyByLabelNames(metric_data["labels"])
