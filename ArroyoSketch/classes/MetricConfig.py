from promql_utilities.data_model.KeyByLabelNames import KeyByLabelNames


# TODO: move to promql_utilities and dedup from all repos
class MetricConfig:
    def __init__(self, yaml_str):
        self.config = {}
        for metric, labels in yaml_str.items():
            self.config[metric] = KeyByLabelNames(labels)
