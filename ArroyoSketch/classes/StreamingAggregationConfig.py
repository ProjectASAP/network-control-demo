import yaml

# from ruamel.yaml import YAML

# TODO: move to promql_utilities and dedup from all repos
from typing import Dict, Tuple
from classes.MetricConfig import MetricConfig
from promql_utilities.data_model.KeyByLabelNames import KeyByLabelNames

yaml.add_representer(
    KeyByLabelNames,
    lambda dumper, data: dumper.represent_list(data.serialize_to_json()),
)

# yaml_writer = YAML()
# yaml_writer.representer.add_representer(
#     KeyByLabelNames,
#     lambda dumper, data: dumper.represent_sequence(
#         "tag:yaml.org,2002:seq", data.serialize_to_json(), flow_style=False
#     ),
# )


class StreamingAggregationConfig:
    aggregationId: int
    aggregationType: str
    aggregationSubType: str
    tumblingWindowSize: int
    spatialFilter: str
    metric: str
    parameters: dict

    labels: Dict[str, KeyByLabelNames]

    def __init__(self):
        self.labels = {
            "rollup": KeyByLabelNames([]),
            "grouping": KeyByLabelNames([]),
            "aggregated": KeyByLabelNames([]),
        }

    @staticmethod
    def from_dict(aggregation_config: dict) -> "StreamingAggregationConfig":
        aggregation = StreamingAggregationConfig()
        aggregation.aggregationId = aggregation_config["aggregationId"]
        aggregation.aggregationType = aggregation_config["aggregationType"]
        aggregation.aggregationSubType = aggregation_config["aggregationSubType"]
        aggregation.tumblingWindowSize = aggregation_config["tumblingWindowSize"]
        aggregation.spatialFilter = aggregation_config["spatialFilter"]
        aggregation.metric = aggregation_config["metric"]
        aggregation.parameters = aggregation_config["parameters"]

        for k, v in aggregation_config["labels"].items():
            if k not in aggregation.labels:
                raise ValueError(f"Invalid label name: {k}")
            if v is not None:
                aggregation.labels[k] = KeyByLabelNames(v)

        return aggregation

    def validate(self, metric_config: MetricConfig):
        configured_labels = KeyByLabelNames([])
        for k, v in self.labels.items():
            assert v is not None
            configured_labels += v

        if metric_config.config[self.metric] != configured_labels:
            raise ValueError(
                "Labels do not match: {} vs {}".format(
                    metric_config.config[self.metric],
                    configured_labels,
                )
            )

    def to_dict(self, metric_config: MetricConfig) -> dict:
        self.validate(metric_config)
        return self.__dict__

    def get_identifying_key(self) -> Tuple:
        keys = [
            self.aggregationType,
            self.aggregationSubType,
            self.tumblingWindowSize,
            self.spatialFilter,
            self.metric,
            tuple(self.parameters.items()),
        ]
        for k in sorted(self.labels.keys()):
            keys.append(k)
            keys.append(tuple(self.labels[k].serialize_to_json()))

        return tuple(keys)
