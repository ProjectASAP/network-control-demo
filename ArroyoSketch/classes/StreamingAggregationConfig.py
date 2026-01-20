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

    # NEW fields for sliding window support (Issue #236)
    windowSize: int  # Window size in seconds (e.g., 900s for 15m)
    slideInterval: int  # Slide/hop interval in seconds (e.g., 30s)
    windowType: str  # "tumbling" or "sliding"

    # DEPRECATED but kept for backward compatibility
    tumblingWindowSize: int  # For reading old configs

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
        # Default to tumbling windows for backward compatibility
        self.windowType = "tumbling"

    @staticmethod
    def from_dict(aggregation_config: dict) -> "StreamingAggregationConfig":
        aggregation = StreamingAggregationConfig()
        aggregation.aggregationId = aggregation_config["aggregationId"]
        aggregation.aggregationType = aggregation_config["aggregationType"]
        aggregation.aggregationSubType = aggregation_config["aggregationSubType"]

        # NEW: Handle new window fields with backward compatibility
        aggregation.windowType = aggregation_config.get("windowType", "tumbling")
        aggregation.windowSize = aggregation_config.get(
            "windowSize", aggregation_config.get("tumblingWindowSize")
        )
        aggregation.slideInterval = aggregation_config.get(
            "slideInterval", aggregation_config.get("tumblingWindowSize")
        )

        # Keep deprecated field for backward compatibility
        aggregation.tumblingWindowSize = aggregation_config.get(
            "tumblingWindowSize", aggregation.windowSize
        )

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
            self.windowType,  # NEW: Include window type
            self.windowSize,  # NEW: Include window size
            self.slideInterval,  # NEW: Include slide interval
            self.tumblingWindowSize,  # Keep for backward compatibility
            self.spatialFilter,
            self.metric,
            tuple(self.parameters.items()),
        ]
        for k in sorted(self.labels.keys()):
            keys.append(k)
            keys.append(tuple(self.labels[k].serialize_to_json()))

        return tuple(keys)
