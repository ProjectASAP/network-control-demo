from typing import Any, Literal
from enum import Enum
from dataclasses import dataclass, field
import yaml
from cattrs import structure


class ServerType(str, Enum):
    PROMETHEUS = 'prometheus'
    ELASTICSEARCH = 'elasticsearch'


@dataclass
class ServerConfig:
    url: str
    api_key: str | None = None
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryGroupConfig:
    id: int
    queries: list[str]
    options: dict[str, Any] = field(default_factory=dict)
    name: str = ""
    backend: ServerType = ServerType.PROMETHEUS


class UpdateMethod(Enum):
    AGGREGATE_BY_TASK = 'aggregate_by_task'
    NO_OP = 'no_op'
    

@dataclass
class TaskUpdateRules:
    query_group_id: int
    update_method: UpdateMethod = UpdateMethod.NO_OP
    options: dict = field(default_factory=dict)


@dataclass
class QueryManagerConfig:
    task_update_rules: list[TaskUpdateRules] = field(default_factory=list)
    query_groups: list[QueryGroupConfig] = field(default_factory=list)
    server_configs: dict[ServerType, ServerConfig] = field(default_factory=dict)


@dataclass
class QueryResult:
    query: str | dict
    buckets: list["QueryResult.Bucket"]

    @dataclass
    class Bucket:
        task_id: str | None
        value: float


def load_query_config(config_path: str) -> QueryManagerConfig:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return structure(config, QueryManagerConfig)