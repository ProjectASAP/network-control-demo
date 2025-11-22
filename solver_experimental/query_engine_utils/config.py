from typing import List, Dict, Any, Literal
from enum import Enum
from dataclasses import dataclass, field
import yaml
from cattrs import structure
from .update_task_info import UpdateMethod


@dataclass
class QueryGroupConfig:
    id: int
    queries: List[str]
    options: Dict[str, Any] = field(default_factory=dict)
    name: str = ""

@dataclass
class TaskUpdateRules:
    query_group_id: int
    update_function: UpdateMethod = UpdateMethod.IDENTITY


@dataclass
class QueryManagerConfig:
    task_update_rules: List[TaskUpdateRules] = field(default_factory=list)
    query_groups: List[QueryGroupConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueryManagerConfig":
        query_groups = [
            QueryGroupConfig(
                id=group_data["id"],
                queries=group_data["queries"],
                options=group_data.get("options", {}),
            )
            for group_data in data.get("query_groups", [])
        ]
        return cls(query_groups=query_groups)


def load_query_config(config_path: str) -> QueryManagerConfig:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return structure(config, QueryManagerConfig)