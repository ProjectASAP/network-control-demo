from typing import List, Dict, Any
from dataclasses import dataclass, field
import yaml


@dataclass
class QueryGroupConfig:
    id: int
    queries: List[str]
    options: Dict[str, Any] = field(default_factory=dict)
    name: str = ""


@dataclass
class QueryManagerConfig:
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
    return QueryManagerConfig.from_dict(config)