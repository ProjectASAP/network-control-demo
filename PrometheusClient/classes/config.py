from typing import List, Dict, Any


class ServerConfig:
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        return cls(name=data["name"], url=data["url"])


class QueryGroupConfig:
    def __init__(
        self,
        id: int,
        queries: List[str],
        # repetitions: int,
        repetition_delay: int,
        options: Dict[str, Any],
        # starting_delay: int,
        # options: Dict[str, Any],
    ):
        # set defaults
        self.starting_delay = 0
        self.repetitions = None

        self.id = id
        self.queries = queries
        # self.repetitions = repetitions
        self.repetition_delay = repetition_delay
        self.__dict__.update(options)
        # self.starting_delay = starting_delay
        # self.options = options

        assert self.repetitions is not None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        # return cls(
        #     id=data["id"],
        #     repetitions=data["repetitions"],
        #     repetition_delay=data["repetition_delay"],
        #     starting_delay=data["starting_delay"] if "starting_delay" in data else 0,
        #     options=data["options"],
        #     queries=data["queries"],
        # )
        return cls(
            id=data["id"],
            queries=data["queries"],
            repetition_delay=data["repetition_delay"],
            options=data["client_options"],
        )


class Config:
    def __init__(
        self, servers: List[ServerConfig], query_groups: List[QueryGroupConfig]
    ):
        self.servers = servers
        self.query_groups = query_groups

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        servers = [ServerConfig.from_dict(server) for server in data["servers"]]
        query_groups = [
            QueryGroupConfig.from_dict(group) for group in data["query_groups"]
        ]
        return cls(servers=servers, query_groups=query_groups)
