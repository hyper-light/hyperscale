from typing import Dict, List, Union
from .types import RequestTypes


class BaseResult:
    __slots__ = (
        "action_id",
        "name",
        "checks",
        "error",
        "source",
        "user",
        "tags",
        "type",
        "time",
        "wait_start",
        "start",
        "connect_end",
        "write_end",
        "complete",
    )

    def __init__(
        self,
        action_id: str,
        name: str,
        source: str,
        user: str,
        tags: List[Dict[str, str]],
        type: Union[RequestTypes, str],
        error: Exception,
    ) -> None:
        self.action_id = action_id
        self.name = name
        self.error = error
        self.source = source
        self.user = user
        self.tags = tags
        self.type = type

        self.time = 0
        self.wait_start = 0
        self.start = 0
        self.connect_end = 0
        self.write_end = 0
        self.complete = 0
