import orjson
from typing import Literal, TypeVar, Type
from pydantic import BaseModel
from hyperscale.core.engines.client.shared.models import (
    CallResult,
    RequestType,
    URLMetadata,
)


T = TypeVar("T", bound=BaseModel)


SFTPTimings = Literal[
    "request_start",
    "connect_start",
    "connect_end",
    "write_start",
    "write_end",
    "read_start",
    "read_end",
    "request_end",
]


class SFTPResponse(CallResult):
    url: URLMetadata
    action: str | None = None
    error: Exception | None = None
    status: int | None = None
    status_message: str | None = None
    content: bytes = b""
    timings: dict[
        SFTPTimings,
        float | None,
    ] | None = None


    @classmethod
    def response_type(cls):
        return RequestType.SFTP
    
    def json(self):
        if self.content:
            return orjson.loads(self.content)

        return {}

    def text(self):
        return self.content.decode()

    def to_model(self, model: Type[T]) -> T:
        return model(**orjson.loads(self.content))
    
    @property
    def successful(self) -> bool:
        return self.error is None
