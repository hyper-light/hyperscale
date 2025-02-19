from typing import (
    Dict,
    Literal,
    Optional,
    Type,
    TypeVar,
)

import orjson
from pydantic import BaseModel

from hyperscale.core.engines.client.shared.models import (
    CallResult,
    RequestType,
    URLMetadata,
)

T = TypeVar("T", bound=BaseModel)


class UDPResponse(CallResult):
    url: URLMetadata
    error: Optional[str] = None
    content: bytes = b""
    timings: Dict[
        Literal[
            "request_start",
            "connect_start",
            "connect_end",
            "write_start",
            "write_end",
            "read_start",
            "read_end",
            "request_end",
        ],
        float | None,
    ] = None

    @classmethod
    def response_type(cls):
        return RequestType.UDP

    def json(self):
        if self.content:
            return orjson.loads(self.content)

        return {}

    def text(self):
        return self.content.decode()

    def to_model(self, model: Type[T]) -> T:
        return model(**orjson.loads(self.content))

    @property
    def data(self):
        return self.content

    @property
    def successful(self):
        return self.error is None

    def check(self):
        return self.error is None

    def context(self):
        return self.error if self.error else "OK"
