from typing import Dict, Optional, Type, TypeVar

import orjson
from pydantic import BaseModel, StrictBytes, StrictFloat, StrictStr

from hyperscale.core_rewrite.engines.client.shared.models import (
    CallResult,
    URLMetadata,
)

T = TypeVar("T", bound=BaseModel)


class UDPResponse(CallResult):
    __slots__ = (
        "url",
        "error",
        "content",
        "timings",
    )

    def __init__(
        self,
        url: URLMetadata,
        error: Optional[StrictStr] = None,
        content: StrictBytes = b"",
        timings: Dict[StrictStr, StrictFloat] = {},
    ):
        super(
            UDPResponse,
            self,
        ).__init__()

        self.url = url
        self.error = error
        self.content = content
        self.timings = timings

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
