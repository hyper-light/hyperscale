import gzip
import re
from typing import Dict, Literal, Optional, Type, TypeVar

import orjson
from pydantic import BaseModel

from hyperscale.core_rewrite.engines.client.shared.models import (
    CallResult,
    Cookies,
    URLMetadata,
)

space_pattern = re.compile(r"\s+")


T = TypeVar("T", bound=BaseModel)


class HTTPResponse(CallResult):
    __slots__ = (
        "url",
        "method",
        "cookies",
        "status",
        "status_message",
        "headers",
        "content",
        "timings",
    )

    def __init__(
        self,
        url: URLMetadata,
        method: Optional[
            Literal[
                "GET",
                "POST",
                "HEAD",
                "OPTIONS",
                "PUT",
                "PATCH",
                "DELETE",
            ]
        ] = None,
        cookies: Optional[Cookies] = None,
        status: Optional[int] = None,
        status_message: Optional[str] = None,
        headers: Dict[bytes, bytes] = {},
        content: bytes = b"",
        timings: Dict[str, float] = {},
    ):
        super(
            HTTPResponse,
            self,
        ).__init__()

        self.url = url
        self.method = method
        self.cookies = cookies
        self.status = status
        self.status_message = status_message
        self.headers = headers
        self.content = content
        self.timings = timings

    def check_success(self) -> bool:
        return self.status and self.status >= 200 and self.status < 300

    def json(self):
        if self.content:
            return orjson.loads(self.content)

        return {}

    def text(self):
        return self.content.decode()

    def to_model(self, model: Type[T]) -> T:
        return model(**orjson.loads(self.content))

    @property
    def data(self, model: Optional[Type[T]] = None):
        content_type = self.headers.get("content-type")

        if model:
            return self.to_model(model)

        try:
            match content_type:
                case "application/json":
                    return self.json()

                case "text/plain":
                    return self.text()

                case "application/gzip":
                    return gzip.decompress(self.content)

                case _:
                    return self.content

        except Exception:
            return self.content
