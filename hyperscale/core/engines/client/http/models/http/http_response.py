import gzip
import re
from typing import Dict, Literal, Type, TypeVar

import orjson
from pydantic import BaseModel

from hyperscale.core.engines.client.shared.models import (
    CallResult,
    Cookies,
    RequestType,
    URLMetadata,
)
from hyperscale.core.engines.client.tracing import Span

space_pattern = re.compile(r"\s+")


T = TypeVar("T", bound=BaseModel)


class HTTPResponse(CallResult):
    url: URLMetadata
    method: Literal[
        "GET",
        "POST",
        "HEAD",
        "OPTIONS",
        "PUT",
        "PATCH",
        "DELETE",
    ] | None = None
    cookies: Cookies | None = None
    status: int | None = None
    status_message: str | None = None
    headers: Dict[bytes, bytes] | None = None
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
    ] | None = None
    redirects: int = 0
    trace: Span | None = None

    @classmethod
    def response_type(cls):
        return RequestType.HTTP

    def json(self):
        if self.content:
            return orjson.loads(self.content)

        return {}

    def text(self):
        return self.content.decode()

    def to_model(self, model: Type[T]) -> T:
        return model(**orjson.loads(self.content))

    @property
    def params(self):
        params: list[tuple[str, str]] = []

        if len(self.url.params) > 0:
            params.extend(
                [
                    tuple(
                        param.split(
                            "=",
                            maxsplit=1,
                        )
                    )
                    for param in self.url.params.split("&")
                ]
            )

        if len(self.url.query) > 0:
            params.extend(
                [
                    tuple(
                        param.split(
                            "=",
                            maxsplit=1,
                        )
                    )
                    for param in self.url.query.split("&")
                ]
            )

        return params

    @property
    def reason(self) -> str | None:
        if self.headers:
            return self.headers.get("reason")

    @property
    def data(self, model: Type[T] | None = None):
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

    @property
    def successful(self) -> bool:
        return self.status and self.status >= 200 and self.status < 300

    def context(self):
        return self.status_message
