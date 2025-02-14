from __future__ import annotations

import gzip
import zlib
from typing import Dict, Literal, Optional, Type, TypeVar, Union

import orjson

from hyperscale.core.engines.client.shared.models import (
    CallResult,
    Cookies,
    RequestType,
    URLMetadata,
)

T = TypeVar("T")


class HTTP2Response(CallResult):
    url: URLMetadata
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
    ] = None
    cookies: Optional[Cookies] = None
    status: Optional[int] = None
    status_message: Optional[str] = None
    headers: Optional[Dict[bytes, bytes]] = None
    content: bytes = b""
    timings: Optional[
        Dict[
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
        ]
    ] = None
    redirects: int = 0

    @classmethod
    def response_type(cls):
        return RequestType.HTTP2

    @property
    def params(self):
        params: list[tuple[str, str]] = []

        if self.url.params and len(self.url.params) > 0:
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

        if self.url.query and len(self.url.query) > 0:
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
    def content_type(self):
        return self.headers.get(b"content-type", "application/text")

    @property
    def compression(self):
        return self.headers.get(b"content-encoding")

    @property
    def version(self) -> Union[str, None]:
        return self.headers.get(b"version")

    @property
    def reason(self) -> Union[str, None]:
        if self.headers:
            return self.headers.get(b"reason")

    @property
    def size(self):
        content_length = self.headers.get(b"content-length")
        if content_length:
            self._size = int(content_length)

        elif len(self.content) > 0:
            self._size = len(self.content)

        else:
            self._size = 0

        return self._size

    def json(self):
        return orjson.loads(self.body)

    def text(self):
        return self.body.decode()

    def to_model(self, model: Type[T]) -> T:
        return model(**orjson.loads(self.body))

    @property
    def body(self) -> bytes:
        data = self.content

        if self.compression == b"gzip":
            data = gzip.decompress(self.content)

        elif self.compression == b"deflate":
            data = zlib.decompress(self.content)

        return data

    @property
    def data(self):
        match self.content_type:
            case "application/json":
                return self.json()

            case "application/text":
                return self.text()

            case _:
                return self.body

    def check(self):
        return self.status >= 200 and self.status < 300

    def context(self):
        return self.status_message

    @property
    def successful(self) -> bool:
        return self.status and self.status >= 200 and self.status < 300
