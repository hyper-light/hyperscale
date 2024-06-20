from __future__ import annotations

import gzip
import zlib
from typing import Dict, Literal, Optional, Type, TypeVar, Union

import orjson
from pydantic import StrictBytes, StrictFloat, StrictInt, StrictStr

from hyperscale.core_rewrite.engines.client.shared.models import (
    CallResult,
    Cookies,
    URLMetadata,
)

T = TypeVar("T")


class HTTP2Response(CallResult):
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
            Literal["GET", "POST", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"]
        ] = None,
        cookies: Union[Optional[Cookies], Optional[None]] = None,
        status: Optional[StrictInt] = None,
        status_message: Optional[StrictStr] = None,
        headers: Dict[StrictBytes, StrictBytes] = {},
        content: StrictBytes = b"",
        timings: Dict[StrictStr, StrictFloat] = {},
    ):
        super(
            HTTP2Response,
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
