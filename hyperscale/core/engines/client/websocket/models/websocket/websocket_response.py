import re
from typing import Dict, Literal, Optional, TypeVar

from pydantic import BaseModel

from hyperscale.core.engines.client.http.models.http import HTTPResponse
from hyperscale.core.engines.client.shared.models import (
    Cookies,
    RequestType,
    URLMetadata,
)

space_pattern = re.compile(r"\s+")


T = TypeVar("T", bound=BaseModel)


class WebsocketResponse(HTTPResponse):
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
        return RequestType.WEBSOCKET
