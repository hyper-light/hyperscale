from __future__ import annotations

from typing import Dict, Literal, Optional

from hyperscale.core.engines.client.http2.models.http2 import HTTP2Response
from hyperscale.core.engines.client.shared.models import (
    Cookies,
    RequestType,
    URLMetadata,
)


class GraphQLHTTP2Response(HTTP2Response):
    url: URLMetadata
    method: Optional[Literal["GET", "POST"]] = None
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

    @classmethod
    def response_type(cls):
        return RequestType.GRAPHQL_HTTP2
