from __future__ import annotations

from typing import Dict, Literal, Optional

from hyperscale.core_rewrite.engines.client.http2.models.http2 import HTTP2Response
from hyperscale.core_rewrite.engines.client.shared.models import (
    Cookies,
    URLMetadata,
)


class GraphQLHTTP2Response(HTTP2Response):
    def __init__(
        self,
        url: URLMetadata,
        method: Optional[Literal["GET", "POST"]] = None,
        cookies: Optional[Cookies] = None,
        status: Optional[int] = None,
        status_message: Optional[str] = None,
        headers: Dict[bytes, bytes] = {},
        content: bytes = b"",
        timings: Dict[str, float] = {},
    ):
        super(
            GraphQLHTTP2Response,
            self,
        ).__init__(
            url,
            method=method,
            cookies=cookies,
            status=status,
            status_message=status_message,
            headers=headers,
            content=content,
            timings=timings,
        )
