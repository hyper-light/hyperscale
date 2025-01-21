import re
from typing import Dict, Literal, Optional

from hyperscale.core.engines.client.http.models.http import HTTPResponse
from hyperscale.core.engines.client.shared.models import (
    Cookies,
    RequestType,
    URLMetadata,
)

space_pattern = re.compile(r"\s+")


class GraphQLResponse(HTTPResponse):
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
        return RequestType.GRAPHQL
