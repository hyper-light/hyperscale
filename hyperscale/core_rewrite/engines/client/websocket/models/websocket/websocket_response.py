import re
from typing import Dict, Literal, Optional, TypeVar, Union

from pydantic import BaseModel, StrictBytes, StrictFloat, StrictInt, StrictStr

from hyperscale.core_rewrite.engines.client.http.models.http import HTTPResponse
from hyperscale.core_rewrite.engines.client.shared.models import (
    Cookies,
    URLMetadata,
)

space_pattern = re.compile(r"\s+")


T = TypeVar("T", bound=BaseModel)


class WebsocketResponse(HTTPResponse):
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
            WebsocketResponse,
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
