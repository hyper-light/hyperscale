from __future__ import annotations

import binascii
from typing import (
    Dict,
    Literal,
    Optional,
)

from google.protobuf.message import Message
from pydantic import StrictBytes, StrictFloat, StrictInt, StrictStr

from hyperscale.core_rewrite.engines.client.http2.models.http2 import HTTP2Response
from hyperscale.core_rewrite.engines.client.shared.models import (
    URLMetadata,
)


class GRPCResponse(HTTP2Response):
    __slots__ = (
        "url",
        "method",
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
        status: Optional[StrictInt] = None,
        status_message: Optional[StrictStr] = None,
        headers: Dict[StrictStr, StrictStr] = {},
        content: StrictBytes = b"",
        timings: Dict[StrictStr, StrictFloat] = {},
    ):
        super(
            GRPCResponse,
            self,
        ).__init__(
            url,
            method=method,
            status=status,
            status_message=status_message,
            headers=headers,
            content=content,
            timings=timings,
        )

    def check_success(self) -> bool:
        return self.status and self.status >= 200 and self.status < 300

    @property
    def data(self):
        wire_msg = binascii.b2a_hex(self.content)

        message_length = wire_msg[4:10]
        msg = wire_msg[10 : 10 + int(message_length, 16) * 2]

        return binascii.a2b_hex(msg)

    def parse(self, protobuf: Message):
        protobuf.ParseFromString(self.data)
        return protobuf
