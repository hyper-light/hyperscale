from __future__ import annotations

import binascii
from typing import Dict, Literal, Optional, TypeVar

from hyperscale.core_rewrite.engines.client.http2.models.http2 import HTTP2Response
from hyperscale.core_rewrite.engines.client.shared.models import (
    RequestType,
    URLMetadata,
)

from .protobuf import Protobuf

T = TypeVar("T")


class GRPCResponse(HTTP2Response):
    __slots__ = ("_data",)

    def __init__(
        self,
        url: URLMetadata,
        method: Optional[Literal["POST"]] = "POST",
        status: Optional[int] = None,
        status_message: Optional[str] = None,
        headers: Dict[bytes, bytes] = {},
        content: bytes = b"",
        timings: Dict[str, float] = {},
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

        self._data: Optional[bytes] = None

    @classmethod
    def response_type(cls):
        return RequestType.GRPC

    def check_success(self) -> bool:
        return self.status and self.status >= 200 and self.status < 300

    @property
    def data(self):
        parsed: bytes = b""
        if self._data is None and self.content:
            wire_msg = binascii.b2a_hex(self.content)

            message_length = wire_msg[4:10]
            msg = wire_msg[10 : 10 + int(message_length, 16) * 2]

            parsed = binascii.a2b_hex(msg)
            self._data = parsed

        return parsed

    def to_protobuf(self, protobuf: Protobuf[T]):
        protobuf.ParseFromString(self.data)
        return protobuf
