from __future__ import annotations

import binascii
from typing import Dict, Literal, Optional, TypeVar

from hyperscale.core.engines.client.http2.models.http2 import HTTP2Response
from hyperscale.core.engines.client.shared.models import (
    RequestType,
    URLMetadata,
)

from .protobuf import Protobuf

T = TypeVar("T")


class GRPCResponse(HTTP2Response):
    url: URLMetadata
    method: Optional[Literal["POST"]] = "POST"
    status: Optional[int] = None
    status_message: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
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

    _data: Optional[bytes] = None

    @classmethod
    def response_type(cls):
        return RequestType.GRPC

    @property
    def grpc_status(self) -> int | None:
        """
        The call's gRPC status code, if present and well formed: the
        grpc-status trailer, or the header of a trailers-only response.
        """
        value = self.trailers.get("grpc-status") if self.trailers else None
        if value is None and self.headers:
            value = self.headers.get("grpc-status")

        if value is not None and value.isdigit():
            return int(value)

        return None

    @property
    def successful(self) -> bool:
        # The HTTP/2 status is 200 for failed calls too: a gRPC call succeeds
        # only when its grpc-status is 0 (OK).
        return self.status == 200 and self.grpc_status == 0

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
