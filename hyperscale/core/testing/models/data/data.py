from typing import (
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
)

import orjson
from pydantic import BaseModel

from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.engines.client.shared.protocols import NEW_LINE
from hyperscale.core.testing.models.base import OptimizedArg

from .data_types import DataValue, OptimizedData
from .data_validator import DataValidator

T = TypeVar("T")


class Data(OptimizedArg, Generic[T]):
    def __init__(self, data: DataValue) -> None:
        super(
            Data,
            self,
        ).__init__()

        validated_data = DataValidator(value=data)
        self.call_name: Optional[str] = None
        self.data = validated_data.value
        self.optimized: Optional[OptimizedData] = None
        self.content_length: OptimizedArg[int] = None
        self.content_type: Optional[str] = None

    async def optimize(self, request_type: RequestType):
        if self.optimized is not None:
            return

        match request_type:
            case RequestType.WEBSOCKET:
                self._optimize_websocket()

            case (
                RequestType.GRAPHQL
                | RequestType.GRAPHQL_HTTP2
                | RequestType.HTTP 
                | RequestType.HTTP2 
                | RequestType.HTTP3
            ):
                self._optimize_http(request_type)

            case (
                RequestType.FTP
                | RequestType.SCP
                | RequestType.SFTP 
                | RequestType.TCP
            ):
                self._optimize_raw()

            case _:
                pass

    def _optimize_http(self, request_type: RequestType):
        if (
            isinstance(self.data, Iterator)
            and not isinstance(self.data, list)
            and request_type not in (RequestType.HTTP, RequestType.GRAPHQL)
        ):
            # HTTP/2 and HTTP/3 have no chunked transfer coding: their frames
            # carry the body as it is (RFC 9113 8.2.2, RFC 9114 4.1).
            self.optimized = b"".join(self.data)
            self.content_length = len(self.optimized)

        elif isinstance(self.data, Iterator) and not isinstance(self.data, list):
            chunks: List[bytes] = []
            for chunk in self.data:
                chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                chunks.append(encoded_chunk)

            self.optimized = chunks
            self.content_length = sum([len(chunk) for chunk in chunks])

        elif isinstance(self.data, BaseModel):
            self.optimized = orjson.dumps(self.data.model_dump())
            self.content_type = "application/json"
            self.content_length = len(self.optimized)

        elif isinstance(self.data, (dict, list)):
            self.optimized = orjson.dumps(self.data)
            self.content_type = "application/json"
            self.content_length = len(self.optimized)

        elif isinstance(self.data, str):
            self.optimized = self.data.encode()
            self.content_length = len(self.optimized)

        elif isinstance(self.data, (memoryview, bytearray)):
            self.optimized = bytes(self.data)
            self.content_length = len(self.optimized)

        else:
            self.optimized = self.data

    def _optimize_websocket(self):
        """
        A WebSocket message's payload: the encoded value, or for an iterator
        its chunks, sent as one fragmented message -- WebSocket frames carry
        their own lengths, so no HTTP chunk framing. A text payload (strings,
        JSON) records its content type; binary ones leave it None.
        """
        if isinstance(self.data, Iterator) and not isinstance(self.data, list):
            chunks = list(self.data)
            if chunks and isinstance(chunks[0], str):
                self.content_type = "text/plain; charset=utf-8"

            self.optimized = [chunk.encode() if isinstance(chunk, str) else bytes(chunk) for chunk in chunks]
            self.content_length = sum(len(chunk) for chunk in self.optimized)

        elif isinstance(self.data, BaseModel):
            self.optimized = orjson.dumps(self.data.model_dump())
            self.content_type = "application/json"
            self.content_length = len(self.optimized)

        elif isinstance(self.data, (dict, list)):
            self.optimized = orjson.dumps(self.data)
            self.content_type = "application/json"
            self.content_length = len(self.optimized)

        elif isinstance(self.data, str):
            self.optimized = self.data.encode()
            self.content_type = "text/plain; charset=utf-8"
            self.content_length = len(self.optimized)

        else:
            self.optimized = bytes(self.data)
            self.content_length = len(self.optimized)

    def _optimize_raw(self):

        if isinstance(self.data, Iterator) and not isinstance(self.data, list):
            chunks: List[bytes] = []
            for chunk in self.data:
                chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                chunks.append(encoded_chunk)

            self.optimized = chunks
        
        elif isinstance(self.data, BaseModel):
            self.optimized = orjson.dumps(self.data.model_dump())

        elif isinstance(self.data, (list, dict)):
            self.optimized = orjson.dumps(self.data)

        elif isinstance(self.data, str):
            self.optimized = self.data.encode()

        elif isinstance(self.data, (memoryview, bytearray)):
            self.optimized = bytes(self.data)

        else:
            self.optimized = self.data
