from typing import Literal
from hyperscale.core.engines.client.shared.models import (
    CallResult,
    RequestType,
    URLMetadata,
)

from .command_type import CommandType
from .transfer_result import TransferResult



SFTPTimings = Literal[
    "request_start",
    "connect_start",
    "connect_start",
    "connect_end",
    "initialization_start",
    "initialization_end",
    "execution_start",
    "exectution_end",
    "close_start",
    "close_end",
    "request_end",
]


class SFTPResponse(CallResult):
    url: URLMetadata
    operation: CommandType | None = None
    error: Exception | None = None
    transferred: dict[bytes, TransferResult] | None = None
    timings: dict[
        SFTPTimings,
        float | None,
    ] | None = None


    @classmethod
    def response_type(cls):
        return RequestType.SFTP
    
    @property
    def successful(self) -> bool:
        return self.error is None
