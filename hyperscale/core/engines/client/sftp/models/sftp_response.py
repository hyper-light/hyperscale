from typing import Literal, TypeVar
from pydantic import BaseModel, AnyUrl
from hyperscale.core.engines.client.shared.models import (
    CallResult,
    RequestType,
)

from .command_type import CommandType
from .transfer_result import TransferResult


T = TypeVar("T", bound=BaseModel)


SFTPTimings = Literal[
    "request_start",
    "connect_start",
    "connect_end",
    "initialization_start",
    "initialization_end",
    "command_start",
    "command_end",
    "request_end",
]


class SFTPResponse(CallResult):
    url: AnyUrl
    action: CommandType | None = None
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
