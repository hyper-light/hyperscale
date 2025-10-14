from typing import Literal
from hyperscale.core.engines.client.shared.models import (
    CallResult,
    RequestType,
)

FTPActionType = Literal[
    "CREATE_ACCOUNT",
    "CHANGE_DIRECTORY",
    "LIST",
    "LIST_DIRECTORY",
    "LIST_DETAILS",
    "MAKE_DIRECTORY",
    "PWD",
    "RECEIVE_BINARY",   
    "RECEIVE_LINES",
    "REMOVE_FILE",
    "REMOVE_DIRECTORY",
    "RENAME",
    "SEND_BINARY",
    "SEND_LINES",
    "SIZE",
]


class FTPResponse(CallResult):
    action: FTPActionType
    data: int | bytes | bytearray | memoryview | None = None
    error: Exception | None = None
    timings: dict[
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
    ] | None = None


    @classmethod
    def response_type(cls):
        return RequestType.FTP