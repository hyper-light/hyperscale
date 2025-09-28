from hyperscale.core.engines.client.shared.models import (
    CallResult,
    RequestType,
    URLMetadata,
)

from typing import Literal


from hyperscale.core.engines.client.sftp.models import TransferResult


SCPTimings = Literal[
    "request_start",
    "connect_start",
    "connect_end",
    "initialization_start",
    "initialization_end",
    "transfer_start",
    "transfer_end",
    "request_end",
]


class SCPResponse(CallResult):
    source_url: URLMetadata
    destination_url: URLMetadata
    operation: Literal["COPY", "SEND", "RECEIVE"]
    error: Exception | None = None
    transferred: dict[bytes, TransferResult] | None = None
    timings: dict[
        SCPTimings,
        float | None,
    ] | None = None

    @classmethod
    def response_type(cls):
        return RequestType.SCP

    @property
    def successful(self) -> bool:
        return self.error is None

