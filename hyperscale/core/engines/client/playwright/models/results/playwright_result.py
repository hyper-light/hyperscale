from typing import Dict, Generic, Literal, Optional, TypeVar

import msgspec

from hyperscale.core.engines.client.playwright.models.browser import (
    BrowserMetadata,
)
from hyperscale.core.engines.client.shared.models import (
    CallResult,
    RequestType,
)

T = TypeVar("T")


class PlaywrightResult(CallResult, Generic[T]):
    command: str
    command_args: msgspec.Struct
    metadata: BrowserMetadata
    url: str
    result: T
    error: Optional[str] = None
    timings: Dict[
        Literal["command_start", "command_end"],
        float | None,
    ] = None
    frame: Optional[str] = None
    source: Literal["page", "frame", "mouse"] = "page"

    @classmethod
    def response_type(cls):
        return RequestType.PLAYWRIGHT

    def check(self):
        return self.error is None

    def context(self):
        return self.error if self.error else "OK"

    @property
    def successful(self):
        return self.error is None
