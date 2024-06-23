from typing import Dict, Generic, Literal, Optional, TypeVar

from pydantic import BaseModel

from hyperscale.core_rewrite.engines.client.playwright.models.browser import (
    BrowserMetadata,
)
from hyperscale.core_rewrite.engines.client.shared.models import (
    CallResult,
    RequestType,
)

T = TypeVar("T")


class PlaywrightResult(CallResult, Generic[T]):
    __slots__ = (
        "command",
        "command_args",
        "metadata",
        "url",
        "result",
        "error",
        "timings",
        "frame",
        "source",
    )

    def __init__(
        self,
        command: str,
        command_args: BaseModel,
        metadata: BrowserMetadata,
        url: str,
        result: T,
        error: Optional[str] = None,
        timings: Dict[
            Literal["command_start", "command_end"],
            float | None,
        ] = {},
        frame: Optional[str] = None,
        source: Literal["page", "frame", "mouse"] = "page",
    ):
        super(
            PlaywrightResult,
            self,
        ).__init__()

        self.command = command
        self.command_args = command_args
        self.metadata = metadata
        self.url = url
        self.result = result
        self.error = error
        self.timings = timings
        self.frame = frame
        self.source = source

    @classmethod
    def response_type(cls):
        return RequestType.PLAYWRIGHT

    def check(self):
        return self.error is None

    def context(self):
        return self.error if self.error else "OK"
