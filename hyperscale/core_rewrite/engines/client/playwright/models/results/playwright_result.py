from typing import Dict, Generic, Literal, Optional, TypeVar

from pydantic import BaseModel, StrictFloat, StrictStr

from hyperscale.core_rewrite.engines.client.playwright.models.browser import (
    BrowserMetadata,
)
from hyperscale.core_rewrite.engines.client.shared.models import CallResult

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
        command: StrictStr,
        command_args: BaseModel,
        metadata: BrowserMetadata,
        url: StrictStr,
        result: T,
        error: Optional[StrictStr] = None,
        timings: Dict[Literal["command_start", "command_end"], StrictFloat] = {
            "command_start": 0,
            "command_end": 0,
        },
        frame: Optional[StrictStr] = None,
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
