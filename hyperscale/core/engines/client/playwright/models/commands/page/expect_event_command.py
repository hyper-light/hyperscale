from typing import (
    Callable,
    Literal,
    Optional,
)

from pydantic import BaseModel, ConfigDict, StrictFloat, StrictInt, StrictStr


class ExpectEventCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    event: Literal[
        "close",
        "console",
        "crash",
        "dialog",
        "domcontentloaded",
        "download",
        "filechooser",
        "frameattached",
        "framedetached",
        "framenavigated",
        "load",
        "pageerror",
        "popup",
        "request",
        "requestfailed",
        "requestfinished",
        "response",
        "websocket",
        "worker",
    ]
    predicate: Optional[Callable[[StrictStr], bool]] = None
    timeout: StrictInt | StrictFloat
