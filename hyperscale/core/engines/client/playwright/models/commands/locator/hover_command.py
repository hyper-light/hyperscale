from typing import Literal, Optional, Sequence

try:

    from playwright.async_api import Position

except Exception:

    class Position:
        pass

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictBool,
    StrictFloat,
    StrictInt,
)


class HoverCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    modifiers: Optional[
        Sequence[Literal["Alt", "Control", "ControlOrMeta", "Meta", "Shift"]]
    ] = None
    postion: Optional[Position] = None
    timeout: StrictInt | StrictFloat
    force: Optional[StrictBool] = None
    no_wait_after: Optional[StrictBool] = None
    trial: Optional[StrictBool] = None
