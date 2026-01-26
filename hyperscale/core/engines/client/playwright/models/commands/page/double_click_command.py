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
    StrictStr,
)


class DoubleClickCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    selector: StrictStr
    modifiers: Optional[
        Sequence[Literal["Alt", "Control", "ControlOrMeta", "Meta", "Shift"]]
    ] = None
    delay: Optional[StrictFloat | StrictInt] = None
    button: Literal["left", "middle", "right"] = "left"
    click_count: Optional[StrictInt] = None
    postion: Optional[Position] = None
    timeout: StrictInt | StrictFloat
    force: Optional[StrictBool] = None
    no_wait_after: Optional[StrictBool] = None
    strict: Optional[StrictBool] = None
    trial: Optional[StrictBool] = None
