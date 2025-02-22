from typing import Optional

try:

    from playwright.async_api import Position

except Exception:
    
    class Position:
        pass

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class DragAndDropCommand(BaseModel):
    source: StrictStr
    target: StrictStr
    source_position: Optional[Position] = None
    target_position: Optional[Position] = None
    timeout: StrictInt | StrictFloat
    force: Optional[StrictBool] = None
    no_wait_after: Optional[StrictBool] = None
    strict: Optional[StrictBool] = None
    trial: Optional[StrictBool] = None

    class Config:
        arbitrary_types_allowed = True