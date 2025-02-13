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
)


class SetCheckedCommand(BaseModel):
    checked: StrictBool
    position: Optional[Position] = None
    force: Optional[StrictBool] = None
    no_wait_after: Optional[StrictBool] = None
    trial: Optional[StrictBool] = None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True
