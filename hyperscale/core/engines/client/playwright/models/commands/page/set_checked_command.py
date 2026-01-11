from typing import Optional

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


class SetCheckedCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    selector: StrictStr
    checked: StrictBool
    position: Optional[Position] = None
    force: Optional[StrictBool] = None
    no_wait_after: Optional[StrictBool] = None
    strict: Optional[StrictBool] = None
    trial: Optional[StrictBool] = None
    timeout: StrictInt | StrictFloat

