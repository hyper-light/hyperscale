from typing import Optional


try:
    from playwright.async_api import (
        Locator,
        Position,
    )

except Exception:

    class Locator:
        pass

    class Position:
        pass

from pydantic import BaseModel, ConfigDict, StrictBool, StrictFloat, StrictInt


class DragToCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    target: Locator
    force: Optional[StrictBool] = None
    no_wait_after: Optional[StrictBool] = None
    trial: Optional[StrictBool] = None
    source_position: Optional[Position] = None
    target_position: Optional[Position] = None
    timeout: Optional[StrictInt | StrictFloat] = None
