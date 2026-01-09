from typing import Callable, Optional

try:

    from playwright.async_api import Page

except Exception:
    class Page:
        pass

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
)


class ExpectPopupCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    predicate: Optional[Callable[[Page], bool]] = None
    timeout: StrictInt | StrictFloat
