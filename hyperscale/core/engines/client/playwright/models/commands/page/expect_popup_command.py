from typing import Callable, Optional

try:

    from playwright.async_api import Page

except Exception:
    class Page:
        pass

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ExpectPopupCommand(BaseModel):
    predicate: Optional[Callable[[Page], bool]] = None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True
