from typing import Callable, Optional

try:

    from playwright.async_api import Request

except Exception:
    class Request:
        pass

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ExpectRequestFinishedCommand(BaseModel):
    predicate: Optional[Callable[[Request], bool]] = None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True
