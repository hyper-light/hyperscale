from typing import Callable, Optional, Pattern

try:
    
    from playwright.async_api import Response

except Exception:
    class Response:
        pass

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ExpectResponseCommand(BaseModel):
    url_or_predicate: Optional[str | Pattern[str] | Callable[[Response], bool]] = None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True
