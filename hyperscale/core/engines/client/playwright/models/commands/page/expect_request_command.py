from typing import Callable, Optional, Pattern

try:

    from playwright.async_api import Request

except Exception:
    class Request:
        pass

from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr


class ExpectRequestCommand(BaseModel):
    url_or_predicate: Optional[StrictStr | Pattern[str] | Callable[[Request], bool]] = (
        None
    )
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True
