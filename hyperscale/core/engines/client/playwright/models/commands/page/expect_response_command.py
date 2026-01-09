from typing import Callable, Optional, Pattern

try:
    
    from playwright.async_api import Response

except Exception:
    class Response:
        pass

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
)


class ExpectResponseCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    url_or_predicate: Optional[str | Pattern[str] | Callable[[Response], bool]] = None
    timeout: StrictInt | StrictFloat
