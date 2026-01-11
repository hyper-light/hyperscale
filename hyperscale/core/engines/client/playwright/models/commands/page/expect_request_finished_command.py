from typing import Callable, Optional

try:

    from playwright.async_api import Request

except Exception:
    class Request:
        pass

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
)


class ExpectRequestFinishedCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    predicate: Optional[Callable[[Request], bool]] = None
    timeout: StrictInt | StrictFloat
