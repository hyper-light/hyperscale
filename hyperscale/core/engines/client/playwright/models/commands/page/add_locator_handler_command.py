from typing import (
    Any,
    Callable,
    Optional,
)

try:

    from playwright.async_api import Locator

except Exception:

    class Locator:
        pass

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictBool,
    StrictFloat,
    StrictInt,
)


class AddLocatorHandlerCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    locator: Locator
    handler: Callable[[Locator], Any] | Callable[[], Any]
    no_wait_after: Optional[StrictBool] = None
    times: Optional[StrictInt] = None
    timeout: StrictInt | StrictFloat
