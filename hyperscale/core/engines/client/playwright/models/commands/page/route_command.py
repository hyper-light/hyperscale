from typing import Any, Callable, Optional, Pattern

try:

    from playwright.async_api import Request, Route

except Exception:
    class Request:
        pass

    class Route:
        pass

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class RouteCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    url: StrictStr | Pattern[str] | Callable[[StrictStr], StrictBool]
    handler: Callable[[Route], Any] | Callable[[Route, Request], Any]
    times: Optional[StrictInt]
    timeout: StrictInt | StrictFloat