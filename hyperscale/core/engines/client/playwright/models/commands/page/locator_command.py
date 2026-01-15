from typing import Optional, Pattern

try:

    from playwright.async_api import Locator

except Exception:
    class Locator:
        pass
    
from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class LocatorCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    selector: StrictStr
    has_text: Optional[StrictStr | Pattern[str]] = None
    has_not_text: Optional[StrictStr | Pattern[str]] = None
    has: Optional[Locator] = None
    has_not: Optional[Locator] = None
    timeout: StrictInt | StrictFloat
