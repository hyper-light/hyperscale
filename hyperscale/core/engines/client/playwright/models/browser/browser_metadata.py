from typing import List, Literal, Optional

try:

    from playwright.async_api import Geolocation

except Exception:
    class Geolocation:
        pass

from pydantic import BaseModel, ConfigDict, StrictStr


class BrowserMetadata(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    browser_type: Optional[
        Literal["safari", "webkit", "firefox", "chrome", "chromium"]
    ] = None
    device_type: Optional[StrictStr] = None
    locale: Optional[StrictStr] = None
    geolocation: Optional[Geolocation] = None
    permissions: Optional[List[StrictStr]] = None
    color_scheme: Optional[StrictStr] = None
