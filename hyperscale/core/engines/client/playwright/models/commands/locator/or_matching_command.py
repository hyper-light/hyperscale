try:
    from playwright.async_api import Locator

except Exception:

    class Locator:
        pass

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class OrMatchingCommand(BaseModel):
    locator: Locator
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True
