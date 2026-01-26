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
)


class OrMatchingCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    locator: Locator
    timeout: StrictInt | StrictFloat
