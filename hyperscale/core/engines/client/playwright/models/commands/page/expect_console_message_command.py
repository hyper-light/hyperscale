from typing import Callable, Optional

try:

    from playwright.async_api import ConsoleMessage

except Exception:

    class ConsoleMessage:
        pass

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
)


class ExpectConsoleMessageCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    predicate: Optional[Callable[[ConsoleMessage], bool]] = None
    timeout: StrictInt | StrictFloat
