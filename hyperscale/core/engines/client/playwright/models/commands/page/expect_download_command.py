from typing import Callable, Optional

try:

    from playwright.async_api import Download

except Exception:

    class Download:
        pass
    
from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
)


class ExpectDownloadCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    predicate: Optional[Callable[[Download], bool]] = None
    timeout: StrictInt | StrictFloat
