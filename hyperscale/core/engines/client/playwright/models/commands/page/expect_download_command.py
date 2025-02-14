from typing import Callable, Optional

try:

    from playwright.async_api import Download

except Exception:

    class Download:
        pass
    
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ExpectDownloadCommand(BaseModel):
    predicate: Optional[Callable[[Download], bool]] = None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True
