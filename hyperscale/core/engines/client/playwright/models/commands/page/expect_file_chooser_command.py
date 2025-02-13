from typing import Callable, Optional

try:

    from playwright.async_api import FileChooser

except Exception:
    class FileChooser:
        pass
    
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ExpectFileChooserCommand(BaseModel):
    predicate: Optional[Callable[[FileChooser], bool]] = None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True
