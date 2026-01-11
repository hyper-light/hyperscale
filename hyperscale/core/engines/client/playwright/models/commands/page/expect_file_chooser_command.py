from typing import Callable, Optional

try:

    from playwright.async_api import FileChooser

except Exception:
    class FileChooser:
        pass
    
from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
)


class ExpectFileChooserCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    predicate: Optional[Callable[[FileChooser], bool]] = None
    timeout: StrictInt | StrictFloat
