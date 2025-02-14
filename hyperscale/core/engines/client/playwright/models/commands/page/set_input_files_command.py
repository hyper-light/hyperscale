from pathlib import Path
from typing import Optional, Sequence

try:

    from playwright.async_api import FilePayload

except Exception:
    class FilePayload:
        pass

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class SetInputFilesCommand(BaseModel):
    selector: StrictStr
    files: (
        StrictStr
        | Path
        | FilePayload
        | Sequence[StrictStr | Path]
        | Sequence[FilePayload]
    )
    strict: Optional[StrictBool] = None
    no_wait_after: Optional[StrictBool] = None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True