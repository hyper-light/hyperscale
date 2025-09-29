import pathlib
from pydantic import BaseModel, StrictStr


class FileGlobValidator(BaseModel):
    filepath: StrictStr | pathlib.Path
    pattern: StrictStr
    path_encoding: StrictStr | None = None