import pathlib
from pydantic import BaseModel, StrictStr


class FileValidator(BaseModel):
    path: StrictStr | pathlib.Path
    path_encoding: StrictStr | None = None