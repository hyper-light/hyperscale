from pydantic import BaseModel, StrictStr


class FileGlobValidator(BaseModel):
    files: tuple[StrictStr, StrictStr]