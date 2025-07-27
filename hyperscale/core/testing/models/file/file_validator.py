from pydantic import BaseModel, StrictStr


class FileValidator(BaseModel):
    path: StrictStr