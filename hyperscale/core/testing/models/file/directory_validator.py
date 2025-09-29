from pydantic import BaseModel, StrictStr


class DirectoryValidator(BaseModel):
    path: StrictStr