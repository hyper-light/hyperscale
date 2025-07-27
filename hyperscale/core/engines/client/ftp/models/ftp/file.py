from pydantic import BaseModel, StrictStr


class File(BaseModel):
    path: StrictStr