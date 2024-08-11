from pydantic import (
    BaseModel,
    StrictInt,
    StrictStr,
    conlist,
)


class Spinner(BaseModel):
    frames: conlist(StrictStr, min_length=1)
    interval: StrictInt
