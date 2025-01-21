from pydantic import (
    BaseModel,
    StrictInt,
    StrictStr,
    conlist,
)


class SpinnerFramesSet(BaseModel):
    frames: conlist(StrictStr, min_length=1)
    interval: StrictInt
    size: StrictInt
