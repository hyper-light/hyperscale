from pydantic import (
    BaseModel,
    StrictInt,
    StrictStr,
    conlist,
)


class SpinnerConfig(BaseModel):
    frames: conlist(StrictStr, min_length=1)
    interval: StrictInt
    size: StrictInt
