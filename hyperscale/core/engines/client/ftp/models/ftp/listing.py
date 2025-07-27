from pydantic import BaseModel, StrictInt
from typing import Literal


class Listing(BaseModel):
    type: Literal['dir', 'file']
    modify: StrictInt
    create: StrictInt
    size: StrictInt | None = None
