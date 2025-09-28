import msgspec
from typing import Literal


class Listing(msgspec.Struct):
    type: Literal['dir', 'file']
    modify: int
    create: int
    size: int | None = None
