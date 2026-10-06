"""Wire model ``Leave`` -- pickled under the wire namespace
``hyperscale.distributed.models.internal`` (see that module)."""

import msgspec
from typing import Literal


class Leave(msgspec.Struct):
    node: tuple[str, int]
    message: Literal['LEAVE'] = 'LEAVE'
