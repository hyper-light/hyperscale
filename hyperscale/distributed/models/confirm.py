"""Wire model ``Confirm`` -- pickled under the wire namespace
``hyperscale.distributed.models.internal`` (see that module)."""

import msgspec
from typing import Literal


class Confirm(msgspec.Struct):
    target: tuple[str, int]
    refuted: int
    required: int
    message: Literal['PROBE'] = 'PROBE'
