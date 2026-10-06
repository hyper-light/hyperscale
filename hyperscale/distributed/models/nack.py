"""Wire model ``Nack`` -- pickled under the wire namespace
``hyperscale.distributed.models.internal`` (see that module)."""

import msgspec
from typing import Literal


class Nack(msgspec.Struct):
    node: tuple[str, int]
    message: Literal['NACK'] = 'NACK'
