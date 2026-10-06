"""Wire model ``Ack`` -- pickled under the wire namespace
``hyperscale.distributed.models.internal`` (see that module)."""

import msgspec
from typing import Literal


class Ack(msgspec.Struct):
    node: tuple[str, int]
    message: Literal['ACK'] = 'ACK'
