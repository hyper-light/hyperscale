"""Wire model ``Eject`` -- pickled under the wire namespace
``hyperscale.distributed.models.internal`` (see that module)."""

import msgspec
from typing import Literal


class Eject(msgspec.Struct):
    target: tuple[str, int]
    confirmed: int
    message: Literal['EJECT'] = 'EJECT'
