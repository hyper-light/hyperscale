"""Wire model ``Probe`` -- pickled under the wire namespace
``hyperscale.distributed.models.internal`` (see that module)."""

import msgspec
from typing import Literal


class Probe(msgspec.Struct):
    target: tuple[str, int]
    comfirmed: int
    required: int
    message: Literal['PROBE'] = 'PROBE'
