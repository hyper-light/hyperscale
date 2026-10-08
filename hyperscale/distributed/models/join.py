"""Wire model ``Join`` -- pickled under the wire namespace
``hyperscale.distributed.models.internal`` (see that module)."""

import msgspec
from typing import Literal


class Join(msgspec.Struct):
    udp_addr: tuple[str, int]
    tcp_addr: tuple[str, int]
    confirmed: int
    message: Literal['JOIN'] = 'JOIN'
