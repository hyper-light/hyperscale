"""``WriteRequest`` -- pickled under the namespace
``hyperscale.distributed.ledger.wal.wal_writer`` (see that module)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass(slots=True)
class WriteRequest:
    data: bytes
    future: asyncio.Future[None]
