"""``_Entry`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.timing_wheel`` (see that module)."""

import asyncio
from dataclasses import dataclass

from .suspicion_state import SuspicionState


@dataclass(slots=True)
class _Entry:
    """Internal entry: suspicion state, expiration deadline, asyncio handle."""

    state: SuspicionState
    expiration_time: float
    timer_handle: asyncio.TimerHandle | None = None
