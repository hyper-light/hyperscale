"""
Request routing state for client.

Per-job routing locks and selected target tracking to prevent race conditions
during leadership changes and enable sticky routing.
"""

import asyncio
from dataclasses import dataclass, field


@dataclass(slots=True)
class RequestRouting:
    """
    Per-job request routing state.

    Tracks both the routing lock (to prevent concurrent routing changes)
    and the selected target (for sticky routing to the same server).
    """

    job_id: str
    routing_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    selected_target: tuple[str, int] | None = None
