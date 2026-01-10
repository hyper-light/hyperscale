"""
Request routing state for client.

Per-job routing locks to prevent race conditions during leadership changes.
"""

import asyncio
from dataclasses import dataclass, field


@dataclass(slots=True)
class RequestRouting:
    """Per-job request routing state."""

    job_id: str
    routing_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
