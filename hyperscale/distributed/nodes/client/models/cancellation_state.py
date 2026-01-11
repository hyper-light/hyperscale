"""
Cancellation tracking state for client.

Tracks cancellation completion events and results per job.
"""

import asyncio
from dataclasses import dataclass, field


@dataclass(slots=True)
class CancellationState:
    """State for tracking job cancellation on the client."""

    job_id: str
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)
    success: bool = False
    errors: list[str] = field(default_factory=list)
