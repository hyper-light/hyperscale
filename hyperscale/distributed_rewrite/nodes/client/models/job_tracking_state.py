"""
Job tracking state for client.

Tracks job status, completion events, callbacks, and target routing.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed_rewrite.models import ClientJobResult


@dataclass(slots=True)
class JobTrackingState:
    """State for tracking a single job on the client."""

    job_id: str
    job_result: ClientJobResult
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)
    callback: Callable[[ClientJobResult], None] | None = None
    target_addr: tuple[str, int] | None = None
