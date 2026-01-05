"""
Job-related models for internal manager tracking.

These models are used by the manager's job tracking system for
internal state management. They are not wire protocol messages.
"""

import asyncio
from dataclasses import dataclass, field

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.jobs.workers.stage_priority import StagePriority


def _create_event() -> asyncio.Event:
    """Factory for creating asyncio.Event in dataclass field."""
    return asyncio.Event()


@dataclass
class PendingWorkflow:
    """
    A workflow waiting to be dispatched.

    Used by WorkflowDispatcher to track workflows that are registered
    but not yet dispatched to workers. Tracks dependency completion,
    dispatch state, timeout for eviction, and retry state.

    Event-driven dispatch:
    - ready_event: Set when dependencies are satisfied AND workflow is ready for dispatch
    - Dispatch loop waits on ready_event instead of polling
    """
    job_id: str
    workflow_id: str
    workflow_name: str
    workflow: Workflow
    vus: int
    priority: StagePriority
    is_test: bool
    dependencies: set[str]           # workflow_ids this depends on
    completed_dependencies: set[str] = field(default_factory=set)
    dispatched: bool = False
    cores_allocated: int = 0

    # Event-driven dispatch: set when dependencies satisfied and ready for dispatch attempt
    ready_event: asyncio.Event = field(default_factory=_create_event)

    # Timeout tracking
    registered_at: float = 0.0           # time.monotonic() when registered
    dispatched_at: float = 0.0           # time.monotonic() when dispatched
    timeout_seconds: float = 300.0       # Max seconds before eviction

    # Dispatch attempt tracking (for the dispatch flag race fix)
    dispatch_in_progress: bool = False   # True while async dispatch is in progress

    # Retry tracking with exponential backoff
    dispatch_attempts: int = 0           # Number of dispatch attempts
    last_dispatch_attempt: float = 0.0   # time.monotonic() of last attempt
    next_retry_delay: float = 1.0        # Seconds until next retry allowed
    max_dispatch_attempts: int = 5       # Max retries before marking failed

    def check_and_signal_ready(self) -> bool:
        """
        Check if workflow is ready for dispatch and signal if so.

        A workflow is ready when:
        - All dependencies are satisfied
        - Not already dispatched
        - Not currently being dispatched
        - Haven't exceeded max retries

        Returns True if workflow is ready (and signals the event).
        """
        if self.dispatched:
            return False
        if self.dispatch_in_progress:
            return False
        if self.dispatch_attempts >= self.max_dispatch_attempts:
            return False
        if not (self.dependencies <= self.completed_dependencies):
            return False

        # Ready - signal the event
        self.ready_event.set()
        return True

    def clear_ready(self) -> None:
        """Clear the ready event (called when dispatch starts or fails)."""
        self.ready_event.clear()
