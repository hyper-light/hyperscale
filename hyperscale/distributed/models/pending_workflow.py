"""Wire model ``PendingWorkflow`` -- pickled under the wire namespace
``hyperscale.distributed.models.jobs`` (see that module)."""

import asyncio
from dataclasses import dataclass, field
from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.jobs.workers.stage_priority import StagePriority
from hyperscale.distributed.protocol.time_quantum import TIME_REMAINDER_EPSILON_SECONDS


def _create_event() -> asyncio.Event:
    """Factory for creating asyncio.Event in dataclass field."""
    return asyncio.Event()


@dataclass(slots=True)
class PendingWorkflow:
    """
    A workflow's entry in the manager's dispatch queue.

    Its lifecycle (AD-54, ``JobManager.workflow_lifecycle``) says whether
    it awaits dispatch -- PENDING -- or a worker took it; the entry holds
    what dispatching it needs: the workflow, its dependencies and which of
    them completed, and the backoff after a failed attempt.

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
    dependencies: set[str]  # workflow_ids this depends on
    completed_dependencies: set[str] = field(default_factory=set)

    # Event-driven dispatch: set when dependencies satisfied and ready for dispatch attempt
    ready_event: asyncio.Event = field(default_factory=_create_event)

    # Backoff after attempts that failed for a cause other than capacity
    failed_dispatch_attempts: int = 0  # Attempts that failed and backed off
    last_dispatch_attempt: float = 0.0  # _DEFAULT_CLOCK.monotonic() the last of them started
    next_retry_delay: float = 1.0  # Seconds the next attempt waits after it
    excluded_worker_ids: set[str] = field(default_factory=set)

    def is_retry_backoff_expired(self, now: float) -> bool:
        """Whether the retry backoff has elapsed at ``now``.

        THE single expiry predicate for dispatch retry pacing — the
        dispatch loop's eligibility filter and the ready-workflow scan
        must agree with the waits computed from the same deadline, so
        they all route through here. Remainders at or below the
        protocol time epsilon count as EXPIRED: composed-float
        deadlines leave positive sub-quantum remainders (observed
        1.6e-11s) that are semantically due — treating them as pending
        while waiting on them armed same-instant timers forever (the
        dispatcher frozen-instant livelock).
        """
        if self.failed_dispatch_attempts == 0:
            return True
        elapsed_since_attempt = now - self.last_dispatch_attempt
        return elapsed_since_attempt >= (
            self.next_retry_delay - TIME_REMAINDER_EPSILON_SECONDS
        )

    def remaining_retry_backoff_seconds(self, now: float) -> float:
        """Remaining backoff at ``now``; sub-epsilon remainders are 0.0
        so no caller ever schedules a wait the clock cannot honor."""
        if self.failed_dispatch_attempts == 0:
            return 0.0
        remaining = self.next_retry_delay - (now - self.last_dispatch_attempt)
        if remaining <= TIME_REMAINDER_EPSILON_SECONDS:
            return 0.0
        return remaining

    def check_and_signal_ready(self) -> bool:
        """
        Signal the dispatch loop once every dependency has completed.

        Callers signal only workflows whose lifecycle is PENDING.

        Returns True if workflow is ready (and signals the event).
        """
        if not (self.dependencies <= self.completed_dependencies):
            return False

        # Ready - signal the event
        self.ready_event.set()
        return True

    def clear_ready(self) -> None:
        """Clear the ready event (called when dispatch starts or fails)."""
        self.ready_event.clear()
