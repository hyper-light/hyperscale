"""
Job-related models for internal manager tracking.

These models are used by the manager's job tracking system for
internal state management. They are not wire protocol messages.
"""

from dataclasses import dataclass, field

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.jobs.workers.stage_priority import StagePriority


@dataclass
class PendingWorkflow:
    """
    A workflow waiting to be dispatched.

    Used by WorkflowDispatcher to track workflows that are registered
    but not yet dispatched to workers. Tracks dependency completion,
    dispatch state, timeout for eviction, and retry state.
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
