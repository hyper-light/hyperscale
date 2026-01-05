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
    but not yet dispatched to workers. Tracks dependency completion
    and dispatch state.
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
