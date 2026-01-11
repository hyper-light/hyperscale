"""
Workflow runtime state for worker.

Tracks the execution state of active workflows including progress,
allocated resources, and job leader information.
"""

from dataclasses import dataclass


@dataclass(slots=True)
class WorkflowRuntimeState:
    """
    Runtime state for an active workflow on this worker.

    Contains all information needed to track execution progress
    and route updates to the correct job leader.
    """

    workflow_id: str
    job_id: str
    status: str
    allocated_cores: int
    fence_token: int
    start_time: float
    job_leader_addr: tuple[str, int] | None = None
    is_orphaned: bool = False
    orphaned_since: float | None = None
    cores_completed: int = 0
    vus: int = 0
