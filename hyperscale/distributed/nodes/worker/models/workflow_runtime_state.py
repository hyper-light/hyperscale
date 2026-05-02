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

    Phase H3 extends this with the secondary and tertiary progress
    counters that feed ``WorkflowProgressSnapshot`` for AD-26
    extension decisions:

    * ``cores_completed`` (existing) — primary, finished-core count.
    * ``step_transitions`` — secondary, AD-33 state-machine
      transition count (PENDING→RUNNING→COMPLETED, FAILED, etc.).
    * ``actions_completed`` — tertiary, sum of action-level
      completions from ``StepStats.completed_count``.

    All three counters are integer monotonic and reset only on
    re-dispatch (which produces a new ``workflow_id`` per AD-10
    fence-token semantics).
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
    # Phase H3: multi-dimensional progress counters for AD-26
    # WorkflowProgressSnapshot (extension witness).
    step_transitions: int = 0
    actions_completed: int = 0
