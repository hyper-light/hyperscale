"""
One requested workflow lifecycle edge (AD-54).
"""

from dataclasses import dataclass

from .workflow_state import WorkflowState


@dataclass(slots=True, frozen=True)
class StateTransition:
    """
    A requested lifecycle edge and whether it was taken.

    ``from_state`` is None when the workflow was unknown (a registration,
    or a request for a workflow the machine does not hold). A rejected
    transition left the workflow's state unchanged. ``installed`` marks a
    state set outright from a snapshot rather than moved along the table.
    """

    job_id: str
    workflow_id: str
    from_state: WorkflowState | None
    to_state: WorkflowState
    timestamp: float
    reason: str
    accepted: bool
    installed: bool
