"""
One workflow's lifecycle (AD-54).
"""

from collections import deque
from dataclasses import dataclass

from .state_transition import StateTransition
from .workflow_state import WorkflowState


@dataclass(slots=True)
class WorkflowLifecycleRecord:
    """
    A workflow's current lifecycle state, the transitions that led to it
    (bounded: the oldest fall off past the machine's history bound), and
    when it last moved.

    ``retry_generation`` counts the times the workflow was requeued
    (FAILED_READY_FOR_RETRY -> PENDING); snapshots merge by it, so a later
    generation's state always wins over an earlier one's.
    """

    state: WorkflowState
    history: deque[StateTransition]
    last_transition_at: float
    retry_generation: int = 0
