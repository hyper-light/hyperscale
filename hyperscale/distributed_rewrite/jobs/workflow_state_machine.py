"""
Workflow State Machine - Enforces valid workflow state transitions.

This module provides a state machine for workflow state management,
ensuring states only advance forward and preventing invalid transitions.
"""

from hyperscale.distributed_rewrite.models import WorkflowStatus


class WorkflowStateMachine:
    """
    State machine for workflow state transitions.

    Enforces valid state transitions and prevents regression.
    State progression: PENDING -> ASSIGNED -> RUNNING -> COMPLETED/FAILED -> AGGREGATED/AGGREGATION_FAILED

    Key rules:
    - States only advance forward, never regress
    - FAILED can transition to RUNNING (retry scenario)
    - Terminal states (AGGREGATED, AGGREGATION_FAILED) cannot transition
    """

    # Valid transitions: from_state -> set of valid to_states
    VALID_TRANSITIONS: dict[WorkflowStatus, set[WorkflowStatus]] = {
        WorkflowStatus.PENDING: {WorkflowStatus.ASSIGNED, WorkflowStatus.FAILED},
        WorkflowStatus.ASSIGNED: {WorkflowStatus.RUNNING, WorkflowStatus.COMPLETED, WorkflowStatus.FAILED},
        WorkflowStatus.RUNNING: {WorkflowStatus.COMPLETED, WorkflowStatus.FAILED},
        WorkflowStatus.COMPLETED: {WorkflowStatus.AGGREGATED, WorkflowStatus.AGGREGATION_FAILED},
        WorkflowStatus.FAILED: {WorkflowStatus.RUNNING, WorkflowStatus.ASSIGNED},  # Retry allowed
        WorkflowStatus.CANCELLED: set(),  # Terminal
        WorkflowStatus.AGGREGATED: set(),  # Terminal
        WorkflowStatus.AGGREGATION_FAILED: set(),  # Terminal
    }

    # State ordering for determining advancement (higher = more advanced)
    STATE_ORDER: dict[WorkflowStatus, int] = {
        WorkflowStatus.PENDING: 0,
        WorkflowStatus.ASSIGNED: 1,
        WorkflowStatus.RUNNING: 2,
        WorkflowStatus.COMPLETED: 3,
        WorkflowStatus.FAILED: 3,  # Same level as COMPLETED (both are "done" states)
        WorkflowStatus.CANCELLED: 3,  # Same level as COMPLETED
        WorkflowStatus.AGGREGATED: 4,
        WorkflowStatus.AGGREGATION_FAILED: 4,
    }

    @classmethod
    def can_transition(cls, from_state: WorkflowStatus, to_state: WorkflowStatus) -> bool:
        """Check if a state transition is valid."""
        if from_state == to_state:
            return False  # No-op, not a real transition
        return to_state in cls.VALID_TRANSITIONS.get(from_state, set())

    @classmethod
    def is_advancement(cls, from_state: WorkflowStatus, to_state: WorkflowStatus) -> bool:
        """Check if to_state is an advancement from from_state (not a regression)."""
        return cls.STATE_ORDER.get(to_state, 0) > cls.STATE_ORDER.get(from_state, 0)

    @classmethod
    def try_transition(cls, from_state: WorkflowStatus, to_state: WorkflowStatus) -> WorkflowStatus:
        """
        Attempt a state transition, returning the resulting state.

        Returns to_state if transition is valid, otherwise returns from_state unchanged.
        """
        if cls.can_transition(from_state, to_state):
            return to_state
        return from_state

    @classmethod
    def advance_state(cls, current_state: WorkflowStatus, desired_state: WorkflowStatus) -> WorkflowStatus:
        """
        Advance state if desired_state is more advanced, otherwise keep current.

        This is the primary method for sub-workflow updates - ensures parent workflow
        state only advances forward and never regresses.
        """
        if cls.is_advancement(current_state, desired_state) and cls.can_transition(current_state, desired_state):
            return desired_state
        return current_state
