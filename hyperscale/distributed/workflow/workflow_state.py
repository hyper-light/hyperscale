"""
Workflow lifecycle states and the edges between them (AD-54).

A parent workflow -- the unit of dependency, retry, cancellation and
client status -- is in exactly one of these states. Sub-workflows (one
per worker the parent was split across) are data on the parent, not
states of their own.
"""

from enum import Enum

from hyperscale.distributed.models import WorkflowStatus


class WorkflowState(Enum):
    """
    Complete workflow lifecycle states (AD-54).

    * PENDING: queued for dispatch, waiting for cores (or for its
      dependencies to complete).
    * DISPATCHED: sent to its workers, which accepted it; not yet known to
      be executing.
    * RUNNING: executing -- a worker reported progress (or a result).
    * COMPLETED / AGGREGATED: finished successfully, from one sub-workflow
      or merged from several (terminal).
    * FAILED: could not finish; terminal unless the retry path moves it on
      in the same step.
    * FAILED_CANCELING_DEPENDENTS / FAILED_READY_FOR_RETRY: the retry path
      -- dependents cleared, then requeued.
    * CANCELLING / CANCELLED: a cancel is propagating; confirmed (terminal).
    """

    PENDING = "pending"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    FAILED_CANCELING_DEPENDENTS = "failed_canceling_deps"
    FAILED_READY_FOR_RETRY = "failed_ready"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    AGGREGATED = "aggregated"


VALID_TRANSITIONS: dict[WorkflowState, frozenset[WorkflowState]] = {
    WorkflowState.PENDING: frozenset(
        {
            WorkflowState.DISPATCHED,
            WorkflowState.CANCELLING,
            WorkflowState.FAILED,
        }
    ),
    WorkflowState.DISPATCHED: frozenset(
        {
            WorkflowState.RUNNING,
            WorkflowState.CANCELLING,
            WorkflowState.FAILED,
        }
    ),
    WorkflowState.RUNNING: frozenset(
        {
            WorkflowState.COMPLETED,
            WorkflowState.FAILED,
            WorkflowState.CANCELLING,
            WorkflowState.AGGREGATED,
        }
    ),
    WorkflowState.FAILED: frozenset(
        {
            WorkflowState.FAILED_CANCELING_DEPENDENTS,
            WorkflowState.CANCELLED,
        }
    ),
    WorkflowState.FAILED_CANCELING_DEPENDENTS: frozenset(
        {WorkflowState.FAILED_READY_FOR_RETRY}
    ),
    WorkflowState.FAILED_READY_FOR_RETRY: frozenset({WorkflowState.PENDING}),
    WorkflowState.CANCELLING: frozenset({WorkflowState.CANCELLED}),
    WorkflowState.COMPLETED: frozenset(),
    WorkflowState.CANCELLED: frozenset(),
    WorkflowState.AGGREGATED: frozenset(),
}

# The one projection of a lifecycle state onto the wire's WorkflowStatus.
# DISPATCHED is ASSIGNED: an accepted, not yet running workflow must stay
# cancellable as one a worker holds. AGGREGATED is COMPLETED: clients read
# any status but COMPLETED as a failure. The retry path's transient states
# read as the status they are passing through.
WORKFLOW_STATUS_BY_WORKFLOW_STATE: dict[WorkflowState, WorkflowStatus] = {
    WorkflowState.PENDING: WorkflowStatus.PENDING,
    WorkflowState.DISPATCHED: WorkflowStatus.ASSIGNED,
    WorkflowState.RUNNING: WorkflowStatus.RUNNING,
    WorkflowState.COMPLETED: WorkflowStatus.COMPLETED,
    WorkflowState.AGGREGATED: WorkflowStatus.COMPLETED,
    WorkflowState.FAILED: WorkflowStatus.FAILED,
    WorkflowState.FAILED_CANCELING_DEPENDENTS: WorkflowStatus.FAILED,
    WorkflowState.FAILED_READY_FOR_RETRY: WorkflowStatus.PENDING,
    WorkflowState.CANCELLING: WorkflowStatus.CANCELLED,
    WorkflowState.CANCELLED: WorkflowStatus.CANCELLED,
}

# A snapshot that carries only a WorkflowStatus (a peer from before the
# lifecycle travelled with it) installs as the state it projects from.
WORKFLOW_STATE_BY_WORKFLOW_STATUS: dict[WorkflowStatus, WorkflowState] = {
    WorkflowStatus.PENDING: WorkflowState.PENDING,
    WorkflowStatus.ASSIGNED: WorkflowState.DISPATCHED,
    WorkflowStatus.RUNNING: WorkflowState.RUNNING,
    WorkflowStatus.COMPLETED: WorkflowState.COMPLETED,
    WorkflowStatus.AGGREGATED: WorkflowState.AGGREGATED,
    WorkflowStatus.FAILED: WorkflowState.FAILED,
    WorkflowStatus.AGGREGATION_FAILED: WorkflowState.FAILED,
    WorkflowStatus.CANCELLED: WorkflowState.CANCELLED,
}

# How far along one retry generation a state is, for merging snapshots
# forward-only: a workflow adopts another manager's view of it only when
# that view is ahead -- a later retry generation, or a higher rank in the
# same one. FAILED and the retry chain's transient states rank with the
# finished ones: a generation ends there.
WORKFLOW_STATE_RANK: dict[WorkflowState, int] = {
    WorkflowState.PENDING: 0,
    WorkflowState.DISPATCHED: 1,
    WorkflowState.RUNNING: 2,
    WorkflowState.CANCELLING: 3,
    WorkflowState.COMPLETED: 4,
    WorkflowState.AGGREGATED: 4,
    WorkflowState.FAILED: 4,
    WorkflowState.FAILED_CANCELING_DEPENDENTS: 4,
    WorkflowState.FAILED_READY_FOR_RETRY: 4,
    WorkflowState.CANCELLED: 4,
}

# The transitions a lifecycle can hold, by phase -- the structure the
# machine's history bound is derived from.
REGISTRATION_TRANSITIONS = 1  # -> PENDING
FINAL_RUN_TRANSITIONS = 3  # PENDING -> DISPATCHED -> RUNNING -> terminal
RETRY_CYCLE_TRANSITIONS = 6  # FAILED -> FCD -> FRR -> PENDING -> DISPATCHED -> RUNNING
CANCELLATION_TRANSITIONS = 2  # -> CANCELLING -> CANCELLED
