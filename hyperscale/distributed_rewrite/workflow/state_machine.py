"""
Workflow State Machine (AD-33).

Complete lifecycle state management for workflows, from pending through
completion, failure, cancellation, and retry. Enforces valid state transitions,
prevents race conditions, and provides observability.
"""

import asyncio
import time
from dataclasses import dataclass
from enum import Enum

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning


class WorkflowState(Enum):
    """
    Complete workflow lifecycle states (AD-33).

    State machine ensures workflows can only transition through valid paths,
    preventing race conditions and maintaining system invariants.
    """
    # Normal execution path
    PENDING = "pending"              # In dispatch queue, waiting for worker
    DISPATCHED = "dispatched"        # Sent to worker, awaiting ack
    RUNNING = "running"              # Worker executing
    COMPLETED = "completed"          # Successfully finished (terminal)

    # Failure & retry path
    FAILED = "failed"                          # Worker died, timeout, execution error
    FAILED_CANCELING_DEPENDENTS = "failed_canceling_deps"  # Cancelling dependent workflows
    FAILED_READY_FOR_RETRY = "failed_ready"   # Dependents cancelled, safe to retry

    # Cancellation path
    CANCELLING = "cancelling"        # Cancel requested, propagating to worker
    CANCELLED = "cancelled"          # Cancelled (terminal)

    # Additional states
    AGGREGATED = "aggregated"        # Results aggregated (terminal)


# Valid state transitions
VALID_TRANSITIONS: dict[WorkflowState, set[WorkflowState]] = {
    WorkflowState.PENDING: {
        WorkflowState.DISPATCHED,     # Normal: selected worker, sending dispatch
        WorkflowState.CANCELLING,     # Cancel requested before dispatch
        WorkflowState.FAILED,         # Worker died during dispatch selection
    },

    WorkflowState.DISPATCHED: {
        WorkflowState.RUNNING,        # Worker acked, started execution
        WorkflowState.CANCELLING,     # Cancel requested after dispatch
        WorkflowState.FAILED,         # Worker died before ack
    },

    WorkflowState.RUNNING: {
        WorkflowState.COMPLETED,      # Execution succeeded
        WorkflowState.FAILED,         # Worker died, timeout, or execution error
        WorkflowState.CANCELLING,     # Cancel requested during execution
        WorkflowState.AGGREGATED,     # Multi-core workflow aggregation
    },

    WorkflowState.FAILED: {
        WorkflowState.FAILED_CANCELING_DEPENDENTS,  # Start cancelling dependents
        WorkflowState.CANCELLED,      # Job-level cancel supersedes retry
    },

    WorkflowState.FAILED_CANCELING_DEPENDENTS: {
        WorkflowState.FAILED_READY_FOR_RETRY,  # All dependents cancelled
    },

    WorkflowState.FAILED_READY_FOR_RETRY: {
        WorkflowState.PENDING,        # Re-queued for retry
    },

    WorkflowState.CANCELLING: {
        WorkflowState.CANCELLED,      # Cancellation confirmed
    },

    # Terminal states - no outbound transitions
    WorkflowState.COMPLETED: set(),
    WorkflowState.CANCELLED: set(),
    WorkflowState.AGGREGATED: set(),
}


@dataclass
class StateTransition:
    """
    Record of a state transition for observability (AD-33).

    Tracked in state history to enable debugging and analysis.
    """
    from_state: WorkflowState
    to_state: WorkflowState
    timestamp: float
    reason: str  # Why transition occurred


class WorkflowStateMachine:
    """
    Manages workflow state transitions with validation (AD-33).

    Ensures workflows can only transition through valid paths,
    preventing race conditions and maintaining system invariants.

    Thread-safe via asyncio.Lock.
    """

    def __init__(self, logger: HyperscaleLogger, node_host: str, node_port: int, node_id: str):
        """
        Initialize workflow state machine.

        Args:
            logger: Logger for state transitions
            node_host: Manager host (for logging)
            node_port: Manager port (for logging)
            node_id: Manager ID (for logging)
        """
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id

        # Current state per workflow
        self._states: dict[str, WorkflowState] = {}

        # State transition history (for debugging)
        self._state_history: dict[str, list[StateTransition]] = {}

        # Lock for atomic state transitions
        self._lock = asyncio.Lock()

    async def transition(
        self,
        workflow_id: str,
        to_state: WorkflowState,
        reason: str = ""
    ) -> bool:
        """
        Attempt to transition workflow to new state.

        Validates transition is allowed, records in history, and logs.

        Args:
            workflow_id: Workflow to transition
            to_state: Target state
            reason: Human-readable reason for transition

        Returns:
            True if transition succeeded, False if invalid
        """
        async with self._lock:
            current_state = self._states.get(workflow_id, WorkflowState.PENDING)

            # Validate transition
            valid_next_states = VALID_TRANSITIONS.get(current_state, set())
            if to_state not in valid_next_states:
                await self._log_invalid_transition(
                    workflow_id, current_state, to_state, reason
                )
                return False

            # Calculate time spent in previous state
            previous_transition_time = 0.0
            if workflow_id in self._state_history and self._state_history[workflow_id]:
                previous_transition_time = self._state_history[workflow_id][-1].timestamp

            transition_duration_ms = (time.monotonic() - previous_transition_time) * 1000.0

            # Record transition
            self._states[workflow_id] = to_state

            # Record in history
            if workflow_id not in self._state_history:
                self._state_history[workflow_id] = []

            self._state_history[workflow_id].append(StateTransition(
                from_state=current_state,
                to_state=to_state,
                timestamp=time.monotonic(),
                reason=reason
            ))

            await self._log_transition(
                workflow_id, current_state, to_state, reason, transition_duration_ms
            )
            return True

    def get_state(self, workflow_id: str) -> WorkflowState:
        """
        Get current state of workflow.

        Args:
            workflow_id: Workflow to query

        Returns:
            Current state (PENDING if never seen)
        """
        return self._states.get(workflow_id, WorkflowState.PENDING)

    def is_in_state(self, workflow_id: str, *states: WorkflowState) -> bool:
        """
        Check if workflow is in any of the given states.

        Args:
            workflow_id: Workflow to check
            *states: States to check against

        Returns:
            True if current state matches any of the given states
        """
        return self.get_state(workflow_id) in states

    def get_history(self, workflow_id: str) -> list[StateTransition]:
        """
        Get complete state history for debugging.

        Args:
            workflow_id: Workflow to query

        Returns:
            List of all state transitions for this workflow
        """
        return self._state_history.get(workflow_id, [])

    def cleanup_workflow(self, workflow_id: str) -> None:
        """
        Remove workflow from tracking (job cleanup).

        Args:
            workflow_id: Workflow to remove
        """
        self._states.pop(workflow_id, None)
        self._state_history.pop(workflow_id, None)

    def get_state_counts(self) -> dict[WorkflowState, int]:
        """
        Get count of workflows in each state.

        Returns:
            Dict mapping state to count
        """
        counts: dict[WorkflowState, int] = {state: 0 for state in WorkflowState}
        for state in self._states.values():
            counts[state] += 1
        return counts

    async def _log_transition(
        self,
        workflow_id: str,
        from_state: WorkflowState,
        to_state: WorkflowState,
        reason: str,
        duration_ms: float
    ) -> None:
        """Log state transition."""
        await self._logger.log(
            ServerDebug(
                message=f"Workflow {workflow_id[:8]}... state: {from_state.value} → {to_state.value} ({reason})",
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id,
            )
        )

    async def _log_invalid_transition(
        self,
        workflow_id: str,
        current_state: WorkflowState,
        attempted_state: WorkflowState,
        reason: str
    ) -> None:
        """Log invalid transition attempt."""
        await self._logger.log(
            ServerWarning(
                message=f"Invalid state transition for workflow {workflow_id[:8]}...: "
                        f"{current_state.value} → {attempted_state.value} (reason: {reason})",
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id,
            )
        )
