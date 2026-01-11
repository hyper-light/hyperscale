"""
Manager workflow lifecycle module (AD-33).

Handles workflow state transitions, dependency resolution, and reschedule handling
per the AD-33 Workflow State Machine specification.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.workflow import (
    WorkflowStateMachine,
    WorkflowState,
)
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.state import ManagerState
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.logging import Logger


class ManagerWorkflowLifecycle:
    """
    Manages workflow lifecycle transitions (AD-33).

    Coordinates:
    - State machine initialization and transitions
    - Dependency resolution between workflows
    - Reschedule handling on failure
    - Completion tracking
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner

    def initialize_state_machine(self, datacenter: str, manager_id: str) -> None:
        """
        Initialize the workflow lifecycle state machine.

        Args:
            datacenter: Datacenter ID for this manager
            manager_id: This manager's ID
        """
        if self._state._workflow_lifecycle_states is None:
            self._state._workflow_lifecycle_states = WorkflowStateMachine(
                datacenter=datacenter,
                manager_id=manager_id,
            )

    async def transition_workflow(
        self,
        workflow_id: str,
        new_state: WorkflowState,
        reason: str | None = None,
    ) -> bool:
        """
        Transition a workflow to a new state.

        Args:
            workflow_id: Workflow ID
            new_state: Target state
            reason: Optional reason for transition

        Returns:
            True if transition succeeded
        """
        if self._state._workflow_lifecycle_states is None:
            return False

        current_state = self._state._workflow_lifecycle_states.get_state(workflow_id)
        success = await self._state._workflow_lifecycle_states.transition(
            workflow_id,
            new_state,
            reason=reason,
        )

        if success:
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Workflow {workflow_id[:8]}... transitioned {current_state} -> {new_state.value}",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )
        else:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Workflow {workflow_id[:8]}... transition to {new_state.value} failed",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )

        return success

    def get_workflow_state(self, workflow_id: str) -> WorkflowState | None:
        """
        Get current state of a workflow.

        Args:
            workflow_id: Workflow ID

        Returns:
            Current WorkflowState or None if not tracked
        """
        if self._state._workflow_lifecycle_states is None:
            return None
        return self._state._workflow_lifecycle_states.get_state(workflow_id)

    def is_workflow_terminal(self, workflow_id: str) -> bool:
        """
        Check if workflow is in a terminal state.

        Args:
            workflow_id: Workflow ID

        Returns:
            True if workflow is COMPLETED, FAILED, or CANCELLED
        """
        state = self.get_workflow_state(workflow_id)
        if state is None:
            return False
        return state in {
            WorkflowState.COMPLETED,
            WorkflowState.FAILED,
            WorkflowState.CANCELLED,
            WorkflowState.AGGREGATED,
        }

    def can_dispatch_workflow(self, workflow_id: str) -> bool:
        """
        Check if workflow can be dispatched.

        Args:
            workflow_id: Workflow ID

        Returns:
            True if workflow is in PENDING state
        """
        state = self.get_workflow_state(workflow_id)
        return state == WorkflowState.PENDING or state is None

    async def mark_workflow_dispatched(self, workflow_id: str, worker_id: str) -> bool:
        """
        Mark workflow as dispatched to a worker.

        Args:
            workflow_id: Workflow ID
            worker_id: Target worker ID

        Returns:
            True if transition succeeded
        """
        return await self.transition_workflow(
            workflow_id,
            WorkflowState.DISPATCHED,
            reason=f"Dispatched to worker {worker_id[:8]}...",
        )

    async def mark_workflow_running(self, workflow_id: str) -> bool:
        """
        Mark workflow as running.

        Args:
            workflow_id: Workflow ID

        Returns:
            True if transition succeeded
        """
        return await self.transition_workflow(
            workflow_id,
            WorkflowState.RUNNING,
        )

    async def mark_workflow_completed(self, workflow_id: str) -> bool:
        """
        Mark workflow as completed.

        Args:
            workflow_id: Workflow ID

        Returns:
            True if transition succeeded
        """
        success = await self.transition_workflow(
            workflow_id,
            WorkflowState.COMPLETED,
        )

        if success:
            # Signal completion event
            event = self._state._workflow_completion_events.get(workflow_id)
            if event:
                event.set()

        return success

    async def mark_workflow_failed(self, workflow_id: str, reason: str) -> bool:
        """
        Mark workflow as failed.

        Args:
            workflow_id: Workflow ID
            reason: Failure reason

        Returns:
            True if transition succeeded
        """
        success = await self.transition_workflow(
            workflow_id,
            WorkflowState.FAILED,
            reason=reason,
        )

        if success:
            # Signal completion event (failure is terminal)
            event = self._state._workflow_completion_events.get(workflow_id)
            if event:
                event.set()

        return success

    async def mark_workflow_cancelled(self, workflow_id: str) -> bool:
        """
        Mark workflow as cancelled.

        Args:
            workflow_id: Workflow ID

        Returns:
            True if transition succeeded
        """
        success = await self.transition_workflow(
            workflow_id,
            WorkflowState.CANCELLED,
        )

        if success:
            event = self._state._workflow_completion_events.get(workflow_id)
            if event:
                event.set()

        return success

    def cleanup_workflow_state(self, workflow_id: str) -> None:
        """
        Cleanup lifecycle state for a workflow.

        Args:
            workflow_id: Workflow ID to cleanup
        """
        self._state._workflow_completion_events.pop(workflow_id, None)
        self._state._workflow_results_locks.pop(workflow_id, None)
