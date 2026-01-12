"""
Worker cancellation handler module (AD-20).

Handles workflow cancellation requests and completion notifications.
Extracted from worker_impl.py for modularity.
"""

import asyncio
import time
from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    WorkflowCancellationQuery,
    WorkflowCancellationResponse,
    WorkflowStatus,
)
from hyperscale.logging.hyperscale_logging_models import ServerInfo

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from hyperscale.distributed.models import WorkflowProgress
    from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
    from .state import WorkerState


class WorkerCancellationHandler:
    """
    Handles workflow cancellation for worker (AD-20).

    Manages cancellation events, polls for cancellation requests,
    and coordinates with RemoteGraphManager for workflow termination.
    """

    def __init__(
        self,
        state: "WorkerState",
        logger: "Logger | None" = None,
        poll_interval: float = 5.0,
    ) -> None:
        """
        Initialize cancellation handler.

        Args:
            state: WorkerState for workflow tracking
            logger: Logger instance for logging
            poll_interval: Interval for polling cancellation requests
        """
        self._state = state
        self._logger = logger
        self._poll_interval = poll_interval
        self._running = False

        # Remote graph manager (set later)
        self._remote_manager: "RemoteGraphManager | None" = None

    def set_remote_manager(self, remote_manager: "RemoteGraphManager") -> None:
        """Set the remote graph manager for workflow cancellation."""
        self._remote_manager = remote_manager

    def create_cancel_event(self, workflow_id: str) -> asyncio.Event:
        """
        Create a cancellation event for a workflow.

        Args:
            workflow_id: Workflow identifier

        Returns:
            asyncio.Event for cancellation signaling
        """
        event = asyncio.Event()
        self._state._workflow_cancel_events[workflow_id] = event
        return event

    def get_cancel_event(self, workflow_id: str) -> asyncio.Event | None:
        """Get cancellation event for a workflow."""
        return self._state._workflow_cancel_events.get(workflow_id)

    def remove_cancel_event(self, workflow_id: str) -> None:
        """Remove cancellation event for a workflow."""
        self._state._workflow_cancel_events.pop(workflow_id, None)

    def signal_cancellation(self, workflow_id: str) -> bool:
        """
        Signal cancellation for a workflow.

        Args:
            workflow_id: Workflow to cancel

        Returns:
            True if event was set, False if workflow not found
        """
        if event := self._state._workflow_cancel_events.get(workflow_id):
            event.set()
            return True
        return False

    async def cancel_workflow(
        self,
        workflow_id: str,
        reason: str,
        task_runner_cancel: callable,
        increment_version: callable,
    ) -> tuple[bool, list[str]]:
        """
        Cancel a workflow and clean up resources.

        Cancels via TaskRunner and RemoteGraphManager, then updates state.

        Args:
            workflow_id: Workflow to cancel
            reason: Cancellation reason
            task_runner_cancel: Function to cancel TaskRunner tasks
            increment_version: Function to increment state version

        Returns:
            Tuple of (success, list of errors)
        """
        errors: list[str] = []

        # Get task token
        token = self._state._workflow_tokens.get(workflow_id)
        if not token:
            return (False, [f"Workflow {workflow_id} not found (no token)"])

        # Signal cancellation via event
        cancel_event = self._state._workflow_cancel_events.get(workflow_id)
        if cancel_event:
            cancel_event.set()

        # Cancel via TaskRunner
        try:
            await task_runner_cancel(token)
        except Exception as exc:
            errors.append(f"TaskRunner cancel failed: {exc}")

        # Get workflow info before cleanup
        progress = self._state._active_workflows.get(workflow_id)
        job_id = progress.job_id if progress else ""

        # Update status
        if workflow_id in self._state._active_workflows:
            self._state._active_workflows[
                workflow_id
            ].status = WorkflowStatus.CANCELLED.value

        # Cancel in RemoteGraphManager
        workflow_name = self._state._workflow_id_to_name.get(workflow_id)
        if workflow_name and self._remote_manager:
            run_id = hash(workflow_id) % (2**31)
            try:
                (
                    success,
                    remote_errors,
                ) = await self._remote_manager.await_workflow_cancellation(
                    run_id,
                    workflow_name,
                    timeout=5.0,
                )
                if not success:
                    errors.append(
                        f"RemoteGraphManager cancellation timed out for {workflow_name}"
                    )
                if remote_errors:
                    errors.extend(remote_errors)
            except Exception as err:
                errors.append(f"RemoteGraphManager error: {str(err)}")

        await increment_version()

        return (True, errors)

    async def run_cancellation_poll_loop(
        self,
        get_manager_addr: callable,
        is_circuit_open: callable,
        send_tcp: callable,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: callable,
        is_running: callable,
    ) -> None:
        """
        Background loop for polling managers for cancellation status.

        Provides robust fallback when push notifications fail.

        Args:
            get_manager_addr: Function to get primary manager TCP address
            is_circuit_open: Function to check if circuit breaker is open
            send_tcp: Function to send TCP data
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            task_runner_run: Function to run async tasks
            is_running: Function to check if worker is running
        """
        self._running = True
        while is_running() and self._running:
            try:
                await asyncio.sleep(self._poll_interval)

                # Skip if no active workflows
                if not self._state._active_workflows:
                    continue

                # Get primary manager address
                manager_addr = get_manager_addr()
                if not manager_addr:
                    continue

                # Check circuit breaker
                if is_circuit_open():
                    continue

                # Poll for each active workflow
                workflows_to_cancel: list[str] = []

                for workflow_id, progress in list(
                    self._state._active_workflows.items()
                ):
                    query = WorkflowCancellationQuery(
                        job_id=progress.job_id,
                        workflow_id=workflow_id,
                    )

                    try:
                        response_data = await send_tcp(
                            manager_addr,
                            "workflow_cancellation_query",
                            query.dump(),
                            timeout=2.0,
                        )

                        if response_data:
                            response = WorkflowCancellationResponse.load(response_data)
                            if response.status == "CANCELLED":
                                workflows_to_cancel.append(workflow_id)

                    except Exception:
                        pass

                # Signal cancellation for workflows manager says are cancelled
                for workflow_id in workflows_to_cancel:
                    if cancel_event := self._state._workflow_cancel_events.get(
                        workflow_id
                    ):
                        if not cancel_event.is_set():
                            cancel_event.set()

                            if self._logger:
                                task_runner_run(
                                    self._logger.log,
                                    ServerInfo(
                                        message=f"Cancelling workflow {workflow_id} via poll (manager confirmed)",
                                        node_host=node_host,
                                        node_port=node_port,
                                        node_id=node_id_short,
                                    ),
                                )

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def stop(self) -> None:
        """Stop the cancellation poll loop."""
        self._running = False
