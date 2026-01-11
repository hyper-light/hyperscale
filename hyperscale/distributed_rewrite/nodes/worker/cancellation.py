"""
Worker cancellation handler module (AD-20).

Handles workflow cancellation requests and completion notifications.
"""

import asyncio
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from hyperscale.distributed_rewrite.models import WorkflowProgress


class WorkerCancellationHandler:
    """
    Handles workflow cancellation for worker (AD-20).

    Manages cancellation events, polls for cancellation requests,
    and notifies managers of cancellation completion.
    """

    def __init__(
        self,
        logger: "Logger",
        poll_interval: float = 5.0,
    ) -> None:
        """
        Initialize cancellation handler.

        Args:
            logger: Logger instance for logging
            poll_interval: Interval for polling cancellation requests
        """
        self._logger = logger
        self._poll_interval = poll_interval
        self._running = False

        # Cancellation tracking
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._cancelled_workflows: set[str] = set()

    def create_cancel_event(self, workflow_id: str) -> asyncio.Event:
        """
        Create a cancellation event for a workflow.

        Args:
            workflow_id: Workflow identifier

        Returns:
            asyncio.Event for cancellation signaling
        """
        event = asyncio.Event()
        self._cancel_events[workflow_id] = event
        return event

    def get_cancel_event(self, workflow_id: str) -> asyncio.Event | None:
        """Get cancellation event for a workflow."""
        return self._cancel_events.get(workflow_id)

    def remove_cancel_event(self, workflow_id: str) -> None:
        """Remove cancellation event for a workflow."""
        self._cancel_events.pop(workflow_id, None)
        self._cancelled_workflows.discard(workflow_id)

    def signal_cancellation(self, workflow_id: str) -> bool:
        """
        Signal cancellation for a workflow.

        Args:
            workflow_id: Workflow to cancel

        Returns:
            True if event was set, False if workflow not found
        """
        if event := self._cancel_events.get(workflow_id):
            event.set()
            self._cancelled_workflows.add(workflow_id)
            return True
        return False

    def is_cancelled(self, workflow_id: str) -> bool:
        """Check if a workflow has been cancelled."""
        return workflow_id in self._cancelled_workflows

    async def cancel_workflow(
        self,
        workflow_id: str,
        reason: str,
        active_workflows: dict[str, "WorkflowProgress"],
        task_runner_cancel: callable,
        workflow_tokens: dict[str, str],
    ) -> tuple[bool, list[str]]:
        """
        Cancel a workflow and clean up resources.

        Args:
            workflow_id: Workflow to cancel
            reason: Cancellation reason
            active_workflows: Active workflows dict
            task_runner_cancel: Function to cancel TaskRunner tasks
            workflow_tokens: Map of workflow_id to task token

        Returns:
            Tuple of (success, list of errors)
        """
        errors: list[str] = []

        # Signal cancellation via event
        if not self.signal_cancellation(workflow_id):
            errors.append(f"No cancel event for workflow {workflow_id}")

        # Cancel via TaskRunner if we have a token
        if token := workflow_tokens.get(workflow_id):
            try:
                await task_runner_cancel(token)
            except Exception as exc:
                errors.append(f"TaskRunner cancel failed: {exc}")

        return (len(errors) == 0, errors)

    async def run_cancellation_poll_loop(
        self,
        get_healthy_managers: callable,
        send_cancel_query: callable,
    ) -> None:
        """
        Background loop for polling cancellation requests from managers.

        Args:
            get_healthy_managers: Function returning list of healthy manager addresses
            send_cancel_query: Function to send cancellation query to manager
        """
        self._running = True
        while self._running:
            try:
                await asyncio.sleep(self._poll_interval)

                managers = get_healthy_managers()
                if not managers:
                    continue

                # Poll first healthy manager for cancellation requests
                for manager_addr in managers:
                    try:
                        await send_cancel_query(manager_addr)
                        break
                    except Exception:
                        continue

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def stop(self) -> None:
        """Stop the cancellation poll loop."""
        self._running = False
