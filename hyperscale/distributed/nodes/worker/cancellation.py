"""
Worker cancellation handler module (AD-20).

Handles workflow cancellation requests and completion notifications.
Extracted from worker_impl.py for modularity.
"""

import asyncio
from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    WorkflowCancellationQuery,
    WorkflowCancellationResponse,
    WorkflowStatus,
)
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerInfo

from hyperscale.distributed.runtime import Clock, RealClock, SendTcp, RunTask
from collections.abc import Awaitable, Callable


_DEFAULT_CLOCK: Clock = RealClock()

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
        *,
        query_timeout: float,
        cancel_timeout: float,
    ) -> None:
        """
        Initialize cancellation handler.

        Args:
            state: WorkerState for workflow tracking
            logger: Logger instance for logging
            poll_interval: Interval for polling cancellation requests
            query_timeout: Seconds a cancellation query to a manager may take
            cancel_timeout: Seconds to wait for a cancelled workflow to stop
        """
        self._state: "WorkerState" = state
        self._logger: "Logger | None" = logger
        self._poll_interval: float = poll_interval
        self._query_timeout: float = query_timeout
        self._cancel_timeout: float = cancel_timeout
        self._running: bool = False

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
        task_runner_cancel: Callable[[str], Awaitable[None]],
        increment_version: Callable[[], Awaitable[int]],
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
        self._set_cancel_event_if_present(workflow_id)

        # Cancel via TaskRunner
        await self._cancel_task_runner_token(task_runner_cancel, token, errors)

        self._mark_active_workflow_cancelled(workflow_id)

        await self._cancel_in_remote_manager(workflow_id, errors)

        await increment_version()

        return (True, errors)

    def _set_cancel_event_if_present(self, workflow_id: str) -> None:
        """Set a workflow's cancel event when it has one (AD-20)."""
        cancel_event = self._state._workflow_cancel_events.get(workflow_id)
        if cancel_event:
            cancel_event.set()

    @staticmethod
    async def _cancel_task_runner_token(
        task_runner_cancel: Callable[[str], Awaitable[None]],
        token: str,
        errors: list[str],
    ) -> None:
        """Cancel the workflow's TaskRunner task, recording a failure in ``errors``."""
        try:
            await task_runner_cancel(token)
        except Exception as exc:
            errors.append(f"TaskRunner cancel failed: {exc}")

    def _mark_active_workflow_cancelled(self, workflow_id: str) -> None:
        """Set an active workflow's status to CANCELLED."""
        # Get workflow info before cleanup
        progress = self._state._active_workflows.get(workflow_id)
        job_id = progress.job_id if progress else ""

        # Update status
        if workflow_id in self._state._active_workflows:
            self._state._active_workflows[
                workflow_id
            ].status = WorkflowStatus.CANCELLED.value

    async def _cancel_in_remote_manager(self, workflow_id: str, errors: list[str]) -> None:
        """Cancel a named workflow in the RemoteGraphManager when one is set."""
        # Cancel in RemoteGraphManager. TWO-step protocol by design:
        # ``cancel_workflow`` SUBMITS cancellation to every node
        # running the workflow, ``await_workflow_cancellation`` then
        # waits for them all to report terminal status. Awaiting
        # WITHOUT initiating (the previous behavior) hit the
        # no-cancellation-initiated branch, which returns instant
        # (True, []) — so the worker reported successful cancellation
        # while its executors ran the workflow to natural completion
        # (measured: a timed-out 100s workflow drained 55.5 virtual
        # seconds AFTER the client observed the timeout terminal —
        # zombie execution burning cores past the job's death).
        workflow_name = self._state._workflow_id_to_name.get(workflow_id)
        if workflow_name and self._remote_manager:
            await self._run_remote_cancellation(workflow_id, workflow_name, errors)

    async def _run_remote_cancellation(
        self,
        workflow_id: str,
        workflow_name: str,
        errors: list[str],
    ) -> None:
        """Submit then await the two-step RemoteGraphManager cancellation."""
        run_id = hash(workflow_id) % (2**31)
        try:
            # Graceful window "2s": by the time the worker cancels,
            # the decision is already made (job timeout, eviction,
            # explicit cancel) — a long graceful phase just extends
            # zombie execution, and the executor's hard-stop
            # escalation fires when this window expires. Must sit
            # inside the 5s terminal-report wait below, or every
            # cancel looks timed-out even when it worked.
            await self._remote_manager.cancel_workflow(
                run_id,
                workflow_name,
                timeout="2s",
            )
            (
                success,
                remote_errors,
            ) = await self._remote_manager.await_workflow_cancellation(
                run_id,
                workflow_name,
                timeout=self._cancel_timeout,
            )
            self._record_remote_cancellation_outcome(success, remote_errors, workflow_name, errors)
        except Exception as err:
            errors.append(f"RemoteGraphManager error: {str(err)}")

    @staticmethod
    def _record_remote_cancellation_outcome(
        success: bool,
        remote_errors: list[str],
        workflow_name: str,
        errors: list[str],
    ) -> None:
        """Append a timeout and any remote errors from the cancellation wait."""
        if not success:
            errors.append(
                f"RemoteGraphManager cancellation timed out for {workflow_name}"
            )
        if remote_errors:
            errors.extend(remote_errors)

    async def run_cancellation_poll_loop(
        self,
        get_manager_addr: Callable[[], tuple[str, int] | None],
        is_circuit_open: Callable[[], bool],
        send_tcp: SendTcp,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: RunTask,
        is_running: Callable[[], bool],
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
        node_identity = (node_host, node_port, node_id_short)
        while self._poll_loop_should_continue(is_running):
            if await self._guarded_poll_pass(
                get_manager_addr,
                is_circuit_open,
                send_tcp,
                task_runner_run,
                node_identity,
            ):
                break

    def _poll_loop_should_continue(self, is_running: Callable[[], bool]) -> bool:
        """Whether both the worker and this poll loop are still running."""
        return bool(is_running() and self._running)

    async def _guarded_poll_pass(
        self,
        get_manager_addr: Callable[[], tuple[str, int] | None],
        is_circuit_open: Callable[[], bool],
        send_tcp: SendTcp,
        task_runner_run: RunTask,
        node_identity: tuple[str, int, str],
    ) -> bool:
        """Run one poll pass; True when the loop was cancelled and must stop."""
        try:
            await self._poll_pass(
                get_manager_addr,
                is_circuit_open,
                send_tcp,
                task_runner_run,
                node_identity,
            )
        except asyncio.CancelledError:
            return True
        except Exception as loop_error:
            self._log_poll_loop_error(task_runner_run, loop_error, node_identity)
        return False

    def _log_poll_loop_error(
        self,
        task_runner_run: RunTask,
        loop_error: Exception,
        node_identity: tuple[str, int, str],
    ) -> None:
        """Log a failed cancellation-poll pass when a logger is set."""
        if self._logger:
            node_host, node_port, node_id_short = node_identity
            task_runner_run(
                self._logger.log,
                ServerDebug(
                    message=f"Cancellation poll loop error: {loop_error}",
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                ),
            )

    async def _poll_pass(
        self,
        get_manager_addr: Callable[[], tuple[str, int] | None],
        is_circuit_open: Callable[[], bool],
        send_tcp: SendTcp,
        task_runner_run: RunTask,
        node_identity: tuple[str, int, str],
    ) -> None:
        """Sleep one poll interval, then poll when any workflow is active."""
        await _DEFAULT_CLOCK.sleep(self._poll_interval)

        # Skip if no active workflows
        if not self._state._active_workflows:
            return

        await self._poll_primary_manager(
            get_manager_addr,
            is_circuit_open,
            send_tcp,
            task_runner_run,
            node_identity,
        )

    async def _poll_primary_manager(
        self,
        get_manager_addr: Callable[[], tuple[str, int] | None],
        is_circuit_open: Callable[[], bool],
        send_tcp: SendTcp,
        task_runner_run: RunTask,
        node_identity: tuple[str, int, str],
    ) -> None:
        """Query the primary manager per active workflow and signal confirmed cancels."""
        # Get primary manager address
        manager_addr = get_manager_addr()
        if not manager_addr:
            return

        # Check circuit breaker
        if is_circuit_open():
            return

        # Poll for each active workflow
        workflows_to_cancel = await self._query_cancelled_workflows(
            manager_addr,
            send_tcp,
            task_runner_run,
            node_identity,
        )

        # Signal cancellation for workflows manager says are cancelled
        self._signal_polled_cancellations(workflows_to_cancel, task_runner_run, node_identity)

    def _signal_polled_cancellations(
        self,
        workflows_to_cancel: list[str],
        task_runner_run: RunTask,
        node_identity: tuple[str, int, str],
    ) -> None:
        """Signal every workflow the manager confirmed as cancelled."""
        for workflow_id in workflows_to_cancel:
            self._signal_polled_cancellation(workflow_id, task_runner_run, node_identity)

    async def _query_cancelled_workflows(
        self,
        manager_addr: tuple[str, int],
        send_tcp: SendTcp,
        task_runner_run: RunTask,
        node_identity: tuple[str, int, str],
    ) -> list[str]:
        """The active workflows the manager reports as CANCELLED."""
        workflows_to_cancel: list[str] = []

        for workflow_id, progress in list(
            self._state._active_workflows.items()
        ):
            if await self._query_workflow_cancelled(
                manager_addr,
                workflow_id,
                progress,
                send_tcp,
                task_runner_run,
                node_identity,
            ):
                workflows_to_cancel.append(workflow_id)

        return workflows_to_cancel

    async def _query_workflow_cancelled(
        self,
        manager_addr: tuple[str, int],
        workflow_id: str,
        progress: "WorkflowProgress",
        send_tcp: SendTcp,
        task_runner_run: RunTask,
        node_identity: tuple[str, int, str],
    ) -> bool:
        """Whether the manager reports one workflow CANCELLED; a failed query logs and says no."""
        query = WorkflowCancellationQuery(
            job_id=progress.job_id,
            workflow_id=workflow_id,
        )

        try:
            return await self._fetch_cancellation_status(manager_addr, query, send_tcp)
        except Exception as poll_error:
            self._log_poll_failure(
                workflow_id,
                manager_addr,
                poll_error,
                task_runner_run,
                node_identity,
            )
        return False

    async def _fetch_cancellation_status(
        self,
        manager_addr: tuple[str, int],
        query: WorkflowCancellationQuery,
        send_tcp: SendTcp,
    ) -> bool:
        """Send one cancellation query; True when the reply status is CANCELLED."""
        response_data, _ = await send_tcp(
            manager_addr,
            "workflow_cancellation_query",
            query.dump(),
            timeout=self._query_timeout,
        )
        # send_tcp returns transport errors rather than raising.
        if isinstance(response_data, Exception):
            raise response_data

        return self._response_says_cancelled(response_data)

    @staticmethod
    def _response_says_cancelled(response_data: bytes) -> bool:
        """Whether a non-empty cancellation response reports CANCELLED."""
        if not response_data:
            return False
        response = WorkflowCancellationResponse.load(response_data)
        return response.status == "CANCELLED"

    def _log_poll_failure(
        self,
        workflow_id: str,
        manager_addr: tuple[str, int],
        poll_error: Exception,
        task_runner_run: RunTask,
        node_identity: tuple[str, int, str],
    ) -> None:
        """Log a failed cancellation query when a logger is set."""
        if self._logger:
            node_host, node_port, node_id_short = node_identity
            task_runner_run(
                self._logger.log,
                ServerDebug(
                    message=(
                        f"Cancellation poll failed for workflow {workflow_id} "
                        f"via manager {manager_addr[0]}:{manager_addr[1]}: {poll_error}"
                    ),
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                ),
            )

    def _signal_polled_cancellation(
        self,
        workflow_id: str,
        task_runner_run: RunTask,
        node_identity: tuple[str, int, str],
    ) -> None:
        """Set a manager-confirmed workflow's cancel event if not already set."""
        cancel_event = self._state._workflow_cancel_events.get(workflow_id)
        if cancel_event and not cancel_event.is_set():
            self._set_polled_cancel_event(cancel_event, workflow_id, task_runner_run, node_identity)

    def _set_polled_cancel_event(
        self,
        cancel_event: asyncio.Event,
        workflow_id: str,
        task_runner_run: RunTask,
        node_identity: tuple[str, int, str],
    ) -> None:
        """Set the cancel event and log the poll-driven cancel."""
        cancel_event.set()

        if self._logger:
            node_host, node_port, node_id_short = node_identity
            task_runner_run(
                self._logger.log,
                ServerInfo(
                    message=f"Cancelling workflow {workflow_id} via poll (manager confirmed)",
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                ),
            )

    def stop(self) -> None:
        """Stop the cancellation poll loop."""
        self._running = False

    def cleanup_stale_events(self, active_workflow_ids: set[str]) -> int:
        """
        Remove cancel events for workflows that are no longer active.

        Defensive cleanup to prevent memory leaks if events outlive
        their workflows due to race conditions or missed cleanup.

        Args:
            active_workflow_ids: Currently active workflow IDs

        Returns:
            Number of stale events removed
        """
        stale_ids = self._stale_cancel_event_ids(active_workflow_ids)

        for workflow_id in stale_ids:
            self._state._workflow_cancel_events.pop(workflow_id, None)
            self._state._cancellation_completion_events.pop(workflow_id, None)
            self._state._cancellation_errors.pop(workflow_id, None)

        return len(stale_ids)

    def _stale_cancel_event_ids(self, active_workflow_ids: set[str]) -> list[str]:
        """Workflow ids holding a cancel event but no longer active."""
        return [
            workflow_id
            for workflow_id in self._state._workflow_cancel_events
            if workflow_id not in active_workflow_ids
        ]

    def get_cancellation_stats(self) -> dict[str, int]:
        """
        Get observability stats for cancellation tracking.

        Returns:
            Dictionary with cancel event counts and active workflow counts
        """
        return {
            "cancel_events": len(self._state._workflow_cancel_events),
            "completion_events": len(self._state._cancellation_completion_events),
            "active_workflows": len(self._state._active_workflows),
            "pending_errors": len(self._state._cancellation_errors),
        }
