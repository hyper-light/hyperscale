"""
TCP handlers for job and workflow cancellation.

Handles cancellation requests and completion notifications (AD-20 compliance).
"""

import time
from typing import TYPE_CHECKING, Any, Callable, Coroutine

from hyperscale.distributed.models import (
    CancelJob,
    JobCancelRequest,
    JobCancelResponse,
    WorkflowCancellationComplete,
    JobCancellationComplete,
)
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger

CancelJobFunc = Callable[
    [JobCancelRequest, tuple[str, int]], Coroutine[Any, Any, bytes]
]
WorkflowCancelledFunc = Callable[..., Coroutine[Any, Any, None]]


class CancelJobHandler:
    """
    Handle legacy CancelJob requests.

    Normalizes legacy format to AD-20 JobCancelRequest internally.
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
        cancel_job_impl,  # Callable implementing actual cancellation
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner
        self._cancel_job_impl = cancel_job_impl

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Process legacy cancel job request.

        Args:
            addr: Source address
            data: Serialized CancelJob message
            clock_time: Logical clock time

        Returns:
            Serialized JobCancelResponse
        """
        try:
            request = CancelJob.load(data)

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Cancel job request (legacy) for job_id={request.job_id[:8]}...",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

            # Normalize to AD-20 format and delegate
            ad20_request = JobCancelRequest(
                job_id=request.job_id,
                requester_id=self._node_id,
                timestamp=time.time(),
                reason=request.reason
                if hasattr(request, "reason")
                else "User requested",
            )

            result = await self._cancel_job_impl(ad20_request, addr)
            return result

        except Exception as e:
            return JobCancelResponse(
                job_id="unknown",
                success=False,
                error=str(e),
            ).dump()


class JobCancelRequestHandler:
    """
    Handle AD-20 compliant job cancellation requests.

    Coordinates cancellation across all workflows in the job.
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
        cancel_job_impl,  # Callable implementing actual cancellation
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner
        self._cancel_job_impl = cancel_job_impl

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Process AD-20 cancel job request.

        Args:
            addr: Source address
            data: Serialized JobCancelRequest message
            clock_time: Logical clock time

        Returns:
            Serialized JobCancelResponse
        """
        try:
            request = JobCancelRequest.load(data)

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Cancel job request (AD-20) for job_id={request.job_id[:8]}... from {request.requester_id[:8]}...",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

            result = await self._cancel_job_impl(request, addr)
            return result

        except Exception as e:
            return JobCancelResponse(
                job_id="unknown",
                success=False,
                error=str(e),
            ).dump()


class WorkflowCancellationCompleteHandler:
    """
    Handle workflow cancellation completion notifications (AD-20).

    Tracks cancellation completion from workers and notifies clients
    when all workflows are cancelled.
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
        handle_workflow_cancelled,  # Callable to process completion
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner
        self._handle_workflow_cancelled = handle_workflow_cancelled

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Process workflow cancellation completion notification.

        Args:
            addr: Source address (worker)
            data: Serialized WorkflowCancellationComplete message
            clock_time: Logical clock time

        Returns:
            b'ok' on success
        """
        try:
            notification = WorkflowCancellationComplete.load(data)

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Workflow {notification.workflow_id[:8]}... cancellation complete for job {notification.job_id[:8]}...",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

            await self._handle_workflow_cancelled(notification)
            return b"ok"

        except Exception as e:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Error handling workflow cancellation complete: {e}",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            return b"error"
