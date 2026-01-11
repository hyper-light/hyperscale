"""
TCP handlers for job and workflow cancellation operations.

Handles cancellation requests:
- Job cancellation from clients
- Single workflow cancellation
- Cancellation completion notifications
"""

import asyncio
from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    CancelAck,
    CancelJob,
    GlobalJobStatus,
    JobCancelRequest,
    JobCancelResponse,
    JobCancellationComplete,
    JobStatus,
    SingleWorkflowCancelRequest,
    SingleWorkflowCancelResponse,
    WorkflowCancellationStatus,
)
from hyperscale.distributed.reliability import (
    RateLimitResponse,
    JitterStrategy,
    RetryConfig,
    RetryExecutor,
)
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerError,
    ServerInfo,
)

from ..state import GateRuntimeState

if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.tracking import GateJobManager
    from taskex import TaskRunner


class GateCancellationHandler:
    """
    Handles job and workflow cancellation operations.

    Provides TCP handler methods for cancellation requests from clients
    and completion notifications from managers.
    """

    def __init__(
        self,
        state: GateRuntimeState,
        logger: Logger,
        task_runner: "TaskRunner",
        job_manager: "GateJobManager",
        datacenter_managers: dict[str, list[tuple[str, int]]],
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        check_rate_limit: Callable[[str, str], tuple[bool, float]],
        send_tcp: Callable,
        get_available_datacenters: Callable[[], list[str]],
    ) -> None:
        """
        Initialize the cancellation handler.

        Args:
            state: Runtime state container
            logger: Async logger instance
            task_runner: Background task executor
            job_manager: Job management service
            datacenter_managers: DC -> manager addresses mapping
            get_node_id: Callback to get this gate's node ID
            get_host: Callback to get this gate's host
            get_tcp_port: Callback to get this gate's TCP port
            check_rate_limit: Callback to check rate limit
            send_tcp: Callback to send TCP messages
            get_available_datacenters: Callback to get available DCs
        """
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._job_manager = job_manager
        self._datacenter_managers = datacenter_managers
        self._get_node_id = get_node_id
        self._get_host = get_host
        self._get_tcp_port = get_tcp_port
        self._check_rate_limit = check_rate_limit
        self._send_tcp = send_tcp
        self._get_available_datacenters = get_available_datacenters

    def _build_cancel_response(
        self,
        use_ad20: bool,
        job_id: str,
        success: bool,
        error: str | None = None,
        cancelled_count: int = 0,
        already_cancelled: bool = False,
        already_completed: bool = False,
    ) -> bytes:
        """Build cancel response in appropriate format (AD-20 or legacy)."""
        if use_ad20:
            return JobCancelResponse(
                job_id=job_id,
                success=success,
                error=error,
                cancelled_workflow_count=cancelled_count,
                already_cancelled=already_cancelled,
                already_completed=already_completed,
            ).dump()
        return CancelAck(
            job_id=job_id,
            cancelled=success,
            error=error,
            workflows_cancelled=cancelled_count,
        ).dump()

    def _is_ad20_cancel_request(self, data: bytes) -> bool:
        """Check if cancel request data is AD-20 format."""
        try:
            JobCancelRequest.load(data)
            return True
        except Exception:
            return False

    async def handle_cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle job cancellation from client (AD-20).

        Supports both legacy CancelJob and new JobCancelRequest formats.
        Uses retry logic with exponential backoff when forwarding to managers.

        Args:
            addr: Client address
            data: Serialized cancel request
            handle_exception: Callback for exception handling

        Returns:
            Serialized cancel response
        """
        try:
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit(client_id, "cancel")
            if not allowed:
                return RateLimitResponse(
                    operation="cancel",
                    retry_after_seconds=retry_after,
                ).dump()

            try:
                cancel_request = JobCancelRequest.load(data)
                job_id = cancel_request.job_id
                fence_token = cancel_request.fence_token
                requester_id = cancel_request.requester_id
                reason = cancel_request.reason
                use_ad20 = True
            except Exception:
                cancel = CancelJob.load(data)
                job_id = cancel.job_id
                fence_token = cancel.fence_token
                requester_id = f"{addr[0]}:{addr[1]}"
                reason = cancel.reason
                use_ad20 = False

            job = self._job_manager.get_job(job_id)
            if not job:
                return self._build_cancel_response(use_ad20, job_id, success=False, error="Job not found")

            if fence_token > 0 and hasattr(job, 'fence_token') and job.fence_token != fence_token:
                error_msg = f"Fence token mismatch: expected {job.fence_token}, got {fence_token}"
                return self._build_cancel_response(use_ad20, job_id, success=False, error=error_msg)

            if job.status == JobStatus.CANCELLED.value:
                return self._build_cancel_response(use_ad20, job_id, success=True, already_cancelled=True)

            if job.status == JobStatus.COMPLETED.value:
                return self._build_cancel_response(
                    use_ad20, job_id, success=False, already_completed=True, error="Job already completed"
                )

            retry_config = RetryConfig(
                max_attempts=3,
                base_delay=0.5,
                max_delay=5.0,
                jitter=JitterStrategy.FULL,
                retryable_exceptions=(ConnectionError, TimeoutError, OSError),
            )

            cancelled_workflows = 0
            errors: list[str] = []

            for dc in self._get_available_datacenters():
                managers = self._datacenter_managers.get(dc, [])
                dc_cancelled = False

                for manager_addr in managers:
                    if dc_cancelled:
                        break

                    retry_executor = RetryExecutor(retry_config)

                    async def send_cancel_to_manager(
                        use_ad20: bool = use_ad20,
                        job_id: str = job_id,
                        requester_id: str = requester_id,
                        fence_token: int = fence_token,
                        reason: str = reason,
                        manager_addr: tuple[str, int] = manager_addr,
                    ):
                        if use_ad20:
                            cancel_data = JobCancelRequest(
                                job_id=job_id,
                                requester_id=requester_id,
                                timestamp=cancel_request.timestamp if 'cancel_request' in dir() else 0,
                                fence_token=fence_token,
                                reason=reason,
                            ).dump()
                        else:
                            cancel_data = CancelJob(
                                job_id=job_id,
                                reason=reason,
                                fence_token=fence_token,
                            ).dump()

                        response, _ = await self._send_tcp(
                            manager_addr,
                            "cancel_job",
                            cancel_data,
                            timeout=5.0,
                        )
                        return response

                    try:
                        response = await retry_executor.execute(
                            send_cancel_to_manager,
                            operation_name=f"cancel_job_dc_{dc}",
                        )

                        if isinstance(response, bytes):
                            try:
                                dc_response = JobCancelResponse.load(response)
                                cancelled_workflows += dc_response.cancelled_workflow_count
                                dc_cancelled = True
                            except Exception:
                                dc_ack = CancelAck.load(response)
                                cancelled_workflows += dc_ack.workflows_cancelled
                                dc_cancelled = True
                    except Exception as error:
                        errors.append(f"DC {dc}: {str(error)}")
                        continue

            job.status = JobStatus.CANCELLED.value
            self._state.increment_state_version()

            error_str = "; ".join(errors) if errors else None
            return self._build_cancel_response(
                use_ad20, job_id, success=True, cancelled_count=cancelled_workflows, error=error_str
            )

        except Exception as error:
            await handle_exception(error, "receive_cancel_job")
            is_ad20 = self._is_ad20_cancel_request(data)
            return self._build_cancel_response(is_ad20, "unknown", success=False, error=str(error))

    async def handle_job_cancellation_complete(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle job cancellation completion push from manager (AD-20).

        Managers push this notification after all workflows in a job have
        reported cancellation completion.

        Args:
            addr: Manager address
            data: Serialized JobCancellationComplete
            handle_exception: Callback for exception handling

        Returns:
            b"OK" or b"ERROR"
        """
        try:
            completion = JobCancellationComplete.load(data)
            job_id = completion.job_id

            await self._logger.log(
                ServerInfo(
                    message=f"Received job cancellation complete for {job_id[:8]}... "
                            f"(success={completion.success}, errors={len(completion.errors)})",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

            if completion.errors:
                self._state._cancellation_errors[job_id].extend(completion.errors)

            event = self._state._cancellation_completion_events.get(job_id)
            if event:
                event.set()

            callback = self._job_manager.get_callback(job_id)
            if callback:
                self._task_runner.run(
                    self._push_cancellation_complete_to_client,
                    job_id,
                    completion,
                    callback,
                )

            return b"OK"

        except Exception as error:
            await handle_exception(error, "receive_job_cancellation_complete")
            return b"ERROR"

    async def _push_cancellation_complete_to_client(
        self,
        job_id: str,
        completion: JobCancellationComplete,
        callback: tuple[str, int],
    ) -> None:
        """Push job cancellation completion to client callback."""
        try:
            await self._send_tcp(
                callback,
                "receive_job_cancellation_complete",
                completion.dump(),
                timeout=2.0,
            )
        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Failed to push cancellation complete to client {callback}: {error}",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

        self._state._cancellation_completion_events.pop(job_id, None)
        self._state._cancellation_errors.pop(job_id, None)

    async def handle_cancel_single_workflow(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle single workflow cancellation request from client (Section 6).

        Gates forward workflow cancellation requests to all datacenters
        that have the job, then aggregate responses.

        Args:
            addr: Client address
            data: Serialized SingleWorkflowCancelRequest
            handle_exception: Callback for exception handling

        Returns:
            Serialized SingleWorkflowCancelResponse
        """
        try:
            request = SingleWorkflowCancelRequest.load(data)

            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit(client_id, "cancel_workflow")
            if not allowed:
                return RateLimitResponse(
                    operation="cancel_workflow",
                    retry_after_seconds=retry_after,
                ).dump()

            await self._logger.log(
                ServerInfo(
                    message=f"Received workflow cancellation request for {request.workflow_id[:8]}... "
                            f"(job {request.job_id[:8]}...)",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

            job_info = self._job_manager.get_job(request.job_id)
            if not job_info:
                return SingleWorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    request_id=request.request_id,
                    status=WorkflowCancellationStatus.NOT_FOUND.value,
                    errors=["Job not found"],
                ).dump()

            target_dcs: list[tuple[str, tuple[str, int]]] = []
            for dc_name, dc_managers in self._datacenter_managers.items():
                if dc_managers:
                    target_dcs.append((dc_name, dc_managers[0]))

            if not target_dcs:
                return SingleWorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    request_id=request.request_id,
                    status=WorkflowCancellationStatus.NOT_FOUND.value,
                    errors=["No datacenters available"],
                ).dump()

            aggregated_dependents: list[str] = []
            aggregated_errors: list[str] = []
            final_status = WorkflowCancellationStatus.NOT_FOUND.value

            for dc_name, dc_addr in target_dcs:
                try:
                    response_data, _ = await self._send_tcp(
                        dc_addr,
                        "receive_cancel_single_workflow",
                        request.dump(),
                        timeout=5.0,
                    )

                    if response_data:
                        response = SingleWorkflowCancelResponse.load(response_data)

                        aggregated_dependents.extend(response.cancelled_dependents)
                        aggregated_errors.extend(response.errors)

                        if response.status == WorkflowCancellationStatus.CANCELLED.value:
                            final_status = WorkflowCancellationStatus.CANCELLED.value
                        elif response.status == WorkflowCancellationStatus.PENDING_CANCELLED.value:
                            if final_status == WorkflowCancellationStatus.NOT_FOUND.value:
                                final_status = WorkflowCancellationStatus.PENDING_CANCELLED.value
                        elif response.status == WorkflowCancellationStatus.ALREADY_CANCELLED.value:
                            if final_status == WorkflowCancellationStatus.NOT_FOUND.value:
                                final_status = WorkflowCancellationStatus.ALREADY_CANCELLED.value

                except Exception as error:
                    aggregated_errors.append(f"DC {dc_name}: {error}")

            return SingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=final_status,
                cancelled_dependents=list(set(aggregated_dependents)),
                errors=aggregated_errors,
            ).dump()

        except Exception as error:
            await handle_exception(error, "receive_cancel_single_workflow")
            return SingleWorkflowCancelResponse(
                job_id="unknown",
                workflow_id="unknown",
                request_id="unknown",
                status=WorkflowCancellationStatus.NOT_FOUND.value,
                errors=[str(error)],
            ).dump()
