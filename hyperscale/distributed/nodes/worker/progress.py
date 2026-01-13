"""
Worker progress reporting module.

Handles sending workflow progress updates and final results to managers.
Implements job leader routing and backpressure-aware delivery.
"""

import time
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    WorkflowFinalResult,
    WorkflowProgress,
    WorkflowProgressAck,
    WorkflowCancellationComplete,
)
from hyperscale.distributed.reliability import (
    BackpressureLevel,
    BackpressureSignal,
    RetryConfig,
    RetryExecutor,
    JitterStrategy,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerError,
    ServerInfo,
    ServerWarning,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from .registry import WorkerRegistry
    from .state import WorkerState


@dataclass
class PendingResult:
    final_result: WorkflowFinalResult
    enqueued_at: float
    retry_count: int = 0
    next_retry_at: float = 0.0


class WorkerProgressReporter:
    """
    Handles progress reporting to managers.

    Routes progress updates to job leaders, handles failover,
    and processes acknowledgments. Respects AD-23 backpressure signals.
    """

    MAX_PENDING_RESULTS = 1000
    RESULT_TTL_SECONDS = 300.0
    MAX_RESULT_RETRIES = 10
    RESULT_RETRY_BASE_DELAY = 5.0

    def __init__(
        self,
        registry: "WorkerRegistry",
        state: "WorkerState",
        logger: "Logger | None" = None,
    ) -> None:
        self._registry = registry
        self._state = state
        self._logger = logger
        self._pending_results: deque[PendingResult] = deque(
            maxlen=self.MAX_PENDING_RESULTS
        )

    async def send_progress_direct(
        self,
        progress: WorkflowProgress,
        send_tcp: callable,
        node_host: str,
        node_port: int,
        node_id_short: str,
        max_retries: int = 2,
        base_delay: float = 0.2,
    ) -> None:
        """
        Send progress update directly to primary manager.

        Used for lifecycle events that need immediate delivery.

        Args:
            progress: Workflow progress to send
            send_tcp: Function to send TCP data
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            max_retries: Maximum retry attempts
            base_delay: Base delay for backoff
        """
        manager_addr = self._registry.get_primary_manager_tcp_addr()
        if not manager_addr:
            return

        primary_id = self._registry._primary_manager_id
        if primary_id and self._registry.is_circuit_open(primary_id):
            return

        circuit = (
            self._registry.get_or_create_circuit(primary_id)
            if primary_id
            else self._registry.get_or_create_circuit_by_addr(manager_addr)
        )

        retry_config = RetryConfig(
            max_attempts=max_retries + 1,
            base_delay=base_delay,
            max_delay=base_delay * (2**max_retries),
            jitter=JitterStrategy.FULL,
        )
        executor = RetryExecutor(retry_config)

        async def attempt_send() -> None:
            response, _ = await send_tcp(
                manager_addr,
                "workflow_progress",
                progress.dump(),
                timeout=1.0,
            )
            if response and isinstance(response, bytes) and response != b"error":
                self._process_ack(response, progress.workflow_id)
            else:
                raise ConnectionError("Invalid or error response from manager")

        try:
            await executor.execute(attempt_send, "progress_update")
            circuit.record_success()
        except Exception as send_error:
            circuit.record_error()
            if self._logger:
                await self._logger.log(
                    ServerWarning(
                        message=f"Failed to send progress update: {send_error}",
                        node_host=node_host,
                        node_port=node_port,
                        node_id=node_id_short,
                    )
                )

    async def send_progress_to_job_leader(
        self,
        progress: WorkflowProgress,
        send_tcp: callable,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> bool:
        """
        Send progress update to job leader.

        Routes to the manager that dispatched the workflow. Falls back
        to other healthy managers if job leader is unavailable.

        Args:
            progress: Workflow progress to send
            send_tcp: Function to send TCP data
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID

        Returns:
            True if sent successfully
        """
        workflow_id = progress.workflow_id
        job_leader_addr = self._state.get_workflow_job_leader(workflow_id)

        # Try job leader first
        if job_leader_addr:
            success = await self._try_send_to_addr(
                progress, job_leader_addr, send_tcp, workflow_id
            )
            if success:
                return True

            if self._logger:
                await self._logger.log(
                    ServerWarning(
                        message=f"Job leader {job_leader_addr} failed for workflow {workflow_id[:16]}..., discovering new leader",
                        node_host=node_host,
                        node_port=node_port,
                        node_id=node_id_short,
                    )
                )

        # Try other healthy managers
        for manager_id in list(self._registry._healthy_manager_ids):
            if manager := self._registry.get_manager(manager_id):
                manager_addr = (manager.tcp_host, manager.tcp_port)

                if manager_addr == job_leader_addr:
                    continue

                if self._registry.is_circuit_open(manager_id):
                    continue

                success = await self._try_send_to_addr(
                    progress, manager_addr, send_tcp, workflow_id
                )
                if success:
                    return True

        return False

    async def _try_send_to_addr(
        self,
        progress: WorkflowProgress,
        manager_addr: tuple[str, int],
        send_tcp: callable,
        workflow_id: str,
    ) -> bool:
        """
        Attempt to send progress to a specific manager.

        Args:
            progress: Progress to send
            manager_addr: Manager address
            send_tcp: TCP send function
            workflow_id: Workflow identifier

        Returns:
            True if send succeeded
        """
        circuit = self._registry.get_or_create_circuit_by_addr(manager_addr)

        try:
            response, _ = await send_tcp(
                manager_addr,
                "workflow_progress",
                progress.dump(),
                timeout=1.0,
            )

            if response and isinstance(response, bytes) and response != b"error":
                self._process_ack(response, workflow_id)
                circuit.record_success()
                return True

            circuit.record_error()
            return False

        except Exception:
            circuit.record_error()
            return False

    async def send_progress_to_all_managers(
        self,
        progress: WorkflowProgress,
        send_tcp: callable,
    ) -> None:
        """
        Send progress to all healthy managers.

        Used for broadcasting important state changes.

        Args:
            progress: Progress to send
            send_tcp: TCP send function
        """
        for manager_id in list(self._registry._healthy_manager_ids):
            if manager := self._registry.get_manager(manager_id):
                if self._registry.is_circuit_open(manager_id):
                    continue

                manager_addr = (manager.tcp_host, manager.tcp_port)
                circuit = self._registry.get_or_create_circuit(manager_id)

                try:
                    response, _ = await send_tcp(
                        manager_addr,
                        "workflow_progress",
                        progress.dump(),
                        timeout=1.0,
                    )

                    if (
                        response
                        and isinstance(response, bytes)
                        and response != b"error"
                    ):
                        self._process_ack(response, progress.workflow_id)
                        circuit.record_success()
                    else:
                        circuit.record_error()

                except Exception:
                    circuit.record_error()

    async def send_final_result(
        self,
        final_result: WorkflowFinalResult,
        send_tcp: callable,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: callable,
        max_retries: int = 3,
        base_delay: float = 0.5,
    ) -> None:
        """
        Send workflow final result to manager.

        Final results are critical and require higher retry count.
        Tries primary manager first, then falls back to others.

        Args:
            final_result: Final result to send
            send_tcp: TCP send function
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            task_runner_run: Function to run async tasks
            max_retries: Maximum retries per manager
            base_delay: Base delay for backoff
        """
        target_managers = []

        if primary_id := self._registry._primary_manager_id:
            target_managers.append(primary_id)

        for manager_id in self._registry._healthy_manager_ids:
            if manager_id not in target_managers:
                target_managers.append(manager_id)

        if not target_managers:
            if self._logger:
                task_runner_run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Cannot send final result for {final_result.workflow_id}: no healthy managers",
                        node_host=node_host,
                        node_port=node_port,
                        node_id=node_id_short,
                    ),
                )
            return

        for manager_id in target_managers:
            if self._registry.is_circuit_open(manager_id):
                continue

            if not (manager := self._registry.get_manager(manager_id)):
                continue

            manager_addr = (manager.tcp_host, manager.tcp_port)
            circuit = self._registry.get_or_create_circuit(manager_id)

            retry_config = RetryConfig(
                max_attempts=max_retries + 1,
                base_delay=base_delay,
                max_delay=base_delay * (2**max_retries),
                jitter=JitterStrategy.FULL,
            )
            executor = RetryExecutor(retry_config)

            async def attempt_send() -> bytes:
                response, _ = await send_tcp(
                    manager_addr,
                    "workflow_final_result",
                    final_result.dump(),
                    timeout=5.0,
                )
                if response and isinstance(response, bytes) and response != b"error":
                    return response
                raise ConnectionError("Invalid or error response")

            try:
                await executor.execute(attempt_send, "final_result")
                circuit.record_success()

                if self._logger:
                    task_runner_run(
                        self._logger.log,
                        ServerDebug(
                            message=f"Sent final result for {final_result.workflow_id} status={final_result.status}",
                            node_host=node_host,
                            node_port=node_port,
                            node_id=node_id_short,
                        ),
                    )
                return

            except Exception as err:
                circuit.record_error()
                if self._logger:
                    await self._logger.log(
                        ServerError(
                            message=f"Failed to send final result for {final_result.workflow_id} to {manager_id}: {err}",
                            node_host=node_host,
                            node_port=node_port,
                            node_id=node_id_short,
                        )
                    )

        self._enqueue_pending_result(final_result)
        if self._logger:
            await self._logger.log(
                ServerWarning(
                    message=f"Queued final result for {final_result.workflow_id} for background retry ({len(self._pending_results)} pending)",
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                )
            )

    async def send_cancellation_complete(
        self,
        job_id: str,
        workflow_id: str,
        success: bool,
        errors: list[str],
        cancelled_at: float,
        node_id: str,
        send_tcp: callable,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """
        Push workflow cancellation completion to manager.

        Fire-and-forget - does not block the cancellation flow.

        Args:
            job_id: Job identifier
            workflow_id: Workflow identifier
            success: Whether cancellation succeeded
            errors: Any errors encountered
            cancelled_at: Timestamp of cancellation
            node_id: Full node ID
            send_tcp: TCP send function
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
        """
        completion = WorkflowCancellationComplete(
            job_id=job_id,
            workflow_id=workflow_id,
            success=success,
            errors=errors,
            cancelled_at=cancelled_at,
            node_id=node_id,
        )

        job_leader_addr = self._state.get_workflow_job_leader(workflow_id)

        if job_leader_addr:
            try:
                await send_tcp(
                    job_leader_addr,
                    "workflow_cancellation_complete",
                    completion.dump(),
                    timeout=5.0,
                )
                return
            except Exception as cancel_error:
                if self._logger:
                    await self._logger.log(
                        ServerDebug(
                            message=f"Failed to send cancellation to job leader: {cancel_error}",
                            node_host=node_host,
                            node_port=node_port,
                            node_id=node_id_short,
                        )
                    )

        for manager_id in list(self._registry._healthy_manager_ids):
            if manager := self._registry.get_manager(manager_id):
                manager_addr = (manager.tcp_host, manager.tcp_port)
                if manager_addr == job_leader_addr:
                    continue

                try:
                    await send_tcp(
                        manager_addr,
                        "workflow_cancellation_complete",
                        completion.dump(),
                        timeout=5.0,
                    )
                    return
                except Exception as fallback_error:
                    if self._logger:
                        await self._logger.log(
                            ServerDebug(
                                message=f"Failed to send cancellation to fallback manager: {fallback_error}",
                                node_host=node_host,
                                node_port=node_port,
                                node_id=node_id_short,
                            )
                        )
                    continue

        if self._logger:
            await self._logger.log(
                ServerWarning(
                    message=f"Failed to push cancellation complete for workflow {workflow_id[:16]}... - no reachable managers",
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                )
            )

    def _process_ack(
        self,
        data: bytes,
        workflow_id: str | None = None,
    ) -> None:
        """
        Process WorkflowProgressAck to update state.

        Updates manager topology, job leader routing, and backpressure.

        Args:
            data: Serialized WorkflowProgressAck
            workflow_id: Workflow ID for job leader update
        """
        try:
            ack = WorkflowProgressAck.load(data)

            # Update primary manager if leadership changed
            if ack.is_leader and self._registry._primary_manager_id != ack.manager_id:
                self._registry.set_primary_manager(ack.manager_id)

            # Update job leader routing
            if workflow_id and ack.job_leader_addr:
                current_leader = self._state.get_workflow_job_leader(workflow_id)
                if current_leader != ack.job_leader_addr:
                    self._state.set_workflow_job_leader(
                        workflow_id, ack.job_leader_addr
                    )

            # Handle backpressure signal (AD-23)
            if ack.backpressure_level > 0:
                signal = BackpressureSignal(
                    level=BackpressureLevel(ack.backpressure_level),
                    suggested_delay_ms=ack.backpressure_delay_ms,
                    batch_only=ack.backpressure_batch_only,
                )
                self._state.set_manager_backpressure(ack.manager_id, signal.level)
                self._state.set_backpressure_delay_ms(
                    max(
                        self._state.get_backpressure_delay_ms(),
                        signal.suggested_delay_ms,
                    )
                )

        except Exception:
            pass
