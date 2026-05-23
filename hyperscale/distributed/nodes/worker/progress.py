"""
Worker progress reporting module.

Handles sending workflow progress updates and final results to managers.
Implements job leader routing and backpressure-aware delivery.
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    WorkflowFinalResult,
    WorkflowFinalResultAck,
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


_TRANSIENT_SEND_ERRORS: tuple[type[BaseException], ...] = (
    asyncio.TimeoutError,
    ConnectionError,
    OSError,
    TimeoutError,
)
"""Errors expected during normal operation (network blips, manager restarts).
Recorded against the circuit breaker so it can trip on a struggling peer."""

_LOCAL_BUG_ERRORS: tuple[type[BaseException], ...] = (
    TypeError,
    AttributeError,
    KeyError,
    IndexError,
)
"""Errors that almost always indicate a bug in our own code, not a peer
problem. Logged at ERROR level but NOT recorded against the circuit breaker —
tripping the circuit on a local bug just hides the bug behind retries."""


def _classify_send_error(error: BaseException) -> tuple[bool, str]:
    """Classify a peer-send exception for circuit-breaker accounting.

    Returns:
        (record_against_circuit, category_label) where category_label is one
        of "transient", "local_bug", or "unknown". Unknown is treated as
        transient for circuit purposes but logged distinctly.
    """
    if isinstance(error, _TRANSIENT_SEND_ERRORS):
        return (True, "transient")
    if isinstance(error, _LOCAL_BUG_ERRORS):
        return (False, "local_bug")
    return (True, "unknown")

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
        task_runner_run: callable | None = None,
    ) -> None:
        self._registry: "WorkerRegistry" = registry
        self._state: "WorkerState" = state
        self._logger: "Logger | None" = logger
        self._task_runner_run: callable | None = task_runner_run
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
            record_circuit, category = _classify_send_error(send_error)
            if record_circuit:
                circuit.record_error()
            if self._logger:
                log_model = ServerError if category == "local_bug" else ServerWarning
                await self._logger.log(
                    log_model(
                        message=(
                            f"Failed to send progress update [{category}]: "
                            f"{type(send_error).__name__}: {send_error}"
                        ),
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

        except Exception as error:
            record_circuit, category = _classify_send_error(error)
            if record_circuit:
                circuit.record_error()
            if self._logger:
                log_model = ServerError if category == "local_bug" else ServerDebug
                await self._logger.log(
                    log_model(
                        message=(
                            f"Progress send to {manager_addr} failed [{category}]: "
                            f"{type(error).__name__}: {error}"
                        ),
                        node_host="worker",
                        node_port=0,
                        node_id="worker",
                    )
                )
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

                except Exception as error:
                    record_circuit, category = _classify_send_error(error)
                    if record_circuit:
                        circuit.record_error()
                    if self._logger:
                        log_model = (
                            ServerError if category == "local_bug" else ServerDebug
                        )
                        await self._logger.log(
                            log_model(
                                message=(
                                    f"Broadcast progress to manager failed [{category}]: "
                                    f"{type(error).__name__}: {error}"
                                ),
                                node_host="worker",
                                node_port=0,
                                node_id="worker",
                            )
                        )

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
        target_addrs: list[tuple[str | None, tuple[str, int]]] = []
        seen_addrs: set[tuple[str, int]] = set()

        def add_target(
            manager_id: str | None,
            manager_addr: tuple[str, int] | None,
        ) -> None:
            if manager_addr is None or manager_addr in seen_addrs:
                return
            target_addrs.append((manager_id, manager_addr))
            seen_addrs.add(manager_addr)

        job_leader_addr = (
            self._state.get_workflow_job_leader(final_result.workflow_id)
            or final_result.job_leader_addr
        )
        if isinstance(job_leader_addr, list):
            job_leader_addr = tuple(job_leader_addr)
        leader_manager = (
            self._registry.get_manager_by_addr(job_leader_addr)
            if job_leader_addr
            else None
        )
        add_target(
            leader_manager.node_id if leader_manager else None,
            job_leader_addr,
        )

        if primary_id := self._registry._primary_manager_id:
            if manager := self._registry.get_manager(primary_id):
                add_target(primary_id, (manager.tcp_host, manager.tcp_port))

        for manager_id in sorted(self._registry._healthy_manager_ids):
            if manager := self._registry.get_manager(manager_id):
                add_target(manager_id, (manager.tcp_host, manager.tcp_port))

        if not target_addrs:
            if self._logger:
                task_runner_run(
                    self._logger.log,
                    ServerWarning(
                        message=(
                            f"Cannot send final result for {final_result.workflow_id}: "
                            "no healthy managers"
                        ),
                        node_host=node_host,
                        node_port=node_port,
                        node_id=node_id_short,
                    ),
                )
            return

        async def send_once(
            manager_addr: tuple[str, int],
        ) -> tuple[bool, tuple[str, int] | None, str | None]:
            response, _ = await send_tcp(
                manager_addr,
                "workflow_final_result",
                final_result.dump(),
                timeout=5.0,
            )
            if isinstance(response, Exception):
                raise response
            if not response or not isinstance(response, bytes):
                raise ConnectionError("Invalid empty response")
            if response == b"ok":
                return True, None, None
            if response == b"error":
                return False, None, "error response"

            ack = WorkflowFinalResultAck.load(response)
            leader_addr = ack.leader_addr
            if isinstance(leader_addr, list):
                leader_addr = tuple(leader_addr)
            if leader_addr:
                self._state.set_workflow_job_leader(
                    final_result.workflow_id,
                    leader_addr,
                )
            if ack.is_leader and ack.manager_id:
                self._registry.set_primary_manager(ack.manager_id)
            if ack.accepted or ack.forwarded or ack.duplicate or ack.stale:
                return True, leader_addr, None
            return False, leader_addr, ack.error or ack.reason or "not accepted"

        target_index = 0
        while target_index < len(target_addrs):
            manager_id, manager_addr = target_addrs[target_index]
            target_index += 1

            if manager_id and self._registry.is_circuit_open(manager_id):
                continue

            circuit = (
                self._registry.get_or_create_circuit(manager_id)
                if manager_id
                else self._registry.get_or_create_circuit_by_addr(manager_addr)
            )

            for attempt in range(max_retries + 1):
                try:
                    accepted, redirect_addr, error_message = await send_once(
                        manager_addr
                    )
                    if accepted:
                        circuit.record_success()

                        if self._logger:
                            task_runner_run(
                                self._logger.log,
                                ServerDebug(
                                    message=(
                                        f"Sent final result for {final_result.workflow_id} "
                                        f"status={final_result.status}"
                                    ),
                                    node_host=node_host,
                                    node_port=node_port,
                                    node_id=node_id_short,
                                ),
                            )
                        return

                    if redirect_addr and redirect_addr not in seen_addrs:
                        redirect_manager = self._registry.get_manager_by_addr(
                            redirect_addr
                        )
                        target_addrs.insert(
                            target_index,
                            (
                                redirect_manager.node_id
                                if redirect_manager
                                else None,
                                redirect_addr,
                            ),
                        )
                        seen_addrs.add(redirect_addr)
                    if self._logger:
                        await self._logger.log(
                            ServerDebug(
                                message=(
                                    f"Final result rejected by {manager_addr}: "
                                    f"{error_message or 'not accepted'}"
                                ),
                                node_host=node_host,
                                node_port=node_port,
                                node_id=node_id_short,
                            )
                        )
                    break

                except Exception as err:
                    record_circuit, category = _classify_send_error(err)
                    if record_circuit:
                        circuit.record_error()
                    if attempt < max_retries:
                        delay = min(
                            base_delay * (2**attempt),
                            base_delay * (2**max_retries),
                        )
                        await asyncio.sleep(
                            delay
                        )
                        continue
                    if self._logger:
                        await self._logger.log(
                            ServerError(
                                message=(
                                    "Failed to send final result for "
                                    f"{final_result.workflow_id} to {manager_addr} "
                                    f"[{category}]: {type(err).__name__}: {err}"
                                ),
                                node_host=node_host,
                                node_port=node_port,
                                node_id=node_id_short,
                            )
                        )
                    break

        self._enqueue_pending_result(final_result)
        if self._logger:
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Queued final result for {final_result.workflow_id} "
                        "for background retry "
                        f"({len(self._pending_results)} pending)"
                    ),
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

            job_leader_addr = ack.job_leader_addr
            if isinstance(job_leader_addr, list):
                job_leader_addr = tuple(job_leader_addr)

            # Update job leader routing
            if workflow_id and job_leader_addr:
                current_leader = self._state.get_workflow_job_leader(workflow_id)
                if current_leader != job_leader_addr:
                    self._state.set_workflow_job_leader(workflow_id, job_leader_addr)

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

        except Exception as error:
            if data != b"ok" and self._logger and self._task_runner_run:
                self._task_runner_run(
                    self._logger.log,
                    ServerDebug(
                        message=f"ACK parse failed (non-legacy payload): {error}",
                        node_host="worker",
                        node_port=0,
                        node_id="worker",
                    ),
                )

    def _enqueue_pending_result(self, final_result: WorkflowFinalResult) -> None:
        now = time.monotonic()
        pending = PendingResult(
            final_result=final_result,
            enqueued_at=now,
            retry_count=0,
            next_retry_at=now + self.RESULT_RETRY_BASE_DELAY,
        )
        self._pending_results.append(pending)

    async def retry_pending_results(
        self,
        send_tcp: callable,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: callable,
    ) -> int:
        """
        Retry sending pending results. Returns number of results removed (sent or expired).

        Should be called periodically from a background loop.
        """
        now = time.monotonic()
        sent_count = 0
        expired_count = 0
        still_pending: list[PendingResult] = []

        while self._pending_results:
            pending = self._pending_results.popleft()

            age = now - pending.enqueued_at
            if age > self.RESULT_TTL_SECONDS:
                expired_count += 1
                if self._logger:
                    task_runner_run(
                        self._logger.log,
                        ServerError(
                            message=f"Dropped expired result for {pending.final_result.workflow_id} after {age:.1f}s",
                            node_host=node_host,
                            node_port=node_port,
                            node_id=node_id_short,
                        ),
                    )
                continue

            if pending.retry_count >= self.MAX_RESULT_RETRIES:
                expired_count += 1
                if self._logger:
                    task_runner_run(
                        self._logger.log,
                        ServerError(
                            message=f"Dropped result for {pending.final_result.workflow_id} after {pending.retry_count} retries",
                            node_host=node_host,
                            node_port=node_port,
                            node_id=node_id_short,
                        ),
                    )
                continue

            if now < pending.next_retry_at:
                still_pending.append(pending)
                continue

            sent = await self._try_send_pending_result(
                pending.final_result,
                send_tcp,
                node_host,
                node_port,
                node_id_short,
            )

            if sent:
                sent_count += 1
            else:
                pending.retry_count += 1
                backoff = self.RESULT_RETRY_BASE_DELAY * (2**pending.retry_count)
                pending.next_retry_at = now + min(backoff, 60.0)
                still_pending.append(pending)

        for item in still_pending:
            self._pending_results.append(item)

        return sent_count + expired_count

    async def _try_send_pending_result(
        self,
        final_result: WorkflowFinalResult,
        send_tcp: callable,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> bool:
        target_addrs: list[tuple[str | None, tuple[str, int]]] = []
        seen_addrs: set[tuple[str, int]] = set()

        def add_target(
            manager_id: str | None,
            manager_addr: tuple[str, int] | None,
        ) -> None:
            if manager_addr is None or manager_addr in seen_addrs:
                return
            target_addrs.append((manager_id, manager_addr))
            seen_addrs.add(manager_addr)

        leader_addr = (
            self._state.get_workflow_job_leader(final_result.workflow_id)
            or final_result.job_leader_addr
        )
        if isinstance(leader_addr, list):
            leader_addr = tuple(leader_addr)
        leader_manager = (
            self._registry.get_manager_by_addr(leader_addr)
            if leader_addr
            else None
        )
        add_target(leader_manager.node_id if leader_manager else None, leader_addr)

        if primary_id := self._registry._primary_manager_id:
            if manager := self._registry.get_manager(primary_id):
                add_target(primary_id, (manager.tcp_host, manager.tcp_port))

        for manager_id in sorted(self._registry._healthy_manager_ids):
            if manager := self._registry.get_manager(manager_id):
                add_target(manager_id, (manager.tcp_host, manager.tcp_port))

        target_index = 0
        while target_index < len(target_addrs):
            manager_id, manager_addr = target_addrs[target_index]
            target_index += 1
            if manager_id and self._registry.is_circuit_open(manager_id):
                continue
            try:
                response, _ = await send_tcp(
                    manager_addr,
                    "workflow_final_result",
                    final_result.dump(),
                    timeout=5.0,
                )
                if isinstance(response, Exception):
                    raise response
                if not response or not isinstance(response, bytes):
                    continue
                if response == b"ok":
                    self._registry.get_or_create_circuit_by_addr(
                        manager_addr
                    ).record_success()
                    return True
                if response == b"error":
                    continue

                ack = WorkflowFinalResultAck.load(response)
                leader_addr = ack.leader_addr
                if isinstance(leader_addr, list):
                    leader_addr = tuple(leader_addr)
                if leader_addr:
                    self._state.set_workflow_job_leader(
                        final_result.workflow_id,
                        leader_addr,
                    )
                    if leader_addr not in seen_addrs:
                        redirect_manager = self._registry.get_manager_by_addr(
                            leader_addr
                        )
                        target_addrs.insert(
                            target_index,
                            (
                                redirect_manager.node_id
                                if redirect_manager
                                else None,
                                leader_addr,
                            ),
                        )
                        seen_addrs.add(leader_addr)
                if ack.is_leader and ack.manager_id:
                    self._registry.set_primary_manager(ack.manager_id)
                if ack.accepted or ack.forwarded or ack.duplicate or ack.stale:
                    self._registry.get_or_create_circuit_by_addr(
                        manager_addr
                    ).record_success()
                    return True
            except Exception as error:
                record_circuit, category = _classify_send_error(error)
                if record_circuit:
                    self._registry.get_or_create_circuit_by_addr(
                        manager_addr
                    ).record_error()
                if self._logger:
                    log_model = ServerError if category == "local_bug" else ServerDebug
                    await self._logger.log(
                        log_model(
                            message=(
                                f"Final result send to {manager_addr} failed [{category}]: "
                                f"{type(error).__name__}: {error}"
                            ),
                            node_host="worker",
                            node_port=0,
                            node_id="worker",
                        )
                    )
                continue

        return False

    def get_pending_result_count(self) -> int:
        return len(self._pending_results)
