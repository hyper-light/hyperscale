"""``WorkerProgressReporter`` -- pickled under the namespace
``hyperscale.distributed.nodes.worker.progress`` (see that module)."""

from typing import TYPE_CHECKING
import asyncio
from collections import deque
from hyperscale.distributed.models import (
    RateLimitResponse,
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
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerError, ServerWarning
from hyperscale.distributed.runtime import Clock, RealClock, SendTcp, RunTask

from .pending_result import PendingResult

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from .config import WorkerConfig
    from .registry import WorkerRegistry
    from .state import WorkerState

_DEFAULT_CLOCK: Clock = RealClock()

_TRANSIENT_SEND_ERRORS: tuple[type[BaseException], ...] = (
    asyncio.TimeoutError,
    ConnectionError,
    OSError,
    TimeoutError,
)

_LOCAL_BUG_ERRORS: tuple[type[BaseException], ...] = (
    TypeError,
    AttributeError,
    KeyError,
    IndexError,
)


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



# How a failed send is logged by its category: a fault of this worker's own
# is an error; any other (the peer, the network) is routine.
_SEND_FAILURE_LOG_MODELS = {"local_bug": ServerError}

class WorkerProgressReporter:
    """
    Handles progress reporting to managers.

    Routes progress updates to job leaders, handles failover,
    and processes acknowledgments. Respects AD-23 backpressure signals.
    """

    def __init__(
        self,
        registry: "WorkerRegistry",
        state: "WorkerState",
        config: "WorkerConfig",
        logger: "Logger | None" = None,
        task_runner_run: RunTask | None = None,
    ) -> None:
        self._registry: "WorkerRegistry" = registry
        self._state: "WorkerState" = state
        self._config: "WorkerConfig" = config
        self._logger: "Logger | None" = logger
        self._task_runner_run: RunTask | None = task_runner_run
        self._pending_results: deque[PendingResult] = deque(
            maxlen=config.pending_result_limit
        )
        # AD-24: manager address -> monotonic instant its rate-limit
        # refusal expires. Bounded by the managers that refused; an entry
        # is dropped the first time it is consulted after expiring.
        self._refused_until: dict[tuple[str, int], float] = {}

    async def send_progress_direct(
        self,
        progress: WorkflowProgress,
        send_tcp: SendTcp,
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
            if self._is_refusing(manager_addr):
                return
            response, _ = await send_tcp(
                manager_addr,
                "workflow_progress",
                progress.dump(),
                timeout=self._config.progress_send_timeout_seconds,
            )
            if not self._accept_response(manager_addr, response, progress.workflow_id):
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
        send_tcp: SendTcp,
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
        send_tcp: SendTcp,
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
        if self._is_refusing(manager_addr):
            return True

        try:
            response, _ = await send_tcp(
                manager_addr,
                "workflow_progress",
                progress.dump(),
                timeout=self._config.progress_send_timeout_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response, Exception):
                raise response

            if self._accept_response(manager_addr, response, workflow_id):
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

    async def send_final_result(
        self,
        final_result: WorkflowFinalResult,
        send_tcp: SendTcp,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: RunTask,
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
        target_addrs, seen_addrs = self._final_result_targets(final_result)

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
                timeout=self._config.tcp_timeout_standard_seconds,
            )
            if isinstance(response, Exception):
                raise response
            if not response or not isinstance(response, bytes):
                raise ConnectionError("Invalid empty response")
            if response == b"ok":
                return True, None, None
            if response == b"error":
                return False, None, "error response"

            # An AD-24 refusal: the manager asks for the result again after
            # its retry-after -- backpressure from a live manager, not an
            # answer about the result (and not an ack to read).
            if (refused_until := self._note_refusal(manager_addr, response)) is not None:
                return False, None, f"rate limited until {refused_until:.3f}"

            ack = WorkflowFinalResultAck.load(response)
            settled, leader_addr = self._take_final_result_ack(final_result.workflow_id, ack)
            if settled:
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

                    self._insert_redirect(target_addrs, seen_addrs, target_index, redirect_addr)
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
                        await _DEFAULT_CLOCK.sleep(
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
        send_tcp: SendTcp,
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
                response, _ = await send_tcp(
                    job_leader_addr,
                    "workflow_cancellation_complete",
                    completion.dump(),
                    timeout=self._config.tcp_timeout_standard_seconds,
                )
                # send_tcp returns transport errors rather than raising.
                if isinstance(response, Exception):
                    raise response
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
                    response, _ = await send_tcp(
                        manager_addr,
                        "workflow_cancellation_complete",
                        completion.dump(),
                        timeout=self._config.tcp_timeout_standard_seconds,
                    )
                    # send_tcp returns transport errors rather than raising.
                    if isinstance(response, Exception):
                        raise response
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

    def _is_refusing(self, manager_addr: tuple[str, int]) -> bool:
        """Whether ``manager_addr`` asked (AD-24) not to be sent progress yet.

        Skipping a send in that window loses nothing: progress snapshots
        are cumulative, so the next one sent supersedes the skipped one.
        """
        if (refused_until := self._refused_until.get(manager_addr)) is None:
            return False
        if _DEFAULT_CLOCK.monotonic() < refused_until:
            return True
        del self._refused_until[manager_addr]
        return False

    def _accept_response(
        self,
        manager_addr: tuple[str, int],
        response: bytes | Exception | None,
        workflow_id: str,
    ) -> bool:
        """Apply a manager's answer to a progress update.

        True for an answer from a live manager: an ack (processed) or an
        AD-24 rate-limit refusal, which is backpressure -- honored by not
        sending to that manager until its retry-after passes -- not a
        failure to fail over from. False for no answer or an error.
        """
        if not response or not isinstance(response, bytes) or response == b"error":
            return False

        if self._note_refusal(manager_addr, response) is not None:
            return True

        self._process_ack(response, workflow_id)
        return True

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
        now = _DEFAULT_CLOCK.monotonic()
        pending = PendingResult(
            final_result=final_result,
            enqueued_at=now,
            retry_count=0,
            next_retry_at=now + self._config.result_retry_base_delay_seconds,
        )
        self._pending_results.append(pending)

    async def retry_pending_results(
        self,
        send_tcp: SendTcp,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: RunTask,
    ) -> int:
        """
        Retry sending pending results. Returns number of results removed (sent or expired).

        Should be called periodically from a background loop.
        """
        now = _DEFAULT_CLOCK.monotonic()
        sent_count = 0
        expired_count = 0
        still_pending: list[PendingResult] = []

        while self._pending_results:
            pending = self._pending_results.popleft()

            # A result is kept until delivered, not dropped by age: a worker
            # isolated for longer than any fixed age would lose the job's
            # output. Attempts happen only while a manager is reachable (the
            # retry loop runs only then), so the attempt budget and the
            # pending-result cap bound it; a manager answers a result for a
            # job that ended stale, which settles it.
            if pending.retry_count >= self._config.result_max_retries:
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

            sent, refused_until = await self._try_send_pending_result(
                pending.final_result,
                send_tcp,
                node_host,
                node_port,
                node_id_short,
            )

            if sent:
                sent_count += 1
                continue
            self._reschedule_pending_result(pending, refused_until, now)
            still_pending.append(pending)

        for item in still_pending:
            self._pending_results.append(item)

        return sent_count + expired_count

    def _reschedule_pending_result(self, pending: PendingResult, refused_until: float | None, now: float) -> None:
        """When an undelivered result goes again. Refused (AD-24) by a live
        manager is backpressure, not a failed delivery: it spends none of
        the result's attempts -- spent, an overload outlasting them dropped
        the result of a job that had run to its end -- and goes again once
        the refusal's retry-after passes. A failed delivery backs off
        exponentially, up to the AD-30 silence threshold."""
        if refused_until is not None:
            pending.next_retry_at = refused_until
            return
        pending.retry_count += 1
        backoff = self._config.result_retry_base_delay_seconds * (2**pending.retry_count)
        pending.next_retry_at = now + min(backoff, self._config.result_retry_max_delay_seconds)

    def seconds_until_next_result_retry(self, now: float) -> float:
        """How long until the soonest pending result is due to go again --
        the backoff base when none is pending."""
        soonest = min(
            (pending.next_retry_at for pending in self._pending_results),
            default=now + self._config.result_retry_base_delay_seconds,
        )
        return max(0.0, soonest - now)

    async def _try_send_pending_result(
        self,
        final_result: WorkflowFinalResult,
        send_tcp: SendTcp,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> tuple[bool, float | None]:
        """Send ``final_result`` to its job leader, else any healthy
        manager: ``(delivered, refused_until)`` -- ``refused_until`` the
        soonest instant a manager that refused it under AD-24 (now or
        still) takes it again, None when none refused."""
        target_addrs, seen_addrs = self._final_result_targets(final_result)
        # When each manager that refused it (AD-24) takes it again.
        refusals: list[float] = []

        target_index = 0
        while target_index < len(target_addrs):
            manager_id, manager_addr = target_addrs[target_index]
            target_index += 1
            if manager_id and self._registry.is_circuit_open(manager_id):
                continue
            if self._is_refusing(manager_addr):
                refusals.append(self._refused_until[manager_addr])
                continue
            try:
                response, _ = await send_tcp(
                    manager_addr,
                    "workflow_final_result",
                    final_result.dump(),
                    timeout=self._config.tcp_timeout_standard_seconds,
                )
                if isinstance(response, Exception):
                    raise response
                if not response or not isinstance(response, bytes) or response == b"error":
                    continue
                if response == b"ok":
                    self._registry.get_or_create_circuit_by_addr(manager_addr).record_success()
                    return True, None
                if (refused_until := self._note_refusal(manager_addr, response)) is not None:
                    refusals.append(refused_until)
                    continue

                settled, leader_addr = self._take_final_result_ack(
                    final_result.workflow_id, WorkflowFinalResultAck.load(response)
                )
                self._insert_redirect(target_addrs, seen_addrs, target_index, leader_addr)
                if settled:
                    self._registry.get_or_create_circuit_by_addr(manager_addr).record_success()
                    return True, None
            except Exception as error:
                await self._log_pending_result_send_failure(manager_addr, error)

        return False, min(refusals, default=None)

    def _final_result_targets(
        self, final_result: WorkflowFinalResult
    ) -> tuple[list[tuple[str | None, tuple[str, int]]], set[tuple[str, int]]]:
        """Where a final result goes, in order: its job's leader, the
        primary manager, then every healthy manager -- each address once."""
        candidates = [
            self._leader_target(final_result),
            self._manager_target(self._registry._primary_manager_id),
            *(self._manager_target(manager_id) for manager_id in sorted(self._registry._healthy_manager_ids)),
        ]
        targets: dict[tuple[str, int], tuple[str | None, tuple[str, int]]] = {}
        for candidate in filter(None, candidates):
            targets.setdefault(candidate[1], candidate)
        return list(targets.values()), set(targets)

    def _leader_target(self, final_result: WorkflowFinalResult) -> tuple[str | None, tuple[str, int]] | None:
        """The result's job leader as a target, when one is known."""
        leader_addr = self._state.get_workflow_job_leader(final_result.workflow_id) or final_result.job_leader_addr
        if not leader_addr:
            return None
        leader_addr = tuple(leader_addr)
        return self._manager_id_at(leader_addr), leader_addr

    def _manager_target(self, manager_id: str | None) -> tuple[str, tuple[str, int]] | None:
        """A known manager as a target (None for an unknown one)."""
        manager = self._registry.get_manager(manager_id) if manager_id else None
        return (manager_id, (manager.tcp_host, manager.tcp_port)) if manager else None

    def _manager_id_at(self, manager_addr: tuple[str, int]) -> str | None:
        """The id of the manager known at ``manager_addr``."""
        manager = self._registry.get_manager_by_addr(manager_addr)
        return manager.node_id if manager else None

    def _insert_redirect(
        self,
        target_addrs: list[tuple[str | None, tuple[str, int]]],
        seen_addrs: set[tuple[str, int]],
        target_index: int,
        redirect_addr: tuple[str, int] | None,
    ) -> None:
        """Try a manager an ack named as the job's leader next -- unless it
        was already a target."""
        if not redirect_addr or redirect_addr in seen_addrs:
            return
        target_addrs.insert(target_index, (self._manager_id_at(redirect_addr), redirect_addr))
        seen_addrs.add(redirect_addr)

    def _take_final_result_ack(
        self, workflow_id: str, ack: WorkflowFinalResultAck
    ) -> tuple[bool, tuple[str, int] | None]:
        """Apply a manager's ack of a final result: the job leader it names,
        and whether it is the primary. Returns whether the ack settles the
        result (taken, forwarded, a duplicate, or for a job that ended) and
        the leader it named."""
        leader_addr = tuple(ack.leader_addr) if ack.leader_addr else None
        if leader_addr:
            self._state.set_workflow_job_leader(workflow_id, leader_addr)
        self._follow_ack_primary(ack)
        return any((ack.accepted, ack.forwarded, ack.duplicate, ack.stale)), leader_addr

    def _follow_ack_primary(self, ack: WorkflowFinalResultAck) -> None:
        """A job leader's ack makes it this worker's primary manager."""
        if ack.is_leader and ack.manager_id:
            self._registry.set_primary_manager(ack.manager_id)

    def _note_refusal(self, manager_addr: tuple[str, int], response: bytes) -> float | None:
        """When ``response`` is an AD-24 refusal: record it and return when
        ``manager_addr`` takes requests again. None for any other answer
        (an ack, read by the caller)."""
        try:
            refusal = RateLimitResponse.load(response)
        except Exception:
            return None  # Not a refusal: the caller reads its own answer.
        if not isinstance(refusal, RateLimitResponse):
            return None
        refused_until = self._refused_until[manager_addr] = _DEFAULT_CLOCK.monotonic() + refusal.retry_after_seconds
        return refused_until

    async def _log_pending_result_send_failure(self, manager_addr: tuple[str, int], error: Exception) -> None:
        """A pending result's send to ``manager_addr`` failed: count it
        against the manager's circuit when it is the manager's fault, and
        log it -- as an error when it is this worker's."""
        record_circuit, category = _classify_send_error(error)
        if record_circuit:
            self._registry.get_or_create_circuit_by_addr(manager_addr).record_error()
        if self._logger is None:
            return
        log_model = _SEND_FAILURE_LOG_MODELS.get(category, ServerDebug)
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

    def get_pending_result_count(self) -> int:
        return len(self._pending_results)
