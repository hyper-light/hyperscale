"""``JobBatchPushHandler`` -- pickled under the namespace
``hyperscale.distributed.nodes.client.handlers.tcp_job_status_push`` (see that module)."""

import inspect
from hyperscale.distributed.models import JobBatchPush
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.status_application import JobStatusApplier
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerWarning


class JobBatchPushHandler:
    """
    Handle batch stats push notifications from gate/manager.

    JobBatchPush contains detailed progress for a single job including
    step-level stats and per-datacenter breakdown.
    """

    def __init__(self, state: ClientState, logger: Logger) -> None:
        self._state = state
        self._logger = logger
        self._status_applier = JobStatusApplier()

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Process job batch push.

        Args:
            addr: Source address (gate/manager)
            data: Serialized JobBatchPush message
            clock_time: Logical clock time

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            push = JobBatchPush.load(data)

            job = self._state._jobs.get(push.job_id)
            if not job:
                return b"ok"

            batch_outcome = self._status_applier.apply_push(
                job,
                push.status,
                push.total_completed,
                push.total_failed,
                push.overall_rate,
                push.elapsed_seconds,
            )
            if batch_outcome.unknown_vocabulary and self._logger:
                await self._logger.log(
                    ServerWarning(
                        message=(
                            f"Batch push for job {push.job_id[:8]} "
                            f"carried status {push.status!r} outside "
                            "the lifecycle vocabulary; not applied"
                        ),
                        node_host="client",
                        node_port=0,
                        node_id="client",
                    )
                )

            progress_callback = self._state._progress_callbacks.get(push.job_id)
            if progress_callback:
                try:
                    # Called on the event loop -- never handed to an executor
                    # thread -- and awaited when it returns an awaitable, so an
                    # async callback, or a sync one wrapping one, both work.
                    if inspect.isawaitable(callback_outcome := progress_callback(push)):
                        await callback_outcome
                except Exception as callback_error:
                    if self._logger:
                        await self._logger.log(
                            ServerWarning(
                                message=f"Job batch progress callback error: {callback_error}",
                                node_host="client",
                                node_port=0,
                                node_id="client",
                            )
                        )

            return b"ok"

        except Exception as error:
            if self._logger:
                await self._logger.log(
                    ServerWarning(
                        message=f"Job batch push handling failed: {error}",
                        node_host="client",
                        node_port=0,
                        node_id="client",
                    )
                )
            return b"error"
