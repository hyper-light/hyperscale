"""``JobStatusPushHandler`` -- pickled under the namespace
``hyperscale.distributed.nodes.client.handlers.tcp_job_status_push`` (see that module)."""

from typing import Callable

from hyperscale.distributed.models import ClientJobResult, JobStatusPush
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.status_application import JobStatusApplier
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerWarning


class JobStatusPushHandler:
    """
    Handle job status push notifications from gate/manager.

    JobStatusPush is a lightweight status update sent periodically during
    job execution. Updates job stats and signals completion if final.
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
        Process job status push.

        Args:
            addr: Source address (gate/manager)
            data: Serialized JobStatusPush message
            clock_time: Logical clock time

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            await self._handle_push(JobStatusPush.load(data))
            return b"ok"

        except Exception as error:
            if self._logger:
                await self._logger.log(
                    ServerWarning(
                        message=f"Job status push handling failed: {error}",
                        node_host="client",
                        node_port=0,
                        node_id="client",
                    )
                )
            return b"error"

    async def _handle_push(self, push: JobStatusPush) -> None:
        """Apply a tracked job's status push, run its callback, and signal completion when final."""
        job = self._state._jobs.get(push.job_id)
        if not job:
            return  # Job not tracked, ignore

        await self._apply_status(job, push)

        # Call user callback if registered
        callback = self._state._job_callbacks.get(push.job_id)
        if callback:
            await self._invoke_status_callback(callback, push)

        # If final, signal completion
        self._signal_if_final(push)

    async def _apply_status(self, job: ClientJobResult, push: JobStatusPush) -> None:
        """Apply the push through the order guard, warning on a status outside the vocabulary."""
        # Order-guarded: pushes race polls with no wire sequence;
        # the applier rejects backward/post-terminal transitions.
        push_outcome = self._status_applier.apply_push(
            job,
            push.status,
            push.total_completed,
            push.total_failed,
            push.overall_rate,
            push.elapsed_seconds,
        )
        if push_outcome.unknown_vocabulary and self._logger:
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Status push for job {push.job_id[:8]} "
                        f"carried status {push.status!r} outside "
                        "the lifecycle vocabulary; not applied"
                    ),
                    node_host="client",
                    node_port=0,
                    node_id="client",
                )
            )

    async def _invoke_status_callback(
        self,
        callback: Callable[[JobStatusPush], None],
        push: JobStatusPush,
    ) -> None:
        """Run the caller's status callback, logging what it raises."""
        try:
            callback(push)
        except Exception as callback_error:
            if self._logger:
                await self._logger.log(
                    ServerWarning(
                        message=f"Job status callback error: {callback_error}",
                        node_host="client",
                        node_port=0,
                        node_id="client",
                    )
                )

    def _signal_if_final(self, push: JobStatusPush) -> None:
        """Set the job's completion event when the push is final."""
        if push.is_final:
            event = self._state._job_events.get(push.job_id)
            if event:
                event.set()
