"""
TCP handler for job status push notifications.

Handles JobStatusPush and JobBatchPush messages from gates/managers.
"""

from hyperscale.distributed.models import JobStatusPush, JobBatchPush
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger


class JobStatusPushHandler:
    """
    Handle job status push notifications from gate/manager.

    JobStatusPush is a lightweight status update sent periodically during
    job execution. Updates job stats and signals completion if final.
    """

    def __init__(self, state: ClientState, logger: Logger) -> None:
        self._state = state
        self._logger = logger

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
            push = JobStatusPush.load(data)

            job = self._state._jobs.get(push.job_id)
            if not job:
                return b'ok'  # Job not tracked, ignore

            # Update job status
            job.status = push.status
            job.total_completed = push.total_completed
            job.total_failed = push.total_failed
            job.overall_rate = push.overall_rate
            job.elapsed_seconds = push.elapsed_seconds

            # Call user callback if registered
            callback = self._state._job_callbacks.get(push.job_id)
            if callback:
                try:
                    callback(push)
                except Exception:
                    pass  # Don't let callback errors break us

            # If final, signal completion
            if push.is_final:
                event = self._state._job_events.get(push.job_id)
                if event:
                    event.set()

            return b'ok'

        except Exception:
            return b'error'


class JobBatchPushHandler:
    """
    Handle batch stats push notifications from gate/manager.

    JobBatchPush contains detailed progress for a single job including
    step-level stats and per-datacenter breakdown.
    """

    def __init__(self, state: ClientState, logger: Logger) -> None:
        self._state = state
        self._logger = logger

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
                return b'ok'  # Job not tracked, ignore

            # Update job status with batch stats
            job.status = push.status
            job.total_completed = push.total_completed
            job.total_failed = push.total_failed
            job.overall_rate = push.overall_rate
            job.elapsed_seconds = push.elapsed_seconds

            return b'ok'

        except Exception:
            return b'error'
