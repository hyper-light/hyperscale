"""
TCP handler for job status push notifications.

Handles JobStatusPush and JobBatchPush messages from gates/managers.
"""

import asyncio

from hyperscale.distributed.models import JobStatusPush, JobBatchPush
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.status_application import (
    JobStatusApplier,
)
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
            push = JobStatusPush.load(data)

            job = self._state._jobs.get(push.job_id)
            if not job:
                return b"ok"  # Job not tracked, ignore

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

            # Call user callback if registered
            callback = self._state._job_callbacks.get(push.job_id)
            if callback:
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

            # If final, signal completion
            if push.is_final:
                event = self._state._job_events.get(push.job_id)
                if event:
                    event.set()

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
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(push)
                    else:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, progress_callback, push)
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
