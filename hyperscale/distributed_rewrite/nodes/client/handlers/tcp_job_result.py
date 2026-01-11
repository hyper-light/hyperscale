"""
TCP handlers for job result notifications.

Handles JobFinalResult (single DC) and GlobalJobResult (multi-DC aggregated).
"""

from hyperscale.distributed_rewrite.models import JobFinalResult, GlobalJobResult
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.logging import Logger


class JobFinalResultHandler:
    """
    Handle final job result from manager (when no gates).

    This is a per-datacenter result with all workflow results.
    Sent when job completes in a single-DC scenario.
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
        Process final job result.

        Args:
            addr: Source manager address
            data: Serialized JobFinalResult message
            clock_time: Logical clock time

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            result = JobFinalResult.load(data)

            job = self._state._jobs.get(result.job_id)
            if not job:
                return b'ok'  # Job not tracked, ignore

            # Update job with final result
            job.status = result.status
            job.total_completed = result.total_completed
            job.total_failed = result.total_failed
            job.elapsed_seconds = result.elapsed_seconds
            if result.errors:
                job.error = "; ".join(result.errors)

            # Signal completion
            event = self._state._job_events.get(result.job_id)
            if event:
                event.set()

            return b'ok'

        except Exception:
            return b'error'


class GlobalJobResultHandler:
    """
    Handle global job result from gate.

    This is the aggregated result across all datacenters.
    Sent when multi-DC job completes.
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
        Process global job result.

        Args:
            addr: Source gate address
            data: Serialized GlobalJobResult message
            clock_time: Logical clock time

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            result = GlobalJobResult.load(data)

            job = self._state._jobs.get(result.job_id)
            if not job:
                return b'ok'  # Job not tracked, ignore

            # Update job with aggregated result
            job.status = result.status
            job.total_completed = result.total_completed
            job.total_failed = result.total_failed
            job.elapsed_seconds = result.elapsed_seconds
            if result.errors:
                job.error = "; ".join(result.errors)

            # Multi-DC specific fields
            job.per_datacenter_results = result.per_datacenter_results
            job.aggregated = result.aggregated

            # Signal completion
            event = self._state._job_events.get(result.job_id)
            if event:
                event.set()

            return b'ok'

        except Exception:
            return b'error'
