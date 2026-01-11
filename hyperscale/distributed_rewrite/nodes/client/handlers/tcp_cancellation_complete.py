"""
TCP handler for job cancellation completion notifications.

Handles JobCancellationComplete messages from gates/managers (AD-20).
"""

from hyperscale.distributed_rewrite.models import JobCancellationComplete
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.logging.hyperscale_logger import Logger


class CancellationCompleteHandler:
    """
    Handle job cancellation completion push from manager or gate (AD-20).

    Called when all workflows in a job have been cancelled. The notification
    includes success status and any errors encountered during cancellation.
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
        Process cancellation completion notification.

        Args:
            addr: Source address (gate/manager)
            data: Serialized JobCancellationComplete message
            clock_time: Logical clock time

        Returns:
            b'OK' on success, b'ERROR' on failure
        """
        try:
            completion = JobCancellationComplete.load(data)
            job_id = completion.job_id

            # Store results for await_job_cancellation
            self._state._cancellation_success[job_id] = completion.success
            self._state._cancellation_errors[job_id] = completion.errors

            # Fire the completion event
            event = self._state._cancellation_events.get(job_id)
            if event:
                event.set()

            return b"OK"

        except Exception:
            return b"ERROR"
