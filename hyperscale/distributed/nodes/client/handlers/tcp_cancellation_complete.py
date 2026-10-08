"""
TCP handler for job cancellation completion notifications.

Handles JobCancellationComplete messages from gates/managers (AD-20).
"""

from hyperscale.distributed.models import JobCancellationComplete
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerWarning


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

            # Only a cancellation this client is waiting on is recorded.
            # A datacenter a gate told to stop running the job -- one it
            # moved the job off (AD-36), or one it completed without
            # (AD-44) -- reports its cancellation here too: recorded, it
            # stayed for the client's lifetime (only a cancellation this
            # client made clears its entries).
            if (event := self._state._cancellation_events.get(job_id)) is None:
                return b"OK"

            # Store results for await_job_cancellation, then fire the
            # completion event
            self._state._cancellation_success[job_id] = completion.success
            self._state._cancellation_errors[job_id] = completion.errors
            event.set()

            return b"OK"

        except Exception as error:
            await self._logger.log(
                ServerWarning(
                    message=f"Cancellation completion handling failed: {error}",
                    node_host="client",
                    node_port=0,
                    node_id="client",
                )
            )
            return b"ERROR"
