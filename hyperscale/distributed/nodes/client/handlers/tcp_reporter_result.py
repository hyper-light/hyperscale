"""
TCP handler for reporter result push notifications.

Handles ReporterResultPush messages indicating reporter submission completion.
"""

from hyperscale.distributed.models import ReporterResultPush, ClientReporterResult
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerWarning


class ReporterResultPushHandler:
    """
    Handle reporter result notification from manager or gate.

    Called when a reporter submission completes (success or failure).
    Updates the job's reporter_results and calls any registered callback.
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
        Process reporter result push.

        Args:
            addr: Source address (gate/manager)
            data: Serialized ReporterResultPush message
            clock_time: Logical clock time

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            push = ReporterResultPush.load(data)

            job = self._state._jobs.get(push.job_id)
            if job:
                # Store the result
                job.reporter_results[push.reporter_type] = ClientReporterResult(
                    reporter_type=push.reporter_type,
                    success=push.success,
                    error=push.error,
                    elapsed_seconds=push.elapsed_seconds,
                    source=push.source,
                    datacenter=push.datacenter,
                )

            # Call user callback if registered
            callback = self._state._reporter_callbacks.get(push.job_id)
            if callback:
                try:
                    callback(push)
                except Exception as callback_error:
                    if self._logger:
                        await self._logger.log(
                            ServerWarning(
                                message=f"Reporter result callback error: {callback_error}",
                                node_host="client",
                                node_port=0,
                                node_id="client",
                            )
                        )

            return b"ok"

        except Exception as handler_error:
            await self._logger.log(
                ServerWarning(
                    message=f"Reporter result push handler error: {handler_error}, payload_length={len(data)}",
                    node_host="client",
                    node_port=0,
                    node_id="client",
                )
            )
            return b"error"
