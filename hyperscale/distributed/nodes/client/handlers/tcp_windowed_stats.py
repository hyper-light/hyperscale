import asyncio
import cloudpickle

from hyperscale.distributed.reliability.rate_limiting import RequestPriority
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerWarning


class WindowedStatsPushHandler:
    """
    Handle windowed stats push from manager or gate.

    Called periodically with time-correlated aggregated stats.
    Rate-limited to prevent overwhelming the client.
    """

    def __init__(self, state: ClientState, logger: Logger, rate_limiter=None) -> None:
        self._state = state
        self._logger = logger
        self._rate_limiter = rate_limiter

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Process windowed stats push.

        Args:
            addr: Source address (gate/manager)
            data: Cloudpickle-serialized WindowedStatsPush message
            clock_time: Logical clock time

        Returns:
            b'ok' on success, b'rate_limited' if throttled, b'error' on failure
        """
        try:
            # Rate limiting: operation "progress_update" has limits of (300, 10.0) = 30/s
            if self._rate_limiter:
                client_id = f"{addr[0]}:{addr[1]}"
                result = self._rate_limiter.check(
                    client_id=client_id,
                    operation="progress_update",
                    priority=RequestPriority.NORMAL,
                )
                if not result.allowed:
                    return b"rate_limited"

            # Import WindowedStatsPush from jobs module (avoid circular import)
            from hyperscale.distributed.jobs import WindowedStatsPush

            push: WindowedStatsPush = cloudpickle.loads(data)

            callback = self._state._progress_callbacks.get(push.job_id)
            if callback:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(push)
                    else:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, callback, push)
                except Exception as callback_error:
                    if self._logger:
                        await self._logger.log(
                            ServerWarning(
                                message=f"Windowed stats callback error: {callback_error}",
                                node_host="client",
                                node_port=0,
                                node_id="client",
                            )
                        )

            return b"ok"

        except Exception:
            return b"error"
