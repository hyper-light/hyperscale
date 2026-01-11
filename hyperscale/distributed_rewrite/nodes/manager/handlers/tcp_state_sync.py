"""
TCP handler for state sync requests.

Handles state synchronization requests from peer managers and workers.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.models import (
    StateSyncRequest,
    StateSyncResponse,
    WorkerStateSnapshot,
    ManagerStateSnapshot,
)
from hyperscale.logging.hyperscale_logging_models import ServerDebug

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.state import ManagerState
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.logging.hyperscale_logger import Logger


class StateSyncRequestHandler:
    """
    Handle state sync requests from peer managers.

    Used during leader election and recovery to synchronize state
    between managers.
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
        get_state_snapshot,  # Callable to get current state snapshot
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner
        self._get_state_snapshot = get_state_snapshot

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Process state sync request.

        Args:
            addr: Source address (peer manager)
            data: Serialized StateSyncRequest message
            clock_time: Logical clock time

        Returns:
            Serialized StateSyncResponse with current state snapshot
        """
        try:
            request = StateSyncRequest.load(data)

            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"State sync request from {request.requester_id[:8]}... for type={request.sync_type}",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )

            # Get current state snapshot
            snapshot = self._get_state_snapshot()

            response = StateSyncResponse(
                responder_id=self._node_id,
                state_version=self._state._state_version,
                manager_state=snapshot,
            )

            return response.dump()

        except Exception as e:
            return StateSyncResponse(
                responder_id=self._node_id,
                state_version=self._state._state_version,
                error=str(e),
            ).dump()
