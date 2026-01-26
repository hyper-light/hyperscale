"""
TCP handler for state sync requests.

Handles state synchronization requests from peer managers and workers.
"""

from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    StateSyncRequest,
    StateSyncResponse,
    WorkerStateSnapshot,
    ManagerStateSnapshot,
)
from hyperscale.logging.hyperscale_logging_models import ServerDebug

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger

GetStateSnapshotFunc = Callable[[], ManagerStateSnapshot]


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
        task_runner: "TaskRunner",
        get_state_snapshot: GetStateSnapshotFunc,
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner
        self._get_state_snapshot: GetStateSnapshotFunc = get_state_snapshot

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
                ),
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
