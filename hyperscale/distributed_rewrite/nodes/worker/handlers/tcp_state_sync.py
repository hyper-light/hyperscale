"""
State sync TCP handler for worker.

Handles state sync requests from new manager leaders.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.models import (
    StateSyncRequest,
    StateSyncResponse,
)

if TYPE_CHECKING:
    from ..server import WorkerServer


class StateSyncHandler:
    """
    Handler for state sync requests from managers.

    Returns worker's current state snapshot for manager synchronization.
    """

    def __init__(self, server: "WorkerServer") -> None:
        """
        Initialize handler with server reference.

        Args:
            server: WorkerServer instance for state access
        """
        self._server = server

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle state sync request from a new manager leader.

        Returns the worker's current state snapshot.

        Args:
            addr: Source address (manager TCP address)
            data: Serialized StateSyncRequest
            clock_time: Logical clock time

        Returns:
            Serialized StateSyncResponse
        """
        try:
            request = StateSyncRequest.load(data)

            response = StateSyncResponse(
                responder_id=self._server._node_id.full,
                current_version=self._server._state_version,
                worker_state=self._server._get_state_snapshot(),
            )
            return response.dump()

        except Exception:
            return b""
