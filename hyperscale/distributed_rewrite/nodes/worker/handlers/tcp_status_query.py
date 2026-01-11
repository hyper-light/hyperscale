"""
Workflow status query TCP handler for worker.

Handles workflow status queries from managers for orphan scanning.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..server import WorkerServer


class WorkflowStatusQueryHandler:
    """
    Handler for workflow status queries from managers.

    Returns list of active workflow IDs for orphan scanning.
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
        Handle workflow status query from manager.

        Used by the manager's orphan scanner to verify which workflows
        are actually running on this worker.

        Args:
            addr: Source address (manager TCP address)
            data: Serialized query (unused)
            clock_time: Logical clock time

        Returns:
            Comma-separated list of active workflow IDs as bytes
        """
        try:
            active_workflow_ids = list(self._server._active_workflows.keys())
            return ",".join(active_workflow_ids).encode("utf-8")
        except Exception:
            return b""
