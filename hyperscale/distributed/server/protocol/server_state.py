import asyncio
from typing import TypeVar, Generic


T = TypeVar("T")


class ServerState(Generic[T]):
    """
    Shared servers state that is available between all protocol instances.
    """

    DEFAULT_MAX_CONNECTIONS: int = 10000

    def __init__(self, max_connections: int | None = None) -> None:
        self.total_requests = 0
        self.connections: set[T] = set()
        self.tasks: set[asyncio.Task[None]] = set()
        self.max_connections = max_connections or self.DEFAULT_MAX_CONNECTIONS
        self.connections_rejected = 0

    def is_at_capacity(self) -> bool:
        """Check if server is at connection capacity (Task 62)."""
        return len(self.connections) >= self.max_connections

    def get_connection_count(self) -> int:
        """Get current active connection count."""
        return len(self.connections)

    def reject_connection(self) -> None:
        """Record a rejected connection."""
        self.connections_rejected += 1