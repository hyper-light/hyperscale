import asyncio
from typing import TypeVar, Generic


T = TypeVar("T")


class ServerState(Generic[T]):
    """
    State shared by every connection one server accepted: the server
    builds one and hands it to each of its connections' protocols (each
    protocol had built its own, so the connection cap counted only the
    connection itself, and nothing could reach a server's connections to
    close them).
    """

    def __init__(self, max_connections: int | None = None) -> None:
        self.total_requests = 0
        self.connections: set[T] = set()
        self.tasks: set[asyncio.Task[None]] = set()
        # None: no cap.
        self.max_connections = max_connections
        self.connections_rejected = 0

    def is_at_capacity(self) -> bool:
        """Check if server is at connection capacity (Task 62)."""
        return self.max_connections is not None and len(self.connections) >= self.max_connections

    def get_connection_count(self) -> int:
        """Get current active connection count."""
        return len(self.connections)

    def reject_connection(self) -> None:
        """Record a rejected connection."""
        self.connections_rejected += 1