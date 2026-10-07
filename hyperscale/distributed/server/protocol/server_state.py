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
        # False once the server starts closing: a connection accepted
        # before its listener closed but whose ``connection_made`` runs
        # after the server aborted its tracked connections is aborted on
        # arrival -- otherwise it stays open and ``Server.wait_closed``
        # waits on it forever.
        self.accepting = True

    def is_at_capacity(self) -> bool:
        """Check if server is at connection capacity (Task 62)."""
        return self.max_connections is not None and len(self.connections) >= self.max_connections

    def get_connection_count(self) -> int:
        """Get current active connection count."""
        return len(self.connections)

    def admits_connection(self) -> bool:
        """Whether a newly accepted connection may join: the server is not
        closing and is below its cap."""
        return self.accepting and not self.is_at_capacity()

    def refuse_connection(self, transport: asyncio.Transport) -> None:
        """Turn away a connection ``admits_connection`` refused: aborted
        while the server closes (``Server.wait_closed`` must not wait on
        it), else closed and counted as rejected at the cap."""
        if not self.accepting:
            transport.abort()
            return
        self.reject_connection()
        transport.close()

    def reject_connection(self) -> None:
        """Record a rejected connection."""
        self.connections_rejected += 1