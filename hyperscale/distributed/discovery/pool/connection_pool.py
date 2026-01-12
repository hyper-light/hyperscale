"""
Connection pool for managing peer connections.

Provides connection pooling with health tracking and automatic cleanup.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Generic, TypeVar, Callable, Awaitable

from hyperscale.distributed.discovery.models.connection_state import (
    ConnectionState,
)


T = TypeVar("T")  # Connection type


@dataclass(slots=True)
class PooledConnection(Generic[T]):
    """A pooled connection with metadata."""

    peer_id: str
    """The peer this connection is to."""

    connection: T
    """The actual connection object."""

    state: ConnectionState = ConnectionState.DISCONNECTED
    """Current connection state."""

    created_at: float = field(default_factory=time.monotonic)
    """When the connection was created."""

    last_used: float = field(default_factory=time.monotonic)
    """When the connection was last used."""

    use_count: int = 0
    """Number of times this connection has been used."""

    consecutive_failures: int = 0
    """Number of consecutive failures on this connection."""


@dataclass
class ConnectionPoolConfig:
    """Configuration for the connection pool."""

    max_connections_per_peer: int = 5
    """Maximum connections to maintain per peer."""

    max_total_connections: int = 100
    """Maximum total connections across all peers."""

    idle_timeout_seconds: float = 300.0
    """Close connections idle longer than this (5 minutes)."""

    max_connection_age_seconds: float = 3600.0
    """Close connections older than this (1 hour)."""

    health_check_interval_seconds: float = 30.0
    """Interval between health checks."""

    max_consecutive_failures: int = 3
    """Evict connection after this many consecutive failures."""

    connection_timeout_seconds: float = 10.0
    """Timeout for establishing new connections."""


@dataclass
class ConnectionPool(Generic[T]):
    """
    Connection pool with health tracking and automatic cleanup.

    Manages a pool of connections to peers with:
    - Per-peer connection limits
    - Global connection limits
    - Idle timeout eviction
    - Age-based eviction
    - Health-based eviction (consecutive failures)

    Usage:
        pool = ConnectionPool(
            config=ConnectionPoolConfig(),
            connect_fn=my_connect_function,
            close_fn=my_close_function,
        )

        # Get or create connection
        conn = await pool.acquire("peer1")
        try:
            result = await use_connection(conn.connection)
            pool.mark_success(conn)
        except Exception:
            pool.mark_failure(conn)
        finally:
            pool.release(conn)
    """

    config: ConnectionPoolConfig = field(default_factory=ConnectionPoolConfig)
    """Pool configuration."""

    connect_fn: Callable[[str], Awaitable[T]] | None = None
    """Function to create a new connection: async fn(peer_id) -> connection."""

    close_fn: Callable[[T], Awaitable[None]] | None = None
    """Function to close a connection: async fn(connection) -> None."""

    health_check_fn: Callable[[T], Awaitable[bool]] | None = None
    """Optional function to check connection health: async fn(connection) -> is_healthy."""

    _connections: dict[str, list[PooledConnection[T]]] = field(
        default_factory=dict, repr=False
    )
    """Map of peer_id to list of pooled connections."""

    _in_use: set[int] = field(default_factory=set, repr=False)
    """Set of connection object IDs that are currently in use."""

    _total_connections: int = field(default=0, repr=False)
    """Total number of connections across all peers."""

    _lock: asyncio.Lock | None = field(default=None, repr=False)
    """Lock for thread-safe operations (lazily initialized)."""

    def _get_lock(self) -> asyncio.Lock:
        """Get or create the lock (lazy initialization for event loop compatibility)."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def acquire(
        self,
        peer_id: str,
        timeout: float | None = None,
    ) -> PooledConnection[T]:
        """
        Acquire a connection to a peer.

        Gets an existing idle connection or creates a new one.

        Args:
            peer_id: The peer to connect to
            timeout: Optional timeout (uses config default if None)

        Returns:
            PooledConnection ready for use

        Raises:
            TimeoutError: If connection cannot be established in time
            RuntimeError: If connect_fn is not configured
        """
        if self.connect_fn is None:
            raise RuntimeError("connect_fn must be configured")

        timeout = timeout or self.config.connection_timeout_seconds

        async with self._get_lock():
            # Try to get existing idle connection
            peer_connections = self._connections.get(peer_id, [])
            for pooled in peer_connections:
                conn_id = id(pooled.connection)
                if (
                    conn_id not in self._in_use
                    and pooled.state == ConnectionState.CONNECTED
                ):
                    # Found idle connection
                    self._in_use.add(conn_id)
                    pooled.last_used = time.monotonic()
                    pooled.use_count += 1
                    return pooled

            # Check limits before creating new connection
            if self._total_connections >= self.config.max_total_connections:
                # Try to evict an idle connection
                evicted = await self._evict_one_idle()
                if not evicted:
                    raise RuntimeError(
                        f"Connection pool exhausted ({self._total_connections} connections)"
                    )

            if len(peer_connections) >= self.config.max_connections_per_peer:
                raise RuntimeError(f"Max connections per peer reached for {peer_id}")

        # Create new connection (outside lock)
        try:
            connection = await asyncio.wait_for(
                self.connect_fn(peer_id),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutError(f"Connection to {peer_id} timed out")

        pooled = PooledConnection(
            peer_id=peer_id,
            connection=connection,
            state=ConnectionState.CONNECTED,
            use_count=1,
        )

        async with self._get_lock():
            peer_connections = self._connections.get(peer_id, [])

            if self._total_connections >= self.config.max_total_connections:
                if self.close_fn is not None:
                    await self.close_fn(connection)
                raise RuntimeError(
                    f"Connection pool exhausted (limit reached during creation)"
                )

            if len(peer_connections) >= self.config.max_connections_per_peer:
                if self.close_fn is not None:
                    await self.close_fn(connection)
                raise RuntimeError(
                    f"Max connections per peer reached for {peer_id} (limit reached during creation)"
                )

            if peer_id not in self._connections:
                self._connections[peer_id] = []
            self._connections[peer_id].append(pooled)
            self._in_use.add(id(connection))
            self._total_connections += 1

        return pooled

    async def release(self, pooled: PooledConnection[T]) -> None:
        """
        Release a connection back to the pool.

        Args:
            pooled: The connection to release
        """
        async with self._get_lock():
            conn_id = id(pooled.connection)
            self._in_use.discard(conn_id)

    async def mark_success(self, pooled: PooledConnection[T]) -> None:
        """
        Mark a connection as successful.

        Resets consecutive failure count.

        Args:
            pooled: The connection that succeeded
        """
        async with self._get_lock():
            pooled.consecutive_failures = 0
            pooled.last_used = time.monotonic()

    async def mark_failure(self, pooled: PooledConnection[T]) -> None:
        """
        Mark a connection as failed.

        Increments consecutive failure count. Connection may be evicted
        if it exceeds max_consecutive_failures.

        Args:
            pooled: The connection that failed
        """
        async with self._get_lock():
            pooled.consecutive_failures += 1
            pooled.last_used = time.monotonic()

            if pooled.consecutive_failures >= self.config.max_consecutive_failures:
                pooled.state = ConnectionState.FAILED

    async def close(self, pooled: PooledConnection[T]) -> None:
        """
        Close and remove a specific connection.

        Args:
            pooled: The connection to close
        """
        pooled.state = ConnectionState.DRAINING

        # Remove from in_use
        conn_id = id(pooled.connection)
        self._in_use.discard(conn_id)

        # Close the connection
        if self.close_fn is not None:
            try:
                await self.close_fn(pooled.connection)
            except Exception:
                pass  # Ignore close errors

        pooled.state = ConnectionState.DISCONNECTED

        # Remove from pool
        async with self._get_lock():
            peer_conns = self._connections.get(pooled.peer_id)
            if peer_conns and pooled in peer_conns:
                peer_conns.remove(pooled)
                self._total_connections -= 1
                if not peer_conns:
                    del self._connections[pooled.peer_id]

    async def close_peer(self, peer_id: str) -> int:
        """
        Close all connections to a peer.

        Args:
            peer_id: The peer to disconnect from

        Returns:
            Number of connections closed
        """
        async with self._get_lock():
            peer_conns = self._connections.pop(peer_id, [])

        closed = 0
        for pooled in peer_conns:
            conn_id = id(pooled.connection)
            self._in_use.discard(conn_id)

            if self.close_fn is not None:
                try:
                    await self.close_fn(pooled.connection)
                except Exception:
                    pass

            closed += 1

        async with self._get_lock():
            self._total_connections -= closed

        return closed

    async def cleanup(self) -> tuple[int, int, int]:
        """
        Clean up idle, old, and failed connections.

        Returns:
            Tuple of (idle_evicted, aged_evicted, failed_evicted)
        """
        now = time.monotonic()
        idle_evicted = 0
        aged_evicted = 0
        failed_evicted = 0

        to_close: list[PooledConnection[T]] = []

        async with self._get_lock():
            for peer_id, connections in list(self._connections.items()):
                for pooled in list(connections):
                    conn_id = id(pooled.connection)

                    # Skip in-use connections
                    if conn_id in self._in_use:
                        continue

                    should_evict = False
                    reason = ""

                    # Check idle timeout
                    idle_time = now - pooled.last_used
                    if idle_time > self.config.idle_timeout_seconds:
                        should_evict = True
                        reason = "idle"
                        idle_evicted += 1

                    # Check age
                    age = now - pooled.created_at
                    if age > self.config.max_connection_age_seconds:
                        should_evict = True
                        reason = "aged"
                        if reason != "idle":
                            aged_evicted += 1

                    # Check failures
                    if pooled.state == ConnectionState.FAILED:
                        should_evict = True
                        reason = "failed"
                        if reason not in ("idle", "aged"):
                            failed_evicted += 1

                    if should_evict:
                        connections.remove(pooled)
                        self._total_connections -= 1
                        to_close.append(pooled)

                # Remove empty peer entries
                if not connections:
                    del self._connections[peer_id]

        # Close connections outside lock
        for pooled in to_close:
            if self.close_fn is not None:
                try:
                    await self.close_fn(pooled.connection)
                except Exception:
                    pass

        return (idle_evicted, aged_evicted, failed_evicted)

    async def _evict_one_idle(self) -> bool:
        """
        Evict the oldest idle connection.

        Returns:
            True if a connection was evicted
        """
        oldest: PooledConnection[T] | None = None
        oldest_time = float("inf")

        for connections in self._connections.values():
            for pooled in connections:
                conn_id = id(pooled.connection)
                if conn_id not in self._in_use:
                    if pooled.last_used < oldest_time:
                        oldest_time = pooled.last_used
                        oldest = pooled

        if oldest is not None:
            peer_conns = self._connections.get(oldest.peer_id)
            if peer_conns:
                peer_conns.remove(oldest)
                self._total_connections -= 1
                if not peer_conns:
                    del self._connections[oldest.peer_id]

            if self.close_fn is not None:
                try:
                    await self.close_fn(oldest.connection)
                except Exception:
                    pass

            return True

        return False

    async def close_all(self) -> int:
        """
        Close all connections.

        Returns:
            Number of connections closed
        """
        async with self._get_lock():
            all_connections: list[PooledConnection[T]] = []
            for connections in self._connections.values():
                all_connections.extend(connections)
            self._connections.clear()
            self._in_use.clear()
            self._total_connections = 0

        for pooled in all_connections:
            if self.close_fn is not None:
                try:
                    await self.close_fn(pooled.connection)
                except Exception:
                    pass

        return len(all_connections)

    def get_peer_connection_count(self, peer_id: str) -> int:
        """Get the number of connections to a specific peer."""
        return len(self._connections.get(peer_id, []))

    def get_stats(self) -> dict[str, int]:
        """Get pool statistics."""
        idle_count = 0
        in_use_count = 0

        for connections in self._connections.values():
            for pooled in connections:
                if id(pooled.connection) in self._in_use:
                    in_use_count += 1
                else:
                    idle_count += 1

        return {
            "total_connections": self._total_connections,
            "in_use": in_use_count,
            "idle": idle_count,
            "peer_count": len(self._connections),
        }

    @property
    def total_connections(self) -> int:
        """Return total number of connections."""
        return self._total_connections

    @property
    def peer_count(self) -> int:
        """Return number of peers with connections."""
        return len(self._connections)
