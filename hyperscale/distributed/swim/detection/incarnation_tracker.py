"""
Incarnation number tracking for SWIM protocol.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Any

from hyperscale.distributed.swim.core.types import Status
from hyperscale.distributed.swim.core.node_state import NodeState
from hyperscale.distributed.swim.core.protocols import LoggerProtocol
from hyperscale.logging.hyperscale_logging_models import ServerDebug


class MessageFreshness(Enum):
    """
    Result of checking message freshness.

    Indicates whether a message should be processed and why it was
    accepted or rejected. This enables appropriate handling per case.
    """

    FRESH = "fresh"
    """Message has new information - process it."""

    DUPLICATE = "duplicate"
    """Same incarnation and same/lower status priority - silent ignore.
    This is completely normal in gossip protocols where the same state
    propagates via multiple paths."""

    STALE = "stale"
    """Lower incarnation than known - indicates delayed message or state drift.
    Worth logging as it may indicate network issues."""

    INVALID = "invalid"
    """Incarnation number failed validation (negative or exceeds max).
    Indicates bug or corruption."""

    SUSPICIOUS = "suspicious"
    """Incarnation jump is suspiciously large - possible attack or serious bug."""


# Maximum valid incarnation number (2^31 - 1 for wide compatibility)
MAX_INCARNATION = 2**31 - 1

# Maximum allowed incarnation jump in a single message
# Larger jumps may indicate attack or corruption
MAX_INCARNATION_JUMP = 1000


@dataclass
class IncarnationTracker:
    """
    Tracks incarnation numbers for SWIM protocol.

    Each node maintains:
    - Its own incarnation number (incremented on refutation)
    - Known incarnation numbers for all other nodes

    Incarnation numbers are used to:
    - Order messages about the same node
    - Allow refutation of false suspicions
    - Prevent old messages from overriding newer state

    Resource limits:
    - max_nodes: Maximum tracked nodes (default 10000)
    - dead_node_retention: How long to keep dead nodes (default 1 hour)
    - Automatic cleanup of stale entries
    """

    self_incarnation: int = 0
    node_states: dict[tuple[str, int], NodeState] = field(default_factory=dict)

    max_nodes: int = 10000
    dead_node_retention_seconds: float = 3600.0

    zombie_detection_window_seconds: float = 60.0
    minimum_rejoin_incarnation_bump: int = 5

    _on_node_evicted: Callable[[tuple[str, int], NodeState], None] | None = None

    _eviction_count: int = 0
    _cleanup_count: int = 0
    _zombie_rejections: int = 0

    _death_timestamps: dict[tuple[str, int], float] = field(default_factory=dict)
    _death_incarnations: dict[tuple[str, int], int] = field(default_factory=dict)

    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0
    _node_id: int = 0

    def __post_init__(self):
        self._lock = asyncio.Lock()
        if not hasattr(self, "_death_timestamps"):
            self._death_timestamps = {}
        if not hasattr(self, "_death_incarnations"):
            self._death_incarnations = {}
        self._zombie_rejections = 0

    def set_logger(
        self,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
        node_id: int,
    ) -> None:
        """Set logger for structured logging."""
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id

    async def _log_debug(self, message: str) -> None:
        """Log a debug message."""
        if self._logger:
            try:
                await self._logger.log(
                    ServerDebug(
                        message=f"[IncarnationTracker] {message}",
                        node_host=self._node_host,
                        node_port=self._node_port,
                        node_id=self._node_id,
                    )
                )
            except Exception:
                pass  # Don't let logging errors propagate

    def get_self_incarnation(self) -> int:
        """Get current incarnation number for this node."""
        return self.self_incarnation

    async def increment_self_incarnation(self) -> int:
        """
        Increment own incarnation number.
        Called when refuting a suspicion about ourselves.
        Returns the new incarnation number.

        Raises:
            OverflowError: If incarnation would exceed MAX_INCARNATION.
        """
        async with self._lock:
            if self.self_incarnation >= MAX_INCARNATION:
                raise OverflowError(
                    f"Incarnation number exhausted (at {MAX_INCARNATION}). "
                    "Node must restart to continue participating in cluster."
                )
            self.self_incarnation += 1
            return self.self_incarnation

    def is_valid_incarnation(self, incarnation: int) -> bool:
        """
        Check if an incarnation number is valid.

        Returns False for:
        - Negative numbers
        - Numbers exceeding MAX_INCARNATION
        """
        return 0 <= incarnation <= MAX_INCARNATION

    def is_suspicious_jump(
        self,
        node: tuple[str, int],
        new_incarnation: int,
    ) -> bool:
        """
        Check if an incarnation jump is suspiciously large.

        Large jumps may indicate:
        - Attack (trying to fast-forward incarnation)
        - Data corruption
        - Node restart with persisted high incarnation

        Returns True if jump exceeds MAX_INCARNATION_JUMP.
        """
        current = self.get_node_incarnation(node)
        jump = new_incarnation - current
        return jump > MAX_INCARNATION_JUMP

    def get_node_state(self, node: tuple[str, int]) -> NodeState | None:
        """Get the current state for a known node."""
        return self.node_states.get(node)

    def get_node_incarnation(self, node: tuple[str, int]) -> int:
        """Get the incarnation number for a node, or 0 if unknown."""
        state = self.node_states.get(node)
        return state.incarnation if state else 0

    async def update_node(
        self,
        node: tuple[str, int],
        status: Status,
        incarnation: int,
        timestamp: float,
        validate: bool = True,
    ) -> bool:
        """
        Update the state of a node.

        Args:
            node: Node address tuple (host, port).
            status: Node status (OK, SUSPECT, DEAD, JOIN).
            incarnation: Node's incarnation number.
            timestamp: Time of this update.
            validate: Whether to validate incarnation number.

        Returns:
            True if the state was updated, False if message was rejected.

        Note:
            If validate=True, invalid or suspicious incarnation numbers
            are rejected and the method returns False.
        """
        if validate:
            if not self.is_valid_incarnation(incarnation):
                return False
            if self.is_suspicious_jump(node, incarnation):
                return False

        async with self._lock:
            if node not in self.node_states:
                self.node_states[node] = NodeState(
                    status=status,
                    incarnation=incarnation,
                    last_update_time=timestamp,
                )
                return True
            return self.node_states[node].update(status, incarnation, timestamp)

    async def remove_node(self, node: tuple[str, int]) -> bool:
        """Remove a node from tracking. Returns True if it existed."""
        async with self._lock:
            if node in self.node_states:
                del self.node_states[node]
                return True
            return False

    def get_all_nodes(self) -> list[tuple[tuple[str, int], NodeState]]:
        """Get all known nodes and their states."""
        return list(self.node_states.items())

    def check_message_freshness(
        self,
        node: tuple[str, int],
        incarnation: int,
        status: Status,
        validate: bool = True,
    ) -> MessageFreshness:
        """
        Check if a message about a node is fresh and why.

        Returns MessageFreshness indicating:
        - FRESH: Message has new information, process it
        - DUPLICATE: Same incarnation, same/lower status (normal in gossip)
        - STALE: Lower incarnation than known
        - INVALID: Incarnation failed validation
        - SUSPICIOUS: Incarnation jump too large

        Args:
            node: Node address tuple.
            incarnation: Incarnation number from message.
            status: Status from message.
            validate: Whether to validate incarnation number.

        Returns:
            MessageFreshness indicating result and reason.
        """
        if validate:
            if not self.is_valid_incarnation(incarnation):
                return MessageFreshness.INVALID
            if self.is_suspicious_jump(node, incarnation):
                return MessageFreshness.SUSPICIOUS

        state = self.node_states.get(node)
        if state is None:
            return MessageFreshness.FRESH
        if incarnation > state.incarnation:
            return MessageFreshness.FRESH
        if incarnation == state.incarnation:
            # Status priority: UNCONFIRMED < JOIN/OK < SUSPECT < DEAD (AD-29)
            # UNCONFIRMED has lowest priority - can be overridden by confirmation
            status_priority = {
                b"UNCONFIRMED": -1,
                b"OK": 0,
                b"JOIN": 0,
                b"SUSPECT": 1,
                b"DEAD": 2,
            }
            if status_priority.get(status, 0) > status_priority.get(state.status, 0):
                return MessageFreshness.FRESH
            return MessageFreshness.DUPLICATE
        return MessageFreshness.STALE

    def is_message_fresh(
        self,
        node: tuple[str, int],
        incarnation: int,
        status: Status,
        validate: bool = True,
    ) -> bool:
        """
        Check if a message about a node is fresh (should be processed).

        This is a convenience wrapper around check_message_freshness()
        that returns a simple boolean for backward compatibility.

        Args:
            node: Node address tuple.
            incarnation: Incarnation number from message.
            status: Status from message.
            validate: Whether to validate incarnation number.

        Returns:
            True if message should be processed, False otherwise.
        """
        return (
            self.check_message_freshness(node, incarnation, status, validate)
            == MessageFreshness.FRESH
        )

    def set_eviction_callback(
        self,
        callback: Callable[[tuple[str, int], NodeState], None],
    ) -> None:
        """Set callback for when nodes are evicted."""
        self._on_node_evicted = callback

    async def cleanup_dead_nodes(self) -> int:
        """
        Remove dead nodes that have exceeded retention period.

        Returns:
            Number of nodes removed.
        """
        now = time.monotonic()
        cutoff = now - self.dead_node_retention_seconds

        async with self._lock:
            to_remove = []
            for node, state in list(self.node_states.items()):
                if state.status == b"DEAD" and state.last_update_time < cutoff:
                    to_remove.append(node)

            removed_nodes: list[tuple[tuple[str, int], NodeState]] = []
            for node in to_remove:
                state = self.node_states.pop(node)
                self._cleanup_count += 1
                removed_nodes.append((node, state))

        for node, state in removed_nodes:
            if self._on_node_evicted:
                try:
                    self._on_node_evicted(node, state)
                except Exception as e:
                    await self._log_debug(
                        f"Eviction callback error for node {node}: "
                        f"{type(e).__name__}: {e}"
                    )

        return len(removed_nodes)

    async def evict_if_needed(self) -> int:
        """
        Evict oldest nodes if we exceed max_nodes limit.

        Eviction priority:
        1. Dead nodes (oldest first)
        2. Suspect nodes (oldest first)
        3. OK nodes (oldest first)

        Returns:
            Number of nodes evicted.
        """
        async with self._lock:
            if len(self.node_states) <= self.max_nodes:
                return 0

            to_evict_count = len(self.node_states) - self.max_nodes + 100

            status_priority = {
                b"UNCONFIRMED": -1,
                b"DEAD": 0,
                b"SUSPECT": 1,
                b"OK": 2,
                b"JOIN": 2,
            }

            sorted_nodes = sorted(
                list(self.node_states.items()),
                key=lambda x: (
                    status_priority.get(x[1].status, 2),
                    x[1].last_update_time,
                ),
            )

            evicted_nodes: list[tuple[tuple[str, int], NodeState]] = []
            for node, state in sorted_nodes[:to_evict_count]:
                del self.node_states[node]
                self._eviction_count += 1
                evicted_nodes.append((node, state))

        for node, state in evicted_nodes:
            if self._on_node_evicted:
                try:
                    self._on_node_evicted(node, state)
                except Exception as e:
                    await self._log_debug(
                        f"Eviction callback error for node {node}: "
                        f"{type(e).__name__}: {e}"
                    )

        return len(evicted_nodes)

    async def cleanup(self) -> dict[str, int]:
        """
        Run all cleanup operations.

        Returns:
            Dict with cleanup stats.
        """
        dead_removed = await self.cleanup_dead_nodes()
        evicted = await self.evict_if_needed()

        return {
            "dead_removed": dead_removed,
            "evicted": evicted,
            "total_nodes": len(self.node_states),
        }

    def get_stats(self) -> dict[str, int]:
        """Get tracker statistics for monitoring."""
        status_counts = {
            b"UNCONFIRMED": 0,
            b"OK": 0,
            b"SUSPECT": 0,
            b"DEAD": 0,
            b"JOIN": 0,
        }
        for state in list(self.node_states.values()):
            status_counts[state.status] = status_counts.get(state.status, 0) + 1

        return {
            "total_nodes": len(self.node_states),
            "unconfirmed_nodes": status_counts.get(b"UNCONFIRMED", 0),
            "ok_nodes": status_counts.get(b"OK", 0),
            "suspect_nodes": status_counts.get(b"SUSPECT", 0),
            "dead_nodes": status_counts.get(b"DEAD", 0),
            "total_evictions": self._eviction_count,
            "total_cleanups": self._cleanup_count,
            "zombie_rejections": self._zombie_rejections,
            "active_death_records": len(self._death_timestamps),
        }

    # =========================================================================
    # AD-29: Peer Confirmation Methods
    # =========================================================================

    async def add_unconfirmed_node(
        self,
        node: tuple[str, int],
        timestamp: float | None = None,
    ) -> bool:
        """
        Add a node as UNCONFIRMED (AD-29 Task 12.3.1).

        Called when a peer is discovered via gossip or configuration but
        hasn't been confirmed via bidirectional communication yet.

        Args:
            node: Node address tuple (host, port)
            timestamp: Optional timestamp (defaults to now)

        Returns:
            True if node was added, False if already exists with higher status
        """
        if timestamp is None:
            timestamp = time.monotonic()

        async with self._lock:
            existing = self.node_states.get(node)
            if existing and existing.status != b"UNCONFIRMED":
                return False

            if node not in self.node_states:
                self.node_states[node] = NodeState(
                    status=b"UNCONFIRMED",
                    incarnation=0,
                    last_update_time=timestamp,
                )
                return True

            return False

    async def confirm_node(
        self,
        node: tuple[str, int],
        incarnation: int = 0,
        timestamp: float | None = None,
    ) -> bool:
        """
        Transition node from UNCONFIRMED to OK (AD-29 Task 12.3.2).

        Called when we receive first successful bidirectional communication
        (probe ACK, heartbeat, valid protocol message).

        Args:
            node: Node address tuple (host, port)
            incarnation: Node's incarnation from the confirming message
            timestamp: Optional timestamp (defaults to now)

        Returns:
            True if node was confirmed, False if not found or already confirmed
        """
        if timestamp is None:
            timestamp = time.monotonic()

        async with self._lock:
            existing = self.node_states.get(node)

            if existing is None:
                self.node_states[node] = NodeState(
                    status=b"OK",
                    incarnation=incarnation,
                    last_update_time=timestamp,
                )
                return True

            if existing.status == b"UNCONFIRMED":
                existing.status = b"OK"
                existing.incarnation = max(existing.incarnation, incarnation)
                existing.last_update_time = timestamp
                return True

            if incarnation > existing.incarnation:
                existing.incarnation = incarnation
                existing.last_update_time = timestamp

            return False

    def is_node_confirmed(self, node: tuple[str, int]) -> bool:
        """
        Check if a node is confirmed (not UNCONFIRMED) (AD-29).

        Returns:
            True if node exists and is not in UNCONFIRMED state
        """
        state = self.node_states.get(node)
        return state is not None and state.status != b"UNCONFIRMED"

    def is_node_unconfirmed(self, node: tuple[str, int]) -> bool:
        """
        Check if a node is in UNCONFIRMED state (AD-29).

        Returns:
            True if node exists and is in UNCONFIRMED state
        """
        state = self.node_states.get(node)
        return state is not None and state.status == b"UNCONFIRMED"

    def can_suspect_node(self, node: tuple[str, int]) -> bool:
        """
        Check if a node can be transitioned to SUSPECT (AD-29 Task 12.3.4).

        Per AD-29: Only CONFIRMED peers can be suspected. UNCONFIRMED peers
        cannot transition to SUSPECT - they must first be confirmed.

        Returns:
            True if node can be suspected (is confirmed and not already DEAD)
        """
        state = self.node_states.get(node)
        if state is None:
            return False

        # AD-29: Cannot suspect unconfirmed peers
        if state.status == b"UNCONFIRMED":
            return False

        # Cannot re-suspect dead nodes
        if state.status == b"DEAD":
            return False

        return True

    def get_nodes_by_state(self, status: Status) -> list[tuple[str, int]]:
        """
        Get all nodes in a specific state (AD-29 Task 12.3.5).

        Args:
            status: The status to filter by

        Returns:
            List of node addresses with that status
        """
        return [
            node for node, state in self.node_states.items() if state.status == status
        ]

    def get_unconfirmed_nodes(self) -> list[tuple[str, int]]:
        """Get all nodes in UNCONFIRMED state."""
        return self.get_nodes_by_state(b"UNCONFIRMED")

    def record_node_death(
        self,
        node: tuple[str, int],
        incarnation_at_death: int,
        timestamp: float | None = None,
    ) -> None:
        """
        Record when a node was marked DEAD for zombie detection.

        Args:
            node: The node address that died
            incarnation_at_death: The incarnation number when the node died
            timestamp: Death timestamp (defaults to now)
        """
        if timestamp is None:
            timestamp = time.monotonic()

        self._death_timestamps[node] = timestamp
        self._death_incarnations[node] = incarnation_at_death

    def clear_death_record(self, node: tuple[str, int]) -> None:
        """Clear death record for a node that has successfully rejoined."""
        self._death_timestamps.pop(node, None)
        self._death_incarnations.pop(node, None)

    def is_potential_zombie(
        self,
        node: tuple[str, int],
        claimed_incarnation: int,
    ) -> bool:
        """
        Check if a rejoining node might be a zombie.

        A node is considered a potential zombie if:
        1. It was recently marked DEAD (within zombie_detection_window)
        2. Its claimed incarnation is not sufficiently higher than its death incarnation

        Args:
            node: The node attempting to rejoin
            claimed_incarnation: The incarnation the node claims to have

        Returns:
            True if the node should be rejected as a potential zombie
        """
        death_timestamp = self._death_timestamps.get(node)
        if death_timestamp is None:
            return False

        now = time.monotonic()
        time_since_death = now - death_timestamp

        if time_since_death > self.zombie_detection_window_seconds:
            self.clear_death_record(node)
            return False

        death_incarnation = self._death_incarnations.get(node, 0)
        required_incarnation = death_incarnation + self.minimum_rejoin_incarnation_bump

        if claimed_incarnation < required_incarnation:
            self._zombie_rejections += 1
            return True

        return False

    def get_required_rejoin_incarnation(self, node: tuple[str, int]) -> int:
        """
        Get the minimum incarnation required for a node to rejoin.

        Returns:
            Minimum incarnation number, or 0 if no death record exists
        """
        death_incarnation = self._death_incarnations.get(node, 0)
        if death_incarnation == 0:
            return 0
        return death_incarnation + self.minimum_rejoin_incarnation_bump

    async def cleanup_death_records(self) -> int:
        """
        Remove death records older than zombie_detection_window.

        Returns:
            Number of records cleaned up
        """
        now = time.monotonic()
        cutoff = now - self.zombie_detection_window_seconds
        to_remove = [
            node
            for node, timestamp in self._death_timestamps.items()
            if timestamp < cutoff
        ]

        for node in to_remove:
            self.clear_death_record(node)

        return len(to_remove)
