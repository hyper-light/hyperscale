"""
Sticky connection manager for maintaining affinity to peers.

Provides connection stickiness with health-based eviction.
"""

import time
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from hyperscale.distributed_rewrite.discovery.models.peer_info import PeerHealth


T = TypeVar("T")  # Connection type


@dataclass(slots=True)
class StickyBinding(Generic[T]):
    """A sticky binding between a key and a peer."""

    key: str
    """The key this binding is for (e.g., job_id)."""

    peer_id: str
    """The peer this key is bound to."""

    created_at: float
    """When the binding was created."""

    last_used: float
    """When the binding was last used."""

    use_count: int = 0
    """Number of times this binding has been used."""

    health: PeerHealth = PeerHealth.HEALTHY
    """Current health of the bound peer."""


@dataclass
class StickyConfig:
    """Configuration for sticky connections."""

    max_bindings: int = 10000
    """Maximum number of sticky bindings to maintain."""

    binding_ttl_seconds: float = 3600.0
    """TTL for sticky bindings (1 hour)."""

    idle_ttl_seconds: float = 300.0
    """Remove bindings not used within this time (5 minutes)."""

    evict_on_unhealthy: bool = True
    """If True, evict bindings when peer becomes unhealthy."""

    health_degradation_threshold: PeerHealth = PeerHealth.DEGRADED
    """Evict bindings when health reaches this level or worse."""


@dataclass
class StickyConnectionManager(Generic[T]):
    """
    Manager for sticky connection bindings.

    Maintains affinity between keys (e.g., job_ids) and peers,
    with health-based eviction for automatic failover.

    Sticky connections provide:
    - Consistent routing for related requests
    - Better cache locality at the peer
    - Predictable behavior for debugging

    Health-based eviction ensures:
    - Automatic failover when peers become unhealthy
    - No manual intervention needed for failures
    - Graceful degradation under load

    Usage:
        manager = StickyConnectionManager()

        # Bind a key to a peer
        manager.bind("job-123", "peer1")

        # Get bound peer (or None)
        peer = manager.get_binding("job-123")

        # Update health (will evict if unhealthy)
        manager.update_peer_health("peer1", PeerHealth.UNHEALTHY)

        # Check if binding exists and is healthy
        if manager.is_bound_healthy("job-123"):
            use_sticky_peer(manager.get_binding("job-123"))
    """

    config: StickyConfig = field(default_factory=StickyConfig)
    """Configuration for sticky bindings."""

    _bindings: dict[str, StickyBinding[T]] = field(default_factory=dict, repr=False)
    """Map of key to sticky binding."""

    _peer_health: dict[str, PeerHealth] = field(default_factory=dict, repr=False)
    """Current health of each peer."""

    _peer_bindings: dict[str, set[str]] = field(default_factory=dict, repr=False)
    """Map of peer_id to set of keys bound to that peer."""

    def bind(self, key: str, peer_id: str) -> StickyBinding[T]:
        """
        Create or update a sticky binding.

        Args:
            key: The key to bind (e.g., job_id)
            peer_id: The peer to bind to

        Returns:
            The created or updated binding
        """
        now = time.monotonic()

        existing = self._bindings.get(key)
        if existing is not None:
            # Update existing binding
            old_peer = existing.peer_id
            if old_peer != peer_id:
                # Remove from old peer's set
                if old_peer in self._peer_bindings:
                    self._peer_bindings[old_peer].discard(key)

            existing.peer_id = peer_id
            existing.last_used = now
            existing.use_count += 1
            existing.health = self._peer_health.get(peer_id, PeerHealth.HEALTHY)

            # Add to new peer's set
            if peer_id not in self._peer_bindings:
                self._peer_bindings[peer_id] = set()
            self._peer_bindings[peer_id].add(key)

            return existing

        # Check binding limit
        if len(self._bindings) >= self.config.max_bindings:
            self._evict_oldest()

        # Create new binding
        binding = StickyBinding(
            key=key,
            peer_id=peer_id,
            created_at=now,
            last_used=now,
            use_count=1,
            health=self._peer_health.get(peer_id, PeerHealth.HEALTHY),
        )
        self._bindings[key] = binding

        # Track in peer's binding set
        if peer_id not in self._peer_bindings:
            self._peer_bindings[peer_id] = set()
        self._peer_bindings[peer_id].add(key)

        return binding

    def get_binding(self, key: str) -> str | None:
        """
        Get the peer_id for a sticky binding.

        Updates last_used time if found.

        Args:
            key: The key to look up

        Returns:
            peer_id if bound, None otherwise
        """
        binding = self._bindings.get(key)
        if binding is None:
            return None

        # Check TTL
        now = time.monotonic()
        if now - binding.created_at > self.config.binding_ttl_seconds:
            self._remove_binding(key)
            return None

        binding.last_used = now
        binding.use_count += 1
        return binding.peer_id

    def get_binding_info(self, key: str) -> StickyBinding[T] | None:
        """
        Get full binding info without updating usage.

        Args:
            key: The key to look up

        Returns:
            StickyBinding if found, None otherwise
        """
        return self._bindings.get(key)

    def is_bound(self, key: str) -> bool:
        """Check if a key has a binding."""
        return key in self._bindings

    def is_bound_healthy(self, key: str) -> bool:
        """
        Check if a key has a healthy binding.

        Args:
            key: The key to check

        Returns:
            True if bound and peer is healthy
        """
        binding = self._bindings.get(key)
        if binding is None:
            return False

        # Check binding age
        now = time.monotonic()
        if now - binding.created_at > self.config.binding_ttl_seconds:
            return False

        # Check health
        peer_health = self._peer_health.get(binding.peer_id, PeerHealth.HEALTHY)
        return peer_health < self.config.health_degradation_threshold

    def unbind(self, key: str) -> bool:
        """
        Remove a sticky binding.

        Args:
            key: The key to unbind

        Returns:
            True if binding was removed
        """
        return self._remove_binding(key)

    def update_peer_health(self, peer_id: str, health: PeerHealth) -> int:
        """
        Update health status for a peer.

        May evict bindings if peer becomes unhealthy.

        Args:
            peer_id: The peer to update
            health: New health status

        Returns:
            Number of bindings evicted (if any)
        """
        self._peer_health[peer_id] = health

        # Update health in all bindings for this peer
        keys = self._peer_bindings.get(peer_id, set())
        for key in keys:
            binding = self._bindings.get(key)
            if binding:
                binding.health = health

        # Check if we should evict
        if (
            self.config.evict_on_unhealthy
            and health >= self.config.health_degradation_threshold
        ):
            return self.evict_peer_bindings(peer_id)

        return 0

    def evict_peer_bindings(self, peer_id: str) -> int:
        """
        Remove all bindings for a peer.

        Args:
            peer_id: The peer to evict bindings for

        Returns:
            Number of bindings evicted
        """
        keys = self._peer_bindings.pop(peer_id, set())
        for key in keys:
            self._bindings.pop(key, None)
        return len(keys)

    def cleanup_expired(self) -> tuple[int, int]:
        """
        Remove expired and idle bindings.

        Returns:
            Tuple of (expired_count, idle_count)
        """
        now = time.monotonic()
        expired_count = 0
        idle_count = 0

        to_remove: list[str] = []

        for key, binding in self._bindings.items():
            age = now - binding.created_at
            idle_time = now - binding.last_used

            if age > self.config.binding_ttl_seconds:
                to_remove.append(key)
                expired_count += 1
            elif idle_time > self.config.idle_ttl_seconds:
                to_remove.append(key)
                idle_count += 1

        for key in to_remove:
            self._remove_binding(key)

        return (expired_count, idle_count)

    def clear(self) -> int:
        """
        Remove all bindings.

        Returns:
            Number of bindings removed
        """
        count = len(self._bindings)
        self._bindings.clear()
        self._peer_bindings.clear()
        return count

    def clear_peer_health(self) -> None:
        """Clear all cached peer health states."""
        self._peer_health.clear()

    def _remove_binding(self, key: str) -> bool:
        """Remove a binding and update tracking."""
        binding = self._bindings.pop(key, None)
        if binding is None:
            return False

        peer_keys = self._peer_bindings.get(binding.peer_id)
        if peer_keys:
            peer_keys.discard(key)
            if not peer_keys:
                del self._peer_bindings[binding.peer_id]

        return True

    def _evict_oldest(self) -> bool:
        """Evict the oldest binding by last_used time."""
        if not self._bindings:
            return False

        oldest_key: str | None = None
        oldest_time = float("inf")

        for key, binding in self._bindings.items():
            if binding.last_used < oldest_time:
                oldest_time = binding.last_used
                oldest_key = key

        if oldest_key:
            return self._remove_binding(oldest_key)

        return False

    def get_peer_binding_count(self, peer_id: str) -> int:
        """Get the number of keys bound to a peer."""
        return len(self._peer_bindings.get(peer_id, set()))

    def get_bound_peers(self) -> list[str]:
        """Get list of peers that have bindings."""
        return list(self._peer_bindings.keys())

    @property
    def binding_count(self) -> int:
        """Return total number of bindings."""
        return len(self._bindings)

    @property
    def peer_count(self) -> int:
        """Return number of peers with bindings."""
        return len(self._peer_bindings)

    def get_stats(self) -> dict[str, int]:
        """Get binding statistics."""
        healthy_count = 0
        unhealthy_count = 0

        for binding in self._bindings.values():
            if binding.health < self.config.health_degradation_threshold:
                healthy_count += 1
            else:
                unhealthy_count += 1

        return {
            "total_bindings": len(self._bindings),
            "healthy_bindings": healthy_count,
            "unhealthy_bindings": unhealthy_count,
            "peer_count": len(self._peer_bindings),
        }
