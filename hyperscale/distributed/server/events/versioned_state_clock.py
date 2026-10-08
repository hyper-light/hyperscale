"""``VersionedStateClock`` -- pickled under the namespace
``hyperscale.distributed.server.events.lamport_clock`` (see that module)."""

import asyncio
from hyperscale.distributed.runtime import Clock, RealClock

from .lamport_clock_impl import LamportClock

# Wall-clock seam for the physical timestamps this module stamps
# alongside logical versions (entity last-update times used for TTL
# cleanup). Logical Lamport time stays on the integer counters; only the
# physical ``monotonic`` reads route through here so age-based eviction
# is deterministic under SIM. ``swap_defaults`` rebinds this singleton.
_DEFAULT_CLOCK: Clock = RealClock()


class VersionedStateClock:
    """
    Extended Lamport clock with per-entity version tracking.

    Tracks versions for multiple entities (e.g., workers, jobs) and
    provides staleness detection to reject outdated updates.

    Usage:
        clock = VersionedStateClock()

        # Update entity state
        version = await clock.update_entity('worker-1', worker_heartbeat)

        # Check if incoming state is stale
        if clock.is_entity_stale('worker-1', incoming_version):
            reject_update()
        else:
            # Accept and update
            await clock.update_entity('worker-1', new_state)
    """

    __slots__ = ("_clock", "_entity_versions", "_lock")

    def __init__(self):
        self._clock = LamportClock()
        # entity_id -> (version, last_update_time)
        self._entity_versions: dict[str, tuple[int, float]] = {}
        self._lock = asyncio.Lock()

    @property
    def time(self) -> int:
        """Current clock time."""
        return self._clock.time

    async def increment(self) -> int:
        """Increment the underlying clock."""
        return await self._clock.increment()

    async def update(self, received_time: int) -> int:
        """Update the underlying clock."""
        return await self._clock.update(received_time)

    async def ack(self, received_time: int) -> int:
        """Acknowledge on the underlying clock."""
        return await self._clock.ack(received_time)

    async def update_entity(
        self,
        entity_id: str,
        version: int | None = None,
    ) -> int:
        """
        Update an entity's version.

        Args:
            entity_id: The entity to update.
            version: Optional explicit version. If None, uses current clock time.

        Returns:
            The new version for this entity.
        """
        async with self._lock:
            if version is None:
                version = await self._clock.increment()
            else:
                # Ensure clock is at least at this version
                await self._clock.ack(version)

            self._entity_versions[entity_id] = (version, _DEFAULT_CLOCK.monotonic())
            return version

    async def get_entity_version(self, entity_id: str) -> int | None:
        """
        Get the current version for an entity.

        Args:
            entity_id: The entity to look up.

        Returns:
            The entity's version, or None if not tracked.
        """
        async with self._lock:
            entry = self._entity_versions.get(entity_id)
            return entry[0] if entry else None

    async def is_entity_stale(
        self,
        entity_id: str,
        incoming_version: int,
    ) -> bool:
        """
        Check if an incoming version is stale for an entity.

        Args:
            entity_id: The entity to check.
            incoming_version: The version of the incoming update.

        Returns:
            True if incoming_version <= current version (stale).
            False if incoming_version > current version (fresh) or entity unknown.
        """
        async with self._lock:
            entry = self._entity_versions.get(entity_id)
            if entry is None:
                return False
            return incoming_version <= entry[0]

    async def should_accept_update(
        self,
        entity_id: str,
        incoming_version: int,
    ) -> bool:
        """
        Check if an update should be accepted.

        Inverse of is_entity_stale for clearer semantics.

        Args:
            entity_id: The entity to check.
            incoming_version: The version of the incoming update.

        Returns:
            True if update should be accepted (newer version).
        """
        return not await self.is_entity_stale(entity_id, incoming_version)

    async def get_all_versions(self) -> dict[str, int]:
        """
        Get all tracked entity versions.

        Returns:
            Dict mapping entity_id to version.
        """
        async with self._lock:
            return {k: v[0] for k, v in self._entity_versions.items()}

    async def remove_entity(self, entity_id: str) -> bool:
        """
        Remove an entity from tracking.

        Args:
            entity_id: The entity to remove.

        Returns:
            True if entity was removed, False if not found.
        """
        async with self._lock:
            return self._entity_versions.pop(entity_id, None) is not None

    async def cleanup_old_entities(self, max_age_seconds: float = 300.0) -> list[str]:
        """
        Remove entities that haven't been updated recently.

        Args:
            max_age_seconds: Maximum age before removal.

        Returns:
            List of removed entity IDs.
        """
        now = _DEFAULT_CLOCK.monotonic()
        removed = []

        async with self._lock:
            for entity_id, (_, last_update) in list(self._entity_versions.items()):
                if now - last_update > max_age_seconds:
                    del self._entity_versions[entity_id]
                    removed.append(entity_id)

        return removed
