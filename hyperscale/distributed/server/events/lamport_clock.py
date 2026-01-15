"""
Lamport Clock Implementation.

A Lamport clock provides logical timestamps for ordering events in a
distributed system. It guarantees that if event A happens-before event B,
then the timestamp of A is less than the timestamp of B.

Operations:
- increment/tick: Local event, advance clock
- update: Receive event, sync with sender's clock and advance
- ack: Acknowledge, sync without advancing (for responses)
"""

import asyncio
from dataclasses import dataclass, field
from typing import TypeVar, Generic


class LamportClock:
    """
    Basic Lamport logical clock for event ordering.

    Thread-safe via asyncio.Lock. All operations are atomic.

    Usage:
        clock = LamportClock()

        # Local event - increment clock
        time = await clock.increment()

        # Send message with current time
        message = {'data': ..., 'clock': clock.time}

        # Receive message - update clock
        time = await clock.update(message['clock'])

        # Acknowledge - sync without increment
        await clock.ack(received_time)
    """

    __slots__ = ("time", "_lock")

    def __init__(self, initial_time: int = 0):
        self.time: int = initial_time
        self._lock = asyncio.Lock()

    async def increment(self) -> int:
        """
        Increment clock for a local event.

        Returns:
            The new clock time.
        """
        async with self._lock:
            self.time += 1
            return self.time

    # Alias for increment - used in some contexts
    tick = increment

    async def update(self, received_time: int) -> int:
        """
        Update clock on receiving a message.

        Sets clock to max(received_time, current_time) + 1.

        Args:
            received_time: The sender's clock time.

        Returns:
            The new clock time.
        """
        async with self._lock:
            self.time = max(received_time, self.time) + 1
            return self.time

    async def ack(self, received_time: int) -> int:
        """
        Acknowledge a message without incrementing.

        Sets clock to max(received_time, current_time).
        Used for responses where we don't want to increment.

        Args:
            received_time: The sender's clock time.

        Returns:
            The new clock time.
        """
        async with self._lock:
            self.time = max(received_time, self.time)
            return self.time

    def compare(self, other_time: int) -> int:
        """
        Compare this clock's time with another.

        Args:
            other_time: Another clock's time.

        Returns:
            -1 if this < other, 0 if equal, 1 if this > other.
        """
        if self.time < other_time:
            return -1
        elif self.time > other_time:
            return 1
        return 0

    def is_stale(self, other_time: int) -> bool:
        """
        Check if another time is stale (older than our current time).

        Args:
            other_time: The time to check.

        Returns:
            True if other_time < self.time (stale), False otherwise.
        """
        return other_time < self.time


EntityT = TypeVar("EntityT")


@dataclass(slots=True)
class VersionedState(Generic[EntityT]):
    """
    State with a version number for staleness detection.

    Attributes:
        entity_id: The ID of the entity this state belongs to.
        version: The Lamport clock time when this state was created.
        data: The actual state data.
    """

    entity_id: str
    version: int
    data: EntityT


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
        import time as time_module

        async with self._lock:
            if version is None:
                version = await self._clock.increment()
            else:
                # Ensure clock is at least at this version
                await self._clock.ack(version)

            self._entity_versions[entity_id] = (version, time_module.monotonic())
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
        import time as time_module

        now = time_module.monotonic()
        removed = []

        async with self._lock:
            for entity_id, (_, last_update) in list(self._entity_versions.items()):
                if now - last_update > max_age_seconds:
                    del self._entity_versions[entity_id]
                    removed.append(entity_id)

        return removed
