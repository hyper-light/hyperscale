"""
Hierarchical Timing Wheel for efficient suspicion timer management.

This implements a two-level timing wheel (coarse + fine) for O(1) timer
operations regardless of the number of active suspicions. Used by the
global layer of hierarchical failure detection.

Design based on Kafka's purgatory timing wheel, adapted for SWIM/Lifeguard.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Generic, TypeVar

from .suspicion_state import SuspicionState


# Type for node address
NodeAddress = tuple[str, int]

# Type variable for wheel entries
T = TypeVar("T")


@dataclass(slots=True)
class WheelEntry(Generic[T]):
    """
    An entry in the timing wheel.

    Tracks the suspicion state and its absolute expiration time.
    """
    node: NodeAddress
    state: T
    expiration_time: float
    # For detecting stale entries after movement between buckets
    epoch: int = 0


@dataclass
class TimingWheelConfig:
    """Configuration for the timing wheel."""
    # Coarse wheel: handles longer timeouts (seconds)
    coarse_tick_ms: int = 1000  # 1 second per tick
    coarse_wheel_size: int = 64  # 64 seconds max before wrap

    # Fine wheel: handles imminent expirations (milliseconds)
    fine_tick_ms: int = 100  # 100ms per tick
    fine_wheel_size: int = 16  # 1.6 seconds max in fine wheel

    # When remaining time is below this, move to fine wheel
    fine_wheel_threshold_ms: int = 2000  # 2 seconds


class TimingWheelBucket:
    """
    A single bucket in the timing wheel.

    Contains entries expiring within the bucket's time range.
    Thread-safe for asyncio via lock.
    """
    __slots__ = ("entries", "_lock")

    def __init__(self) -> None:
        self.entries: dict[NodeAddress, WheelEntry[SuspicionState]] = {}
        self._lock = asyncio.Lock()

    async def add(self, entry: WheelEntry[SuspicionState]) -> None:
        """Add an entry to this bucket."""
        async with self._lock:
            self.entries[entry.node] = entry

    async def remove(self, node: NodeAddress) -> WheelEntry[SuspicionState] | None:
        """Remove and return an entry from this bucket."""
        async with self._lock:
            return self.entries.pop(node, None)

    async def pop_all(self) -> list[WheelEntry[SuspicionState]]:
        """Remove and return all entries from this bucket."""
        async with self._lock:
            entries = list(self.entries.values())
            self.entries.clear()
            return entries

    async def get(self, node: NodeAddress) -> WheelEntry[SuspicionState] | None:
        """Get an entry without removing it."""
        async with self._lock:
            return self.entries.get(node)

    def __len__(self) -> int:
        return len(self.entries)


class TimingWheel:
    """
    Hierarchical timing wheel for suspicion timer management.

    Provides O(1) operations for:
    - Adding a suspicion (insert into bucket)
    - Extending a suspicion (move to later bucket)
    - Cancelling a suspicion (remove from bucket)
    - Expiring suspicions (pop bucket on tick)

    Architecture:
    - Coarse wheel: For suspicions > 2s from expiration
    - Fine wheel: For suspicions within 2s of expiration
    - Single timer advances wheels, expiring entries as needed

    When LHM changes, all entries can be shifted efficiently by
    adjusting expiration times and moving between buckets.
    """

    def __init__(
        self,
        config: TimingWheelConfig | None = None,
        on_expired: Callable[[NodeAddress, SuspicionState], None] | None = None,
    ) -> None:
        if config is None:
            config = TimingWheelConfig()

        self._config = config
        self._on_expired = on_expired

        # Create wheel buckets
        self._coarse_wheel: list[TimingWheelBucket] = [
            TimingWheelBucket() for _ in range(config.coarse_wheel_size)
        ]
        self._fine_wheel: list[TimingWheelBucket] = [
            TimingWheelBucket() for _ in range(config.fine_wheel_size)
        ]

        # Current positions in each wheel
        self._coarse_position: int = 0
        self._fine_position: int = 0

        # Base time for calculating bucket positions
        self._base_time: float = time.monotonic()

        # Track which wheel each node is in for efficient removal
        self._node_locations: dict[NodeAddress, tuple[str, int, int]] = {}
        # Format: (wheel_type, bucket_idx, epoch)

        # Epoch counter for detecting stale operations
        self._global_epoch: int = 0

        # Advancement task
        self._advance_task: asyncio.Task | None = None
        self._running: bool = False

        # Lock for structural modifications
        self._lock = asyncio.Lock()

        # Stats
        self._entries_added: int = 0
        self._entries_removed: int = 0
        self._entries_expired: int = 0
        self._entries_moved: int = 0
        self._cascade_count: int = 0  # Times fine wheel filled from coarse

    def _calculate_bucket_index(
        self,
        expiration_time: float,
        wheel_type: str,
    ) -> int:
        """Calculate which bucket an expiration time maps to."""
        now = time.monotonic()
        remaining_ms = (expiration_time - now) * 1000

        if wheel_type == "fine":
            ticks = int(remaining_ms / self._config.fine_tick_ms)
            return (self._fine_position + ticks) % self._config.fine_wheel_size
        else:
            ticks = int(remaining_ms / self._config.coarse_tick_ms)
            return (self._coarse_position + ticks) % self._config.coarse_wheel_size

    def _should_use_fine_wheel(self, expiration_time: float) -> bool:
        """Determine if an entry should go in the fine wheel."""
        now = time.monotonic()
        remaining_ms = (expiration_time - now) * 1000
        return remaining_ms <= self._config.fine_wheel_threshold_ms

    async def add(
        self,
        node: NodeAddress,
        state: SuspicionState,
        expiration_time: float,
    ) -> bool:
        """
        Add a suspicion to the timing wheel.

        Returns True if added successfully, False if already exists.
        """
        async with self._lock:
            # Check if already tracked
            if node in self._node_locations:
                return False

            self._global_epoch += 1
            epoch = self._global_epoch

            entry = WheelEntry(
                node=node,
                state=state,
                expiration_time=expiration_time,
                epoch=epoch,
            )

            # Determine which wheel
            if self._should_use_fine_wheel(expiration_time):
                bucket_idx = self._calculate_bucket_index(expiration_time, "fine")
                await self._fine_wheel[bucket_idx].add(entry)
                self._node_locations[node] = ("fine", bucket_idx, epoch)
            else:
                bucket_idx = self._calculate_bucket_index(expiration_time, "coarse")
                await self._coarse_wheel[bucket_idx].add(entry)
                self._node_locations[node] = ("coarse", bucket_idx, epoch)

            self._entries_added += 1
            return True

    async def remove(self, node: NodeAddress) -> SuspicionState | None:
        """
        Remove a suspicion from the timing wheel.

        Returns the state if found and removed, None otherwise.
        """
        async with self._lock:
            location = self._node_locations.pop(node, None)
            if location is None:
                return None

            wheel_type, bucket_idx, _ = location

            if wheel_type == "fine":
                entry = await self._fine_wheel[bucket_idx].remove(node)
            else:
                entry = await self._coarse_wheel[bucket_idx].remove(node)

            if entry:
                self._entries_removed += 1
                return entry.state
            return None

    async def update_expiration(
        self,
        node: NodeAddress,
        new_expiration_time: float,
    ) -> bool:
        """
        Update the expiration time for a suspicion.

        Moves the entry to the appropriate bucket if needed.
        Returns True if updated, False if node not found.
        """
        async with self._lock:
            location = self._node_locations.get(node)
            if location is None:
                return False

            old_wheel_type, old_bucket_idx, old_epoch = location

            # Get the entry
            if old_wheel_type == "fine":
                entry = await self._fine_wheel[old_bucket_idx].remove(node)
            else:
                entry = await self._coarse_wheel[old_bucket_idx].remove(node)

            if entry is None:
                # Entry was already removed (race condition)
                self._node_locations.pop(node, None)
                return False

            # Update expiration
            entry.expiration_time = new_expiration_time
            self._global_epoch += 1
            entry.epoch = self._global_epoch

            # Determine new location
            if self._should_use_fine_wheel(new_expiration_time):
                new_bucket_idx = self._calculate_bucket_index(new_expiration_time, "fine")
                await self._fine_wheel[new_bucket_idx].add(entry)
                self._node_locations[node] = ("fine", new_bucket_idx, entry.epoch)
            else:
                new_bucket_idx = self._calculate_bucket_index(new_expiration_time, "coarse")
                await self._coarse_wheel[new_bucket_idx].add(entry)
                self._node_locations[node] = ("coarse", new_bucket_idx, entry.epoch)

            self._entries_moved += 1
            return True

    async def contains(self, node: NodeAddress) -> bool:
        """Check if a node is being tracked in the wheel."""
        async with self._lock:
            return node in self._node_locations

    async def get_state(self, node: NodeAddress) -> SuspicionState | None:
        """Get the suspicion state for a node without removing it."""
        async with self._lock:
            location = self._node_locations.get(node)
            if location is None:
                return None

            wheel_type, bucket_idx, _ = location

            if wheel_type == "fine":
                entry = await self._fine_wheel[bucket_idx].get(node)
            else:
                entry = await self._coarse_wheel[bucket_idx].get(node)

            return entry.state if entry else None

    async def _advance_fine_wheel(self) -> list[WheelEntry[SuspicionState]]:
        """
        Advance the fine wheel by one tick.

        Returns expired entries.
        """
        expired = await self._fine_wheel[self._fine_position].pop_all()
        self._fine_position = (self._fine_position + 1) % self._config.fine_wheel_size
        return expired

    async def _advance_coarse_wheel(self) -> list[WheelEntry[SuspicionState]]:
        """
        Advance the coarse wheel by one tick.

        Returns entries that need to be cascaded to the fine wheel.
        """
        entries = await self._coarse_wheel[self._coarse_position].pop_all()
        self._coarse_position = (self._coarse_position + 1) % self._config.coarse_wheel_size
        return entries

    async def _cascade_to_fine_wheel(
        self,
        entries: list[WheelEntry[SuspicionState]],
    ) -> list[WheelEntry[SuspicionState]]:
        """
        Move entries from coarse wheel to fine wheel.

        Returns any entries that have already expired.
        """
        now = time.monotonic()
        expired: list[WheelEntry[SuspicionState]] = []

        for entry in entries:
            if entry.expiration_time <= now:
                expired.append(entry)
                self._node_locations.pop(entry.node, None)
            else:
                bucket_idx = self._calculate_bucket_index(entry.expiration_time, "fine")
                await self._fine_wheel[bucket_idx].add(entry)
                self._node_locations[entry.node] = ("fine", bucket_idx, entry.epoch)

        if entries:
            self._cascade_count += 1

        return expired

    async def _process_expired(
        self,
        entries: list[WheelEntry[SuspicionState]],
    ) -> None:
        """Process expired entries by calling the callback."""
        for entry in entries:
            # Remove from tracking
            self._node_locations.pop(entry.node, None)
            self._entries_expired += 1

            # Call callback outside of lock
            if self._on_expired:
                try:
                    self._on_expired(entry.node, entry.state)
                except Exception:
                    # Don't let callback errors stop the wheel
                    pass

    async def _tick(self) -> None:
        """
        Perform one tick of the timing wheel.

        This advances the fine wheel and potentially the coarse wheel,
        expiring any entries that have reached their timeout.
        """
        async with self._lock:
            now = time.monotonic()

            # Always advance fine wheel
            fine_expired = await self._advance_fine_wheel()

            # Check if we need to advance coarse wheel
            # (every fine_wheel_size ticks of fine wheel = 1 coarse tick)
            coarse_expired: list[WheelEntry[SuspicionState]] = []
            if self._fine_position == 0:
                cascade_entries = await self._advance_coarse_wheel()
                coarse_expired = await self._cascade_to_fine_wheel(cascade_entries)

            all_expired = fine_expired + coarse_expired

        # Process expired entries outside of lock
        await self._process_expired(all_expired)

    async def _advance_loop(self) -> None:
        """Main loop that advances the wheel at the configured tick rate."""
        tick_interval = self._config.fine_tick_ms / 1000.0

        while self._running:
            try:
                await asyncio.sleep(tick_interval)
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception:
                # Log but continue - wheel must keep advancing
                pass

    def start(self) -> None:
        """Start the timing wheel advancement loop."""
        if self._running:
            return

        self._running = True
        self._base_time = time.monotonic()
        self._advance_task = asyncio.create_task(self._advance_loop())

    async def stop(self) -> None:
        """Stop the timing wheel and cancel all pending expirations."""
        self._running = False

        if self._advance_task and not self._advance_task.done():
            self._advance_task.cancel()
            try:
                await self._advance_task
            except asyncio.CancelledError:
                pass

        self._advance_task = None

    async def clear(self) -> None:
        """Clear all entries from the wheel."""
        async with self._lock:
            for bucket in self._fine_wheel:
                await bucket.pop_all()
            for bucket in self._coarse_wheel:
                await bucket.pop_all()
            self._node_locations.clear()

    def get_stats(self) -> dict[str, int]:
        """Get timing wheel statistics."""
        return {
            "entries_added": self._entries_added,
            "entries_removed": self._entries_removed,
            "entries_expired": self._entries_expired,
            "entries_moved": self._entries_moved,
            "cascade_count": self._cascade_count,
            "current_entries": len(self._node_locations),
            "fine_position": self._fine_position,
            "coarse_position": self._coarse_position,
        }

    async def apply_lhm_adjustment(self, multiplier: float) -> int:
        """
        Apply LHM adjustment to all entries.

        When Local Health Multiplier increases, we need to extend all
        suspicion timeouts proportionally. This is done by adjusting
        expiration times and moving entries to appropriate buckets.

        Returns the number of entries adjusted.
        """
        if multiplier == 1.0:
            return 0

        async with self._lock:
            adjusted_count = 0
            now = time.monotonic()

            # Collect all entries to adjust
            all_entries: list[tuple[NodeAddress, WheelEntry[SuspicionState]]] = []

            for bucket in self._fine_wheel:
                entries = await bucket.pop_all()
                for entry in entries:
                    all_entries.append((entry.node, entry))

            for bucket in self._coarse_wheel:
                entries = await bucket.pop_all()
                for entry in entries:
                    all_entries.append((entry.node, entry))

            self._node_locations.clear()

            # Re-insert with adjusted expiration times
            for node, entry in all_entries:
                # Calculate new expiration time
                remaining = entry.expiration_time - now
                new_remaining = remaining * multiplier
                new_expiration = now + new_remaining

                entry.expiration_time = new_expiration
                self._global_epoch += 1
                entry.epoch = self._global_epoch

                # Re-insert into appropriate wheel
                if self._should_use_fine_wheel(new_expiration):
                    bucket_idx = self._calculate_bucket_index(new_expiration, "fine")
                    await self._fine_wheel[bucket_idx].add(entry)
                    self._node_locations[node] = ("fine", bucket_idx, entry.epoch)
                else:
                    bucket_idx = self._calculate_bucket_index(new_expiration, "coarse")
                    await self._coarse_wheel[bucket_idx].add(entry)
                    self._node_locations[node] = ("coarse", bucket_idx, entry.epoch)

                adjusted_count += 1

            return adjusted_count

    # =========================================================================
    # Synchronous Accessors (for quick checks without async overhead)
    # =========================================================================

    def contains_sync(self, node: NodeAddress) -> bool:
        """Synchronously check if node has an active suspicion."""
        return node in self._node_locations

    def get_state_sync(self, node: NodeAddress) -> SuspicionState | None:
        """Synchronously get suspicion state for a node."""
        location = self._node_locations.get(node)
        if not location:
            return None

        wheel_type, bucket_idx, epoch = location

        if wheel_type == "fine":
            bucket = self._fine_wheel[bucket_idx]
        else:
            bucket = self._coarse_wheel[bucket_idx]

        # Direct access to bucket entries
        for entry in bucket._entries.values():
            if entry.node == node and entry.epoch == epoch:
                return entry.state

        return None
