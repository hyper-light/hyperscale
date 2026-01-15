"""
Worker state gossip buffer for cross-manager worker visibility (AD-48).

Disseminates worker state updates (registration, death, eviction) across
managers using the same O(log n) piggyback strategy as membership gossip.
"""

import heapq
import math
import time
from dataclasses import dataclass, field
from typing import Any

from hyperscale.distributed.models.worker_state import (
    WorkerStateUpdate,
    WorkerStatePiggybackUpdate,
)

MAX_WORKER_STATE_PIGGYBACK_SIZE = 600

WORKER_STATE_SEPARATOR = b"#|w"

ENTRY_SEPARATOR = b"|"


@dataclass(slots=True)
class WorkerStateGossipBuffer:
    """
    Buffer for worker state updates to be piggybacked on SWIM messages.

    Same dissemination strategy as membership gossip:
    - Updates broadcast lambda * log(n) times
    - Higher incarnation replaces lower
    - Stale updates cleaned up periodically
    """

    updates: dict[str, WorkerStatePiggybackUpdate] = field(default_factory=dict)
    broadcast_multiplier: int = 3
    max_updates: int = 500
    stale_age_seconds: float = 60.0
    max_piggyback_size: int = MAX_WORKER_STATE_PIGGYBACK_SIZE

    _evicted_count: int = 0
    _stale_removed_count: int = 0
    _size_limited_count: int = 0
    _oversized_updates_count: int = 0
    _overflow_count: int = 0

    _on_overflow: Any = None

    def set_overflow_callback(self, callback: Any) -> None:
        self._on_overflow = callback

    def add_update(
        self,
        update: WorkerStateUpdate,
        number_of_managers: int = 1,
    ) -> bool:
        """
        Add or update a worker state update in the buffer.

        If an update for the same worker exists with lower incarnation,
        it is replaced. Updates with equal or higher incarnation are
        only replaced if the new state has higher priority (dead > alive).
        """
        worker_id = update.worker_id

        if worker_id not in self.updates and len(self.updates) >= self.max_updates:
            self.cleanup_stale()
            self.cleanup_broadcast_complete()

            if len(self.updates) >= self.max_updates:
                self._evict_oldest()

        max_broadcasts = max(
            1, int(self.broadcast_multiplier * math.log(number_of_managers + 1))
        )

        existing = self.updates.get(worker_id)

        if existing is None:
            self.updates[worker_id] = WorkerStatePiggybackUpdate(
                update=update,
                timestamp=time.monotonic(),
                max_broadcasts=max_broadcasts,
            )
            return True

        if update.incarnation > existing.update.incarnation:
            self.updates[worker_id] = WorkerStatePiggybackUpdate(
                update=update,
                timestamp=time.monotonic(),
                max_broadcasts=max_broadcasts,
            )
            return True

        if update.incarnation == existing.update.incarnation:
            if update.is_dead_state() and existing.update.is_alive_state():
                self.updates[worker_id] = WorkerStatePiggybackUpdate(
                    update=update,
                    timestamp=time.monotonic(),
                    max_broadcasts=max_broadcasts,
                )
                return True

        return False

    def get_updates_to_piggyback(
        self, max_count: int = 5
    ) -> list[WorkerStatePiggybackUpdate]:
        max_count = max(1, min(max_count, 100))
        candidates = (u for u in self.updates.values() if u.should_broadcast())
        return heapq.nsmallest(max_count, candidates, key=lambda u: u.broadcast_count)

    def mark_broadcasts(self, updates: list[WorkerStatePiggybackUpdate]) -> None:
        for update in updates:
            worker_id = update.update.worker_id
            if worker_id in self.updates:
                self.updates[worker_id].mark_broadcast()
                if not self.updates[worker_id].should_broadcast():
                    del self.updates[worker_id]

    MAX_ENCODE_COUNT = 100

    def encode_piggyback(
        self,
        max_count: int = 5,
        max_size: int | None = None,
    ) -> bytes:
        max_count = max(1, min(max_count, self.MAX_ENCODE_COUNT))

        if max_size is None:
            max_size = self.max_piggyback_size

        updates = self.get_updates_to_piggyback(max_count)
        if not updates:
            return b""

        result_parts: list[bytes] = []
        total_size = 3
        included_updates: list[WorkerStatePiggybackUpdate] = []

        for piggyback_update in updates:
            encoded = piggyback_update.update.to_bytes()
            update_size = len(encoded) + 1

            if update_size > max_size:
                self._oversized_updates_count += 1
                continue

            if total_size + update_size > max_size:
                self._size_limited_count += 1
                break

            result_parts.append(encoded)
            total_size += update_size
            included_updates.append(piggyback_update)

        if not result_parts:
            return b""

        self.mark_broadcasts(included_updates)
        return WORKER_STATE_SEPARATOR + ENTRY_SEPARATOR.join(result_parts)

    def encode_piggyback_with_base(
        self,
        base_message: bytes,
        max_count: int = 5,
    ) -> bytes:
        from .gossip_buffer import MAX_UDP_PAYLOAD

        remaining = MAX_UDP_PAYLOAD - len(base_message)
        if remaining <= 0:
            return b""

        return self.encode_piggyback(max_count, max_size=remaining)

    MAX_DECODE_UPDATES = 100

    @classmethod
    def decode_piggyback(
        cls, data: bytes, max_updates: int = 100
    ) -> list[WorkerStateUpdate]:
        if not data or not data.startswith(WORKER_STATE_SEPARATOR):
            return []

        bounded_max = min(max_updates, cls.MAX_DECODE_UPDATES)

        updates = []
        parts = data[3:].split(ENTRY_SEPARATOR)
        for part in parts:
            if len(updates) >= bounded_max:
                break
            if part:
                update = WorkerStateUpdate.from_bytes(part)
                if update:
                    updates.append(update)
        return updates

    def clear(self) -> None:
        self.updates.clear()

    def remove_worker(self, worker_id: str) -> bool:
        if worker_id in self.updates:
            del self.updates[worker_id]
            return True
        return False

    def _evict_oldest(self, count: int = 10) -> int:
        if not self.updates:
            return 0

        oldest = heapq.nsmallest(
            count,
            self.updates.items(),
            key=lambda x: x[1].timestamp,
        )

        evicted = 0
        for worker_id, _ in oldest:
            del self.updates[worker_id]
            self._evicted_count += 1
            evicted += 1

        if evicted > 0:
            self._overflow_count += 1
            if self._on_overflow:
                try:
                    self._on_overflow(evicted, self.max_updates)
                except Exception:
                    pass

        return evicted

    def cleanup_stale(self) -> int:
        now = time.monotonic()
        cutoff = now - self.stale_age_seconds

        to_remove = [
            worker_id
            for worker_id, update in self.updates.items()
            if update.timestamp < cutoff
        ]

        for worker_id in to_remove:
            del self.updates[worker_id]
            self._stale_removed_count += 1

        return len(to_remove)

    def cleanup_broadcast_complete(self) -> int:
        to_remove = [
            worker_id
            for worker_id, update in self.updates.items()
            if not update.should_broadcast()
        ]

        for worker_id in to_remove:
            del self.updates[worker_id]

        return len(to_remove)

    def cleanup(self) -> dict[str, int]:
        stale = self.cleanup_stale()
        complete = self.cleanup_broadcast_complete()

        return {
            "stale_removed": stale,
            "complete_removed": complete,
            "pending_updates": len(self.updates),
        }

    def get_stats(self) -> dict[str, Any]:
        return {
            "pending_updates": len(self.updates),
            "total_evicted": self._evicted_count,
            "total_stale_removed": self._stale_removed_count,
            "size_limited_count": self._size_limited_count,
            "oversized_updates": self._oversized_updates_count,
            "overflow_events": self._overflow_count,
            "max_piggyback_size": self.max_piggyback_size,
            "max_updates": self.max_updates,
        }
